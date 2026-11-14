package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type nativeReadPause struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *nativeReadPause) unblock() { g.once.Do(func() { close(g.release) }) }

type nativeReadLeaseIO struct {
	DurableIO
	pause  atomic.Pointer[nativeReadPause]
	closed atomic.Int64
}

type nativeReadLeaseFile struct {
	DurableFile
	owner  *nativeReadLeaseIO
	closed atomic.Bool
}

func (f *nativeReadLeaseFile) ReadAt(data []byte, offset int64) (int, error) {
	if gate := f.owner.pause.Swap(nil); gate != nil {
		close(gate.entered)
		<-gate.release
	}
	return f.DurableFile.ReadAt(data, offset)
}

func (f *nativeReadLeaseFile) Close() error {
	err := f.DurableFile.Close()
	if err == nil && f.closed.CompareAndSwap(false, true) {
		f.owner.closed.Add(1)
	}
	return err
}

func (d *nativeReadLeaseIO) wrap(name string, file DurableFile) DurableFile {
	if file != nil && strings.HasPrefix(name, "segments/SEGMENT.") {
		return &nativeReadLeaseFile{DurableFile: file, owner: d}
	}
	return file
}

func (d *nativeReadLeaseIO) CreateExclusive(name string) (DurableFile, error) {
	file, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(name, file), err
}

func (d *nativeReadLeaseIO) OpenAppend(name string) (DurableFile, error) {
	file, err := d.DurableIO.OpenAppend(name)
	return d.wrap(name, file), err
}

func nativeReadLeaseJournal(t *testing.T) (*Journal, *nativeReadLeaseIO, Publication) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native directory backend requires Darwin or Linux")
	}
	directory, err := OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	disk := &nativeReadLeaseIO{DurableIO: directory}
	journal, err := CreateJournal(disk, JournalOptions{SegmentTargetBytes: MinSegmentTargetBytes})
	if err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
		if err := directory.Close(); err != nil {
			t.Error(err)
		}
	})
	ack := faultPublished(t, journal)
	for sequence := 1; sequence <= 2; sequence++ {
		ack, err = journal.Publish(context.Background(), faultNextTransaction(ack, sequence))
		if err != nil {
			t.Fatal(err)
		}
	}
	return journal, disk, ack
}

func awaitNativeRead[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("native read interleaving did not reach its next boundary")
		var zero T
		return zero
	}
}

type nativeReadResult struct {
	data []byte
	n    int
	err  error
}

func pauseNativeRead(t *testing.T, disk *nativeReadLeaseIO, reader io.ReaderAt, size int) (*nativeReadPause, <-chan nativeReadResult) {
	t.Helper()
	gate := &nativeReadPause{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	disk.pause.Store(gate)
	finished := make(chan nativeReadResult, 1)
	go func() {
		data := bytes.Repeat([]byte{0xa5}, size)
		n, err := reader.ReadAt(data, 0)
		finished <- nativeReadResult{data: data, n: n, err: err}
	}()
	awaitNativeRead(t, gate.entered)
	return gate, finished
}

func nativePublishedPrefix(t *testing.T, journal *Journal, ack Publication) []byte {
	t.Helper()
	data := make([]byte, int(ack.PublishedEnd))
	if n, err := journal.ReadAt(data, 0); err != nil || n != len(data) {
		t.Fatalf("read acknowledged native prefix: %d %v", n, err)
	}
	return data
}

func TestNativeActiveReadDoesNotBlockPublisherOrMetadata(t *testing.T) {
	for _, explicitPin := range []bool{false, true} {
		t.Run(map[bool]string{false: "journal", true: "pin"}[explicitPin], func(t *testing.T) {
			journal, disk, before := nativeReadLeaseJournal(t)
			expected := nativePublishedPrefix(t, journal, before)
			var reader io.ReaderAt = journal
			if explicitPin {
				pin, err := journal.PinReader()
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close()
				reader = pin
			}
			gate, read := pauseNativeRead(t, disk, reader, len(expected)+17)
			type publicationResult struct {
				ack Publication
				err error
			}
			metadata := make(chan publicationResult, 1)
			go func() {
				ack, err := journal.Published()
				metadata <- publicationResult{ack: ack, err: err}
			}()
			if got := awaitNativeRead(t, metadata); got.err != nil || got.ack != before {
				t.Fatalf("active read blocked or changed its published metadata: %+v %v", got.ack, got.err)
			}
			published := make(chan publicationResult, 1)
			go func() {
				ack, err := journal.Publish(context.Background(), faultNextTransaction(before, 3))
				published <- publicationResult{ack: ack, err: err}
			}()
			result := awaitNativeRead(t, published)
			if result.err != nil || result.ack.Position.LSN != 3 {
				t.Fatalf("paused read blocked native publication: %+v %v", result.ack, result.err)
			}
			gate.unblock()
			got := awaitNativeRead(t, read)
			if got.n != len(expected) || !errors.Is(got.err, io.EOF) || !bytes.Equal(got.data[:got.n], expected) {
				t.Fatalf("read escaped its captured end or changed prefix: n=%d err=%v", got.n, got.err)
			}
		})
	}
}

func TestNativeActiveLeaseSurvivesPinCloseAndCompaction(t *testing.T) {
	for _, explicitPin := range []bool{false, true} {
		t.Run(map[bool]string{false: "temporary", true: "closed-pin"}[explicitPin], func(t *testing.T) {
			journal, disk, before := nativeReadLeaseJournal(t)
			expected := nativePublishedPrefix(t, journal, before)
			oldSegments := len(journal.segments)
			var reader io.ReaderAt = journal
			var pin *PinnedReader
			if explicitPin {
				var err error
				pin, err = journal.PinReader()
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close()
				reader = pin
			}
			gate, read := pauseNativeRead(t, disk, reader, len(expected))
			if pin != nil {
				if err := pin.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := pin.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed pin accepted another read: %v", err)
				}
			}
			report, err := journal.CompactAuditFull(context.Background())
			if err != nil || report.After.Generation != before.Generation+1 || report.PinnedSegments != oldSegments || len(report.ReclaimedSegments) != 0 {
				t.Fatalf("compaction failed to retain the active layout: %+v %v", report, err)
			}
			removed, pending, err := journal.ReclaimRetired(context.Background())
			if err != nil || len(removed) != 0 || pending != oldSegments || disk.closed.Load() != 0 {
				t.Fatalf("active native files retired early: removed=%v pinned=%d closed=%d err=%v", removed, pending, disk.closed.Load(), err)
			}
			gate.unblock()
			got := awaitNativeRead(t, read)
			if got.err != nil || got.n != len(expected) || !bytes.Equal(got.data, expected) {
				t.Fatalf("active old-generation read changed: %d %v", got.n, got.err)
			}
			removed, pending, err = journal.ReclaimRetired(context.Background())
			if err != nil || len(removed) != oldSegments || pending != 0 || disk.closed.Load() != int64(oldSegments) {
				t.Fatalf("completed read retained old native files: removed=%v pinned=%d closed=%d err=%v", removed, pending, disk.closed.Load(), err)
			}
			if data := nativePublishedPrefix(t, journal, report.After); !bytes.Equal(data[SegmentHeaderSize:], expected[SegmentHeaderSize:]) {
				t.Fatal("replacement layout changed canonical bytes")
			}
		})
	}
}

func TestNativeCloseWaitsForActiveReadAndPreservesFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "fence-then-close"}[fenced], func(t *testing.T) {
			journal, disk, before := nativeReadLeaseJournal(t)
			expected := nativePublishedPrefix(t, journal, before)
			pin, err := journal.PinReader()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			gate, read := pauseNativeRead(t, disk, pin, len(expected))
			if fenced {
				if err := journal.Fence(ErrCorruption); !errors.Is(err, ErrStoreFenced) {
					t.Fatal(err)
				}
				if _, err := pin.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrStoreFenced) {
					t.Fatalf("fenced pin accepted a new read: %v", err)
				}
			}
			if err := pin.Close(); err != nil {
				t.Fatal(err)
			}
			closed := make(chan error, 1)
			go func() { closed <- journal.Close() }()
			deadline := time.Now().Add(3 * time.Second)
			for journal.Status().State != JournalClosing && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if journal.Status().State != JournalClosing || disk.closed.Load() != 0 {
				t.Fatal("Close did not retain files for its active read")
			}
			if _, err := journal.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrClosing) {
				t.Fatalf("closing journal accepted another read: %v", err)
			}
			select {
			case err := <-closed:
				t.Fatalf("Close completed before native ReadAt returned: %v", err)
			default:
			}
			gate.unblock()
			got := awaitNativeRead(t, read)
			if fenced {
				if got.n != 0 || !errors.Is(got.err, ErrStoreFenced) || !bytes.Equal(got.data, make([]byte, len(got.data))) {
					t.Fatalf("Close concealed a Fence from its active read: n=%d err=%v", got.n, got.err)
				}
			} else if got.err != nil || got.n != len(expected) || !bytes.Equal(got.data, expected) {
				t.Fatalf("Close invalidated its earlier active read: %d %v", got.n, got.err)
			}
			if err := awaitNativeRead(t, closed); err != nil {
				t.Fatal(err)
			}
			reopened, report, err := OpenJournal(disk, JournalOptions{SegmentTargetBytes: MinSegmentTargetBytes})
			if err != nil || report.Publication != before {
				t.Fatalf("read lifecycle changed recovered publication: %+v %v", report, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativePinSnapshotSurvivesConcurrentRotation(t *testing.T) {
	journal, _, before := nativeReadLeaseJournal(t)
	expected := nativePublishedPrefix(t, journal, before)
	pin, err := journal.PinReader()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	stop := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		data := make([]byte, len(expected)+1)
		for {
			if n, err := pin.ReadAt(data, 0); n != len(expected) || !errors.Is(err, io.EOF) || !bytes.Equal(data[:n], expected) {
				finished <- errors.New("immutable pinned layout changed during rotation")
				return
			}
			select {
			case <-stop:
				finished <- nil
				return
			default:
			}
		}
	}()
	ack := before
	for sequence := 3; sequence <= 20; sequence++ {
		ack, err = journal.Publish(context.Background(), faultNextTransaction(ack, sequence))
		if err != nil {
			close(stop)
			awaitNativeRead(t, finished)
			t.Fatal(err)
		}
	}
	close(stop)
	if err := awaitNativeRead(t, finished); err != nil {
		t.Fatal(err)
	}
	if pin.Publication() != before || faultPublished(t, journal) != ack {
		t.Fatal("rotation changed the pin's publication witness")
	}
}
