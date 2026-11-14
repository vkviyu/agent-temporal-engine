package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

type nativePublicationPauseIO struct {
	DurableIO
	segmentSync atomic.Pointer[nativeReadPause]
	currentSync atomic.Pointer[nativeReadPause]
	opened      atomic.Int64
	closed      atomic.Int64
}

type nativePublicationPauseFile struct {
	DurableFile
	owner  *nativePublicationPauseIO
	closed atomic.Bool
}

func (f *nativePublicationPauseFile) Sync() error {
	if err := f.DurableFile.Sync(); err != nil {
		return err
	}
	if gate := f.owner.segmentSync.Swap(nil); gate != nil {
		close(gate.entered)
		<-gate.release
	}
	return nil
}

func (f *nativePublicationPauseFile) Close() error {
	err := f.DurableFile.Close()
	if err == nil && f.closed.CompareAndSwap(false, true) {
		f.owner.closed.Add(1)
	}
	return err
}

func (d *nativePublicationPauseIO) wrap(name string, file DurableFile) DurableFile {
	if file != nil && strings.HasPrefix(name, "segments/SEGMENT.") {
		d.opened.Add(1)
		return &nativePublicationPauseFile{DurableFile: file, owner: d}
	}
	return file
}

func (d *nativePublicationPauseIO) CreateExclusive(name string) (DurableFile, error) {
	file, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(name, file), err
}

func (d *nativePublicationPauseIO) OpenAppend(name string) (DurableFile, error) {
	file, err := d.DurableIO.OpenAppend(name)
	return d.wrap(name, file), err
}

func (d *nativePublicationPauseIO) SyncDir(name string) error {
	if err := d.DurableIO.SyncDir(name); err != nil {
		return err
	}
	if name == "." {
		if gate := d.currentSync.Swap(nil); gate != nil {
			close(gate.entered)
			<-gate.release
		}
	}
	return nil
}

func nativePublicationJournal(t *testing.T) (*Journal, *nativePublicationPauseIO, Publication) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native directory backend requires Darwin or Linux")
	}
	directory, err := OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	disk := &nativePublicationPauseIO{DurableIO: directory}
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
	ack, err := journal.Publish(context.Background(), faultNextTransaction(faultPublished(t, journal), 1))
	if err != nil {
		t.Fatal(err)
	}
	return journal, disk, ack
}

type nativePublicationResult struct {
	ack Publication
	err error
}

func pauseNativePublication(t *testing.T, journal *Journal, disk *nativePublicationPauseIO, before Publication, afterCurrent bool) (*nativeReadPause, <-chan nativePublicationResult) {
	t.Helper()
	gate := &nativeReadPause{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	if afterCurrent {
		disk.currentSync.Store(gate)
	} else {
		disk.segmentSync.Store(gate)
	}
	tx := faultNextTransaction(before, 2)
	tx.Records = append(tx.Records, Record{Kind: KindObject, Payload: bytes.Repeat([]byte("r"), 4096)})
	finished := make(chan nativePublicationResult, 1)
	go func() {
		ack, err := journal.Publish(context.Background(), tx)
		finished <- nativePublicationResult{ack: ack, err: err}
	}()
	awaitNativeRead(t, gate.entered)
	return gate, finished
}

func TestNativePinnedReadStartsWhilePublicationSyncIsPaused(t *testing.T) {
	for _, afterCurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "segment-sync", true: "current-synced"}[afterCurrent], func(t *testing.T) {
			journal, disk, before := nativePublicationJournal(t)
			expected := nativePublishedPrefix(t, journal, before)
			pin, err := journal.PinReader()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			gate, publishing := pauseNativePublication(t, journal, disk, before, afterCurrent)
			for _, reader := range []io.ReaderAt{pin, journal} {
				finished := make(chan nativeReadResult, 1)
				go func() {
					data := make([]byte, len(expected)+1)
					n, err := reader.ReadAt(data, 0)
					finished <- nativeReadResult{data: data, n: n, err: err}
				}()
				got := awaitNativeRead(t, finished)
				if got.n != len(expected) || !errors.Is(got.err, io.EOF) || !bytes.Equal(got.data[:got.n], expected) {
					t.Fatalf("P1 blocked or changed the fixed read cut: %d %v", got.n, got.err)
				}
			}
			if got, err := journal.Published(); err != nil || got != before {
				t.Fatalf("P1 changed publication metadata before installation: %+v %v", got, err)
			}
			newPin, err := journal.PinReader()
			if err != nil || newPin.Publication() != before {
				t.Fatalf("P1 blocked a new old-cut pin: %v", err)
			}
			if err := newPin.Close(); err != nil {
				t.Fatal(err)
			}
			gate.unblock()
			result := awaitNativeRead(t, publishing)
			if result.err != nil || result.ack.Position.LSN != 2 || pin.Publication() != before {
				t.Fatalf("native publication changed its old pin: %+v %v", result.ack, result.err)
			}
		})
	}
}

func TestNativeFenceDuringPublicationRetainsUnknownResources(t *testing.T) {
	for _, afterCurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[afterCurrent], func(t *testing.T) {
			journal, disk, before := nativePublicationJournal(t)
			gate, publishing := pauseNativePublication(t, journal, disk, before, afterCurrent)
			if err := journal.Fence(ErrCorruption); !errors.Is(err, ErrStoreFenced) {
				t.Fatal(err)
			}
			gate.unblock()
			result := awaitNativeRead(t, publishing)
			if result.ack != (Publication{}) || !errors.Is(result.err, ErrCommitUnknown) || !errors.Is(result.err, ErrCorruption) {
				t.Fatalf("Fence after P1 began returned an acknowledgement: %+v %v", result.ack, result.err)
			}
			if got := journal.Status(); got.State != JournalFenced || got.LastPublication != before {
				t.Fatalf("Fence installed an unacknowledged candidate: %+v", got)
			}
			if _, err := journal.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrStoreFenced) {
				t.Fatalf("fenced publication allowed a new read: %v", err)
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			if disk.opened.Load() != disk.closed.Load() {
				t.Fatalf("unknown candidate lost native handles: opened=%d closed=%d", disk.opened.Load(), disk.closed.Load())
			}
			reopened, report, err := OpenJournal(disk, JournalOptions{SegmentTargetBytes: MinSegmentTargetBytes, TailPolicy: TailQuarantine})
			if err != nil {
				t.Fatal(report, err)
			}
			defer reopened.Close()
			wantLSN := uint64(1)
			if afterCurrent {
				wantLSN = 2
			}
			if !report.Verified || !report.PublicationSynced || report.Publication.Position.LSN != wantLSN {
				t.Fatalf("reopening did not adjudicate durable CURRENT: %+v", report)
			}
		})
	}
}

func TestNativeVerifyReleasesMetadataAndHonorsConcurrentFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "fence"}[fenced], func(t *testing.T) {
			journal, disk, before := nativeReadLeaseJournal(t)
			expected := nativePublishedPrefix(t, journal, before)
			pin, err := journal.PinReader()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			gate := &nativeReadPause{entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(gate.unblock)
			disk.pause.Store(gate)
			verified := make(chan nativePublicationResult, 1)
			go func() {
				ack, _, err := journal.VerifyPublished(context.Background(), nil)
				verified <- nativePublicationResult{ack: ack, err: err}
			}()
			awaitNativeRead(t, gate.entered)
			read := make(chan nativeReadResult, 1)
			go func() {
				data := make([]byte, len(expected))
				n, err := pin.ReadAt(data, 0)
				read <- nativeReadResult{data: data, n: n, err: err}
			}()
			got := awaitNativeRead(t, read)
			if got.err != nil || got.n != len(expected) || !bytes.Equal(got.data, expected) {
				t.Fatalf("verification blocked its pinned reader: %d %v", got.n, got.err)
			}
			if ack, err := journal.Published(); err != nil || ack != before {
				t.Fatal(ack, err)
			}
			if fenced {
				if err := journal.Fence(ErrCorruption); !errors.Is(err, ErrStoreFenced) {
					t.Fatal(err)
				}
			}
			gate.unblock()
			result := awaitNativeRead(t, verified)
			if fenced && (result.ack != (Publication{}) || !errors.Is(result.err, ErrStoreFenced)) || !fenced && (result.ack != before || result.err != nil) {
				t.Fatalf("verification concealed its concurrent lifecycle change: %+v %v", result.ack, result.err)
			}
		})
	}
}
