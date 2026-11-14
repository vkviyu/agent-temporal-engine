package storage

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

type nativeDestructorIO struct {
	DurableIO
	closePause  atomic.Pointer[nativeReadPause]
	removePause atomic.Pointer[nativeReadPause]
	ownerPause  atomic.Pointer[nativeReadPause]
	ownerCloses atomic.Int64
}

type nativeDestructorFile struct {
	DurableFile
	owner *nativeDestructorIO
}

func (f *nativeDestructorFile) Close() error {
	if gate := f.owner.closePause.Swap(nil); gate != nil {
		close(gate.entered)
		<-gate.release
	}
	return f.DurableFile.Close()
}

func (d *nativeDestructorIO) wrap(name string, file DurableFile) DurableFile {
	if file != nil && strings.HasPrefix(name, "segments/SEGMENT.") {
		return &nativeDestructorFile{DurableFile: file, owner: d}
	}
	return file
}

func (d *nativeDestructorIO) CreateExclusive(name string) (DurableFile, error) {
	file, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(name, file), err
}

func (d *nativeDestructorIO) OpenAppend(name string) (DurableFile, error) {
	file, err := d.DurableIO.OpenAppend(name)
	return d.wrap(name, file), err
}

func (d *nativeDestructorIO) Remove(name string) error {
	if strings.HasPrefix(name, "segments/SEGMENT.") {
		if gate := d.removePause.Swap(nil); gate != nil {
			close(gate.entered)
			<-gate.release
		}
	}
	return d.DurableIO.Remove(name)
}

type nativeDestructorOwner struct {
	OwnerLock
	disk *nativeDestructorIO
}

func (o *nativeDestructorOwner) Close() error {
	if gate := o.disk.ownerPause.Swap(nil); gate != nil {
		close(gate.entered)
		<-gate.release
	}
	err := o.OwnerLock.Close()
	if err == nil {
		o.disk.ownerCloses.Add(1)
	}
	return err
}

func (d *nativeDestructorIO) AcquireOwnerLock() (OwnerLock, error) {
	owner, err := d.DurableIO.AcquireOwnerLock()
	if owner != nil {
		return &nativeDestructorOwner{OwnerLock: owner, disk: d}, err
	}
	return nil, err
}

func nativeDestructorJournal(t *testing.T) (*Journal, *nativeReadLeaseIO, *nativeDestructorIO, Publication) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native directory backend requires Darwin or Linux")
	}
	directory, err := OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	destructor := &nativeDestructorIO{DurableIO: directory}
	disk := &nativeReadLeaseIO{DurableIO: destructor}
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
	return journal, disk, destructor, ack
}

func assertNativeDestructorMetadata(t *testing.T, journal *Journal, expected Publication, state JournalState, cause error) {
	t.Helper()
	statuses := make(chan JournalStatus, 1)
	go func() { statuses <- journal.Status() }()
	if status := awaitNativeRead(t, statuses); status.State != state || status.LastPublication != expected {
		t.Fatalf("native destructor blocked or changed metadata: %+v", status)
	}
	type publicationResult struct {
		publication Publication
		err         error
	}
	publications := make(chan publicationResult, 1)
	go func() {
		publication, err := journal.Published()
		publications <- publicationResult{publication: publication, err: err}
	}()
	got := awaitNativeRead(t, publications)
	if cause != nil {
		if got.publication != (Publication{}) || !errors.Is(got.err, cause) {
			t.Fatalf("closing destructor exposed an active publication: %+v %v", got.publication, got.err)
		}
	} else if got.err != nil || got.publication != expected {
		t.Fatalf("retirement changed the active publication: %+v %v", got.publication, got.err)
	}
}

func TestNativeSegmentCloseKeepsMetadataResponsiveAndOwnerHeld(t *testing.T) {
	journal, disk, destructor, before := nativeDestructorJournal(t)
	gate := &nativeReadPause{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	destructor.closePause.Store(gate)
	finished := make(chan error, 1)
	go func() { finished <- journal.Close() }()
	awaitNativeRead(t, gate.entered)
	assertNativeDestructorMetadata(t, journal, before, JournalClosing, ErrClosing)
	if disk.closed.Load() != 0 || destructor.ownerCloses.Load() != 0 {
		t.Fatal("native ownership retired before the paused segment close")
	}
	owner, err := disk.AcquireOwnerLock()
	if owner != nil {
		_ = owner.Close()
	}
	if !errors.Is(err, ErrOwnerBusy) {
		t.Fatalf("owner lock released before its native files: %v", err)
	}
	if _, err := journal.PinReader(); !errors.Is(err, ErrClosing) {
		t.Fatalf("closing journal granted another layout: %v", err)
	}
	gate.unblock()
	if err := awaitNativeRead(t, finished); err != nil {
		t.Fatal(err)
	}
	if destructor.ownerCloses.Load() != 1 || disk.closed.Load() == 0 {
		t.Fatal("native resources did not retire in file-before-owner order")
	}
	if err := journal.Close(); err != nil || destructor.ownerCloses.Load() != 1 {
		t.Fatalf("completed Close repeated owner retirement: %v", err)
	}
}

func TestNativeOwnerCloseKeepsMetadataResponsive(t *testing.T) {
	journal, disk, destructor, before := nativeDestructorJournal(t)
	gate := &nativeReadPause{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	destructor.ownerPause.Store(gate)
	finished := make(chan error, 1)
	go func() { finished <- journal.Close() }()
	awaitNativeRead(t, gate.entered)
	assertNativeDestructorMetadata(t, journal, before, JournalClosing, ErrClosing)
	if disk.closed.Load() == 0 || destructor.ownerCloses.Load() != 0 {
		t.Fatal("owner retirement did not follow confirmed native file retirement")
	}
	owner, err := disk.AcquireOwnerLock()
	if owner != nil {
		_ = owner.Close()
	}
	if !errors.Is(err, ErrOwnerBusy) {
		t.Fatalf("paused owner close released its native lock early: %v", err)
	}
	gate.unblock()
	if err := awaitNativeRead(t, finished); err != nil {
		t.Fatal(err)
	}
	if destructor.ownerCloses.Load() != 1 {
		t.Fatal("native owner did not retire after its Close resumed")
	}
}

func TestNativeRetirementIOKeepsCurrentPinnedLayoutReadable(t *testing.T) {
	for _, operation := range []string{"close", "remove"} {
		t.Run(operation, func(t *testing.T) {
			journal, disk, destructor, before := nativeDestructorJournal(t)
			expected := nativePublishedPrefix(t, journal, before)
			oldPin, err := journal.PinReader()
			if err != nil {
				t.Fatal(err)
			}
			defer oldPin.Close()
			report, err := journal.CompactAuditFull(context.Background())
			if err != nil || report.PinnedSegments == 0 || len(report.ReclaimedSegments) != 0 || disk.closed.Load() != 0 {
				t.Fatalf("compaction did not retain the old pin: %+v %v", report, err)
			}
			currentPin, err := journal.PinReader()
			if err != nil {
				t.Fatal(err)
			}
			defer currentPin.Close()
			if err := oldPin.Close(); err != nil {
				t.Fatal(err)
			}
			gate := &nativeReadPause{entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(gate.unblock)
			if operation == "close" {
				destructor.closePause.Store(gate)
			} else {
				destructor.removePause.Store(gate)
			}
			type reclaimResult struct {
				removed []string
				pinned  int
				err     error
			}
			finished := make(chan reclaimResult, 1)
			go func() {
				removed, pinned, err := journal.ReclaimRetired(context.Background())
				finished <- reclaimResult{removed: removed, pinned: pinned, err: err}
			}()
			awaitNativeRead(t, gate.entered)
			assertNativeDestructorMetadata(t, journal, report.After, JournalReady, nil)
			read := make(chan nativeReadResult, 1)
			go func() {
				data := make([]byte, len(expected))
				n, err := currentPin.ReadAt(data, 0)
				read <- nativeReadResult{data: data, n: n, err: err}
			}()
			got := awaitNativeRead(t, read)
			if got.err != nil || got.n != len(expected) || !bytes.Equal(got.data[SegmentHeaderSize:], expected[SegmentHeaderSize:]) {
				t.Fatalf("retired %s blocked or changed a current pinned read: n=%d err=%v", operation, got.n, got.err)
			}
			if _, err := oldPin.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrClosed) {
				t.Fatalf("retirement allowed a new old-layout reader: %v", err)
			}
			gate.unblock()
			result := awaitNativeRead(t, finished)
			if result.err != nil || result.pinned != 0 || len(result.removed) != report.PinnedSegments || disk.closed.Load() != int64(report.PinnedSegments) {
				t.Fatalf("retirement did not finish its selected files: %+v closed=%d", result, disk.closed.Load())
			}
			if data := nativePublishedPrefix(t, journal, report.After); !bytes.Equal(data[SegmentHeaderSize:], expected[SegmentHeaderSize:]) {
				t.Fatal("retirement changed the current native layout")
			}
		})
	}
}
