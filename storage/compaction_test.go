package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func compactionSeed(t testing.TB) (*faultIO, *Journal, []Transaction) {
	t.Helper()
	f, j, before, first := rotationSeed(t)
	txs, batch := rotationBatch(t, before)
	if _, err := j.PublishPreparedBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	return f, j, append([]Transaction{first}, txs...)
}

func TestCompactionPreservesExactLogAddressesAndPinnedLayout(t *testing.T) {
	f, j, txs := compactionSeed(t)
	before := faultPublished(t, j)
	old := rotationManifest(t, f)
	pin, err := j.PinReader()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, int(before.PublishedEnd)-SegmentHeaderSize)
	if _, err = pin.ReadAt(data, SegmentHeaderSize); err != nil {
		t.Fatal(err)
	}
	report, err := j.CompactAuditFull(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Before != before || report.After.Position != before.Position || report.After.Fence() != before.Fence() || report.After.PostRootSet != before.PostRootSet || report.After.Generation != before.Generation+1 || report.After.PublishedEnd != before.PublishedEnd || report.PinnedSegments != len(rotationDescriptors(old)) || len(report.ReclaimedSegments) != 0 {
		t.Fatalf("bad replacement proof: %+v", report)
	}
	if m := rotationManifest(t, f); m.SegmentID == old.SegmentID || m.LayoutSource == nil || m.LayoutSource.Position != before.Position || len(m.RetiredSegments) != len(rotationDescriptors(old)) {
		t.Fatal("source/retirement certificate absent")
	}
	got := make([]byte, len(data))
	if _, err = pin.ReadAt(got, SegmentHeaderSize); err != nil || !bytes.Equal(data, got) {
		t.Fatalf("old pin changed: %v", err)
	}
	rotationAssertLogicalTransactions(t, j, txs)
	if _, err = j.CompactAuditFull(context.Background()); !errors.Is(err, ErrRetirement) {
		t.Fatalf("replaced live pinned generation: %v", err)
	}
	// Logical publication may continue while an old layout is pinned; the new
	// manifest must carry the remaining physical retirement obligation.
	next := faultNextTransaction(faultPublished(t, j), 10)
	if _, err = j.Publish(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if _, err = pin.ReadAt(make([]byte, 1), int64(before.PublishedEnd)); !errors.Is(err, io.EOF) {
		t.Fatal("pin disclosed future bytes")
	}
	if err = pin.Close(); err != nil {
		t.Fatal(err)
	}
	removed, pending, err := j.ReclaimRetired(context.Background())
	if err != nil || pending != 0 || len(removed) != len(rotationDescriptors(old)) {
		t.Fatalf("retirement %+v %d %v", removed, pending, err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	opened, recovered, err := OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if recovered.Publication.Position.LSN != next.LSN {
		t.Fatal("lost append following compaction")
	}
	rotationAssertLogicalTransactions(t, opened, append(txs, next))
}

func TestCompactionCrashAtEveryMutationKeepsCompleteCanonicalHistory(t *testing.T) {
	f, j, _ := compactionSeed(t)
	if _, err := j.CompactAuditFull(context.Background()); err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), f.calls...)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s-after-%t", call.Step, call.Op, after), func(t *testing.T) {
				f, j, txs := compactionSeed(t)
				before := faultPublished(t, j)
				f.failAt(call.Step, after)
				_, err := j.CompactAuditFull(context.Background())
				if err == nil {
					t.Fatal("injected failure hidden")
				}
				crashed := f.crash()
				opened, report, err := OpenJournal(crashed, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
				if err != nil {
					t.Fatalf("recovery failed: %+v %v", report, err)
				}
				defer opened.Close()
				p := faultPublished(t, opened)
				if p.Fence() != before.Fence() || p.PostRootSet != before.PostRootSet || p.PublishedEnd != before.PublishedEnd {
					t.Fatal("compaction changed logical history")
				}
				rotationAssertLogicalTransactions(t, opened, txs)
			})
		}
	}
}

func TestCompactionCancelBeforeStagingAndSourceCorruptionAreNonDestructive(t *testing.T) {
	f, j, _ := compactionSeed(t)
	defer j.Close()
	before := faultTreeSnapshot(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := j.CompactAuditFull(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("canceled compaction mutated store")
	}
	// Corrupt an authenticated published frame, retaining the old manifest.
	first := j.segments[0]
	name := segmentName(first.descriptor.SegmentID)
	data := []byte(faultReadAll(t, f, name))
	data[first.descriptor.HeaderSize+HeaderSize] ^= 1
	replayReplaceFile(t, f, name, data)
	f.resetFaults()
	before = faultTreeSnapshot(t, f)
	if _, err := j.CompactAuditFull(context.Background()); !errors.Is(err, ErrStoreFenced) {
		t.Fatalf("corrupt source copied: %v", err)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("corrupt source was mutated or removed")
	}
}

func TestCompactionRetirementFailuresCanBeRetriedWithoutReopening(t *testing.T) {
	f, j, _ := compactionSeed(t)
	if _, err := j.CompactAuditFull(context.Background()); err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), f.calls...)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	known := false
	replaced := false
	for _, call := range trace {
		if call.Op == "ReplaceAtomic" && call.To == "CURRENT" {
			replaced = true
			continue
		}
		if replaced && !known && call.Op == "SyncDir" && call.Name == "." {
			known = true
			continue
		}
		if !known {
			continue
		}
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s-after-%t", call.Step, call.Op, after), func(t *testing.T) {
				f, j, txs := compactionSeed(t)
				defer j.Close()
				before := faultPublished(t, j)
				f.failAt(call.Step, after)
				report, err := j.CompactAuditFull(context.Background())
				if !errors.Is(err, ErrRetirement) || report.After.Fence() != before.Fence() {
					t.Fatalf("known publication lost to cleanup error: %+v %v", report, err)
				}
				if j.Status().State != JournalReady {
					t.Fatal("retryable cleanup fenced known durable layout")
				}
				f.resetFaults()
				if _, _, err = j.ReclaimRetired(context.Background()); err != nil {
					t.Fatal(err)
				}
				intents, err := f.List("compactions")
				if err != nil || len(intents) != 0 {
					t.Fatal("intent not retired", intents, err)
				}
				if _, err = j.CompactAuditFull(context.Background()); err != nil {
					t.Fatal("cleanup retry prevented next compaction", err)
				}
				rotationAssertLogicalTransactions(t, j, txs)
			})
		}
	}
}

type compactionGateIO struct {
	DurableIO
	armed   bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type compactionReadCloseIO struct {
	DurableIO
	armed    bool
	retained *compactionReadCloseFile
}
type compactionReadCloseFile struct {
	ReadFile
	failures int
}

func (f *compactionReadCloseFile) Close() error {
	if f.failures > 0 {
		f.failures--
		return errFaultIO
	}
	return f.ReadFile.Close()
}
func (d *compactionReadCloseIO) OpenRead(name string) (ReadFile, error) {
	f, err := d.DurableIO.OpenRead(name)
	if err != nil || !d.armed || !strings.HasPrefix(name, "manifests/") {
		return f, err
	}
	d.armed = false
	d.retained = &compactionReadCloseFile{ReadFile: f, failures: 2}
	return d.retained, nil
}

func TestCompactionPreStagingReadCloseFailureRetainsOwnerUntilCleanup(t *testing.T) {
	f := newFaultIO()
	d := &compactionReadCloseIO{DurableIO: f}
	j, err := CreateJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx := faultNextTransaction(faultPublished(t, j), 1)
	if _, err = j.Publish(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	d.armed = true
	if _, err = j.CompactAuditFull(context.Background()); !errors.Is(err, ErrStoreFenced) || !errors.Is(err, errFaultIO) {
		t.Fatalf("read cleanup ownership lost: %v", err)
	}
	if err = j.Close(); !errors.Is(err, errFaultIO) {
		t.Fatalf("pending manifest close not retried: %v", err)
	}
	if lock, err := f.AcquireOwnerLock(); err == nil {
		lock.Close()
		t.Fatal("owner released behind live metadata handle")
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err := f.AcquireOwnerLock()
	if err != nil {
		t.Fatal("owner not released after cleanup", err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func (d *compactionGateIO) CreateExclusive(name string) (DurableFile, error) {
	if d.armed && strings.HasPrefix(name, "segments/") {
		d.once.Do(func() { close(d.entered); <-d.release })
	}
	return d.DurableIO.CreateExclusive(name)
}

func TestCompactionMaintenanceKeepsReadsAndCloseIsSticky(t *testing.T) {
	f := newFaultIO()
	gate := &compactionGateIO{DurableIO: f, entered: make(chan struct{}), release: make(chan struct{})}
	j, err := CreateJournal(gate, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx := faultNextTransaction(faultPublished(t, j), 1)
	before, err := j.Publish(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	gate.armed = true
	done := make(chan error, 1)
	go func() { _, err := j.CompactAuditFull(context.Background()); done <- err }()
	<-gate.entered
	if _, err = j.PinReader(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("new layout pin during maintenance: %v", err)
	}
	if _, err = j.Publish(context.Background(), faultNextTransaction(before, 2)); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("concurrent write entered layout rewrite: %v", err)
	}
	rotationAssertLogicalTransactions(t, j, []Transaction{tx})
	closing := make(chan error, 1)
	go func() { closing <- j.Close() }()
	// Wait until Close has installed the sticky request, without racing by
	// directly reading the publisher's state.
	for {
		j.mu.Lock()
		requested := j.closeRequested
		j.mu.Unlock()
		if requested {
			break
		}
		runtime.Gosched()
	}
	if _, err = j.Published(); !errors.Is(err, ErrClosing) {
		t.Fatalf("close request did not revoke new grants: %v", err)
	}
	close(gate.release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = <-closing; err != nil {
		t.Fatal(err)
	}
	if j.Status().State != JournalClosed {
		t.Fatal("maintenance reopened a closing journal")
	}
}
