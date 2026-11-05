package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// faultSeedSyncedTail interrupts a real Publish after its append and file sync,
// before it can create a manifest. The reboot therefore has one acknowledged
// publication and one complete, synced but unpublished transaction in its tail.
func faultSeedSyncedTail(t testing.TB) (*faultIO, Publication, Transaction, []byte) {
	t.Helper()
	f, j, acknowledged, first := faultSeedAcknowledged(t)
	second := faultNextTransaction(acknowledged, 2)
	tail, _, err := EncodeTransaction(second, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	injected := 0
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "CreateExclusive" && strings.HasPrefix(b.Name, "manifests/.STAGE.") && !b.After {
			injected++
			return errFaultIO
		}
		return nil
	}
	if _, err := j.Publish(context.Background(), second); !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) || injected != 1 {
		t.Fatalf("could not interrupt publication after syncing its tail: injected=%d error=%v", injected, err)
	}
	reboot := f.crash()
	_, segment := faultSegmentBytes(t, reboot)
	if uint64(len(segment)) != acknowledged.PublishedEnd+uint64(len(tail)) || !bytes.Equal(segment[acknowledged.PublishedEnd:], tail) {
		t.Fatal("fixture does not contain the complete durable unpublished tail")
	}
	reboot.resetFaults()
	return reboot, acknowledged, first, tail
}

func faultHasCompleteQuarantine(t testing.TB, f *faultIO, tail []byte) bool {
	t.Helper()
	names, err := f.List("quarantine")
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if strings.HasPrefix(name, "TAIL.") && bytes.Equal([]byte(faultReadAll(t, f, "quarantine/"+name)), tail) {
			return true
		}
	}
	return false
}

func TestJournalQuarantineCrashAtEveryMutation(t *testing.T) {
	baseline, acknowledged, _, _ := faultSeedSyncedTail(t)
	recovered, report, err := OpenJournal(baseline, JournalOptions{TailPolicy: TailQuarantine})
	if err != nil || recovered == nil || report.Publication != acknowledged || !report.PublicationSynced {
		t.Fatalf("baseline quarantine failed: %+v %v", report, err)
	}
	trace := append([]faultCall(nil), baseline.calls...)
	_ = recovered.Close()
	if len(trace) == 0 {
		t.Fatal("quarantine did not exercise fault model")
	}
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s/after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f, acknowledged, first, originalTail := faultSeedSyncedTail(t)
				_, originalSegment := faultSegmentBytes(t, f)
				f.resetFaults()
				f.failAt(call.Step, after)
				opened, _, err := OpenJournal(f, JournalOptions{TailPolicy: TailQuarantine})
				if opened != nil || !errors.Is(err, errFaultIO) {
					t.Fatalf("repair fault returned a usable journal or lost its error: journal=%v error=%v", opened, err)
				}
				if len(f.calls) < call.Step || f.calls[call.Step-1].Op != call.Op {
					t.Fatalf("repair trace diverged at %+v: got %+v", call, f.calls)
				}
				reboot := f.crash()
				_, crashedSegment := faultSegmentBytes(t, reboot)
				if uint64(len(crashedSegment)) < acknowledged.PublishedEnd || !bytes.Equal(crashedSegment[:acknowledged.PublishedEnd], originalSegment[:acknowledged.PublishedEnd]) {
					t.Fatal("interrupted quarantine damaged the acknowledged prefix")
				}
				hasTail := uint64(len(crashedSegment)) > acknowledged.PublishedEnd
				if hasTail {
					if !bytes.Equal(crashedSegment[acknowledged.PublishedEnd:], originalTail) {
						t.Fatal("interrupted quarantine left a partial or changed durable active tail")
					}
				} else if !faultHasCompleteQuarantine(t, reboot, originalTail) {
					t.Fatal("tail was durably truncated before an exact quarantine copy became durable")
				}
				faultAssertPublishedPrefix(t, reboot, acknowledged, []Transaction{first})
				beforeDefaultOpen := faultTreeSnapshot(t, reboot)
				opened, report, err := OpenJournal(reboot, JournalOptions{})
				if hasTail {
					if opened != nil || !errors.Is(err, ErrRecoveryRequired) || !report.Verified || report.PublicationSynced || report.Publication != acknowledged || report.TailBytes != uint64(len(originalTail)) {
						t.Fatalf("default recovery did not preserve the old ACK and tail: %+v %v", report, err)
					}
					if afterDefaultOpen := faultTreeSnapshot(t, reboot); !reflect.DeepEqual(beforeDefaultOpen, afterDefaultOpen) {
						t.Fatal("default recovery changed the interrupted repair evidence")
					}
					opened, report, err = OpenJournal(reboot, JournalOptions{TailPolicy: TailQuarantine})
				}
				if err != nil || opened == nil || !report.Verified || !report.PublicationSynced || report.Publication != acknowledged {
					t.Fatalf("repair retry did not recover the original ACK: %+v %v", report, err)
				}
				if got := faultPublished(t, opened); got != acknowledged || got.Position.LSN != 1 {
					t.Fatalf("repair promoted an unpublished transaction: %+v", got)
				}
				faultAssertPublishedPrefix(t, reboot, acknowledged, []Transaction{first})
				if !faultHasCompleteQuarantine(t, reboot, originalTail) {
					t.Fatal("completed recovery has no complete quarantine evidence")
				}
				if err := opened.Close(); err != nil {
					t.Fatal(err)
				}
				// Retrying may have added a second diagnostic copy. Only one
				// complete durable copy is required; partial copies may remain.
				reboot = reboot.crash()
				if !faultHasCompleteQuarantine(t, reboot, originalTail) {
					t.Fatal("completed repair lost its quarantine evidence after reboot")
				}
				final, finalReport, err := OpenJournal(reboot, JournalOptions{})
				if err != nil || final == nil || finalReport.Publication != acknowledged || !finalReport.PublicationSynced || finalReport.TailBytes != 0 {
					t.Fatalf("completed repair was not durable: %+v %v", finalReport, err)
				}
				faultAssertPublishedPrefix(t, reboot, faultPublished(t, final), []Transaction{first})
				_ = final.Close()
			})
		}
	}
}

func TestJournalProcessRestartSyncsObservedCurrent(t *testing.T) {
	f, publisher, acknowledged, first := faultSeedAcknowledged(t)
	second := faultNextTransaction(acknowledged, 2)
	want := faultSuccessorPublication(t, acknowledged, second)
	injected := 0
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "ReplaceAtomic" && b.To == "CURRENT" && b.After {
			injected++
			return errFaultIO
		}
		return nil
	}
	if p, err := publisher.Publish(context.Background(), second); p != (Publication{}) || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) || injected != 1 {
		t.Fatalf("could not interrupt after the visible CURRENT replacement: publication=%+v injected=%d error=%v", p, injected, err)
	}
	if last := f.calls[len(f.calls)-1]; last.Op != "ReplaceAtomic" || last.To != "CURRENT" {
		t.Fatalf("publication continued after replacement instead of stopping before directory sync: %+v", f.calls)
	}
	observed, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
	if err != nil || observed.Generation != want.Generation {
		t.Fatalf("process restart fixture did not expose the new CURRENT: %+v %v", observed, err)
	}
	// Keep the same volatile namespace: this is a process restart rather than a
	// power loss. Close must not accidentally supply the missing directory sync.
	f.resetFaults()
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.calls {
		if call.Op == "SyncDir" {
			t.Fatal("Close supplied the missing publication directory sync")
		}
	}
	f.resetFaults()
	restarted, report, err := OpenJournal(f, JournalOptions{})
	if err != nil || restarted == nil || !report.Verified || !report.PublicationSynced || report.Publication != want {
		t.Fatalf("process restart did not confirm the observed new CURRENT: %+v %v", report, err)
	}
	syncedRoot := false
	for _, call := range f.calls {
		if call.Op == "SyncDir" && call.Name == "." {
			syncedRoot = true
		}
	}
	if !syncedRoot {
		t.Fatal("OpenJournal returned ready without syncing the observed CURRENT namespace")
	}
	faultAssertPublishedPrefix(t, f, faultPublished(t, restarted), []Transaction{first, second})
	// The Open witness, rather than Close or the interrupted Publish, must make
	// this exact new publication survive the next power-loss-model crash.
	reboot := f.crash()
	final, finalReport, err := OpenJournal(reboot, JournalOptions{})
	if err != nil || final == nil || finalReport.Publication != want || !finalReport.PublicationSynced || finalReport.TailBytes != 0 {
		t.Fatalf("observed publication was lost after Open established its witness: %+v %v", finalReport, err)
	}
	if got := faultPublished(t, final); got.Position.LSN != 2 || got != want {
		t.Fatalf("reboot did not recover the confirmed second publication: %+v", got)
	}
	faultAssertPublishedPrefix(t, reboot, want, []Transaction{first, second})
	_ = final.Close()
}
