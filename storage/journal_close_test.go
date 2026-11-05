package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestJournalCloseRetriesBeforeReleasingOwnership(t *testing.T) {
	for _, stage := range []string{"segment", "owner"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", stage, after), func(t *testing.T) {
				f, j, acknowledged, _ := faultSeedAcknowledged(t)
				activeSegment := segmentName(j.segmentID)
				faultOp, faultName := "Close", activeSegment
				if stage == "owner" {
					faultOp, faultName = "OwnerLock.Close", "."
				}
				injected := 0
				f.onBoundary = func(b faultBoundary) error {
					if injected == 0 && b.Op == faultOp && b.Name == faultName && b.After == after {
						injected++
						return errFaultIO
					}
					return nil
				}
				if err := j.Close(); !errors.Is(err, errFaultIO) || injected != 1 {
					t.Fatalf("close failure was lost: injected=%d error=%v", injected, err)
				}
				if status := j.Status(); status.State != JournalClosing || status.LastPublication != acknowledged {
					t.Fatalf("failed close lost its state or diagnostic ACK: %+v", status)
				}
				if stage == "segment" {
					for _, call := range f.calls {
						if call.Op == "OwnerLock.Close" {
							t.Fatalf("owner released before segment close was confirmed: %+v", f.calls)
						}
					}
				}
				// Before errors retain the owner; a segment failure also retains
				// it after the file has retired. An owner after-error has already
				// released ownership despite the unknown return to its caller.
				wantLocked := stage == "segment" || !after
				if f.locked != wantLocked {
					t.Fatalf("lock held=%t, want %t after failed %s close", f.locked, wantLocked, stage)
				}
				if wantLocked {
					if _, err := f.AcquireOwnerLock(); !errors.Is(err, errFaultLocked) {
						t.Fatalf("another owner acquired during pending close: %v", err)
					}
				}
				callsWhileClosing := len(f.calls)
				if p, err := j.Published(); p != (Publication{}) || !errors.Is(err, ErrClosing) {
					t.Fatalf("closing journal exposed an active fence: %+v %v", p, err)
				}
				if p, err := j.Publish(context.Background(), faultNextTransaction(acknowledged, 2)); p != (Publication{}) || !errors.Is(err, ErrClosing) {
					t.Fatalf("closing journal accepted a publication: %+v %v", p, err)
				}
				if len(f.calls) != callsWhileClosing {
					t.Fatal("closing journal attempted IO for Published/Publish")
				}
				if err := j.Close(); err != nil {
					t.Fatalf("close retry could not finish a retired/remaining handle: %v", err)
				}
				if status := j.Status(); status.State != JournalClosed || status.LastPublication != acknowledged {
					t.Fatalf("successful retry did not finish close: %+v", status)
				}
				if f.locked {
					t.Fatal("successful close retained the owner lock")
				}
				segmentCloses := 0
				for _, call := range f.calls {
					if call.Op == "Close" {
						if call.Name != activeSegment {
							t.Fatalf("close touched a metadata handle: %+v", call)
						}
						segmentCloses++
					}
				}
				if stage == "owner" && segmentCloses != 1 {
					t.Fatalf("retry repeated an already confirmed segment close: %d", segmentCloses)
				}
				finishedCalls := len(f.calls)
				if err := j.Close(); err != nil || len(f.calls) != finishedCalls {
					t.Fatalf("repeated completed close performed IO: calls=%d->%d error=%v", finishedCalls, len(f.calls), err)
				}
				if _, err := j.Published(); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed Published error: %v", err)
				}
				if _, err := j.Publish(context.Background(), faultNextTransaction(acknowledged, 2)); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed Publish error: %v", err)
				}
				if len(f.calls) != finishedCalls {
					t.Fatal("closed journal attempted publication IO")
				}
				// Confirm the completed close permits a fresh owner and did not
				// change the acknowledged publication.
				reopened, report, err := OpenJournal(f, JournalOptions{})
				if err != nil || report.Publication != acknowledged || !report.PublicationSynced {
					t.Fatalf("completed close did not permit reopen: %+v %v", report, err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestJournalOpenRequiresSyncedPublicationWitness(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after=%t", after), func(t *testing.T) {
			f, j, acknowledged, _ := faultSeedAcknowledged(t)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			beforeOpen := faultTreeSnapshot(t, f)
			f.resetFaults()
			injected := 0
			f.onBoundary = func(b faultBoundary) error {
				if b.Op == "SyncDir" && b.Name == "." && b.After == after {
					injected++
					return errFaultIO
				}
				return nil
			}
			opened, report, err := OpenJournal(f, JournalOptions{})
			if opened != nil || !errors.Is(err, errFaultIO) || !errors.Is(err, ErrRecoveryIncomplete) || injected != 1 {
				t.Fatalf("witness sync failure returned usable journal or lost error: journal=%v report=%+v injected=%d error=%v", opened, report, injected, err)
			}
			if !report.Verified || report.PublicationSynced || report.Publication != acknowledged || report.PublishedBytes != acknowledged.PublishedEnd || report.ScannedTransactions != 1 || report.TailBytes != 0 {
				t.Fatalf("witness failure misreported verified publication: %+v", report)
			}
			if f.locked {
				t.Fatal("failed witness recovery retained ownership")
			}
			if afterOpen := faultTreeSnapshot(t, f); !reflect.DeepEqual(beforeOpen, afterOpen) {
				t.Fatal("witness sync failure changed publication bytes or names")
			}
			f.resetFaults()
			opened, report, err = OpenJournal(f, JournalOptions{})
			if err != nil || opened == nil || !report.Verified || !report.PublicationSynced || report.Publication != acknowledged {
				t.Fatalf("retry could not establish a publication witness: %+v %v", report, err)
			}
			if got := faultPublished(t, opened); got != acknowledged {
				t.Fatalf("retry returned a different publication: %+v", got)
			}
			if err := opened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
