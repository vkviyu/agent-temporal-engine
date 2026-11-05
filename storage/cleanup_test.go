package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func cleanupAssertNoOwnerClose(t testing.TB, calls []faultCall) {
	t.Helper()
	for _, call := range calls {
		if call.Op == "OwnerLock.Close" {
			t.Fatalf("owner lock was released ahead of pending resource cleanup: %+v", calls)
		}
	}
}

func cleanupAssertCloseOrder(t testing.TB, calls []faultCall, files ...string) {
	t.Helper()
	want := make([]faultCall, 0, len(files)+1)
	for _, name := range files {
		want = append(want, faultCall{Step: len(want) + 1, Op: "Close", Name: name})
	}
	want = append(want, faultCall{Step: len(want) + 1, Op: "OwnerLock.Close", Name: "."})
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("resources did not retire file-first and owner-last: got=%+v want=%+v", calls, want)
	}
}

func TestCreateJournalRetainsFailedIdentityCleanup(t *testing.T) {
	f := newFaultIO()
	closeFailures := 0
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Close" && b.Name == "IDENTITY" && !b.After {
			closeFailures++
			return errFaultIO
		}
		return nil
	}
	j, err := CreateJournal(f, JournalOptions{})
	var cleanup *CleanupError
	if j != nil || !errors.Is(err, errFaultIO) || !errors.Is(err, ErrIncompleteStore) || !errors.As(err, &cleanup) {
		t.Fatalf("constructor discarded its pending identity handle or cause: journal=%v cleanup=%v error=%v", j, cleanup, err)
	}
	if closeFailures < 2 {
		t.Fatalf("fixture did not keep identity close failing through deferred cleanup: %d", closeFailures)
	}
	if !f.locked {
		t.Fatal("constructor lost owner lock while identity close was pending")
	}
	cleanupAssertNoOwnerClose(t, f.calls)
	if _, lockErr := f.AcquireOwnerLock(); !errors.Is(lockErr, errFaultLocked) {
		t.Fatalf("another owner acquired during pending constructor cleanup: %v", lockErr)
	}
	f.resetFaults()
	if closeErr := cleanup.Close(); closeErr != nil {
		t.Fatalf("retained constructor cleanup could not finish: %v", closeErr)
	}
	cleanupAssertCloseOrder(t, f.calls, "IDENTITY")
	if f.locked {
		t.Fatal("completed constructor cleanup retained owner lock")
	}
	finishedCalls := len(f.calls)
	if closeErr := cleanup.Close(); closeErr != nil || len(f.calls) != finishedCalls {
		t.Fatalf("completed constructor cleanup was not idempotent: %v", closeErr)
	}
	if !errors.Is(err, errFaultIO) || !errors.Is(err, ErrIncompleteStore) {
		t.Fatalf("cleanup retry erased the original constructor diagnosis: %v", err)
	}
}

func TestOpenJournalRetainsFailedCurrentReadCleanup(t *testing.T) {
	f, old, acknowledged, _ := faultSeedAcknowledged(t)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	closeFailures := 0
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Close" && b.Name == "CURRENT" && !b.After {
			closeFailures++
			return errFaultIO
		}
		return nil
	}
	j, report, err := OpenJournal(f, JournalOptions{})
	var cleanup *CleanupError
	if j != nil || !errors.Is(err, errFaultIO) || !errors.As(err, &cleanup) {
		t.Fatalf("open discarded its pending CURRENT read handle: journal=%v report=%+v cleanup=%v error=%v", j, report, cleanup, err)
	}
	if closeFailures < 2 || !f.locked {
		t.Fatalf("open did not retain ownership through persistent read-handle close failure: failures=%d locked=%t", closeFailures, f.locked)
	}
	cleanupAssertNoOwnerClose(t, f.calls)
	if _, lockErr := f.AcquireOwnerLock(); !errors.Is(lockErr, errFaultLocked) {
		t.Fatalf("another owner acquired during pending open cleanup: %v", lockErr)
	}
	f.resetFaults()
	if closeErr := cleanup.Close(); closeErr != nil {
		t.Fatalf("retained recovery cleanup could not finish: %v", closeErr)
	}
	cleanupAssertCloseOrder(t, f.calls, "CURRENT")
	if f.locked {
		t.Fatal("completed open cleanup retained owner lock")
	}
	finishedCalls := len(f.calls)
	if closeErr := cleanup.Close(); closeErr != nil || len(f.calls) != finishedCalls {
		t.Fatalf("completed open cleanup was not idempotent: %v", closeErr)
	}
	reopened, report, openErr := OpenJournal(f, JournalOptions{})
	if openErr != nil || reopened == nil || !report.PublicationSynced || report.Publication != acknowledged {
		t.Fatalf("cleanup retry did not allow a fresh recovery of the original ACK: %+v %v", report, openErr)
	}
	if closeErr := reopened.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, errFaultIO) {
		t.Fatalf("cleanup retry erased the original open diagnosis: %v", err)
	}
}

func TestJournalCloseDrainsFailedPublicationCleanup(t *testing.T) {
	f, j, acknowledged, _ := faultSeedAcknowledged(t)
	activeSegment := segmentName(j.segmentID)
	var temporaryManifest string
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Close" && strings.HasPrefix(b.Name, "manifests/.STAGE.") && !b.After {
			temporaryManifest = b.Name
			return errFaultIO
		}
		return nil
	}
	p, err := j.Publish(context.Background(), faultNextTransaction(acknowledged, 2))
	var cleanup *CleanupError
	if p != (Publication{}) || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) || !errors.As(err, &cleanup) || temporaryManifest == "" {
		t.Fatalf("publication discarded its failed temporary close: publication=%+v cleanup=%v error=%v", p, cleanup, err)
	}
	if status := j.Status(); status.State != JournalFenced || status.LastPublication != acknowledged {
		t.Fatalf("failed publication did not preserve a fenced old ACK: %+v", status)
	}
	if closeErr := j.Close(); !errors.Is(closeErr, errFaultIO) {
		t.Fatalf("close did not retry the pending temporary handle: %v", closeErr)
	}
	if status := j.Status(); status.State != JournalClosing || status.LastPublication != acknowledged {
		t.Fatalf("failed pending cleanup did not leave journal closing: %+v", status)
	}
	if !f.locked {
		t.Fatal("journal released ownership before its temporary handle retired")
	}
	cleanupAssertNoOwnerClose(t, f.calls)
	for _, call := range f.calls {
		if call.Op == "Close" && call.Name == activeSegment {
			t.Fatal("journal closed the active segment ahead of pending temporary cleanup")
		}
	}
	if _, lockErr := f.AcquireOwnerLock(); !errors.Is(lockErr, errFaultLocked) {
		t.Fatalf("another owner acquired during pending publication cleanup: %v", lockErr)
	}
	f.resetFaults()
	if closeErr := j.Close(); closeErr != nil {
		t.Fatalf("journal could not finish retained cleanup: %v", closeErr)
	}
	cleanupAssertCloseOrder(t, f.calls, temporaryManifest, activeSegment)
	if status := j.Status(); status.State != JournalClosed || status.LastPublication != acknowledged || f.locked {
		t.Fatalf("successful cleanup did not finish journal close: %+v locked=%t", status, f.locked)
	}
	finishedCalls := len(f.calls)
	if closeErr := j.Close(); closeErr != nil || len(f.calls) != finishedCalls {
		t.Fatalf("completed journal close repeated cleanup IO: %v", closeErr)
	}
	if closeErr := cleanup.Close(); closeErr != nil || len(f.calls) != finishedCalls {
		t.Fatalf("publication's retained temporary handle was not retired: %v", closeErr)
	}
	if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) {
		t.Fatalf("successful close erased the unknown-publication diagnosis: %v", err)
	}
}

type cleanupRetireProbe struct {
	name    string
	trace   *[]string
	fail    bool
	retired bool
}

func (p *cleanupRetireProbe) Close() error {
	*p.trace = append(*p.trace, p.name)
	if p.retired {
		return fmt.Errorf("cleanup probe %s closed twice", p.name)
	}
	if p.fail {
		return errFaultIO
	}
	p.retired = true
	return nil
}

func TestCleanupJoinedErrorsDeduplicateAndRetireInOrder(t *testing.T) {
	var trace []string
	firstFile := &cleanupRetireProbe{name: "first", trace: &trace, fail: true}
	secondFile := &cleanupRetireProbe{name: "second", trace: &trace, fail: true}
	owner := &cleanupRetireProbe{name: "owner", trace: &trace}
	first := &CleanupError{cause: fmt.Errorf("first: %w", errFaultIO), pending: []io.Closer{firstFile}}
	second := &CleanupError{cause: fmt.Errorf("second: %w", errFaultIO), pending: []io.Closer{secondFile}}
	joined := errors.Join(first, fmt.Errorf("wrapped second: %w", second), errors.Join(first, second))
	if got := pendingCleanups(joined); !reflect.DeepEqual(got, []io.Closer{first, second}) {
		t.Fatalf("joined cleanup handles were duplicated or reordered: %#v", got)
	}
	combinedErr := cleanupOnError(joined, owner)
	var combined *CleanupError
	if !errors.As(combinedErr, &combined) || combined == first || combined == second || !errors.Is(combinedErr, errFaultIO) {
		t.Fatalf("cleanup group did not retain joined errors and remaining resources: %v", combinedErr)
	}
	if !reflect.DeepEqual(trace, []string{"first"}) || owner.retired || secondFile.retired {
		t.Fatalf("failed first resource did not stop retirement: %v", trace)
	}
	firstFile.fail = false
	if err := combined.Close(); !errors.Is(err, errFaultIO) {
		t.Fatalf("second resource failure was lost: %v", err)
	}
	if !reflect.DeepEqual(trace, []string{"first", "first", "second"}) || !firstFile.retired || secondFile.retired || owner.retired {
		t.Fatalf("cleanup did not advance serially to the second resource: %v", trace)
	}
	secondFile.fail = false
	if err := combined.Close(); err != nil {
		t.Fatalf("serial cleanup retry failed: %v", err)
	}
	if !reflect.DeepEqual(trace, []string{"first", "first", "second", "second", "owner"}) || !secondFile.retired || !owner.retired {
		t.Fatalf("retired resources were repeated or owner was released out of order: %v", trace)
	}
	finishedTrace := append([]string(nil), trace...)
	if err := combined.Close(); err != nil || !reflect.DeepEqual(trace, finishedTrace) {
		t.Fatalf("completed cleanup group was not idempotent: trace=%v error=%v", trace, err)
	}
	if !errors.Is(combinedErr, errFaultIO) {
		t.Fatalf("retirement erased the original cleanup cause: %v", combinedErr)
	}
}

func TestJournalConstructorsReleaseLeaseReturnedWithAcquireError(t *testing.T) {
	for _, constructor := range []string{"create", "open"} {
		t.Run(constructor, func(t *testing.T) {
			f := newFaultIO()
			if constructor == "open" {
				created, err := CreateJournal(f, JournalOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := created.Close(); err != nil {
					t.Fatal(err)
				}
			}
			f.resetFaults()
			injected := 0
			f.onBoundary = func(b faultBoundary) error {
				if b.Op == "AcquireOwnerLock" && b.After {
					injected++
					return errFaultIO
				}
				return nil
			}
			var j *Journal
			var err error
			if constructor == "create" {
				j, err = CreateJournal(f, JournalOptions{})
			} else {
				j, _, err = OpenJournal(f, JournalOptions{})
			}
			if j != nil || !errors.Is(err, errFaultIO) || injected != 1 {
				t.Fatalf("constructor lost acquisition error: journal=%v injected=%d error=%v", j, injected, err)
			}
			if f.locked {
				t.Fatal("constructor discarded a lease returned alongside the acquisition error")
			}
			want := []faultCall{
				{Step: 1, Op: "AcquireOwnerLock", Name: "."},
				{Step: 2, Op: "OwnerLock.Close", Name: "."},
			}
			if !reflect.DeepEqual(f.calls, want) {
				t.Fatalf("constructor did not retire the errored lease immediately: got=%+v want=%+v", f.calls, want)
			}
			f.resetFaults()
			owner, acquireErr := f.AcquireOwnerLock()
			if acquireErr != nil || owner == nil {
				t.Fatalf("ownership could not be reacquired without a crash: %v", acquireErr)
			}
			if closeErr := owner.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		})
	}
}
