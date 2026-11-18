package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"testing"
)

// Keep this fixture independent of tail_reject_contract_test.go. A real
// compaction supplies the authenticated retired layout; only the interrupted
// next compaction's intent/candidate and ordinary unpublished bytes are added.
type r08CleanupFixture struct {
	f           *faultIO
	publication Publication
	txs         []Transaction
	active      string
	orphan      string
	candidate   string
	intent      string
	intentBytes string
	retired     []string
	protected   map[string]string
	tail        string
	malformed   bool
}

func r08NewCleanupFixture(t testing.TB, malformed bool) *r08CleanupFixture {
	t.Helper()
	f, j, txs := compactionSeed(t)
	pin, err := j.PinReader()
	if err != nil {
		t.Fatal(err)
	}
	compacted, err := j.CompactAuditFull(context.Background())
	if err != nil || compacted.PinnedSegments == 0 || len(compacted.ReclaimedSegments) != 0 {
		t.Fatalf("fixture did not retain the old layout: %+v %v", compacted, err)
	}
	if err := pin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	m := rotationManifest(t, f)
	s := &r08CleanupFixture{
		f: f, publication: compacted.After, txs: txs, malformed: malformed,
		active:    segmentName(m.SegmentID),
		orphan:    segmentName(strings.Repeat("e", 32)),
		candidate: segmentName(strings.Repeat("c", 32)),
		intent:    intentName(strings.Repeat("b", 32)),
		tail:      "R08 durable unpublished active tail",
		protected: make(map[string]string),
	}
	for _, retired := range m.RetiredSegments {
		name := segmentName(retired.SegmentID)
		s.retired = append(s.retired, name)
		// Confirm this is a physical cleanup obligation, not just an absent
		// name carried by a historical manifest.
		if faultReadAll(t, f, name) == "" {
			t.Fatalf("empty retired fixture: %s", name)
		}
	}
	if len(s.retired) == 0 {
		t.Fatal("fixture has no retired segments")
	}
	names, err := f.List("compactions")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if err := f.Remove("compactions/" + name); err != nil {
			t.Fatal(err)
		}
	}
	manifestBytes := []byte(faultReadAll(t, f, manifestName(m.ManifestID)))
	intentBytes := []byte("interrupted malformed compaction intent")
	if !malformed {
		intentBytes, err = encodeCompactionIntent(compactionIntent{
			Store: m.Store, ID: strings.Repeat("b", 32),
			Source: LayoutSource{
				ManifestID: m.ManifestID, ManifestDigest: Digest(sha256.Sum256(manifestBytes)),
				Generation: m.Generation, PublishedEnd: m.PublishedEnd,
				Position: m.Position, PostRootSet: m.PostRootSet,
			},
			Segments: []string{strings.Repeat("c", 32)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := writeSyncedFile(f, s.candidate, []byte("interrupted redundant candidate")); err != nil {
			t.Fatal(err)
		}
	}
	s.intentBytes = string(intentBytes)
	if err := writeSyncedFile(f, s.intent, intentBytes); err != nil {
		t.Fatal(err)
	}
	if err := writeSyncedFile(f, s.orphan, []byte("ordinary orphan without a compaction certificate")); err != nil {
		t.Fatal(err)
	}
	tail, err := f.OpenAppend(s.active)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeAll(tail, []byte(s.tail)); err != nil {
		t.Fatal(err)
	}
	if err := tail.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := tail.Close(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"segments", "compactions", "."} {
		if err := f.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	// Start from a durable fixture. A later crash must not accidentally erase
	// the tail/orphan and turn rejection into a successful open.
	s.f = f.crash()
	for name, data := range faultTreeSnapshot(t, s.f) {
		if name == "CURRENT" || name == "IDENTITY" || strings.HasPrefix(name, "manifests/") ||
			name == s.active || name == s.orphan {
			s.protected[name] = data
		}
	}
	s.f.resetFaults()
	return s
}

func (s *r08CleanupFixture) assertPreserved(t testing.TB) {
	t.Helper()
	for _, call := range s.f.calls {
		if call.Op == "Truncate" ||
			(call.Name == s.active || call.Name == s.orphan) &&
				(call.Op == "Write" || call.Op == "Remove" || call.Op == "InstallNew" || call.Op == "ReplaceAtomic") {
			t.Fatalf("TailReject mutated ordinary unpublished bytes: %+v", call)
		}
	}
	tree := faultTreeSnapshot(t, s.f)
	for name, want := range s.protected {
		if got, ok := tree[name]; !ok || got != want {
			t.Fatalf("cleanup changed protected bytes/name %s (present=%t)", name, ok)
		}
	}
	if s.malformed {
		if _, present := tree[s.intent]; !present {
			evidence := false
			for name, data := range tree {
				if strings.HasPrefix(name, "quarantine/COMPACTION.") && data == s.intentBytes {
					evidence = true
				}
			}
			if !evidence {
				t.Fatal("malformed intent removed without exact quarantine evidence")
			}
		}
	}
}

func (s *r08CleanupFixture) assertReport(t testing.TB, report RecoveryReport) {
	t.Helper()
	// TailBytes includes ordinary orphan bytes as well as the active suffix.
	wantTailBytes := uint64(len(s.tail) + len(s.protected[s.orphan]))
	if !report.Verified || report.PublicationSynced || report.Publication != s.publication ||
		report.PublishedBytes != s.publication.PublishedEnd ||
		report.ScannedTransactions != uint64(len(s.txs)) || report.TailBytes != wantTailBytes ||
		!reflect.DeepEqual(report.OrphanSegments, []string{s.orphan}) {
		t.Fatalf("cleanup lost verified publication/tail/orphan diagnostics: %+v", report)
	}
}

func (s *r08CleanupFixture) retryReject(t testing.TB) {
	t.Helper()
	s.f.resetFaults()
	j, report, err := OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
	if j != nil || !errors.Is(err, ErrRecoveryRequired) || errors.Is(err, ErrRecoveryIncomplete) ||
		errors.Is(err, errFaultIO) || report.TailDisposition != "preserved" || s.f.locked {
		t.Fatalf("fault-free retry did not reject the preserved tail: journal=%v report=%+v err=%v", j, report, err)
	}
	s.assertReport(t, report)
	s.assertPreserved(t)
	names, err := s.f.List("compactions")
	if err != nil || len(names) != 0 {
		t.Fatalf("retry left compaction intents: %v %v", names, err)
	}
	removed := append([]string(nil), s.retired...)
	if !s.malformed {
		removed = append(removed, s.candidate)
	}
	for _, name := range removed {
		file, err := s.f.OpenRead(name)
		if file != nil {
			_ = file.Close()
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("retry left authorized cleanup pending at %s: %v", name, err)
		}
	}
}

func TestR08TailRejectCleanupFaultsPreservePublicationAndRetry(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, stage := range []string{
			"remove-candidate", "remove-intent", "remove-retired",
			"sync-segments-intent", "sync-segments-retired", "sync-compactions",
			"sync-quarantine", "close-intent",
		} {
			if malformed && stage == "remove-candidate" || !malformed && stage == "sync-quarantine" {
				continue
			}
			for _, after := range []bool{false, true} {
				for _, crash := range []bool{false, true} {
					t.Run(fmt.Sprintf("malformed=%t/%s/after=%t/crash=%t", malformed, stage, after, crash), func(t *testing.T) {
						s := r08NewCleanupFixture(t, malformed)
						op, name, occurrence := "Remove", s.intent, 1
						switch stage {
						case "remove-candidate":
							name = s.candidate
						case "remove-retired":
							name = s.retired[0]
						case "sync-segments-intent":
							op, name = "SyncDir", "segments"
						case "sync-segments-retired":
							op, name, occurrence = "SyncDir", "segments", 2
						case "sync-compactions":
							op, name = "SyncDir", "compactions"
						case "sync-quarantine":
							op, name = "SyncDir", "quarantine"
						case "close-intent":
							op = "Close"
						}
						matched, injected := 0, 0
						s.f.onBoundary = func(b faultBoundary) error {
							if b.Op == op && b.Name == name && b.After == after {
								matched++
								if matched == occurrence {
									injected++
									return errFaultIO
								}
							}
							return nil
						}
						j, report, err := OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
						if j != nil || injected != 1 || !errors.Is(err, ErrRecoveryIncomplete) || !errors.Is(err, errFaultIO) {
							t.Fatalf("cleanup fault lost classification/cause: journal=%v injected=%d report=%+v err=%v", j, injected, report, err)
						}
						s.assertReport(t, report)
						var pending *CleanupError
						if errors.As(err, &pending) || s.f.locked {
							t.Fatalf("one-shot fault left cleanup resources: locked=%t err=%v", s.f.locked, err)
						}
						s.assertPreserved(t)
						if crash {
							s.f = s.f.crash()
							s.assertPreserved(t)
						}
						s.retryReject(t)
						// Check durability even when the first retry took place in
						// the same process after an uncertain Remove/SyncDir.
						s.f = s.f.crash()
						s.assertPreserved(t)
						s.retryReject(t)
					})
				}
			}
		}
	}
}

func TestR08TailRejectCleanupRepeatedFileThenOwnerFailures(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, target := range []string{"intent", "orphan"} {
			t.Run(fmt.Sprintf("malformed=%t/%s", malformed, target), func(t *testing.T) {
				s := r08NewCleanupFixture(t, malformed)
				fileName := s.intent
				if target == "orphan" {
					fileName = s.orphan
				}
				fileCause := errors.New("R08 ReadFile.Close failure")
				ownerCause := errors.New("R08 OwnerLock.Close failure")
				fileFails, ownerFails := true, true
				fileAttempts, ownerAttempts := 0, 0
				s.f.onBoundary = func(b faultBoundary) error {
					if b.After {
						return nil
					}
					// An orphan handle closes during error unwinding. Give that
					// unwind a real recovery-cleanup error to preserve as well.
					if target == "orphan" && b.Op == "Remove" && b.Name == s.retired[0] {
						return errFaultIO
					}
					if b.Op == "Close" && b.Name == fileName {
						fileAttempts++
						if fileFails {
							return fileCause
						}
					}
					if b.Op == "OwnerLock.Close" {
						ownerAttempts++
						if ownerFails {
							return ownerCause
						}
					}
					return nil
				}
				j, report, openErr := OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
				var cleanup *CleanupError
				if j != nil || !errors.Is(openErr, ErrRecoveryIncomplete) || !errors.Is(openErr, fileCause) ||
					!errors.As(openErr, &cleanup) || fileAttempts == 0 {
					t.Fatalf("open lost pending ReadFile or diagnosis: journal=%v report=%+v err=%v", j, report, openErr)
				}
				s.assertReport(t, report)
				assertLocked := func() {
					t.Helper()
					if !s.f.locked {
						t.Fatal("owner released while cleanup was pending")
					}
					owner, err := s.f.AcquireOwnerLock()
					if owner != nil {
						_ = owner.Close()
					}
					if !errors.Is(err, errFaultLocked) {
						t.Fatalf("second owner acquired during cleanup: %v", err)
					}
				}
				assertLocked()
				for i := 0; i < 3; i++ {
					before := fileAttempts
					if err := cleanup.Close(); !errors.Is(err, fileCause) || fileAttempts != before+1 {
						t.Fatalf("file retry %d lost pending handle/cause: attempts=%d err=%v", i, fileAttempts, err)
					}
					if ownerAttempts != 0 {
						t.Fatal("owner close attempted before pending files closed")
					}
					assertLocked()
				}
				fileFails = false
				start := len(s.f.calls)
				if err := cleanup.Close(); !errors.Is(err, ownerCause) {
					t.Fatalf("FILE -> OWNER retry lost owner failure: %v", err)
				}
				files := []string{s.orphan, s.active}
				if target == "intent" {
					files = append([]string{s.intent}, files...)
				}
				cleanupAssertCloseOrder(t, r08RenumberCalls(s.f.calls[start:]), files...)
				assertLocked()
				for i := 0; i < 3; i++ {
					start := len(s.f.calls)
					if err := cleanup.Close(); !errors.Is(err, ownerCause) {
						t.Fatalf("owner retry %d lost cause: %v", i, err)
					}
					cleanupAssertCloseOrder(t, r08RenumberCalls(s.f.calls[start:]))
					assertLocked()
				}
				ownerFails = false
				start = len(s.f.calls)
				if err := cleanup.Close(); err != nil || s.f.locked {
					t.Fatalf("final owner retry failed: locked=%t err=%v", s.f.locked, err)
				}
				cleanupAssertCloseOrder(t, r08RenumberCalls(s.f.calls[start:]))
				done := len(s.f.calls)
				if err := cleanup.Close(); err != nil || len(s.f.calls) != done {
					t.Fatalf("completed cleanup repeated IO: %v", err)
				}
				if !errors.Is(openErr, ErrRecoveryIncomplete) || !errors.Is(openErr, fileCause) ||
					target == "orphan" && !errors.Is(openErr, errFaultIO) {
					t.Fatalf("cleanup retry erased the original open error: %v", openErr)
				}
				// Neither OpenJournal nor CleanupError.Close takes a context;
				// retries exercise their actual API, without inventing cancellation.
				s.f.resetFaults()
				s.assertPreserved(t)
				s.retryReject(t)
				s.f = s.f.crash()
				s.assertPreserved(t)
				s.retryReject(t)
			})
		}
	}
}

func TestR08TailRejectCleanupOwnerFailurePreservesOpenError(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, after := range []bool{false, true} {
			for _, crash := range []bool{false, true} {
				t.Run(fmt.Sprintf("malformed=%t/after=%t/crash=%t", malformed, after, crash), func(t *testing.T) {
					s := r08NewCleanupFixture(t, malformed)
					ownerCause := errors.New("R08 owner close during failed open")
					ownerAttempts := 0
					s.f.onBoundary = func(b faultBoundary) error {
						if b.Op == "Remove" && b.Name == s.retired[0] && !b.After {
							return errFaultIO
						}
						if b.Op == "OwnerLock.Close" && b.After == after {
							ownerAttempts++
							return ownerCause
						}
						return nil
					}
					j, report, openErr := OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
					var cleanup *CleanupError
					if j != nil || !errors.As(openErr, &cleanup) || ownerAttempts != 1 ||
						!errors.Is(openErr, ErrRecoveryIncomplete) || !errors.Is(openErr, errFaultIO) ||
						!errors.Is(openErr, ownerCause) {
						t.Fatalf("open lost recovery/owner errors or cleanup handle: journal=%v err=%v", j, openErr)
					}
					s.assertReport(t, report)
					// An after-error has already released the modeled lock; a
					// before-error must retain it through every failed retry.
					if s.f.locked == after {
						t.Fatalf("owner lock state disagrees with fault boundary: after=%t locked=%t", after, s.f.locked)
					}
					if !after {
						for i := 0; i < 3; i++ {
							start := len(s.f.calls)
							if err := cleanup.Close(); !errors.Is(err, ownerCause) || !s.f.locked {
								t.Fatalf("owner failure retry %d lost cause/lock: %v", i, err)
							}
							cleanupAssertCloseOrder(t, r08RenumberCalls(s.f.calls[start:]))
						}
					}
					s.assertPreserved(t)
					if crash {
						// Crash with cleanup still pending: old handles become
						// invalid, and the new process must recover independently.
						s.f = s.f.crash()
					} else {
						s.f.resetFaults()
						if err := cleanup.Close(); err != nil || s.f.locked {
							t.Fatalf("owner retry after fault removal failed: %v", err)
						}
						cleanupAssertCloseOrder(t, s.f.calls)
						done := len(s.f.calls)
						if err := cleanup.Close(); err != nil || len(s.f.calls) != done {
							t.Fatalf("confirmed owner was closed again: %v", err)
						}
					}
					if !errors.Is(openErr, ErrRecoveryIncomplete) || !errors.Is(openErr, errFaultIO) ||
						!errors.Is(openErr, ownerCause) {
						t.Fatalf("retry erased original open causes: %v", openErr)
					}
					s.assertPreserved(t)
					s.retryReject(t)
					s.f = s.f.crash()
					s.retryReject(t)
				})
			}
		}
	}
}

func r08RenumberCalls(calls []faultCall) []faultCall {
	out := append([]faultCall(nil), calls...)
	for i := range out {
		out[i].Step = i + 1
	}
	return out
}
