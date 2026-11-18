package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type r08Fixture struct {
	f           *faultIO
	publication Publication
	manifest    Manifest
	txs         []Transaction
	active      string
	tail        string
	pending     map[string]string
	intents     []string
	candidates  []string
	retired     []string
	orphans     []string
}

// Compose real interrupted compaction artifacts with a real selected compacted
// layout. Retired bytes survive through a pin and crash, never by recreating
// already reclaimed source segments.
func r08Seed(t *testing.T) *r08Fixture {
	t.Helper()
	f, j, txs := compactionSeed(t)
	s := &r08Fixture{f: f, txs: txs, pending: map[string]string{}}
	for _, empty := range []bool{false, true} {
		f.resetFaults()
		hit := false
		f.onBoundary = func(b faultBoundary) error {
			if !b.After && ((!empty && b.Op == "CreateExclusive" && strings.HasPrefix(b.Name, "manifests/.STAGE.")) ||
				(empty && b.Op == "Write" && strings.HasPrefix(b.Name, "segments/"))) {
				hit = true
				return errFaultIO
			}
			return nil
		}
		if _, err := j.CompactAuditFull(context.Background()); !hit || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) {
			t.Fatalf("pending compaction fault missed: hit=%t err=%v", hit, err)
		}
		var intentPath, candidate string
		for _, c := range f.calls {
			if c.Op == "CreateExclusive" && strings.HasPrefix(c.Name, "compactions/INTENT.") {
				intentPath = c.Name
			}
			if c.Op == "CreateExclusive" && strings.HasPrefix(c.Name, "segments/") {
				candidate = c.Name
			}
		}
		if intentPath == "" || candidate == "" {
			t.Fatal("interrupted compaction did not create intent and candidate")
		}
		in, err := decodeCompactionIntent([]byte(faultReadAll(t, f, intentPath)))
		if err != nil || len(in.Segments) != 1 || segmentName(in.Segments[0]) != candidate {
			t.Fatalf("pending artifact binding: %+v %v", in, err)
		}
		for _, name := range []string{intentPath, candidate} {
			s.pending[name] = faultReadAll(t, f, name)
		}
		if (len(s.pending[candidate]) == 0) != empty {
			t.Fatal("candidate did not reach the intended write boundary")
		}
		s.intents = append(s.intents, intentPath)
		f.resetFaults()
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		j, _, err = OpenJournal(f, JournalOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	pin, err := j.PinReader()
	if err != nil {
		t.Fatal(err)
	}
	report, err := j.CompactAuditFull(context.Background())
	if err != nil || report.PinnedSegments < 2 || len(report.ReclaimedSegments) != 0 {
		t.Fatalf("real compaction did not retain pinned source bytes: %+v %v", report, err)
	}
	s.publication = faultPublished(t, j)
	s.manifest = rotationManifest(t, f)
	s.active = segmentName(s.manifest.SegmentID)
	if s.manifest.LayoutSource == nil || len(s.manifest.RetiredSegments) < 2 {
		t.Fatal("fixture lacks certified source and retirement descriptors")
	}
	for _, desc := range s.manifest.RetiredSegments {
		name := segmentName(desc.SegmentID)
		if len(faultReadAll(t, f, name)) == 0 {
			t.Fatal("retired source bytes were not retained", name)
		}
		s.retired = append(s.retired, name)
	}
	// Crash while the reader still pins the source: no Close/reclaim/reopen
	// gets a chance to discharge the selected retirement obligation.
	if _, err := pin.ReadAt(make([]byte, 1), SegmentHeaderSize); err != nil {
		t.Fatal(err)
	}
	s.f = f.crash()
	// Restore the exact intent/candidate bytes emitted above, not invented
	// certificates. Both sources remain in the selected canonical history.
	for name, raw := range s.pending {
		if err := writeSyncedFile(s.f, name, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	s.orphans = []string{segmentName(strings.Repeat("e", 32)), segmentName(strings.Repeat("f", 32))}
	for i, name := range s.orphans {
		raw := []byte("ordinary orphan: not a compaction candidate")
		if i == 1 {
			raw = nil
		}
		if err := writeSyncedFile(s.f, name, raw); err != nil {
			t.Fatal(err)
		}
	}
	tail, _, err := EncodeTransaction(faultNextTransaction(s.publication, 99), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s.tail = string(tail)
	replayAppendTail(t, s.f, s.active, tail)
	for _, dir := range []string{"segments", "compactions", "."} {
		if err := s.f.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	before := faultTreeSnapshot(t, s.f)
	s.f = s.f.crash()
	r08AssertTree(t, s.f, before)
	// The report follows sorted intent enumeration, not creation order.
	sort.Strings(s.intents)
	for _, name := range s.intents {
		in, err := decodeCompactionIntent([]byte(s.pending[name]))
		if err != nil {
			t.Fatal(err)
		}
		s.candidates = append(s.candidates, segmentName(in.Segments[0]))
	}
	return s
}

func r08AssertTree(t *testing.T, f *faultIO, want map[string]string) {
	t.Helper()
	got := faultTreeSnapshot(t, f)
	if !reflect.DeepEqual(got, want) {
		for name, raw := range want {
			if actual, ok := got[name]; !ok || actual != raw {
				t.Errorf("file changed or disappeared: %s (present=%t)", name, ok)
			}
		}
		for name := range got {
			if _, ok := want[name]; !ok {
				t.Errorf("unexpected file: %s", name)
			}
		}
		t.Fatal("recovery changed files outside its exact contract")
	}
}

func r08AssertRemovals(t *testing.T, calls []faultCall, want []string, evidence bool) {
	t.Helper()
	var got []string
	rootSyncs := 0
	for _, c := range calls {
		switch c.Op {
		case "AcquireOwnerLock", "OwnerLock.Close", "OpenAppend", "Close":
		case "Remove":
			if rootSyncs == 0 {
				t.Fatalf("cleanup preceded publication stabilization: %+v", c)
			}
			got = append(got, c.Name)
		case "SyncDir":
			if c.Name == "." {
				rootSyncs++
			} else if c.Name != "segments" && c.Name != "compactions" && !(evidence && c.Name == "quarantine") {
				t.Fatalf("unexpected sync before tail rejection: %+v", c)
			}
		case "Mkdir", "CreateExclusive", "Write", "Sync":
			if !evidence || (c.Name != "quarantine" && !strings.HasPrefix(c.Name, "quarantine/COMPACTION.")) {
				t.Fatalf("tail rejection mutated non-intent evidence: %+v", c)
			}
		default:
			t.Fatalf("tail rejection attempted forbidden operation: %+v", c)
		}
	}
	wantRootSyncs := 1 // Stabilize CURRENT before certified layout cleanup.
	if evidence {
		wantRootSyncs++ // Persist the quarantine directory before intent evidence.
	}
	if rootSyncs != wantRootSyncs {
		t.Fatalf("unexpected root sync count: got=%d want=%d", rootSyncs, wantRootSyncs)
	}
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("removal attempts differ: got=%v want=%v", got, want)
	}
}

func (s *r08Fixture) open(t *testing.T, policy TailPolicy) (*Journal, RecoveryReport, error) {
	t.Helper()
	var replayed []Transaction
	j, report, err := OpenJournalWithReplay(s.f, JournalOptions{TailPolicy: policy}, func(tx ScannedTransaction, _ io.ReaderAt) error {
		replayAssertNoMutation(t, s.f.calls)
		replayed = append(replayed, tx.Transaction)
		return nil
	})
	if !reflect.DeepEqual(replayed, s.txs) {
		t.Fatal("recovery did not replay exactly the selected published history")
	}
	return j, report, err
}

func (s *r08Fixture) rejectReport(before map[string]string) RecoveryReport {
	return RecoveryReport{
		Verified: true, Publication: s.publication, PublishedBytes: s.publication.PublishedEnd,
		ScannedTransactions: uint64(len(s.txs)), TailBytes: uint64(len(s.tail) + len(before[s.orphans[0]])),
		TailDisposition: "preserved", OrphanSegments: s.orphans, ReclaimedSegments: s.retired,
	}
}

func TestR08TailRejectReconcilesCertifiedLayoutBeforeRejectingCombinedTailAndOrphans(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		name := "valid-intents"
		if malformed {
			name = "valid-and-malformed-intents"
		}
		t.Run(name, func(t *testing.T) {
			s := r08Seed(t)
			malformedName := intentName(strings.Repeat("d", 32))
			malformedBytes := "interrupted recognized compaction intent"
			if malformed {
				if err := writeSyncedFile(s.f, malformedName, []byte(malformedBytes)); err != nil {
					t.Fatal(err)
				}
				if err := s.f.SyncDir("compactions"); err != nil {
					t.Fatal(err)
				}
			}
			expectedTree := faultTreeSnapshot(t, s.f)
			for pass := 0; pass < 3; pass++ {
				s.f.resetFaults()
				j, got, err := s.open(t, TailReject)
				if j != nil || !errors.Is(err, ErrRecoveryRequired) || s.f.locked {
					t.Fatalf("reject must return no journal and release ownership: journal=%v report=%+v err=%v", j, got, err)
				}
				want := s.rejectReport(expectedTree)
				removals := append([]string(nil), s.retired...)
				if pass == 0 {
					want.DiscardedCompactionSegments = s.candidates
					for path := range s.pending {
						delete(expectedTree, path)
						removals = append(removals, path)
					}
					if malformed {
						if len(got.QuarantineFiles) != 1 || !strings.HasPrefix(got.QuarantineFiles[0], "quarantine/COMPACTION.") {
							t.Fatalf("missing malformed intent evidence: %+v", got)
						}
						want.QuarantineFiles = got.QuarantineFiles
						expectedTree["quarantine/"] = "directory"
						expectedTree[got.QuarantineFiles[0]] = malformedBytes
						delete(expectedTree, malformedName)
						removals = append(removals, malformedName)
					}
				}
				// ReclaimedSegments reports the certified retirement obligation
				// even on retries where Remove sees an already absent file.
				for _, path := range s.retired {
					delete(expectedTree, path)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("pass %d report:\ngot  %+v\nwant %+v", pass, got, want)
				}
				r08AssertRemovals(t, s.f.calls, removals, malformed && pass == 0)
				// Includes CURRENT, IDENTITY, selected manifest, source proofs,
				// published prefix, complete active tail and BOTH ordinary orphans.
				r08AssertTree(t, s.f, expectedTree)
				if pass != 1 {
					s.f = s.f.crash()
					r08AssertTree(t, s.f, expectedTree)
				}
			}
			s.finishQuarantine(t, expectedTree)
		})
	}
}

func (s *r08Fixture) finishQuarantine(t *testing.T, expectedTree map[string]string) {
	t.Helper()
	s.f.resetFaults()
	j, got, err := s.open(t, TailQuarantine)
	if err != nil || j == nil {
		t.Fatalf("explicit quarantine did not finish: %+v %v", got, err)
	}
	want := s.rejectReport(expectedTree)
	want.PublicationSynced = true
	want.TailDisposition = "quarantined"
	want.QuarantineFile = got.QuarantineFile
	want.QuarantineFiles = got.QuarantineFiles
	if !reflect.DeepEqual(got, want) || len(got.QuarantineFiles) != 3 ||
		got.QuarantineFile == "" || got.QuarantineFiles[0] != got.QuarantineFile {
		t.Fatalf("explicit quarantine report: got=%+v want=%+v", got, want)
	}
	expectedTree["quarantine/"] = "directory"
	expectedTree[got.QuarantineFile] = s.tail
	for i, orphan := range s.orphans {
		evidence := got.QuarantineFiles[i+1]
		if !strings.HasPrefix(evidence, "quarantine/ORPHAN.") ||
			!strings.HasSuffix(evidence, "."+strings.TrimPrefix(orphan, "segments/SEGMENT.")) {
			t.Fatal("orphan evidence is not bound to its original name", evidence)
		}
		expectedTree[evidence] = expectedTree[orphan]
		delete(expectedTree, orphan)
	}
	expectedTree[s.active] = expectedTree[s.active][:s.publication.PublishedEnd]
	r08AssertTree(t, s.f, expectedTree)
	rotationAssertLogicalTransactions(t, j, s.txs)
	if faultPublished(t, j) != s.publication {
		t.Fatal("quarantine promoted the complete unpublished transaction")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	s.f = s.f.crash()
	r08AssertTree(t, s.f, expectedTree)
	j, got, err = s.open(t, TailReject)
	want = RecoveryReport{
		Verified: true, PublicationSynced: true, Publication: s.publication,
		PublishedBytes: s.publication.PublishedEnd, ScannedTransactions: uint64(len(s.txs)),
		TailDisposition: "none", ReclaimedSegments: s.retired,
	}
	if err != nil || j == nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("quarantine was not durable across reopen: %+v %v", got, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	r08AssertTree(t, s.f, expectedTree)
}

func TestR08TailRejectUnknownIntentNamesFailBeforeVerificationOrMutation(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"UNKNOWN", ErrRecoveryRequired},
		{"INTENT.not-a-metadata-id", ErrMalformed},
		{"intent." + strings.Repeat("a", 32), ErrRecoveryRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := r08Seed(t)
			if err := writeSyncedFile(s.f, "compactions/"+tc.name, []byte("unknown evidence")); err != nil {
				t.Fatal(err)
			}
			if err := s.f.SyncDir("compactions"); err != nil {
				t.Fatal(err)
			}
			before := faultTreeSnapshot(t, s.f)
			for pass := 0; pass < 2; pass++ {
				s.f.resetFaults()
				j, report, err := OpenJournalWithReplay(s.f, JournalOptions{TailPolicy: TailReject}, func(ScannedTransaction, io.ReaderAt) error {
					t.Fatal("unknown intent reached replay")
					return nil
				})
				if j != nil || !errors.Is(err, tc.err) || !reflect.DeepEqual(report, RecoveryReport{}) || s.f.locked {
					t.Fatalf("unknown intent failed too late: journal=%v report=%+v err=%v", j, report, err)
				}
				replayAssertNoMutation(t, s.f.calls)
				r08AssertTree(t, s.f, before)
				s.f = s.f.crash()
				r08AssertTree(t, s.f, before)
			}
		})
	}
}

func TestR08TailRejectVerificationAndReplayFailuresPrecedeAllCleanup(t *testing.T) {
	for _, failure := range []string{"published-frame", "final-prefix", "selected-source", "pending-source", "replay"} {
		for _, policy := range []TailPolicy{TailReject, TailQuarantine} {
			t.Run(failure+"/"+string(policy), func(t *testing.T) {
				s := r08Seed(t)
				// Even malformed recognized evidence must not be copied before
				// prefix/replay/source authentication has finished.
				if err := writeSyncedFile(s.f, intentName(strings.Repeat("c", 32)), []byte("malformed intent")); err != nil {
					t.Fatal(err)
				}
				if err := s.f.SyncDir("compactions"); err != nil {
					t.Fatal(err)
				}
				wantErr := ErrCorruption
				wantReplay := len(s.txs)
				switch failure {
				case "published-frame":
					raw := []byte(faultReadAll(t, s.f, s.active))
					raw[SegmentHeaderSize+HeaderSize] ^= 1
					replayReplaceFile(t, s.f, s.active, raw)
					wantReplay = 0
				case "final-prefix":
					m := s.manifest
					m.PrefixDigest[0] ^= 1
					raw, err := EncodeManifest(m)
					if err != nil {
						t.Fatal(err)
					}
					c, err := DecodeCurrent([]byte(faultReadAll(t, s.f, "CURRENT")))
					if err != nil {
						t.Fatal(err)
					}
					c.ManifestDigest = Digest(sha256.Sum256(raw))
					current, err := EncodeCurrent(c)
					if err != nil {
						t.Fatal(err)
					}
					replayReplaceFile(t, s.f, manifestName(m.ManifestID), raw)
					replayReplaceFile(t, s.f, "CURRENT", current)
				case "selected-source":
					path := manifestName(s.manifest.LayoutSource.ManifestID)
					raw := []byte(faultReadAll(t, s.f, path))
					raw[len(raw)-1] ^= 1
					replayReplaceFile(t, s.f, path, raw)
					wantReplay = 0
				case "pending-source":
					for path, raw := range s.pending {
						if !strings.HasPrefix(path, "compactions/") {
							continue
						}
						in, err := decodeCompactionIntent([]byte(raw))
						if err != nil {
							t.Fatal(err)
						}
						in.Source.ManifestDigest[0] ^= 1
						changed, err := encodeCompactionIntent(in)
						if err != nil {
							t.Fatal(err)
						}
						replayReplaceFile(t, s.f, path, changed)
						break
					}
				case "replay":
					wantErr = errors.New("R08 semantic replay rejection")
				}
				before := faultTreeSnapshot(t, s.f)
				for pass := 0; pass < 2; pass++ {
					s.f.resetFaults()
					replayed := 0
					j, report, err := OpenJournalWithReplay(s.f, JournalOptions{TailPolicy: policy}, func(tx ScannedTransaction, _ io.ReaderAt) error {
						replayAssertNoMutation(t, s.f.calls)
						if replayed >= len(s.txs) || !reflect.DeepEqual(tx.Transaction, s.txs[replayed]) {
							t.Fatal("failure path replayed unselected history")
						}
						replayed++
						if failure == "replay" && replayed == len(s.txs) {
							return wantErr
						}
						return nil
					})
					if j != nil || !errors.Is(err, wantErr) || errors.Is(err, ErrRecoveryRequired) ||
						!reflect.DeepEqual(report, RecoveryReport{}) || replayed != wantReplay || s.f.locked {
						t.Fatalf("failure ordering: journal=%v replayed=%d want=%d report=%+v err=%v", j, replayed, wantReplay, report, err)
					}
					replayAssertNoMutation(t, s.f.calls)
					r08AssertTree(t, s.f, before)
					s.f = s.f.crash()
					r08AssertTree(t, s.f, before)
				}
			})
		}
	}
}
