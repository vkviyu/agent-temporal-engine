package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

type r08PublicationFixture struct {
	f                      *faultIO
	oldCurrent, newCurrent string
	oldPublication         Publication
	publication            Publication
	txs                    []Transaction
	active, intent, orphan string
	retired                []string
}

func r08DurableCurrent(f *faultIO) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.nodes[f.nodes[1].durable["CURRENT"]].synced)
}

// Keep the process alive after a real compaction replaces CURRENT but fails
// BEFORE its root sync. Crashing or syncing the root here would hide R08.
func r08UnsyncedPublication(t *testing.T, tail, orphan bool) *r08PublicationFixture {
	t.Helper()
	f, j, txs := compactionSeed(t)
	s := &r08PublicationFixture{
		f: f, txs: txs, oldPublication: faultPublished(t, j),
		oldCurrent: faultReadAll(t, f, "CURRENT"),
	}
	replaced, injected, manifestsSynced := false, false, false
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "SyncDir" && b.Name == "manifests" && b.After {
			manifestsSynced = true
		}
		if b.Op == "ReplaceAtomic" && b.To == "CURRENT" && b.After {
			if !manifestsSynced {
				t.Fatal("CURRENT replaced before manifest directory became durable")
			}
			replaced = true
		}
		if replaced && b.Op == "SyncDir" && b.Name == "." && !b.After {
			injected = true
			return errFaultIO
		}
		return nil
	}
	if _, err := j.CompactAuditFull(context.Background()); !injected ||
		!errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) ||
		j.Status().State != JournalFenced {
		t.Fatalf("missed pre-root-sync compaction failure: injected=%t err=%v", injected, err)
	}
	s.newCurrent = faultReadAll(t, f, "CURRENT")
	m := rotationManifest(t, f)
	s.publication = s.oldPublication
	s.publication.Generation = m.Generation
	s.active = segmentName(m.SegmentID)
	if s.newCurrent == s.oldCurrent || m.LayoutSource == nil || len(m.RetiredSegments) < 2 {
		t.Fatal("fixture lacks a newly selected compacted layout and retired sources")
	}
	for _, desc := range m.RetiredSegments {
		s.retired = append(s.retired, segmentName(desc.SegmentID))
	}
	names, err := f.List("compactions")
	if err != nil || len(names) != 1 {
		t.Fatalf("real compaction did not retain its intent: %v %v", names, err)
	}
	s.intent = "compactions/" + names[0]
	f.resetFaults()
	if tail {
		raw, _, err := EncodeTransaction(faultNextTransaction(s.publication, 99), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		replayAppendTail(t, f, s.active, raw)
	}
	if orphan {
		s.orphan = segmentName(strings.Repeat("e", 32))
		if err := writeSyncedFile(f, s.orphan, []byte("ordinary durable orphan")); err != nil {
			t.Fatal(err)
		}
		if err := f.SyncDir("segments"); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.calls {
		if call.Op == "Remove" || call.Op == "SyncDir" && call.Name == "." {
			t.Fatalf("fixture accidentally repaired the publication window: %+v", call)
		}
	}
	if f.locked || faultReadAll(t, f, "CURRENT") != s.newCurrent || r08DurableCurrent(f) != s.oldCurrent {
		t.Fatal("closing fenced journal changed live/durable CURRENT or retained ownership")
	}
	f.resetFaults()
	return s
}

func (s *r08PublicationFixture) reject(t *testing.T, publication Publication) RecoveryReport {
	t.Helper()
	var replayed []Transaction
	j, report, err := OpenJournalWithReplay(s.f, JournalOptions{TailPolicy: TailReject}, func(tx ScannedTransaction, _ io.ReaderAt) error {
		replayAssertNoMutation(t, s.f.calls)
		replayed = append(replayed, tx.Transaction)
		return nil
	})
	if j != nil || !errors.Is(err, ErrRecoveryRequired) || errors.Is(err, ErrRecoveryIncomplete) ||
		!report.Verified || report.PublicationSynced || report.Publication != publication ||
		report.TailDisposition != "preserved" || s.f.locked || !reflect.DeepEqual(replayed, s.txs) {
		t.Fatalf("TailReject must preserve the selected history across crash: report=%+v err=%v", report, err)
	}
	return report
}

func TestR08TailRejectUnsyncedPublicationSurvivesCrash(t *testing.T) {
	for _, mode := range []string{"tail", "orphan", "tail-and-orphan"} {
		t.Run(mode, func(t *testing.T) {
			s := r08UnsyncedPublication(t, mode != "orphan", mode != "tail")
			before := faultTreeSnapshot(t, s.f)
			report := s.reject(t, s.publication)
			if !reflect.DeepEqual(report.ReclaimedSegments, s.retired) {
				t.Fatalf("TailReject skipped certified retirement: %+v", report)
			}
			delete(before, s.intent)
			for _, name := range s.retired {
				delete(before, name)
			}
			r08AssertTree(t, s.f, before)
			calls := append([]faultCall(nil), s.f.calls...)
			s.f = s.f.crash()
			// Before the fix this follows old CURRENT into the now durably
			// deleted source segments and fails with ErrCorruption.
			s.reject(t, s.publication)
			r08AssertTree(t, s.f, before)
			rootSyncs := 0
			for _, call := range calls {
				if call.Op == "SyncDir" && call.Name == "." {
					rootSyncs++
				}
				if call.Op == "Remove" && rootSyncs != 1 {
					t.Fatalf("destructive cleanup preceded publication stabilization: %+v", call)
				}
				if call.Op == "SyncDir" && call.Name == "manifests" {
					t.Fatal("already durable manifest directory was unnecessarily synced")
				}
			}
			if rootSyncs != 1 {
				t.Fatalf("reject must stabilize once without a final Ready sync: %d", rootSyncs)
			}
			s.f = s.f.crash()
			s.reject(t, s.publication)
			r08AssertTree(t, s.f, before)
		})
	}
}

func TestR08TailRejectUnsyncedPublicationOverRetiredLayoutSurvivesCrash(t *testing.T) {
	for _, publication := range []string{"checkpoint", "publish"} {
		for _, mode := range []string{"tail", "orphan", "tail-and-orphan"} {
			t.Run(publication+"/"+mode, func(t *testing.T) {
				f, j, txs := compactionSeed(t)
				pin, err := j.PinReader()
				if err != nil {
					t.Fatal(err)
				}
				compacted, err := j.CompactAuditFull(context.Background())
				if err != nil || compacted.PinnedSegments < 2 || len(compacted.ReclaimedSegments) != 0 {
					t.Fatalf("compaction did not complete with pinned retirement: %+v %v", compacted, err)
				}
				source := rotationManifest(t, f)
				s := &r08PublicationFixture{
					f: f, txs: txs, oldPublication: faultPublished(t, j),
					oldCurrent: faultReadAll(t, f, "CURRENT"),
				}
				if source.LayoutSource == nil || len(source.RetiredSegments) != compacted.PinnedSegments ||
					r08DurableCurrent(f) != s.oldCurrent {
					t.Fatal("completed compaction lacks durable selection and certified retirement")
				}
				// No intent remains: recovery must stabilize CURRENT solely
				// because this later publication inherits retired segments.
				if names, err := f.List("compactions"); err != nil || len(names) != 0 {
					t.Fatalf("completed compaction left intents: %v %v", names, err)
				}
				retiredBytes := map[string]string{}
				for _, desc := range source.RetiredSegments {
					name := segmentName(desc.SegmentID)
					s.retired = append(s.retired, name)
					retiredBytes[name] = faultReadAll(t, f, name)
					if retiredBytes[name] == "" {
						t.Fatal("pinned retired segment is empty", name)
					}
				}
				f.resetFaults()
				replaced, injected, manifestsSynced := false, false, false
				f.onBoundary = func(b faultBoundary) error {
					if b.Op == "SyncDir" && b.Name == "manifests" && b.After {
						manifestsSynced = true
					}
					if b.Op == "ReplaceAtomic" && b.To == "CURRENT" && b.After {
						if !manifestsSynced {
							t.Fatal("CURRENT replaced before manifest directory sync")
						}
						replaced = true
					}
					// Checkpoint staging also syncs the root before replacement;
					// only fail the publication witness after ReplaceAtomic.
					if replaced && b.Op == "SyncDir" && b.Name == "." && !b.After {
						injected = true
						return errFaultIO
					}
					return nil
				}
				if publication == "checkpoint" {
					var certified []Transaction
					_, err = j.CreateRecoveryCheckpoint(context.Background(), 1,
						func(tx ScannedTransaction, _ io.ReaderAt) error {
							certified = append(certified, tx.Transaction)
							return nil
						}, func(p Publication, w io.Writer) error {
							if p != s.oldPublication || !reflect.DeepEqual(certified, txs) {
								t.Fatal("checkpoint did not certify the complete compacted history")
							}
							return json.NewEncoder(w).Encode(certified)
						})
				} else {
					next := faultNextTransaction(s.oldPublication, 40)
					_, err = j.Publish(context.Background(), next)
					s.txs = append(s.txs, next)
				}
				if !injected || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) ||
					j.Status().State != JournalFenced {
					t.Fatalf("missed pre-root-sync %s failure: injected=%t err=%v", publication, injected, err)
				}
				s.newCurrent = faultReadAll(t, f, "CURRENT")
				m := rotationManifest(t, f)
				s.publication = publicationFor(j.identity, m)
				s.active = segmentName(m.SegmentID)
				if s.newCurrent == s.oldCurrent || m.Generation != source.Generation+1 ||
					!reflect.DeepEqual(m.LayoutSource, source.LayoutSource) ||
					!reflect.DeepEqual(m.RetiredSegments, source.RetiredSegments) {
					t.Fatal("later publication did not inherit the certified retirement")
				}
				if publication == "checkpoint" {
					want := s.oldPublication
					want.Generation++
					ref := m.RecoveryCheckpoint
					if s.publication != want || ref == nil || ref.Schema != 1 ||
						ref.Source.ManifestID != source.ManifestID || ref.Source.Generation != source.Generation ||
						ref.Source.Position != source.Position || ref.Source.PostRootSet != source.PostRootSet ||
						ref.Source.PublishedEnd != source.PublishedEnd ||
						!reflect.DeepEqual(m.Segments, source.Segments) || m.SegmentID != source.SegmentID {
						t.Fatal("checkpoint changed history/layout or bound the wrong source")
					}
				} else if m.RecoveryCheckpoint != nil || m.Position.LSN != s.oldPublication.Position.LSN+1 {
					t.Fatal("ordinary publication did not select exactly one new transaction")
				}
				f.resetFaults()
				var tailBytes uint64
				if mode != "orphan" {
					raw, _, err := EncodeTransaction(faultNextTransaction(s.publication, 99), Limits{})
					if err != nil {
						t.Fatal(err)
					}
					replayAppendTail(t, f, s.active, raw)
					tailBytes = uint64(len(raw))
				}
				var orphans []string
				if mode != "tail" {
					s.orphan = segmentName(strings.Repeat("e", 32))
					raw := []byte("ordinary durable orphan")
					if err := writeSyncedFile(f, s.orphan, raw); err != nil {
						t.Fatal(err)
					}
					if err := f.SyncDir("segments"); err != nil {
						t.Fatal(err)
					}
					orphans = []string{s.orphan}
					tailBytes += uint64(len(raw))
				}
				// Enter Closing while the pin still protects retirement. Release
				// it only to finish Close, never through ReclaimRetired or reopen.
				if err := j.Close(); !errors.Is(err, ErrReadersPinned) || !f.locked ||
					j.Status().State != JournalClosing {
					t.Fatalf("close did not retain ownership behind the pin: %v", err)
				}
				if err := pin.Close(); err != nil {
					t.Fatal(err)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
				for _, call := range f.calls {
					if call.Op == "Remove" || call.Op == "SyncDir" && call.Name == "." {
						t.Fatalf("fixture repaired the publication window: %+v", call)
					}
				}
				if f.locked || faultReadAll(t, f, "CURRENT") != s.newCurrent || r08DurableCurrent(f) != s.oldCurrent {
					t.Fatal("close changed the live/durable selection or retained ownership")
				}
				for name, raw := range retiredBytes {
					if faultReadAll(t, f, name) != raw {
						t.Fatal("close reclaimed or changed retired bytes", name)
					}
				}
				wantTree := faultTreeSnapshot(t, f)
				for _, name := range s.retired {
					delete(wantTree, name)
				}
				wantReport := RecoveryReport{
					Verified: true, Publication: s.publication, PublishedBytes: s.publication.PublishedEnd,
					ScannedTransactions: uint64(len(s.txs)), TailBytes: tailBytes,
					TailDisposition: "preserved", OrphanSegments: orphans, ReclaimedSegments: s.retired,
				}
				if publication == "checkpoint" {
					wantReport.CheckpointTransactions = uint64(len(txs))
				}
				for pass := 0; pass < 2; pass++ {
					s.f.resetFaults()
					var restored []Transaction
					loads := 0
					opened, report, err := OpenJournalWithCheckpoint(s.f, JournalOptions{TailPolicy: TailReject},
						func(tx ScannedTransaction, _ io.ReaderAt) error {
							replayAssertNoMutation(t, s.f.calls)
							restored = append(restored, tx.Transaction)
							return nil
						}, func(ref RecoveryCheckpoint, r io.Reader, _ io.ReaderAt) (ReplayFunc, error) {
							replayAssertNoMutation(t, s.f.calls)
							loads++
							if m.RecoveryCheckpoint == nil || ref != *m.RecoveryCheckpoint {
								t.Fatal("recovery loaded an unselected checkpoint")
							}
							raw, err := io.ReadAll(r)
							if err != nil {
								return nil, err
							}
							if err := json.Unmarshal(raw, &restored); err != nil {
								return nil, err
							}
							return func(ScannedTransaction, io.ReaderAt) error {
								t.Fatal("recovery promoted the unpublished tail past the checkpoint")
								return nil
							}, nil
						})
					wantLoads := 0
					if publication == "checkpoint" {
						wantLoads = 1
					}
					if opened != nil || !errors.Is(err, ErrRecoveryRequired) || errors.Is(err, ErrRecoveryIncomplete) ||
						s.f.locked || !reflect.DeepEqual(report, wantReport) ||
						loads != wantLoads || !reflect.DeepEqual(restored, s.txs) {
						t.Fatalf("pass %d lost selected publication/checkpoint: report=%+v loads=%d err=%v", pass, report, loads, err)
					}
					r08AssertRemovals(t, s.f.calls, s.retired, false)
					r08AssertTree(t, s.f, wantTree)
					if r08DurableCurrent(s.f) != s.newCurrent {
						t.Fatal("retirement cleanup did not stabilize the later CURRENT")
					}
					s.f = s.f.crash()
					r08AssertTree(t, s.f, wantTree)
				}
				// Full replay must agree with the checkpoint projection too.
				s.reject(t, s.publication)
				r08AssertTree(t, s.f, wantTree)
			})
		}
	}
}

func TestR08TailRejectPublicationStabilizationFaults(t *testing.T) {
	for _, after := range []bool{false, true} {
		for _, crashFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("after=%t/crash-first=%t", after, crashFirst), func(t *testing.T) {
				s := r08UnsyncedPublication(t, true, true)
				before := faultTreeSnapshot(t, s.f)
				injected := 0
				s.f.onBoundary = func(b faultBoundary) error {
					if b.Op == "SyncDir" && b.Name == "." && b.After == after {
						injected++
						return errFaultIO
					}
					return nil
				}
				j, report, err := OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
				if j != nil || injected != 1 || !errors.Is(err, ErrRecoveryIncomplete) ||
					!errors.Is(err, errFaultIO) || errors.Is(err, ErrRecoveryRequired) ||
					!report.Verified || report.PublicationSynced || report.Publication != s.publication ||
					len(report.ReclaimedSegments) != 0 || len(report.DiscardedCompactionSegments) != 0 ||
					len(report.QuarantineFiles) != 0 || s.f.locked {
					t.Fatalf("stabilization fault lost cause or allowed cleanup: report=%+v err=%v", report, err)
				}
				for _, call := range s.f.calls {
					switch call.Op {
					case "AcquireOwnerLock", "OpenAppend", "Close", "OwnerLock.Close":
					case "SyncDir":
						if call.Name != "." {
							t.Fatalf("cleanup sync after stabilization error: %+v", call)
						}
					default:
						t.Fatalf("cleanup IO after stabilization error: %+v", call)
					}
				}
				r08AssertTree(t, s.f, before)
				wantCurrent := s.oldCurrent
				if after {
					wantCurrent = s.newCurrent
				}
				if r08DurableCurrent(s.f) != wantCurrent {
					t.Fatal("fault boundary did not preserve the expected durable CURRENT")
				}
				wantPublication := s.publication
				if crashFirst {
					s.f = s.f.crash()
					if !after {
						// The old publication remains valid because no source
						// removal was attempted. The ordinary orphan still rejects.
						wantPublication = s.oldPublication
					}
				}
				s.f.resetFaults()
				s.reject(t, wantPublication)
				stable := faultTreeSnapshot(t, s.f)
				s.f = s.f.crash()
				s.reject(t, wantPublication)
				r08AssertTree(t, s.f, stable)
			})
		}
	}
}

func TestR08PublicationStabilizationWaitsForOrphanValidation(t *testing.T) {
	for _, policy := range []TailPolicy{TailReject, TailQuarantine} {
		t.Run(string(policy), func(t *testing.T) {
			s := r08UnsyncedPublication(t, true, true)
			if err := writeSyncedFile(s.f, "segments/UNKNOWN", []byte("unmanaged evidence")); err != nil {
				t.Fatal(err)
			}
			before := faultTreeSnapshot(t, s.f)
			s.f.resetFaults()
			replayed := 0
			j, report, err := OpenJournalWithReplay(s.f, JournalOptions{TailPolicy: policy}, func(ScannedTransaction, io.ReaderAt) error {
				replayed++
				return nil
			})
			if j != nil || !errors.Is(err, ErrRecoveryRequired) || !report.Verified ||
				report.PublicationSynced || replayed != len(s.txs) || s.f.locked {
				t.Fatalf("orphan validation did not precede stabilization: %+v %v", report, err)
			}
			replayAssertNoMutation(t, s.f.calls)
			r08AssertTree(t, s.f, before)
			if r08DurableCurrent(s.f) != s.oldCurrent {
				t.Fatal("orphan validation failure stabilized CURRENT")
			}
		})
	}
}

func TestR08PublicationStabilizationDoesNotGrantReady(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after=%t", after), func(t *testing.T) {
			s := r08UnsyncedPublication(t, false, false)
			rootSyncs, injected := 0, 0
			s.f.onBoundary = func(b faultBoundary) error {
				if b.Op == "SyncDir" && b.Name == "." {
					if !b.After {
						rootSyncs++
					}
					// The first root sync stabilizes cleanup; only the
					// existing final root sync may grant Ready.
					if rootSyncs == 2 && b.After == after {
						injected++
						return errFaultIO
					}
				}
				return nil
			}
			j, report, err := OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
			if j != nil || injected != 1 || !errors.Is(err, ErrRecoveryIncomplete) ||
				!errors.Is(err, errFaultIO) || !report.Verified || report.PublicationSynced ||
				report.Publication != s.publication || !reflect.DeepEqual(report.ReclaimedSegments, s.retired) ||
				s.f.locked || r08DurableCurrent(s.f) != s.newCurrent {
				t.Fatalf("early stabilization granted Ready or lost durable selection: %+v %v", report, err)
			}
			s.f = s.f.crash()
			j, report, err = OpenJournal(s.f, JournalOptions{TailPolicy: TailReject})
			if err != nil || j == nil || !report.Verified || !report.PublicationSynced ||
				report.Publication != s.publication {
				t.Fatalf("final witness retry failed: %+v %v", report, err)
			}
			rotationAssertLogicalTransactions(t, j, s.txs)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
