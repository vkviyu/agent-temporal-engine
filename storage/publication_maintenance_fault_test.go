package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// These tests start with real journal publications, not manufactured maintenance
// fixtures. The fault is in the publisher; maintenance must run BEFORE recovery
// has an opportunity to stabilize CURRENT, repair a tail, or consume an intent.
type r07Publication struct {
	f                *faultIO
	j                *Journal
	kind             string
	old, next        Publication
	txs              []Transaction
	tx               Transaction
	oldManifest      string
	candidate        string
	checkpoint       string
	checkpointSource string
	intent           string
}

func r07Seed(t *testing.T, kind string) *r07Publication {
	t.Helper()
	s := &r07Publication{kind: kind}
	if kind == "publish" {
		var first Transaction
		s.f, s.j, s.old, first = faultSeedAcknowledged(t)
		s.txs = []Transaction{first}
		s.tx = faultNextTransaction(s.old, 2)
		s.next = faultSuccessorPublication(t, s.old, s.tx)
	} else {
		s.f, s.j, s.txs = compactionSeed(t)
		if kind == "compact-after-checkpoint" {
			p := certifyTestCheckpoint(t, s.j)
			ref := *s.j.manifest.RecoveryCheckpoint
			s.checkpoint = checkpointName(ref.ID)
			s.checkpointSource = manifestName(ref.Source.ManifestID)
			// A real suffix distinguishes checkpoint acceleration from full
			// replay and proves compaction keeps logical addresses unchanged.
			tx := faultNextTransaction(p, 40)
			if _, err := s.j.Publish(context.Background(), tx); err != nil {
				t.Fatal(err)
			}
			s.txs = append(s.txs, tx)
		}
		s.old = faultPublished(t, s.j)
		s.next = s.old
		s.next.Generation++
	}
	s.oldManifest = manifestName(s.j.manifest.ManifestID)
	if kind == "checkpoint" {
		s.checkpointSource = s.oldManifest
	}
	s.f.resetFaults()
	return s
}

func (s *r07Publication) run() (Publication, error) {
	switch s.kind {
	case "publish":
		return s.j.Publish(context.Background(), s.tx)
	case "checkpoint":
		var replayed []Transaction
		return s.j.CreateRecoveryCheckpoint(context.Background(), 1,
			func(tx ScannedTransaction, _ io.ReaderAt) error {
				replayed = append(replayed, tx.Transaction)
				return nil
			}, func(p Publication, w io.Writer) error {
				if p != s.old || !reflect.DeepEqual(replayed, s.txs) {
					return ErrCorruption
				}
				_, err := io.WriteString(w, "certified-test-projection")
				return err
			})
	default:
		report, err := s.j.CompactAuditFull(context.Background())
		return report.After, err
	}
}

func (s *r07Publication) capture(t *testing.T) {
	t.Helper()
	for _, c := range s.f.calls {
		if c.Op != "CreateExclusive" {
			continue
		}
		switch {
		case strings.HasPrefix(c.Name, "manifests/.STAGE."):
			s.candidate = strings.TrimPrefix(c.Name, "manifests/.STAGE.")
		case strings.HasPrefix(c.Name, "checkpoints/CHECKPOINT."):
			s.checkpoint = c.Name
		case strings.HasPrefix(c.Name, "compactions/INTENT."):
			s.intent = c.Name
		}
	}
	if s.candidate == "" {
		t.Fatal("publication never reached installPublication")
	}
}

// Pin the entire installer trace, including create/close, so added or skipped
// mutations cannot silently reduce the matrix. Random IDs differ between runs;
// operation, source role, destination and ordinal must still agree.
func r07InstallTrace(t *testing.T, s *r07Publication) []faultCall {
	t.Helper()
	s.capture(t)
	stage := "manifests/.STAGE." + s.candidate
	current := ".CURRENT." + s.candidate
	want := []faultCall{
		{Op: "CreateExclusive", Name: stage},
		{Op: "Write", Name: stage},
		{Op: "Sync", Name: stage},
		{Op: "Close", Name: stage},
		{Op: "InstallNew", Name: stage, To: manifestName(s.candidate)},
		{Op: "SyncDir", Name: "manifests"},
		{Op: "CreateExclusive", Name: current},
		{Op: "Write", Name: current},
		{Op: "Sync", Name: current},
		{Op: "Close", Name: current},
		{Op: "ReplaceAtomic", Name: current, To: "CURRENT"},
		{Op: "SyncDir", Name: "."},
	}
	start := -1
	for i, c := range s.f.calls {
		if c.Op == "CreateExclusive" && c.Name == stage {
			start = i
			break
		}
	}
	if start < 0 || start+len(want) > len(s.f.calls) {
		t.Fatalf("incomplete installer trace: %+v", s.f.calls)
	}
	got := append([]faultCall(nil), s.f.calls[start:start+len(want)]...)
	for i := range want {
		want[i].Step = got[i].Step
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("installer mutation coverage changed:\ngot  %+v\nwant %+v", got, want)
	}
	return got
}

func TestR07PublicationMaintenanceEveryInstallBoundary(t *testing.T) {
	for _, kind := range []string{"publish", "checkpoint", "compact-after-checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			baseline := r07Seed(t, kind)
			if got, err := baseline.run(); err != nil || got != baseline.next {
				t.Fatalf("baseline publication: %+v %v", got, err)
			}
			trace := r07InstallTrace(t, baseline)
			if err := baseline.j.Close(); err != nil {
				t.Fatal(err)
			}
			for index, call := range trace {
				for _, after := range []bool{false, true} {
					for _, crashFirst := range []bool{false, true} {
						name := fmt.Sprintf("%02d-%s/after=%t/crash-first=%t", index+1, call.Op, after, crashFirst)
						t.Run(name, func(t *testing.T) {
							s := r07Seed(t, kind)
							s.f.failAt(call.Step, after)
							got, err := s.run()
							if got != (Publication{}) || !errors.Is(err, errFaultIO) || !errors.Is(err, ErrCommitUnknown) {
								t.Fatalf("publication fault hidden: %+v %v", got, err)
							}
							s.capture(t)
							actual := s.f.calls[call.Step-1]
							normalize := func(c faultCall, id string) faultCall {
								c.Name = strings.ReplaceAll(c.Name, id, "<candidate>")
								c.To = strings.ReplaceAll(c.To, id, "<candidate>")
								return c
							}
							if normalize(actual, s.candidate) != normalize(call, baseline.candidate) {
								t.Fatalf("injected wrong mutation: %+v instead of %+v", actual, call)
							}
							// Live CURRENT changes at ReplaceAtomic; durable CURRENT
							// changes only at the final root sync in this IO model.
							want := s.old
							if (!crashFirst && (index > 10 || index == 10 && after)) ||
								(crashFirst && index == 11 && after) {
								want = s.next
							}
							r07ReleaseOrCrash(t, s, crashFirst)
							r07AssertAuthority(t, s, want)
							r07MaintainAndRecover(t, s, want)
						})
					}
				}
			}
			t.Logf("covered %d installer calls x 2 fault sides x 2 handoff modes = %d cases", len(trace), len(trace)*4)
		})
	}
}

func r07ReleaseOrCrash(t *testing.T, s *r07Publication, crashFirst bool) {
	t.Helper()
	if status := s.j.Status(); status.State != JournalFenced || status.LastPublication != s.old {
		t.Fatalf("uncertain publication lost last ACK or fencing: %+v", status)
	}
	s.f.resetFaults()
	before := faultTreeSnapshot(t, s.f)
	if _, err := Maintain(context.Background(), s.f, RetentionPolicy{DropPublicationStaging: true}); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatalf("maintenance acquired publisher's live ownership: %v", err)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, s.f)) {
		t.Fatal("busy maintenance changed the interrupted publication")
	}
	if crashFirst {
		// No Close or recovery before this crash: only durable names/bytes
		// survive. In contrast the release branch retains unsynced staging.
		s.f = s.f.crash()
	} else {
		if err := s.j.Close(); err != nil {
			t.Fatal("release failed publisher", err)
		}
		if !reflect.DeepEqual(before, faultTreeSnapshot(t, s.f)) {
			t.Fatal("publisher Close changed publication bytes")
		}
	}
}

func r07AssertAuthority(t *testing.T, s *r07Publication, want Publication) Manifest {
	t.Helper()
	c, err := DecodeCurrent([]byte(faultReadAll(t, s.f, "CURRENT")))
	if err != nil {
		t.Fatal(err)
	}
	name := s.oldManifest
	if want == s.next {
		name = manifestName(s.candidate)
	}
	raw := []byte(faultReadAll(t, s.f, name))
	m, err := DecodeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	identity := []byte(faultReadAll(t, s.f, "IDENTITY"))
	if manifestName(c.ManifestID) != name || c.Store != want.Store || c.Generation != want.Generation ||
		c.ManifestDigest != Digest(sha256.Sum256(raw)) || c.IdentityDigest != Digest(sha256.Sum256(identity)) ||
		m.Store != want.Store || m.Generation != want.Generation || m.Position != want.Position ||
		m.PostRootSet != want.PostRootSet || m.PublishedEnd != want.PublishedEnd {
		t.Fatalf("mixed publication: CURRENT=%+v manifest=%+v want=%+v", c, m, want)
	}
	return m
}

func r07SortedSet(set map[string]bool) []string {
	var out []string
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// An operation-specific oracle: the only possible source edges are the source
// captured before checkpoint creation and, for compaction, its actual intent
// and layout source. It does not call the maintenance classifier or invent
// certificates. All remaining files were emitted by the real journal.
func r07ExpectedInventory(t *testing.T, s *r07Publication, tree map[string]string, want Publication) MaintenanceReport {
	t.Helper()
	m := r07AssertAuthority(t, s, want)
	chosen := map[string]bool{"CURRENT": true, "IDENTITY": true, manifestName(m.ManifestID): true}
	proofs, unknown := map[string]bool{}, map[string]bool{}
	if m.RecoveryCheckpoint != nil {
		chosen[checkpointName(m.RecoveryCheckpoint.ID)] = true
	}
	completeCandidate := false
	for _, name := range []string{"manifests/.STAGE." + s.candidate, manifestName(s.candidate)} {
		if raw, ok := tree[name]; ok {
			candidate, err := DecodeManifest([]byte(raw))
			if err != nil {
				unknown[name] = true
			} else {
				if candidate.ManifestID != s.candidate || candidate.Generation != s.next.Generation ||
					candidate.Position != s.next.Position || candidate.PostRootSet != s.next.PostRootSet ||
					candidate.PublishedEnd != s.next.PublishedEnd {
					t.Fatal("complete staging contains a mixed candidate")
				}
				completeCandidate = true
			}
		}
	}
	if raw, ok := tree[".CURRENT."+s.candidate]; ok {
		if _, err := DecodeCurrent([]byte(raw)); err != nil {
			unknown[".CURRENT."+s.candidate] = true
		}
	}
	switch s.kind {
	case "checkpoint":
		if completeCandidate {
			proofs[s.checkpointSource] = true
			proofs[s.checkpoint] = true
		}
	case "compact-after-checkpoint":
		proofs[s.checkpointSource] = true
		if _, pending := tree[s.intent]; pending {
			proofs[s.intent] = true
			proofs[s.oldManifest] = true
		}
		if completeCandidate {
			proofs[s.oldManifest] = true
		}
	}
	for name := range chosen {
		delete(proofs, name)
	}
	report := MaintenanceReport{SelectedMetadata: r07SortedSet(chosen), RetainedProofMetadata: r07SortedSet(proofs)}
	for name := range tree {
		if strings.HasSuffix(name, "/") {
			continue
		}
		if strings.HasPrefix(name, "segments/SEGMENT.") {
			report.PublishedSegments++
		}
		if strings.HasPrefix(name, "manifests/MANIFEST.") {
			report.PublishedManifests++
		}
		if strings.HasPrefix(name, "quarantine/") {
			report.QuarantineBefore = append(report.QuarantineBefore, strings.TrimPrefix(name, "quarantine/"))
		}
		if chosen[name] || proofs[name] {
			continue
		}
		if strings.HasPrefix(name, "checkpoints/") {
			// Removing a valid but unselected checkpoint publication can
			// leave a loose stream on the NEXT pass. It must block, not vanish.
			unknown[name] = true
		}
		if !unknown[name] && (strings.HasPrefix(name, "manifests/") || strings.HasPrefix(name, ".CURRENT.")) {
			report.StagedMetadata = append(report.StagedMetadata, name)
		}
	}
	report.UnknownMetadata = r07SortedSet(unknown)
	report.ReclamationBlockers = r07SortedSet(unknown)
	report.PublicationReclamationBlocked = len(unknown) != 0
	sort.Strings(report.StagedMetadata)
	sort.Strings(report.QuarantineBefore)
	return report
}

func r07MaintenancePasses(t *testing.T, s *r07Publication, want Publication, passes int) {
	t.Helper()
	for pass := 0; pass < passes; pass++ {
		before := faultTreeSnapshot(t, s.f)
		expected := r07ExpectedInventory(t, s, before, want)
		s.f.resetFaults()
		inspect, err := Maintain(context.Background(), s.f, RetentionPolicy{})
		if err != nil || !reflect.DeepEqual(inspect, expected) {
			t.Fatalf("inspection pass %d:\ngot  %+v\nwant %+v\nerr %v", pass, inspect, expected, err)
		}
		for _, c := range s.f.calls {
			switch c.Op {
			case "AcquireOwnerLock", "OwnerLock.Close", "Close":
			default:
				t.Fatalf("inspection mutated/durabilized storage: %+v", c)
			}
		}
		if !reflect.DeepEqual(before, faultTreeSnapshot(t, s.f)) {
			t.Fatal("inspection changed files")
		}
		s.f.resetFaults()
		if !expected.PublicationReclamationBlocked {
			expected.RemovedStagedMetadata = expected.StagedMetadata
		}
		report, err := Maintain(context.Background(), s.f, RetentionPolicy{DropPublicationStaging: true})
		if err != nil || !reflect.DeepEqual(report, expected) {
			t.Fatalf("opt-in pass %d:\ngot  %+v\nwant %+v\nerr %v", pass, report, expected, err)
		}
		for _, c := range s.f.calls {
			switch c.Op {
			case "AcquireOwnerLock", "OwnerLock.Close", "Close", "Remove", "SyncDir":
			default:
				t.Fatalf("maintenance rewrote publication or proof bytes: %+v", c)
			}
		}
		for _, name := range expected.RemovedStagedMetadata {
			delete(before, name)
		}
		// Covers every selected/retained proof, unknown partial file, checkpoint,
		// active/retired/candidate segment and quarantine byte, not just CURRENT.
		if !reflect.DeepEqual(before, faultTreeSnapshot(t, s.f)) {
			t.Fatal("maintenance changed anything other than reported staged removals")
		}
		r07AssertAuthority(t, s, want)
	}
}

func r07Recover(t *testing.T, s *r07Publication, want Publication) {
	t.Helper()
	m := r07AssertAuthority(t, s, want)
	wantTxs := append([]Transaction(nil), s.txs...)
	if s.kind == "publish" && want == s.next {
		wantTxs = append(wantTxs, s.tx)
	}
	var full []Transaction
	var suffix []uint64
	var loads int
	open := func(policy TailPolicy) (*Journal, RecoveryReport, error) {
		full, suffix, loads = nil, nil, 0
		return OpenJournalWithCheckpoint(s.f, JournalOptions{TailPolicy: policy},
			func(tx ScannedTransaction, _ io.ReaderAt) error {
				full = append(full, tx.Transaction)
				return nil
			}, func(ref RecoveryCheckpoint, r io.Reader, source io.ReaderAt) (ReplayFunc, error) {
				loads++
				if m.RecoveryCheckpoint == nil || ref != *m.RecoveryCheckpoint {
					t.Fatal("recovery loaded an unselected checkpoint")
				}
				return testCheckpointLoader(t, &suffix)(ref, r, source)
			})
	}
	before := faultTreeSnapshot(t, s.f)
	j, report, err := open(TailReject)
	if errors.Is(err, ErrRecoveryRequired) {
		if s.kind != "publish" || want != s.old || j != nil || !report.Verified ||
			report.PublicationSynced || report.Publication != want || report.TailBytes == 0 {
			t.Fatalf("unexpected recovery-required outcome: %+v %v", report, err)
		}
		if !reflect.DeepEqual(before, faultTreeSnapshot(t, s.f)) {
			t.Fatal("default recovery repaired tail before explicit opt-in")
		}
		j, report, err = open(TailQuarantine)
		encoded, _, encodeErr := EncodeTransaction(s.tx, Limits{})
		if encodeErr != nil || err != nil || report.QuarantineFile == "" {
			t.Fatalf("explicit tail recovery: %+v %v %v", report, err, encodeErr)
		}
		if faultReadAll(t, s.f, report.QuarantineFile) != string(encoded) {
			t.Fatal("quarantine lost the exact unpublished transaction")
		}
	}
	if err != nil || j == nil {
		t.Fatalf("maintenance stranded journal recovery: %+v %v", report, err)
	}
	if !report.Verified || !report.PublicationSynced || report.Publication != want ||
		faultPublished(t, j) != want || report.ScannedTransactions != uint64(len(wantTxs)) {
		t.Fatalf("old/new publication or full physical history lost: %+v want=%+v", report, want)
	}
	var checkpointTransactions uint64
	if m.RecoveryCheckpoint != nil {
		checkpointTransactions = m.RecoveryCheckpoint.Source.Position.LSN
	}
	if report.CheckpointTransactions != checkpointTransactions {
		t.Fatal("checkpoint silently fell back after maintenance", report)
	}
	if m.RecoveryCheckpoint == nil {
		if loads != 0 || !reflect.DeepEqual(full, wantTxs) || len(suffix) != 0 {
			t.Fatal("unselected checkpoint affected replay", loads, full, suffix)
		}
	} else {
		var wantSuffix []uint64
		for _, tx := range wantTxs {
			if tx.LSN > checkpointTransactions {
				wantSuffix = append(wantSuffix, tx.LSN)
			}
		}
		if loads != 1 || len(full) != 0 || !reflect.DeepEqual(suffix, wantSuffix) {
			t.Fatal("checkpoint/suffix replay changed", loads, full, suffix, wantSuffix)
		}
	}
	rotationAssertLogicalTransactions(t, j, wantTxs)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	r07AssertAuthority(t, s, want)
}

func r07MaintainAndRecover(t *testing.T, s *r07Publication, want Publication) {
	t.Helper()
	r07MaintenancePasses(t, s, want, 3)
	s.f = s.f.crash()
	// A blocked pass syncs names, NOT unsynced partial file bytes. After power
	// loss such files may be empty; they must remain unknown blockers.
	r07MaintenancePasses(t, s, want, 2)
	r07Recover(t, s, want)
	r07MaintenancePasses(t, s, want, 2)
	s.f = s.f.crash()
	r07Recover(t, s, want)
}

func TestR07PublicationMaintenancePartialMetadataWrites(t *testing.T) {
	for _, manifest := range []bool{true, false} {
		for _, limit := range []int{0, 7} {
			for _, crashFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("manifest=%t/limit=%d/crash-first=%t", manifest, limit, crashFirst), func(t *testing.T) {
					s := r07Seed(t, "publish")
					prefix := ".CURRENT."
					if manifest {
						prefix = "manifests/.STAGE."
					}
					var hit string
					s.f.onBoundary = func(b faultBoundary) error {
						if b.Op == "Write" && strings.HasPrefix(b.Name, prefix) {
							hit = b.Name
							if !b.After {
								// The hook holds the model mutex. Limit only this
								// metadata write, never the already-synced segment.
								s.f.writeLimit = limit
							} else if limit > 0 {
								return errFaultIO // positive n AND error
							}
						}
						return nil
					}
					got, err := s.run()
					cause := io.ErrNoProgress
					if limit > 0 {
						cause = errFaultIO
					}
					if hit == "" || got != (Publication{}) || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, cause) {
						t.Fatalf("short/zero metadata write missed: hit=%q got=%+v err=%v", hit, got, err)
					}
					s.capture(t)
					if len(faultReadAll(t, s.f, hit)) != limit {
						t.Fatal("fault did not leave the expected partial metadata")
					}
					r07ReleaseOrCrash(t, s, crashFirst)
					tree := faultTreeSnapshot(t, s.f)
					expected := r07ExpectedInventory(t, s, tree, s.old)
					if expected.PublicationReclamationBlocked != !crashFirst {
						t.Fatal("partial metadata visibility did not match handoff mode", expected)
					}
					if !crashFirst && !reflect.DeepEqual(expected.UnknownMetadata, []string{hit}) {
						t.Fatal("partial metadata was not the exact unknown blocker", expected)
					}
					r07MaintainAndRecover(t, s, s.old)
				})
			}
		}
	}
}

func TestR07PublicationMaintenanceCheckpointCompactionReopen(t *testing.T) {
	s := r07Seed(t, "compact-after-checkpoint")
	ack, err := s.run()
	if err != nil || ack != s.next {
		t.Fatalf("real checkpoint/suffix/compaction failed: %+v %v", ack, err)
	}
	r07InstallTrace(t, s)
	m := r07AssertAuthority(t, s, ack)
	if m.LayoutSource == nil || manifestName(m.LayoutSource.ManifestID) != s.oldManifest ||
		m.RecoveryCheckpoint == nil || checkpointName(m.RecoveryCheckpoint.ID) != s.checkpoint ||
		len(m.RetiredSegments) < 2 {
		t.Fatal("fixture did not produce checkpoint and multi-segment retirement proofs")
	}
	if err := s.j.Close(); err != nil {
		t.Fatal(err)
	}
	report := r07ExpectedInventory(t, s, faultTreeSnapshot(t, s.f), ack)
	wantProofs := r07SortedSet(map[string]bool{s.oldManifest: true, s.checkpointSource: true})
	if report.PublicationReclamationBlocked || !reflect.DeepEqual(report.RetainedProofMetadata, wantProofs) ||
		len(report.StagedMetadata) == 0 {
		t.Fatalf("real compaction did not exercise reclamation plus retained proofs: %+v", report)
	}
	r07MaintainAndRecover(t, s, ack)
}
