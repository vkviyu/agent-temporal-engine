package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func maintenanceWrite(t testing.TB, f *faultIO, name string, b []byte) {
	t.Helper()
	if err := writeSyncedFile(f, name, b); err != nil {
		t.Fatal(err)
	}
	dir := "."
	if at := strings.LastIndexByte(name, '/'); at >= 0 {
		dir = name[:at]
	}
	if err := f.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
}

func maintenanceManifestBytes(t testing.TB, m Manifest) []byte {
	t.Helper()
	b, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func maintenanceCurrentBytes(t testing.TB, f *faultIO, m Manifest) []byte {
	t.Helper()
	id := []byte(faultReadAll(t, f, "IDENTITY"))
	b, err := EncodeCurrent(Current{Store: m.Store, ManifestID: m.ManifestID,
		Generation: m.Generation, IdentityDigest: Digest(sha256.Sum256(id)),
		ManifestDigest: Digest(sha256.Sum256(maintenanceManifestBytes(t, m)))})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func maintenanceSourceFor(t testing.TB, m Manifest) LayoutSource {
	t.Helper()
	return LayoutSource{ManifestID: m.ManifestID, ManifestDigest: Digest(sha256.Sum256(maintenanceManifestBytes(t, m))),
		Generation: m.Generation, PublishedEnd: m.PublishedEnd, Position: m.Position, PostRootSet: m.PostRootSet}
}

func maintenanceSorted(names ...string) []string {
	sort.Strings(names)
	return names
}

func maintenanceFixture(t testing.TB) (*faultIO, Manifest, []string) {
	t.Helper()
	f := newFaultIO()
	j, err := CreateJournal(f, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	old := j.manifest
	if _, err := j.Publish(context.Background(), faultNextTransaction(faultPublished(t, j), 1)); err != nil {
		t.Fatal(err)
	}
	selected := j.manifest
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	candidate := selected
	candidate.ManifestID = strings.Repeat("a", 32)
	candidate.Generation++
	stage := "manifests/.STAGE." + candidate.ManifestID
	current := ".CURRENT." + candidate.ManifestID
	maintenanceWrite(t, f, stage, maintenanceManifestBytes(t, candidate))
	maintenanceWrite(t, f, manifestName(candidate.ManifestID), maintenanceManifestBytes(t, candidate))
	maintenanceWrite(t, f, current, maintenanceCurrentBytes(t, f, candidate))
	f.resetFaults()
	return f, selected, maintenanceSorted(manifestName(old.ManifestID), stage, manifestName(candidate.ManifestID), current)
}

func maintenanceAssertNoMutation(t testing.TB, calls []faultCall) {
	t.Helper()
	for _, c := range calls {
		switch c.Op {
		case "AcquireOwnerLock", "Close", "OwnerLock.Close":
		default:
			t.Fatalf("inspection mutated storage: %+v", c)
		}
	}
}

func TestPublicationMaintenanceInventoryAndOptIn(t *testing.T) {
	f, selected, candidates := maintenanceFixture(t)
	before := faultTreeSnapshot(t, f)
	report, err := Maintain(context.Background(), f, RetentionPolicy{})
	want := MaintenanceReport{PublishedSegments: 1, PublishedManifests: 3,
		SelectedMetadata: maintenanceSorted("CURRENT", "IDENTITY", manifestName(selected.ManifestID)),
		StagedMetadata:   candidates}
	if err != nil || !reflect.DeepEqual(report, want) {
		t.Fatalf("inspection: got=%+v want=%+v err=%v", report, want, err)
	}
	maintenanceAssertNoMutation(t, f.calls)
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("inspection changed bytes")
	}
	report, err = Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	want.RemovedStagedMetadata = candidates
	if err != nil || !reflect.DeepEqual(report, want) {
		t.Fatalf("cleanup: got=%+v want=%+v err=%v", report, want, err)
	}
	after := faultTreeSnapshot(t, f)
	for _, name := range candidates {
		if _, ok := after[name]; ok {
			t.Fatalf("candidate retained: %s", name)
		}
		delete(before, name)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("cleanup changed authority or segment bytes")
	}
	again, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	if err != nil || len(again.StagedMetadata) != 0 || len(again.RemovedStagedMetadata) != 0 {
		t.Fatal("not idempotent", again, err)
	}
	opened, _, err := OpenJournal(f.crash(), JournalOptions{})
	if err != nil {
		t.Fatal("cleanup damaged recovery", err)
	}
	defer opened.Close()
	if faultPublished(t, opened).Position != selected.Position {
		t.Fatal("cleanup changed selected publication")
	}
}

func TestPublicationMaintenanceTransitiveProofsAndPendingIntent(t *testing.T) {
	f, j, txs := compactionSeed(t)
	first := j.manifest
	if _, err := j.CompactAuditFull(context.Background()); err != nil {
		t.Fatal(err)
	}
	compacted := j.manifest
	certifyTestCheckpoint(t, j)
	checkpoint := j.manifest
	if _, err := j.CompactAuditFull(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := j.manifest
	certifyTestCheckpoint(t, j)
	selected := j.manifest
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// A source otherwise obsolete is pinned by a valid pending intent.
	loose := first
	loose.ManifestID = strings.Repeat("b", 32)
	maintenanceWrite(t, f, manifestName(loose.ManifestID), maintenanceManifestBytes(t, loose))
	intent := compactionIntent{Store: first.Store, ID: strings.Repeat("c", 32),
		Source: maintenanceSourceFor(t, loose), Segments: []string{strings.Repeat("d", 32)}}
	b, err := encodeCompactionIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	maintenanceWrite(t, f, intentName(intent.ID), b)
	before := faultTreeSnapshot(t, f)
	report, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	wantProofs := maintenanceSorted(manifestName(first.ManifestID), manifestName(compacted.ManifestID),
		manifestName(checkpoint.ManifestID), manifestName(second.ManifestID), manifestName(loose.ManifestID),
		checkpointName(checkpoint.RecoveryCheckpoint.ID), intentName(intent.ID))
	if err != nil || report.PublicationReclamationBlocked || !reflect.DeepEqual(report.RetainedProofMetadata, wantProofs) {
		t.Fatalf("lost transitive proof closure: %+v err=%v want=%v", report, err, wantProofs)
	}
	wantSelected := maintenanceSorted("CURRENT", "IDENTITY", manifestName(selected.ManifestID), checkpointName(selected.RecoveryCheckpoint.ID))
	if !reflect.DeepEqual(report.SelectedMetadata, wantSelected) {
		t.Fatal("selected checkpoint not retained", report)
	}
	for _, name := range append(wantProofs, wantSelected...) {
		if got := faultReadAll(t, f, name); got != before[name] {
			t.Fatalf("proof changed: %s", name)
		}
	}
	// Remove only the test intent, whose fake target segment is deliberately
	// absent. Ordinary recovery still authenticates the preserved deep graph.
	if err := f.Remove(intentName(intent.ID)); err != nil {
		t.Fatal(err)
	}
	var suffix []uint64
	opened, recovered, err := OpenJournalWithCheckpoint(f, JournalOptions{}, nil, testCheckpointLoader(t, &suffix))
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if recovered.CheckpointTransactions != uint64(len(txs)) || len(suffix) != 0 {
		t.Fatal("checkpoint recovery changed", recovered, suffix)
	}
	rotationAssertLogicalTransactions(t, opened, txs)
}

func TestPublicationMaintenanceObsoleteProofChainNeedsLaterPass(t *testing.T) {
	f, selected, _ := maintenanceFixture(t)
	source := selected
	source.ManifestID = strings.Repeat("b", 32)
	maintenanceWrite(t, f, manifestName(source.ManifestID), maintenanceManifestBytes(t, source))
	leaf := selected
	leaf.ManifestID = strings.Repeat("c", 32)
	leaf.Generation++
	leaf.RecoveryCheckpoint = &RecoveryCheckpoint{Store: selected.Store, ID: strings.Repeat("d", 32), Schema: 1,
		Size: 1, Digest: Digest{1}, PrefixDigest: Digest{2}, Source: maintenanceSourceFor(t, source)}
	maintenanceWrite(t, f, manifestName(leaf.ManifestID), maintenanceManifestBytes(t, leaf))
	report, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	wantProofs := maintenanceSorted(manifestName(source.ManifestID), checkpointName(leaf.RecoveryCheckpoint.ID))
	if err != nil || !reflect.DeepEqual(report.RetainedProofMetadata, wantProofs) {
		t.Fatal("obsolete source not conservatively pinned", report, err)
	}
	report, err = Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	if err != nil || !reflect.DeepEqual(report.RemovedStagedMetadata, []string{manifestName(source.ManifestID)}) {
		t.Fatal("unreferenced source not reclaimed on later pass", report, err)
	}
}

func TestPublicationMaintenanceUnknownEvidenceBlocksRemoval(t *testing.T) {
	for _, kind := range []string{"partial-manifest", "foreign-manifest", "wrong-manifest-id", "noncanonical-manifest",
		"unknown-manifest-name", "uppercase-name", "zero-id", "partial-current", "wrong-current-suffix",
		"foreign-current", "identity-digest", "current-manifest-digest", "current-generation",
		"partial-intent", "foreign-intent", "unknown-intent-name", "bad-intent-source", "future-intent-source",
		"loose-checkpoint", "loose-checkpoint-stage", "unknown-root", "oversize-manifest", "oversize-current", "oversize-intent"} {
		t.Run(kind, func(t *testing.T) {
			f, selected, _ := maintenanceFixture(t)
			m := selected
			m.ManifestID = strings.Repeat("e", 32)
			m.Generation++
			name := "manifests/.STAGE." + m.ManifestID
			b := maintenanceManifestBytes(t, m)
			switch kind {
			case "partial-manifest":
				b = b[:20]
			case "foreign-manifest":
				m.Store = model.StoreID(strings.Repeat("f", 32))
				b = maintenanceManifestBytes(t, m)
			case "wrong-manifest-id":
				name = "manifests/.STAGE." + strings.Repeat("f", 32)
			case "noncanonical-manifest":
				// Canonical envelope with a nonminimal generation varint.
				end := len(b) - sha256.Size
				payload := append([]byte(nil), b[8:end]...)
				payload = append(payload[:19], append([]byte{payload[19] | 0x80, 0}, payload[20:]...)...)
				var err error
				b, err = metadataEnvelope(manifestMagic, payload)
				if err != nil {
					t.Fatal(err)
				}
			case "unknown-manifest-name":
				name = "manifests/MANIFEST.unknown"
			case "uppercase-name":
				name = "manifests/.STAGE." + strings.Repeat("E", 32)
			case "zero-id":
				name = "manifests/.STAGE." + strings.Repeat("0", 32)
			case "partial-current", "wrong-current-suffix", "foreign-current", "identity-digest", "current-manifest-digest", "current-generation":
				name = ".CURRENT." + m.ManifestID
				c, err := DecodeCurrent(maintenanceCurrentBytes(t, f, m))
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "wrong-current-suffix":
					name = ".CURRENT." + strings.Repeat("f", 32)
				case "foreign-current":
					c.Store = model.StoreID(strings.Repeat("f", 32))
				case "identity-digest":
					c.IdentityDigest[0] ^= 1
				case "current-manifest-digest":
					maintenanceWrite(t, f, manifestName(m.ManifestID), maintenanceManifestBytes(t, m))
					c.ManifestDigest[0] ^= 1
				case "current-generation":
					maintenanceWrite(t, f, "manifests/.STAGE."+m.ManifestID, maintenanceManifestBytes(t, m))
					c.Generation++
				}
				b, err = EncodeCurrent(c)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "partial-current" {
					b = b[:20]
				}
			case "partial-intent", "foreign-intent", "unknown-intent-name", "bad-intent-source", "future-intent-source", "oversize-intent":
				if err := f.Mkdir("compactions"); err != nil {
					t.Fatal(err)
				}
				in := compactionIntent{Store: selected.Store, ID: m.ManifestID,
					Source: maintenanceSourceFor(t, selected), Segments: []string{strings.Repeat("d", 32)}}
				name = intentName(in.ID)
				switch kind {
				case "foreign-intent":
					in.Store = model.StoreID(strings.Repeat("f", 32))
				case "unknown-intent-name":
					name = "compactions/UNRECOGNIZED"
				case "bad-intent-source":
					in.Source.ManifestDigest[0] ^= 1
				case "future-intent-source":
					maintenanceWrite(t, f, manifestName(m.ManifestID), maintenanceManifestBytes(t, m))
					in.Source = maintenanceSourceFor(t, m)
				}
				var err error
				b, err = encodeCompactionIntent(in)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "partial-intent" {
					b = b[:20]
				} else if kind == "oversize-intent" {
					b = make([]byte, maxCompactionIntentBytes+1)
				}
			case "loose-checkpoint", "loose-checkpoint-stage":
				if err := f.Mkdir("checkpoints"); err != nil {
					t.Fatal(err)
				}
				name = checkpointName(m.ManifestID)
				if kind == "loose-checkpoint-stage" {
					name = "checkpoints/.STAGE." + m.ManifestID
				}
				b = []byte("unowned stream")
			case "unknown-root":
				name = ".FUTURE." + m.ManifestID
			case "oversize-manifest":
				b = make([]byte, MaxManifestBytes+1)
			case "oversize-current":
				name = ".CURRENT." + m.ManifestID
				b = make([]byte, MaxMetadataBytes+1)
			}
			maintenanceWrite(t, f, name, b)
			before := faultTreeSnapshot(t, f)
			report, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
			if err != nil || !report.PublicationReclamationBlocked ||
				!reflect.DeepEqual(report.UnknownMetadata, []string{name}) ||
				!reflect.DeepEqual(report.ReclamationBlockers, []string{name}) || len(report.RemovedStagedMetadata) != 0 {
				t.Fatalf("unknown evidence was not explicitly blocked: %+v err=%v", report, err)
			}
			if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("unknown evidence or possible proof was deleted")
			}
		})
	}
}

func TestPublicationMaintenanceCurrentWithoutManifestAndDataFiles(t *testing.T) {
	f, m, candidates := maintenanceFixture(t)
	m.ManifestID = strings.Repeat("e", 32)
	name := ".CURRENT." + m.ManifestID
	maintenanceWrite(t, f, name, maintenanceCurrentBytes(t, f, m))
	// Segments are outside this metadata cleanup, even with a stage-like name.
	maintenanceWrite(t, f, "segments/.STAGE."+m.ManifestID, []byte("unselected data"))
	maintenanceWrite(t, f, segmentName(m.ManifestID), []byte("unselected segment"))
	before := faultTreeSnapshot(t, f)
	report, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	want := maintenanceSorted(append(candidates, name)...)
	if err != nil || !reflect.DeepEqual(report.RemovedStagedMetadata, want) || report.PublishedSegments != 2 {
		t.Fatal(report, err)
	}
	for _, n := range want {
		delete(before, n)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("metadata cleanup removed data files")
	}
}

func TestPublicationMaintenanceRejectsAuthorityBeforeAnyMutation(t *testing.T) {
	for _, kind := range []string{"missing-current", "current-checksum", "identity-digest", "identity-store",
		"manifest-digest", "manifest-store", "manifest-generation", "manifest-id", "profile"} {
		t.Run(kind, func(t *testing.T) {
			f, m, _ := maintenanceFixture(t)
			if err := f.Mkdir("quarantine"); err != nil {
				t.Fatal(err)
			}
			maintenanceWrite(t, f, "quarantine/evidence", []byte("keep"))
			c, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
			if err != nil {
				t.Fatal(err)
			}
			var d DurableIO = f
			switch kind {
			case "missing-current":
				if err := f.Remove("CURRENT"); err != nil {
					t.Fatal(err)
				}
			case "current-checksum":
				b := []byte(faultReadAll(t, f, "CURRENT"))
				b[0] ^= 1
				replayReplaceFile(t, f, "CURRENT", b)
			case "identity-digest":
				c.IdentityDigest[0] ^= 1
			case "identity-store":
				c.Store = model.StoreID(strings.Repeat("f", 32))
			case "manifest-digest":
				c.ManifestDigest[0] ^= 1
			case "manifest-store":
				m.Store = model.StoreID(strings.Repeat("f", 32))
			case "manifest-generation":
				m.Generation++
			case "manifest-id":
				m.ManifestID = strings.Repeat("f", 32)
			case "profile":
				d = maintenanceProfileIO{f}
			}
			if strings.HasPrefix(kind, "manifest-") && kind != "manifest-digest" {
				b := maintenanceManifestBytes(t, m)
				replayReplaceFile(t, f, manifestName(c.ManifestID), b)
				c.ManifestDigest = Digest(sha256.Sum256(b))
			}
			if kind != "missing-current" && kind != "current-checksum" {
				b, err := EncodeCurrent(c)
				if err != nil {
					t.Fatal(err)
				}
				replayReplaceFile(t, f, "CURRENT", b)
			}
			before := faultTreeSnapshot(t, f)
			f.resetFaults()
			_, err = Maintain(context.Background(), d, RetentionPolicy{DropPublicationStaging: true, DropQuarantine: true})
			want := ErrCorruption
			if kind == "missing-current" {
				want = ErrIncompleteStore
			} else if kind == "profile" {
				want = ErrUnsupported
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatalf("authority not fail-closed: %v", err)
			}
			maintenanceAssertNoMutation(t, f.calls)
		})
	}
}

type maintenanceProfileIO struct{ DurableIO }

func (maintenanceProfileIO) Profile() DurabilityProfile { return "other" }

func TestPublicationMaintenanceRejectsBrokenTransitiveSource(t *testing.T) {
	for _, kind := range []string{"digest", "missing", "binding", "retirement", "checkpoint-digest"} {
		t.Run(kind, func(t *testing.T) {
			f, j, _ := compactionSeed(t)
			source := j.manifest
			if _, err := j.CompactAuditFull(context.Background()); err != nil {
				t.Fatal(err)
			}
			certifyTestCheckpoint(t, j)
			m := j.manifest
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "digest":
				raw := maintenanceManifestBytes(t, source)
				raw[len(raw)-1] ^= 1
				replayReplaceFile(t, f, manifestName(source.ManifestID), raw)
			case "missing":
				if err := f.Remove(manifestName(source.ManifestID)); err != nil {
					t.Fatal(err)
				}
			case "binding":
				m.LayoutSource.Generation++
				checkpointReplaceManifest(t, f, m)
			case "retirement":
				m.RetiredSegments[0].PrefixDigest[0] ^= 1
				checkpointReplaceManifest(t, f, m)
			case "checkpoint-digest":
				m.RecoveryCheckpoint.Source.ManifestDigest[0] ^= 1
				checkpointReplaceManifest(t, f, m)
			}
			before := faultTreeSnapshot(t, f)
			f.resetFaults()
			_, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
			if !errors.Is(err, ErrCorruption) || !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("broken proof accepted", err)
			}
			maintenanceAssertNoMutation(t, f.calls)
		})
	}
}

func TestPublicationMaintenanceFileCloseRetainsOwner(t *testing.T) {
	for _, target := range []string{"CURRENT", "IDENTITY", "selected", "stage", "intent"} {
		for _, acquireError := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/open-error=%t", target, acquireError), func(t *testing.T) {
				f, selected, _ := maintenanceFixture(t)
				name := target
				switch target {
				case "selected":
					name = manifestName(selected.ManifestID)
				case "stage":
					name = "manifests/.STAGE." + strings.Repeat("a", 32)
				case "intent":
					if err := f.Mkdir("compactions"); err != nil {
						t.Fatal(err)
					}
					name = intentName(strings.Repeat("b", 32))
					maintenanceWrite(t, f, name, []byte("partial evidence"))
				}
				businessErr := errors.New("open returned live file with error")
				d := maintenanceReadIO{DurableIO: f}
				d.open = func(n string) (ReadFile, error) {
					file, err := f.OpenRead(n)
					if n == name && acquireError {
						return file, businessErr
					}
					return file, err
				}
				f.resetFaults()
				f.onBoundary = func(b faultBoundary) error {
					if b.Op == "Close" && b.Name == name && !b.After {
						return errFaultIO
					}
					return nil
				}
				_, err := Maintain(context.Background(), d, RetentionPolicy{DropPublicationStaging: true})
				var cleanup *CleanupError
				if !errors.As(err, &cleanup) || !errors.Is(err, errFaultIO) || !f.locked ||
					acquireError && !errors.Is(err, businessErr) {
					t.Fatalf("lost file/owner cleanup: %v locked=%t", err, f.locked)
				}
				cleanupAssertNoOwnerClose(t, f.calls)
				maintenanceAssertNoMutation(t, f.calls)
				if err := cleanup.Close(); !errors.Is(err, errFaultIO) || !f.locked {
					t.Fatal("failed retry released owner", err)
				}
				f.resetFaults()
				if err := cleanup.Close(); err != nil {
					t.Fatal(err)
				}
				cleanupAssertCloseOrder(t, f.calls, name)
				owner, err := f.AcquireOwnerLock()
				if err != nil {
					t.Fatal(err)
				}
				defer owner.Close()
				count := len(f.calls)
				if err := cleanup.Close(); err != nil || !f.locked || len(f.calls) != count {
					t.Fatal("completed handle released later owner", err)
				}
			})
		}
	}
}

type maintenanceReadIO struct {
	DurableIO
	open func(string) (ReadFile, error)
}

func (d maintenanceReadIO) OpenRead(name string) (ReadFile, error) { return d.open(name) }

func TestPublicationMaintenanceRemovalAndSyncRetries(t *testing.T) {
	baseline, _, _ := maintenanceFixture(t)
	if _, err := Maintain(context.Background(), baseline, RetentionPolicy{DropPublicationStaging: true}); err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), baseline.calls...)
	for _, call := range trace {
		if call.Op != "Remove" && call.Op != "SyncDir" {
			continue
		}
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d-%s-%s-after=%t", call.Step, call.Op, call.Name, after), func(t *testing.T) {
				f, selected, candidates := maintenanceFixture(t)
				before := faultTreeSnapshot(t, f)
				f.resetFaults()
				f.failAt(call.Step, after)
				_, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
				if !errors.Is(err, errFaultIO) {
					t.Fatal("injected boundary not reached", err)
				}
				f.resetFaults()
				if _, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true}); err != nil {
					t.Fatal("retry failed", err)
				}
				synced := map[string]bool{}
				for _, c := range f.calls {
					if c.Op == "SyncDir" {
						synced[c.Name] = true
					}
				}
				if !synced["."] || !synced["manifests"] {
					t.Fatal("empty retry skipped directory durability", synced)
				}
				crashed := f.crash()
				for _, name := range candidates {
					delete(before, name)
				}
				if !reflect.DeepEqual(before, faultTreeSnapshot(t, crashed)) {
					t.Fatal("retry failed durable removal or changed authority")
				}
				opened, report, err := OpenJournal(crashed, JournalOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer opened.Close()
				if report.Publication.Position != selected.Position {
					t.Fatal("retry changed publication")
				}
			})
		}
	}
}

func TestPublicationMaintenanceStabilizesObservedCurrentBeforeDeletion(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			f, j, before, _ := faultSeedAcknowledged(t)
			f.onBoundary = func(b faultBoundary) error {
				if b.Op == "SyncDir" && b.Name == "." && !b.After {
					return errFaultIO
				}
				return nil
			}
			_, err := j.Publish(context.Background(), faultNextTransaction(before, 2))
			if !errors.Is(err, ErrCommitUnknown) {
				t.Fatal(err)
			}
			f.resetFaults()
			observed := rotationManifest(t, f)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			removed := false
			f.onBoundary = func(b faultBoundary) error {
				if b.Op == "Remove" && b.After {
					removed = true
				}
				if removed && b.Op == "SyncDir" && b.Name == "manifests" && b.After == after {
					return errFaultIO
				}
				return nil
			}
			_, err = Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
			if !errors.Is(err, errFaultIO) || !removed {
				t.Fatal("post-removal sync fault missed", err)
			}
			opened, report, err := OpenJournal(f.crash(), JournalOptions{})
			if err != nil {
				t.Fatal("old CURRENT resurrected after its manifest was removed", err)
			}
			defer opened.Close()
			if report.Publication.Position != observed.Position {
				t.Fatal("observed publication was not stabilized")
			}
		})
	}
}

func TestPublicationMaintenanceCrashAtEveryRemovalAndSync(t *testing.T) {
	baseline, _, _ := maintenanceFixture(t)
	if _, err := Maintain(context.Background(), baseline, RetentionPolicy{DropPublicationStaging: true}); err != nil {
		t.Fatal(err)
	}
	for _, call := range append([]faultCall(nil), baseline.calls...) {
		if call.Op != "Remove" && call.Op != "SyncDir" {
			continue
		}
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d-%s-after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f, selected, _ := maintenanceFixture(t)
				f.failAt(call.Step, after)
				if _, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true}); !errors.Is(err, errFaultIO) {
					t.Fatal("injected crash boundary missed", err)
				}
				crashed := f.crash()
				// Reopen before retrying cleanup: recovery must not depend
				// on finishing the interrupted maintenance pass.
				opened, report, err := OpenJournal(crashed, JournalOptions{})
				if err != nil {
					t.Fatal("interrupted cleanup stranded authority", err)
				}
				if report.Publication.Position != selected.Position || report.Publication.PostRootSet != selected.PostRootSet {
					t.Fatal("crash changed the selected publication", report)
				}
				if err := opened.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := Maintain(context.Background(), crashed, RetentionPolicy{DropPublicationStaging: true}); err != nil {
					t.Fatal("cleanup failed after reboot", err)
				}
			})
		}
	}
}

func TestPublicationMaintenanceInspectionBudgetAndCancellation(t *testing.T) {
	for _, dir := range []string{"segments", "manifests", "quarantine", "compactions", ".", "checkpoints"} {
		for _, mode := range []string{"budget", "cancel", "io-error"} {
			t.Run(dir+"/"+mode, func(t *testing.T) {
				f, _, _ := maintenanceFixture(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				d := maintenanceListIO{DurableIO: f}
				d.list = func(n string) ([]string, error) {
					if n == dir {
						switch mode {
						case "budget":
							return make([]string, MaxMaintenanceEntries+1), nil
						case "cancel":
							cancel()
						case "io-error":
							return nil, errFaultIO
						}
					}
					return f.List(n)
				}
				_, err := Maintain(ctx, d, RetentionPolicy{DropQuarantine: true, DropPublicationStaging: true})
				want := ErrLimit
				if mode == "cancel" {
					want = context.Canceled
				} else if mode == "io-error" {
					want = errFaultIO
				}
				if !errors.Is(err, want) {
					t.Fatalf("inspection limit/failure lost: %v", err)
				}
				maintenanceAssertNoMutation(t, f.calls)
			})
		}
	}
}

type maintenanceSizedFile struct {
	ReadFile
	size  int64
	reads *int
}

func (f maintenanceSizedFile) Size() (int64, error) { return f.size, nil }
func (f maintenanceSizedFile) ReadAt(p []byte, off int64) (int, error) {
	*f.reads++
	return f.ReadFile.ReadAt(p, off)
}

func TestPublicationMaintenanceReadBoundsAndCancellation(t *testing.T) {
	f, _, _ := maintenanceFixture(t)
	for _, mode := range []string{"oversize", "aggregate", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			d := maintenanceReadIO{DurableIO: f}
			d.open = func(n string) (ReadFile, error) {
				file, err := f.OpenRead(n)
				if err != nil {
					return file, err
				}
				size := int64(MaxMetadataBytes)
				if mode == "oversize" {
					size++
				}
				if mode == "cancel" {
					cancel()
				}
				return maintenanceSizedFile{ReadFile: file, size: size, reads: &reads}, nil
			}
			scan := publicationMaintenance{ctx: ctx, io: d}
			if mode == "aggregate" {
				scan.bytes = MaxMaintenanceMetadataBytes - 1
			}
			_, err := scan.read("CURRENT", MaxMetadataBytes)
			want := ErrLimit
			if mode == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || reads != 0 {
				t.Fatalf("read occurred before budget/cancel check: err=%v reads=%d", err, reads)
			}
		})
	}
}

func TestPublicationMaintenanceLateReadFailurePrecedesAllRemoval(t *testing.T) {
	for _, mode := range []string{"io-error", "aggregate-budget"} {
		t.Run(mode, func(t *testing.T) {
			f, _, _ := maintenanceFixture(t)
			if err := f.Mkdir("quarantine"); err != nil {
				t.Fatal(err)
			}
			maintenanceWrite(t, f, "quarantine/evidence", []byte("keep"))
			// A compaction intent is read after all manifests have already
			// been inspected and possible candidates identified.
			if err := f.Mkdir("compactions"); err != nil {
				t.Fatal(err)
			}
			name := intentName(strings.Repeat("e", 32))
			maintenanceWrite(t, f, name, []byte("evidence"))
			d := maintenanceReadIO{DurableIO: f}
			d.open = func(n string) (ReadFile, error) {
				if n == name {
					return nil, errFaultIO
				}
				return f.OpenRead(n)
			}
			before := faultTreeSnapshot(t, f)
			f.resetFaults()
			var err error
			if mode == "aggregate-budget" {
				// Exhaustion is fatal, not an unknown classification that
				// could permit the later quarantine mutation.
				scan := publicationMaintenance{ctx: context.Background(), io: f, bytes: MaxMaintenanceMetadataBytes - 1}
				var report MaintenanceReport
				err = scan.inspect(nil, &report)
			} else {
				_, err = Maintain(context.Background(), d, RetentionPolicy{DropPublicationStaging: true, DropQuarantine: true})
			}
			want := errFaultIO
			if mode == "aggregate-budget" {
				want = ErrLimit
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("late inspection failure mutated files", err)
			}
			maintenanceAssertNoMutation(t, f.calls)
		})
	}
}

func TestPublicationMaintenanceCancellationDuringRemoval(t *testing.T) {
	for _, cancelAt := range []int{1, 4} {
		t.Run(fmt.Sprint(cancelAt), func(t *testing.T) {
			f, _, candidates := maintenanceFixture(t)
			before := faultTreeSnapshot(t, f)
			f.resetFaults()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			count := 0
			f.onBoundary = func(b faultBoundary) error {
				if b.Op == "Remove" && b.After {
					count++
					if count == cancelAt {
						cancel()
					}
				}
				return nil
			}
			report, err := Maintain(ctx, f, RetentionPolicy{DropPublicationStaging: true})
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(report.RemovedStagedMetadata, candidates[:cancelAt]) {
				t.Fatal("cancellation lost partial removal report", report, err)
			}
			f.resetFaults()
			report, err = Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
			want := append([]string(nil), candidates[cancelAt:]...)
			if err != nil || !reflect.DeepEqual(report.RemovedStagedMetadata, want) {
				t.Fatal("cancellation retry did not finish", report, err)
			}
			for _, name := range candidates {
				delete(before, name)
			}
			if !reflect.DeepEqual(before, faultTreeSnapshot(t, f.crash())) {
				t.Fatal("cancellation retry was not durable")
			}
		})
	}
}

func TestPublicationMaintenanceEmptyQuarantineRetrySyncsRemoval(t *testing.T) {
	f, _, _ := maintenanceFixture(t)
	if err := f.Mkdir("quarantine"); err != nil {
		t.Fatal(err)
	}
	maintenanceWrite(t, f, "quarantine/evidence", []byte("discard"))
	if err := f.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "SyncDir" && b.Name == "quarantine" && !b.After {
			return errFaultIO
		}
		return nil
	}
	report, err := Maintain(context.Background(), f, RetentionPolicy{DropQuarantine: true})
	if !errors.Is(err, errFaultIO) || len(report.RemovedQuarantine) != 1 {
		t.Fatal(report, err)
	}
	f.resetFaults()
	report, err = Maintain(context.Background(), f, RetentionPolicy{DropQuarantine: true})
	if err != nil || len(report.RemovedQuarantine) != 0 {
		t.Fatal(report, err)
	}
	if _, err := f.crash().OpenRead("quarantine/evidence"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("empty retry did not persist quarantine removal", err)
	}
}

func TestPublicationMaintenanceUnknownDoesNotDisableExplicitQuarantine(t *testing.T) {
	f, _, _ := maintenanceFixture(t)
	if err := f.Mkdir("quarantine"); err != nil {
		t.Fatal(err)
	}
	maintenanceWrite(t, f, "quarantine/evidence", []byte("explicitly discard"))
	maintenanceWrite(t, f, "manifests/MANIFEST.unknown", []byte("keep"))
	report, err := Maintain(context.Background(), f, RetentionPolicy{DropQuarantine: true, DropPublicationStaging: true})
	if err != nil || !report.PublicationReclamationBlocked || len(report.RemovedStagedMetadata) != 0 ||
		!reflect.DeepEqual(report.RemovedQuarantine, []string{"evidence"}) {
		t.Fatal(report, err)
	}
	if _, err := f.OpenRead("quarantine/evidence"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
}

// Keep the assertion independent of decoding: metadata cleanup must not alter
// even a single byte of a selected publication or retained proof.
func TestPublicationMaintenanceSelectedStageCopyIsNotAuthority(t *testing.T) {
	f, selected, _ := maintenanceFixture(t)
	stage := "manifests/.STAGE." + selected.ManifestID
	maintenanceWrite(t, f, stage, maintenanceManifestBytes(t, selected))
	current := []byte(faultReadAll(t, f, "CURRENT"))
	maintenanceWrite(t, f, ".CURRENT."+selected.ManifestID, current)
	report, err := Maintain(context.Background(), f, RetentionPolicy{DropPublicationStaging: true})
	if err != nil || report.PublicationReclamationBlocked || len(report.RemovedStagedMetadata) != 6 {
		t.Fatal(report, err)
	}
	if !bytes.Equal(current, []byte(faultReadAll(t, f, "CURRENT"))) {
		t.Fatal("CURRENT bytes changed")
	}
	if !bytes.Equal(maintenanceManifestBytes(t, selected), []byte(faultReadAll(t, f, manifestName(selected.ManifestID)))) {
		t.Fatal("selected manifest bytes changed")
	}
}
