package acceptance_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// These experiments measure deterministic engine mechanisms, not LLM task
// success. Their deliberately weak baselines are named in each test and never
// used as a replacement production context selector or migration protocol.
func evidenceID(text string) model.ObjectID {
	return model.ObjectID(fmt.Sprintf("%x", sha256.Sum256([]byte(text))))
}

func evidenceAnchor(space model.SpaceID, lsn uint64) model.SnapshotAnchor {
	return model.SnapshotAnchor{Space: space, Fence: model.CommitFence{Store: "evidence-store", LSN: lsn, CommitDigest: model.CommitDigest(evidenceID(fmt.Sprintf("fence-%d", lsn)))}}
}

func promoteEvidence(t testing.TB, store *memory.Store, space model.SpaceID, claim string, app memory.Applicability, sources []memory.Source, counters []memory.Source) memory.Memory {
	t.Helper()
	candidate, err := store.PutCandidate(memory.Memory{Space: space, Kind: memory.KindDecision, Claim: []byte(claim), Sources: sources, Applicability: app})
	if err != nil {
		t.Fatal(err)
	}
	validated, err := store.Validate(candidate.ID, memory.Validation{StrategyVersion: 1, Passed: true, Evidence: sources, Counterexamples: counters})
	if err != nil {
		t.Fatal(err)
	}
	promoted, err := store.Promote(validated.ID)
	if err != nil {
		t.Fatal(err)
	}
	return promoted
}

func TestMemoryApplicabilityAgainstClaimOnlyLookup(t *testing.T) {
	store := memory.New()
	app := memory.Applicability{Goal: "repair", Environment: "go1.27", SchemaVersion: 1, Tags: []string{"go"}}
	source := func(space model.SpaceID, name string) []memory.Source {
		return []memory.Source{{SourceID: evidenceID(name), Anchor: evidenceAnchor(space, 1)}}
	}
	good := promoteEvidence(t, store, "project-a", "restart after validating the configuration", app, source("project-a", "good"), nil)
	old := app
	old.Environment = "go1.26"
	wrongEnvironment := promoteEvidence(t, store, "project-a", "restart using a legacy configuration", old, source("project-a", "legacy"), nil)
	wrong := app
	wrong.Goal = "deploy"
	wrongGoal := promoteEvidence(t, store, "project-a", "restart after deploying a different service", wrong, source("project-a", "different-goal"), nil)
	otherProject := promoteEvidence(t, store, "project-b", "restart after validating the configuration", app, source("project-b", "other-project"), nil)
	// A claim-only baseline retrieves all four matching claims. Ground truth is
	// declared by this fixture's project, goal, and environment, independently
	// of the Search implementation.
	claimOnly := []memory.Memory{good, wrongEnvironment, wrongGoal, otherProject}
	if len(claimOnly) != 4 {
		t.Fatal("invalid comparison corpus")
	}
	query := memory.Query{Space: "project-a", Text: "restart", Goal: "repair", Environment: "go1.27", Tags: []string{"go"}, At: evidenceAnchor("project-a", 4)}
	results, err := store.Search(query, 10)
	if err != nil || len(results) != 1 || results[0].Memory.ID != good.ID {
		t.Fatalf("scoped query included an inapplicable record: %+v %v", results, err)
	}
	query.Space, query.At = "project-b", evidenceAnchor("project-b", 4)
	other, err := store.Search(query, 10)
	if err != nil || len(other) != 1 || other[0].Memory.ID != otherProject.ID {
		t.Fatalf("project isolation failed: %+v %v", other, err)
	}
	query.Space, query.At = "project-c", evidenceAnchor("project-c", 4)
	if absent, err := store.Search(query, 10); err != nil || len(absent) != 0 {
		t.Fatalf("memory crossed projects without an import: %+v %v", absent, err)
	}
	t.Log("fixture relevance: claim-only baseline 1/4; scoped Search 1/1; foreign-space automatic reuse 0 records; cross-space import is not implemented by this test")
}

func TestExperienceKeepsCounterexamplesAndWithdrawalInvalidatesReuse(t *testing.T) {
	store := memory.New()
	success := memory.Source{SourceID: evidenceID("successful-file-validation"), Anchor: evidenceAnchor("project-a", 1)}
	counter := memory.Source{SourceID: evidenceID("failed-alternative-counterexample"), Anchor: evidenceAnchor("project-a", 2)}
	m := promoteEvidence(t, store, "project-a", "select validated output and retain the failed alternative's constraint", memory.Applicability{Goal: "repair", Environment: "go1.27"}, []memory.Source{success}, []memory.Source{counter})
	omitted := model.NodeRef{Kind: model.NodeRevision, Revision: model.RevisionID(evidenceID("failed-branch"))}
	path, err := store.BuildExperiencePath("project-a", evidenceAnchor("project-a", 4), []memory.Step{{Node: model.NodeRef{Kind: model.NodeRevision, Revision: model.RevisionID(evidenceID("selected-branch"))}, Memory: m.ID, Outcome: "validated against the file"}}, []model.NodeRef{omitted}, memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{success}, Counterexamples: []memory.Source{counter}}, memory.Applicability{Goal: "repair", Environment: "go1.27"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := memory.CanonicalPath(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := memory.DecodePath(before)
	if err != nil || len(decoded.Validation.Counterexamples) != 1 || decoded.Validation.Counterexamples[0].SourceID != counter.SourceID || len(decoded.Omitted) != 1 || decoded.Omitted[0] != omitted {
		t.Fatalf("experience omitted the useful failure or its gap: %+v %v", decoded, err)
	}
	if _, err := store.ReusablePath(path.ID, evidenceAnchor("project-a", 5)); err != nil {
		t.Fatal("valid path could not be reused", err)
	}
	if _, err := store.RetractSource(counter.SourceID, memory.Retraction{Kind: memory.RetractionUseBan, Reason: "counterexample source is no longer approved for reuse"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReusablePath(path.ID, evidenceAnchor("project-a", 5)); !errors.Is(err, memory.ErrNotEligible) {
		t.Fatalf("path retained reuse eligibility after counterexample ban: %v", err)
	}
	results, err := store.Search(memory.Query{Space: "project-a", Text: "select", Goal: "repair", Environment: "go1.27", At: evidenceAnchor("project-a", 5)}, 10)
	if err != nil || len(results) != 0 {
		t.Fatalf("withdrawn derived memory remained searchable: %+v %v", results, err)
	}
	audit, err := store.GetPath(path.ID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := memory.CanonicalPath(audit)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("withdrawal rewrote the immutable audit path", err)
	}
	t.Log("provenance: counterexample 1/1 and omitted branch 1/1 retained; after UseBan reusable paths 0 and searchable derived memories 0; original audit bytes unchanged")
}

func TestEarlyEvidenceRecallUsesExactKnowledgePosition(t *testing.T) {
	key := model.KnowledgeKey{Actor: "worker", Execution: "long-task"}
	snapshot := knowledge.Snapshot{}
	for i := uint64(1); i <= 128; i++ {
		label := fmt.Sprintf("noise-%03d", i)
		if i == 1 {
			label = "mandatory: preserve the original file"
		} else if i == 2 {
			label = "counterexample: replacement without validation lost data"
		}
		o := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: evidenceAnchor("project-a", i)}, Payload: evidenceID(label), Sequence: i, Label: label}
		o.ID = knowledge.ObservationID(o)
		snapshot.Observations = append(snapshot.Observations, o)
	}
	versions, err := knowledge.Revisions(snapshot, "project-a", key)
	if err != nil {
		t.Fatal(err)
	}
	const cut = 64
	selected, revision, err := knowledge.SelectRevision(snapshot, "project-a", key, versions[cut].ID)
	if err != nil || revision.Sequence != cut || len(selected.Observations) != cut {
		t.Fatalf("historical position changed: %+v %v", revision, err)
	}
	// Compare recall by two explicitly recorded IDs with a last-eight-observation
	// baseline. This measures addressable history, not automatic prompt selection.
	wanted := map[model.ObjectID]bool{snapshot.Observations[0].ID: true, snapshot.Observations[1].ID: true}
	latestWindow, recalled := 0, 0
	for _, o := range selected.Observations[len(selected.Observations)-8:] {
		if wanted[o.ID] {
			latestWindow++
		}
	}
	for _, o := range selected.Observations {
		if wanted[o.ID] {
			recalled++
		}
		if o.Sequence > cut {
			t.Fatal("future observation entered the fixed source position")
		}
	}
	if latestWindow != 0 || recalled != 2 {
		t.Fatalf("recall counts: recent-window=%d exact-ID=%d", latestWindow, recalled)
	}
	for round := 0; round < 3; round++ {
		restored := knowledge.Snapshot{}
		for _, observation := range selected.Observations {
			raw, err := observation.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := knowledge.DecodeObservation(raw)
			if err != nil {
				t.Fatal("reconstruction lost the early evidence", err)
			}
			restored.Observations = append(restored.Observations, decoded)
		}
		if err := restored.Validate(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(restored, selected) {
			t.Fatal("canonical record roundtrip changed the historical observations")
		}
		selected = restored
		_, preservedRevision, err := knowledge.SelectRevision(selected, "project-a", key, revision.ID)
		if err != nil || preservedRevision != revision {
			t.Fatalf("reconstruction changed the historical revision: %+v %v", preservedRevision, err)
		}
		preserved := 0
		for _, o := range selected.Observations {
			if wanted[o.ID] {
				preserved++
			}
		}
		if preserved != len(wanted) {
			t.Fatal("canonical record roundtrip lost the early evidence identities")
		}
	}
	t.Log("exact cut 64/128 observations; latest-eight recall 0/2; explicit recorded-ID recall 2/2; future leakage 0; three canonical record reconstructions unchanged; this does not measure model recall or fixed-token prompt selection")
}
