package acceptance_test

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// The fixed two-token costs are explicitly synthetic estimates. Both policies
// face the same ten-token capacity; no provider/model is called. Ground truth
// comes from the fixture's two constraints and three linked evidence records,
// rather than from reimplementing SelectContext's ranking algorithm.
func contextEvidenceRequest(t *testing.T) (knowledge.ContextSelectionRequest, []model.ObjectID) {
	t.Helper()
	key := model.KnowledgeKey{Actor: "worker", Execution: "context-task"}
	snapshot := knowledge.Snapshot{}
	labels := []string{"goal: preserve the file", "acceptance: validate output", "conclusion: select the verified result", "evidence: file matches the expected bytes", "counterexample: unchecked overwrite lost data"}
	for i := uint64(1); i <= 32; i++ {
		label := "recent irrelevant diagnostic"
		if i <= uint64(len(labels)) {
			label = labels[i-1]
		}
		o := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: evidenceAnchor("project-a", i)}, Payload: evidenceID(label + string(rune(i))), Sequence: i, Label: label}
		o.ID = knowledge.ObservationID(o)
		snapshot.Observations = append(snapshot.Observations, o)
	}
	versions, err := knowledge.Revisions(snapshot, "project-a", key)
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := knowledge.Frontier(snapshot, key)
	if err != nil {
		t.Fatal(err)
	}
	anchor := evidenceAnchor("project-a", 32)
	anchor.Knowledge = &key
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: versions[len(versions)-1].ID}
	critical := make([]model.ObjectID, len(labels))
	for i := range critical {
		critical[i] = snapshot.Observations[i].ID
	}
	request := knowledge.ContextSelectionRequest{Snapshot: snapshot, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: frontier}, Capacity: knowledge.ContextCapacity{Window: 16, OutputReserve: 4, FramingReserve: 2}, Mandatory: append([]model.ObjectID(nil), critical[:2]...), Tokenizer: "synthetic-two-token-segments/v1", CountQuality: "estimated", PolicyVersion: "benefit_per_marginal_token/v1"}
	for _, o := range snapshot.Observations {
		request.Costs = append(request.Costs, knowledge.ContextCost{Observation: o.ID, Tokens: 2})
	}
	useful := knowledge.ContextBundle{Conclusion: critical[2], Evidence: []model.ObjectID{critical[3]}, Counterexamples: []model.ObjectID{critical[4]}, Benefit: 100}
	useful.ID = knowledge.ContextBundleID(useful)
	noise := knowledge.ContextBundle{Conclusion: snapshot.Observations[len(snapshot.Observations)-1].ID, Benefit: 1}
	noise.ID = knowledge.ContextBundleID(noise)
	request.Bundles = []knowledge.ContextBundle{noise, useful}
	return request, critical
}

func TestFixedCapacityRetainsConstraintsAndFailureBundle(t *testing.T) {
	request, critical := contextEvidenceRequest(t)
	selection, err := knowledge.SelectContext(request)
	if err != nil {
		t.Fatal(err)
	}
	if selection.InputLimit != 10 || selection.UsedTokens != 10 || selection.CountQuality != "estimated" || len(selection.Selected) != len(critical) || len(selection.Omitted) != 27 {
		t.Fatalf("capacity or cost provenance changed: %+v", selection)
	}
	selected := map[model.ObjectID]bool{}
	for _, id := range selection.Selected {
		selected[id] = true
	}
	for _, id := range critical {
		if !selected[id] {
			t.Fatalf("dropped required constraint or atomic failure bundle member: %s", id)
		}
	}
	baselineHits := 0
	for _, o := range request.Snapshot.Observations[len(request.Snapshot.Observations)-5:] {
		if slices.Contains(critical, o.ID) {
			baselineHits++
		}
	}
	if baselineHits != 0 {
		t.Fatal("invalid recency-only comparison fixture")
	}
	// Input proposal order must not change selected identities or token cost.
	slices.Reverse(request.Snapshot.Observations)
	slices.Reverse(request.Costs)
	slices.Reverse(request.Bundles)
	again, err := knowledge.SelectContext(request)
	if err != nil || !reflect.DeepEqual(selection.Selected, again.Selected) || selection.UsedTokens != again.UsedTokens || selection.ProposalDigest != again.ProposalDigest {
		t.Fatalf("input iteration order changed context selection: %+v %v", again, err)
	}
	t.Log("equal estimated input budget 10: recency-only critical recall 0/5, SelectContext 5/5 (mandatory 2/2, conclusion+evidence+counterexample 3/3); no model task-success claim")
}

func TestFixedCapacityCannotImportFutureKnowledge(t *testing.T) {
	request, _ := contextEvidenceRequest(t)
	key := *request.Source.Anchor.Knowledge
	versions, err := knowledge.Revisions(request.Snapshot, request.Source.Anchor.Space, key)
	if err != nil {
		t.Fatal(err)
	}
	request.Source.Anchor.Cursor.Revision = versions[16].ID
	request.Source.Anchor.Fence = evidenceAnchor("project-a", 16).Fence
	past, _, err := knowledge.SelectRevision(request.Snapshot, "project-a", key, versions[16].ID)
	if err != nil {
		t.Fatal(err)
	}
	request.Source.Frontier, err = knowledge.Frontier(past, key)
	if err != nil {
		t.Fatal(err)
	}
	// The proposal still contains costs and a bundle from observations 17..32.
	// The snapshot itself contains those records, so rejection must come from
	// the selected historical position rather than mere object existence.
	if _, err := knowledge.SelectContext(request); !errors.Is(err, knowledge.ErrUnknownObservation) {
		t.Fatalf("a future proposal member entered a strict historical context: %v", err)
	}
}

func TestFixedCapacityCannotSilentlyDropMandatoryConstraints(t *testing.T) {
	request, _ := contextEvidenceRequest(t)
	request.Capacity = knowledge.ContextCapacity{Window: 9, OutputReserve: 4, FramingReserve: 2} // three input tokens, four mandatory
	if _, err := knowledge.SelectContext(request); !errors.Is(err, knowledge.ErrContextBudgetUnsatisfied) {
		t.Fatalf("mandatory overflow must be explicit, got %v", err)
	}
}
