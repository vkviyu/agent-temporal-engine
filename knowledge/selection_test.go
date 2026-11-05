package knowledge

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func selectionFixture(t *testing.T, n int) (ContextSelectionRequest, []model.ObjectID) {
	t.Helper()
	snapshot := Snapshot{}
	ids := make([]model.ObjectID, n)
	for i := range ids {
		o := snapshotObservation("space", "one", uint64(i+1))
		snapshot.Observations = append(snapshot.Observations, o)
		ids[i] = o.ID
	}
	key := snapshot.Observations[0].Key
	chain, err := Revisions(snapshot, "space", key)
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := Frontier(snapshot, key)
	if err != nil {
		t.Fatal(err)
	}
	anchor := snapshot.Observations[0].Source.Anchor.Clone()
	anchor.Knowledge = &key
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: "space", Revision: chain[len(chain)-1].ID}
	req := ContextSelectionRequest{Snapshot: snapshot, Source: SourceAnchor{Anchor: anchor, Frontier: frontier}, Tokenizer: "test/framed-v1", CountQuality: TokenCountExact, Capacity: ContextCapacity{Window: 100}}
	for _, id := range ids {
		req.Costs = append(req.Costs, ContextCost{Observation: id, Tokens: 2})
	}
	return req, ids
}

func selectionBundle(conclusion model.ObjectID, evidence, counters []model.ObjectID, benefit uint64) ContextBundle {
	bundle := ContextBundle{Conclusion: conclusion, Evidence: evidence, Counterexamples: counters, Benefit: benefit}
	bundle.ID = ContextBundleID(bundle)
	return bundle
}

func TestContextMandatoryIncludesTransitiveEvidenceAndCounterexample(t *testing.T) {
	req, ids := selectionFixture(t, 3)
	req.Mandatory = []model.ObjectID{ids[0]}
	req.Bundles = []ContextBundle{
		selectionBundle(ids[0], []model.ObjectID{ids[1]}, nil, 1),
		selectionBundle(ids[1], nil, []model.ObjectID{ids[2]}, 1),
	}
	req.Capacity = ContextCapacity{Window: 7, OutputReserve: 1, FramingReserve: 1}
	got, err := SelectContext(req)
	if !errors.Is(err, ErrContextBudgetUnsatisfied) || !reflect.DeepEqual(got, ContextSelection{}) {
		t.Fatalf("mandatory closure was weakened to fit: %+v %v", got, err)
	}
	req.Capacity.Window = 8
	got, err = SelectContext(req)
	if err != nil || got.UsedTokens != 6 || got.InputLimit != 6 || len(got.Mandatory) != 3 || len(got.Selected) != 3 || len(got.Omitted) != 0 {
		t.Fatalf("mandatory evidence closure: %+v %v", got, err)
	}
}

func TestContextSharedEvidenceChargedOnceAndAllBundleMembersRetained(t *testing.T) {
	req, ids := selectionFixture(t, 4)
	for i := range req.Costs {
		req.Costs[i].Tokens = 10
	}
	req.Capacity.Window = 40
	req.Bundles = []ContextBundle{
		selectionBundle(ids[0], []model.ObjectID{ids[2]}, []model.ObjectID{ids[3]}, 30),
		selectionBundle(ids[1], []model.ObjectID{ids[2]}, []model.ObjectID{ids[3]}, 20),
	}
	got, err := SelectContext(req)
	if err != nil || got.UsedTokens != 40 || len(got.Selected) != 4 || len(got.SelectedBundles) != 2 || len(got.Omitted) != 0 {
		t.Fatalf("shared support charged more than once: %+v %v", got, err)
	}
	chosen := map[model.ObjectID]bool{}
	for _, id := range got.Selected {
		if chosen[id] {
			t.Fatal("a selected observation appeared twice")
		}
		chosen[id] = true
	}
}

func TestContextSelectionRejectsInvalidProposalsWithoutPartialPlan(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ContextSelectionRequest, []model.ObjectID)
	}{
		{"duplicate costs", func(r *ContextSelectionRequest, _ []model.ObjectID) { r.Costs = append(r.Costs, r.Costs[0]) }},
		{"missing cost", func(r *ContextSelectionRequest, ids []model.ObjectID) {
			r.Mandatory = []model.ObjectID{ids[0]}
			r.Costs = r.Costs[1:]
		}},
		{"unknown observation", func(r *ContextSelectionRequest, _ []model.ObjectID) {
			r.Costs[0].Observation = model.ObjectID(digestString('e'))
		}},
		{"wrong frontier", func(r *ContextSelectionRequest, _ []model.ObjectID) {
			r.Source.Frontier = model.ObjectID(digestString('e'))
		}},
		{"unfixed cursor", func(r *ContextSelectionRequest, _ []model.ObjectID) { r.Source.Anchor.Cursor = nil }},
		{"unsupported policy", func(r *ContextSelectionRequest, _ []model.ObjectID) { r.PolicyVersion = "best_quality_unversioned" }},
		{"unlabelled estimates", func(r *ContextSelectionRequest, _ []model.ObjectID) { r.CountQuality = "" }},
		{"reserve overflow", func(r *ContextSelectionRequest, _ []model.ObjectID) { r.Capacity.OutputReserve = ^uint64(0) }},
		{"same conclusion competing bundles", func(r *ContextSelectionRequest, ids []model.ObjectID) {
			r.Bundles = []ContextBundle{selectionBundle(ids[0], nil, nil, 1), selectionBundle(ids[0], []model.ObjectID{ids[1]}, nil, 2)}
		}},
		{"forged bundle identity", func(r *ContextSelectionRequest, ids []model.ObjectID) {
			r.Bundles = []ContextBundle{selectionBundle(ids[0], nil, nil, 1)}
			r.Bundles[0].ID = model.ObjectID(digestString('e'))
		}},
		{"contradictory support classification", func(r *ContextSelectionRequest, ids []model.ObjectID) {
			r.Bundles = []ContextBundle{selectionBundle(ids[0], []model.ObjectID{ids[1]}, []model.ObjectID{ids[1]}, 1)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, ids := selectionFixture(t, 3)
			tc.mutate(&req, ids)
			got, err := SelectContext(req)
			if err == nil || !reflect.DeepEqual(got, ContextSelection{}) {
				t.Fatalf("invalid proposal produced a plan: %+v %v", got, err)
			}
		})
	}
}

func TestContextOmissionScopeAndOwnedOutput(t *testing.T) {
	req, ids := selectionFixture(t, 4)
	req.Mandatory = []model.ObjectID{ids[0]}
	req.Capacity.Window = 2
	req.Bundles = []ContextBundle{selectionBundle(ids[1], nil, nil, 0), selectionBundle(ids[2], nil, nil, 1)}
	got, err := SelectContext(req)
	if err != nil || len(got.Omitted) != 3 || got.UsedTokens != 2 {
		t.Fatalf("omissions: %+v %v", got, err)
	}
	reasons := map[model.ObjectID]string{}
	for _, omitted := range got.Omitted {
		reasons[omitted.Observation] = omitted.Reason
	}
	if reasons[ids[1]] != "zero_benefit" || reasons[ids[2]] != "capacity" || reasons[ids[3]] != "not_proposed" {
		t.Fatalf("omission reasons: %+v", reasons)
	}
	got.Source.Anchor.Knowledge.Actor = "mutated"
	got.Source.Anchor.Cursor.Revision = model.RevisionID(digestString('e'))
	if req.Source.Anchor.Knowledge.Actor == "mutated" || req.Source.Anchor.Cursor.Revision == model.RevisionID(digestString('e')) {
		t.Fatal("selection output shares caller's source pointers")
	}
}
