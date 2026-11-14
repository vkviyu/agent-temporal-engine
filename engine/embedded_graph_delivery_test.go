package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/memory"
)

func TestLoadGraphRestoresOwnedAncestryAndHonorsCurrentWithdrawal(t *testing.T) {
	ctx := context.Background()
	s, base, left, right, current := graphDivergence(t, false)
	graphCoreApply(t, s.e, "load-graph-join", graph.Command{Join: &graph.JoinCommand{Branch: "main", Expected: 1, Left: left.ID, Right: right.ID, ExpectedDomainRoot: current}})
	branch, err := s.e.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "host-space", ID: "main"})
	if err != nil || !branch.Found {
		t.Fatalf("joined head is unavailable: %+v %v", branch, err)
	}
	head, err := s.e.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "host-space", ID: branch.Value.Head})
	if err != nil || !head.Found || head.Value.StateParent != left.ID || len(head.Value.JoinParents) != 1 || head.Value.JoinParents[0] != right.ID {
		t.Fatalf("fixture has no distinct state and join ancestry: %+v %v", head, err)
	}
	loaded, err := s.e.LoadGraph(ctx, "host-space", head.Value.ID, 4)
	if err != nil || loaded == nil {
		t.Fatalf("LoadGraph could not restore ordered ancestry: %v", err)
	}
	for _, want := range []graph.Revision{base, left, right, head.Value} {
		got, err := loaded.Get(want.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("LoadGraph lost revision %s: got=%+v err=%v", want.ID, got, err)
		}
		ancestor, err := loaded.IsAncestor(want.ID, head.Value.ID)
		if err != nil || !ancestor {
			t.Fatalf("LoadGraph lost ancestry %s: ancestor=%v err=%v", want.ID, ancestor, err)
		}
	}
	if _, err := loaded.Fork("caller-only", head.Value.ID); err != nil {
		t.Fatal(err)
	}
	local, err := loaded.AddRevision("host-space", head.Value.ID, nil, head.Value.StateRoot, nil, head.Value.Entries)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := s.e.LoadGraph(ctx, "host-space", head.Value.ID, 4)
	if err != nil || reloaded == nil {
		t.Fatalf("LoadGraph reload failed after caller mutation: %v", err)
	}
	if _, err := reloaded.Branch("caller-only"); !errors.Is(err, graph.ErrUnknownBranch) {
		t.Fatalf("caller branch changed subsequent LoadGraph: %v", err)
	}
	if _, err := reloaded.Get(local.ID); !errors.Is(err, graph.ErrUnknownRevision) {
		t.Fatalf("caller revision changed subsequent LoadGraph: %v", err)
	}
	if limited, err := s.e.LoadGraph(ctx, "host-space", head.Value.ID, 3); limited != nil || !errors.Is(err, graph.ErrLimit) {
		t.Fatalf("LoadGraph exposed a partial bounded graph: store=%v err=%v", limited, err)
	}
	if len(left.Entries) != 1 {
		t.Fatal("fixture has no unambiguous source entry")
	}
	applyMemory(t, s.e, "load-graph-withdraw-source", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: left.Entries[0].Value, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "withdraw a source entry from graph disclosure"}}})
	if _, err := s.e.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "host-space", ID: head.Value.ID}); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatalf("graph revision disclosed the withdrawn source: %v", err)
	}
	if denied, err := s.e.LoadGraph(ctx, "host-space", head.Value.ID, 4); denied != nil || !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatalf("LoadGraph disclosed the withdrawn source: store=%v err=%v", denied, err)
	}
	s.reopen()
	if denied, err := s.e.LoadGraph(ctx, "host-space", head.Value.ID, 4); denied != nil || !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatalf("LoadGraph lost source withdrawal after reopen: store=%v err=%v", denied, err)
	}
}
