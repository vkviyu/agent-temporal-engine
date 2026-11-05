package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestExplicitKnowledgeImportSeedsFreshExecutionFromSelectedPastAndReplays(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "import-origin-one", one)
	first, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := first.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: source.Store, Space: "host-space", Revision: first.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	// The target did not exist at the source Fence; seeding must authenticate
	// its current ownership without pretending it existed in the past.
	target := model.KnowledgeKey{Actor: "fresh-actor", Execution: "fresh"}
	x := agentprofile.Execution{ID: target.Execution, Task: "kt", Actor: target.Actor, Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("import-fresh-execution"), Command: agentprofile.Command{Spawn: &x}})
	two := knowledgeObservation(payload, source, "one", 2)
	applyObservation(t, s.e, "import-origin-two", two)
	before := integrationSnapshot(t, s.e)
	empty, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &target})
	if err != nil || empty.Found || empty.Revision.Sequence != 0 {
		t.Fatalf("fresh execution implicitly inherited=%+v %v", empty, err)
	}
	c := knowledge.ImportCommand{Target: target, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: first.Frontier, ViewDigest: view.ViewDigest}, Observations: []model.ObjectID{one.ID}}
	req := api.ApplyKnowledgeRequest{Key: agentKey("seed-fresh-execution"), Command: knowledge.Command{Import: &c}}
	ack, err := s.e.ApplyKnowledge(ctx, req)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("explicit seed rejected=%+v %v", ack, err)
	}
	check := func() {
		t.Helper()
		got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &target})
		if err != nil || len(got.Snapshot.Observations) != 1 || got.Revision.Sequence != 1 {
			t.Fatalf("seed target=%+v %v", got, err)
		}
		item := got.Snapshot.Observations[0]
		if item.ImportedFrom != one.ID || item.Payload != payload || !reflect.DeepEqual(item.Source.Anchor, anchor) || item.Key != target {
			t.Fatalf("source cut/provenance lost=%+v", item)
		}
		origin, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
		if err != nil || len(origin.Snapshot.Observations) != 2 || origin.Revision.Sequence != 2 {
			t.Fatalf("import mutated origin=%+v %v", origin, err)
		}
	}
	check()
	bad := c
	bad.Observations = []model.ObjectID{two.ID}
	bad.Target = model.KnowledgeKey{Actor: "same-actor", Execution: "two"}
	if out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("import-future-member"), Command: knowledge.Command{Import: &bad}}); !errors.Is(err, knowledge.ErrUnknownObservation) || out.Code != api.CodeRejected {
		t.Fatalf("future source imported=%+v %v", out, err)
	}
	if out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("import-stale-target-cas"), Command: knowledge.Command{Import: &c}}); !errors.Is(err, knowledge.ErrObservationSequence) || out.Code != api.CodeRejected {
		t.Fatalf("stale target imported twice=%+v %v", out, err)
	}
	if retry, err := s.e.ApplyKnowledge(ctx, req); err != nil || retry != ack {
		t.Fatalf("import receipt changed=%+v %v", retry, err)
	}
	if now := integrationSnapshot(t, s.e); now.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("denial/retry advanced ledger")
	}
	s.reopen()
	check()
	if retry, err := s.e.ApplyKnowledge(ctx, req); err != nil || retry != ack {
		t.Fatalf("replayed seed receipt changed=%+v %v", retry, err)
	}
}
