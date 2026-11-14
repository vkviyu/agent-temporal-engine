package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestCoreCompactionRetainsFiveRootsHistoryEventsAndExactReceipts(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true, Journal: storage.JournalOptions{SegmentTargetBytes: 1024}})
	task := agentTask("compact-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("compact-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "compact-exec", Task: task.ID, Actor: "compact-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("compact-spawn"), Command: agentprofile.Command{Spawn: &x}})
	req := integrationRequest("compact-source", integrationPut([]byte("source"), integrationBytes('a')))
	ack := integrationMutate(t, s.e, req)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatal(entries, err)
	}
	payload := entries.Items[0].Value
	r := graphRevisionFromCurrent(t, s, "compact-revision", "")
	graphCoreApply(t, s.e, "compact-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r.ID}})
	k := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	o := knowledge.Observation{Key: k, Source: knowledge.SourceAnchor{Anchor: anchor}, Payload: payload, Sequence: 1, Label: "retain source"}
	o.ID = knowledge.ObservationID(o)
	kr, ka := applyObservation(t, s.e, "compact-observation", o)
	kaAnchor := model.SnapshotAnchor{Space: "host-space", Fence: ka.Fence, Knowledge: &k}
	kv, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &kaAnchor})
	if err != nil {
		t.Fatal(err)
	}
	kp, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: kv.Ref, Layer: "content"})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("retain independent provenance"), Sources: []memory.Source{{SourceID: payload, Anchor: anchor}}}
	applyMemory(t, s.e, "compact-memory", memory.Command{PutCandidate: &m})
	before := integrationSnapshot(t, s.e)
	for _, root := range []model.ObjectID{before.Roots.DomainStates, before.Roots.ExecutionKnowledge, before.Roots.BranchRefs, before.Roots.LiveControl, before.Roots.CommandReceipts} {
		if root == "" {
			t.Fatal("fixture did not cover five roots")
		}
	}
	report, err := s.e.CompactAuditFull(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.RetiredSegments < 2 || report.PinnedSegments == 0 || len(report.ReclaimedSegments)+report.PinnedSegments != report.RetiredSegments || report.After.Fence() != before.Fence {
		t.Fatalf("incomplete physical replacement: %+v", report)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("compaction changed semantic snapshot")
	}
	if got, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref}); err != nil || !reflect.DeepEqual(got, entries) {
		t.Fatalf("active historical view changed: %+v %v", got, err)
	}
	if got, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: kv.Ref, Layer: "content"}); err != nil || !reflect.DeepEqual(got, kp) {
		t.Fatalf("knowledge view changed: %+v %v", got, err)
	}
	for _, v := range []model.ViewRef{view.Ref, kv.Ref} {
		if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v}); err != nil {
			t.Fatal(err)
		}
	}
	removed, pinned, err := s.e.ReclaimRetired(ctx)
	if err != nil || pinned != 0 || len(removed) != report.PinnedSegments {
		t.Fatalf("closed Views retained layout pins: removed=%v pinned=%d err=%v", removed, pinned, err)
	}
	for i := 0; i < 2; i++ {
		if i == 1 {
			s.reopen()
		}
		if got := integrationMutate(t, s.e, req); got != ack {
			t.Fatal("source receipt changed")
		}
		if got, err := s.e.ApplyKnowledge(ctx, kr); err != nil || got != ka {
			t.Fatal("knowledge receipt changed", got, err)
		}
		if got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &kaAnchor}); err != nil || len(got.Snapshot.Observations) != 1 {
			t.Fatal("knowledge source lost", got, err)
		}
		if after := integrationSnapshot(t, s.e); after != before {
			t.Fatal("reopen/retry changed roots or history")
		}
	}
	next := integrationMutate(t, s.e, integrationRequest("compact-next", integrationPut([]byte("after"), integrationBytes('b'))))
	if next.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("layout generation leaked into logical transaction sequence")
	}
	if _, err = s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal("repeated compaction", err)
	}
	s.reopen()
	if got := integrationMutate(t, s.e, req); got != ack {
		t.Fatal("old receipt lost after second layout")
	}
}
