package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestFormalEventsAtomicOccurrenceHistoryAndRecovery(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true, AgentProfile: true})
	before := integrationSnapshot(t, s.e)
	view0, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	r := integrationRequest("event-one", integrationPut([]byte("a"), integrationBytes('1')))
	one := integrationMutate(t, s.e, r)
	view1, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	two := integrationMutate(t, s.e, integrationRequest("event-two", integrationPut([]byte("a"), integrationBytes('2'))))
	list := func(v model.ViewRef) []api.NodeSummary {
		t.Helper()
		p, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: v, Layer: "events", EntityKind: "Event"})
		if err != nil {
			t.Fatal(err)
		}
		return p.Items
	}
	if got := list(view0.Ref); len(got) != 0 {
		t.Fatal("future events leaked into genesis view")
	}
	if got := list(view1.Ref); len(got) != 1 {
		t.Fatalf("cut contained %d events", len(got))
	}
	other, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "another-owner"})
	if err != nil {
		t.Fatal(err)
	}
	if got := list(other.Ref); len(got) != 0 {
		t.Fatal("another principal read audit stream")
	}
	latest, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	var firstID, secondID model.EventID
	for _, n := range list(latest.Ref) {
		d, err := s.e.GetNode(ctx, api.GetNodeRequest{View: latest.Ref, Ref: n.Ref})
		if err != nil {
			t.Fatal(err)
		}
		var ev model.Event
		if json.Unmarshal(d.Data, &ev) != nil {
			t.Fatal("missing typed Event detail")
		}
		if ev.RecordedBy != "host-principal" || ev.Stream.Owner != "host-principal" {
			t.Fatal("untrusted event owner")
		}
		switch ev.Sequence {
		case 1:
			firstID = ev.ID
			if ev.Transaction != one.TxID || len(ev.CausalParents) != 0 {
				t.Fatal("first occurrence proof changed")
			}
		case 2:
			secondID = ev.ID
			if ev.Transaction != two.TxID || len(ev.CausalParents) != 1 {
				t.Fatal("second occurrence proof changed")
			}
		default:
			t.Fatal("non-contiguous event stream")
		}
	}
	if firstID == "" || secondID == "" || firstID == secondID {
		t.Fatal("occurrence identity missing")
	}
	if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view1.Ref, Ref: model.NodeRef{Kind: model.NodeEvent, Event: secondID}}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatal("old view read future event")
	}
	if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: other.Ref, Ref: model.NodeRef{Kind: model.NodeEvent, Event: firstID}}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatal("principal guessed another EventID")
	}
	if again := integrationMutate(t, s.e, r); again != one {
		t.Fatal("receipt retry created another occurrence")
	}
	for _, v := range []model.ViewRef{view0.Ref, view1.Ref, other.Ref, latest.Ref} {
		if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v}); err != nil {
			t.Fatal(err)
		}
	}
	s.reopen()
	past := model.SnapshotAnchor{Space: "host-space", Fence: one.Fence}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &past})
	if err != nil {
		t.Fatal(err)
	}
	if got := list(v.Ref); len(got) != 1 || got[0].Ref.Event != firstID {
		t.Fatal("replay changed event cut/identity")
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	if before.Fence.LSN != 0 {
		t.Fatal("bad fixture")
	}
}

func TestGraphRevisionMayReferenceOnlyAuthenticatedFormalEvents(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	ack := integrationMutate(t, s.e, integrationRequest("event-proof", integrationPut([]byte("x"), integrationBytes('p'))))
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil || len(events.Items) != 1 {
		t.Fatalf("events=%+v %v", events, err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil {
		t.Fatal(err)
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	state := make([]graph.Entry, len(entries.Items))
	for i, x := range entries.Items {
		state[i] = graph.Entry{Key: x.Key, Value: x.Value}
	}
	r := api.ApplyGraphRequest{Key: agentKey("reference-event"), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{Root: ack.StateRoot, Entries: state, Events: []model.EventID{events.Items[0].Ref.Event}}}}
	out, err := s.e.ApplyGraph(ctx, r)
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("formal occurrence rejected: %+v %v", out, err)
	}
	graphView, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	typed, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: graphView.Ref, Layer: "events", EntityKind: "Event", EventOperation: "Graph.AddRevision"})
	if err != nil || len(typed.Items) != 1 {
		t.Fatalf("typed graph event operation filter failed: %+v %v", typed, err)
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: graphView.Ref})
	r.Key = agentKey("unknown-event")
	r.Command.AddRevision.Events = []model.EventID{model.EventID(integrationID('e'))}
	if out, err = s.e.ApplyGraph(ctx, r); err == nil || out.Code != api.CodeRejected {
		t.Fatalf("guessed EventID accepted=%+v %v", out, err)
	}
}

func TestKnowledgeAdoptsFormalEventOnlyFromPublishedOwnerCut(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	task := agentTask("event-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "event-exec", Task: task.ID, Actor: "event-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	ack := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("spawn"), Command: agentprofile.Command{Spawn: &x}})
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil || len(events.Items) != 4 {
		t.Fatalf("events=%+v %v", events, err)
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	o := knowledge.Observation{Key: model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}}, Payload: model.ObjectID(events.Items[0].Ref.Event), Sequence: 1, Label: "adopted audit evidence"}
	o.ID = knowledge.ObservationID(o)
	if a, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("adopt-event"), Command: knowledge.Command{Observation: &o}}); err != nil || a.Code != api.CodeDurableAck {
		t.Fatalf("Event source adoption=%+v %v", a, err)
	}
	kv, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Knowledge: &o.Key})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: kv.Ref})
	p, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: kv.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil || len(p.Items) != 1 || p.Items[0].Ref.Event != model.EventID(o.Payload) {
		t.Fatalf("knowledge events=%+v %v", p, err)
	}
}
