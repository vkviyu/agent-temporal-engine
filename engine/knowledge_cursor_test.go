package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestKnowledgeCursorKeepsExactAdoptedPrefixAcrossControlChangesAndReplay(t *testing.T) {
	ctx := context.Background()
	s, oldPayload, source := knowledgeFixture(t)
	key := model.KnowledgeKey{Actor: "same-actor", Execution: "one"}
	empty, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil || empty.Revision == nil || empty.Revision.Sequence != 0 || empty.Found {
		t.Fatalf("empty knowledge position=%+v %v", empty, err)
	}
	one := knowledgeObservation(oldPayload, source, "one", 1)
	_, oneAck := applyObservation(t, s.e, "cursor-observe-one", one)
	first, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil || first.Revision == nil || first.Revision.Sequence != 1 || first.Revision.Parent != empty.Revision.ID {
		t.Fatalf("first position=%+v %v", first, err)
	}
	newAck := integrationMutate(t, s.e, integrationRequest("cursor-new-evidence", integrationPut([]byte("evidence"), integrationBytes('b'))))
	stateView, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: stateView.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatal(err)
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: stateView.Ref})
	newPayload := entries.Items[0].Value
	two := knowledgeObservation(newPayload, newAck.Fence, "one", 2)
	applyObservation(t, s.e, "cursor-observe-two", two)
	latest, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil || latest.Revision == nil || latest.Revision.ID == first.Revision.ID {
		t.Fatalf("head did not advance=%+v %v", latest, err)
	}
	selected := latest.Anchor.Clone()
	selected.Cursor = &model.Cursor{Store: selected.Fence.Store, Space: selected.Space, Revision: first.Revision.ID}
	var digest model.ObjectID
	check := func() {
		t.Helper()
		got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &selected})
		if err != nil || !reflect.DeepEqual(got.Anchor, selected) || got.Revision == nil || *got.Revision != *first.Revision || got.Frontier != first.Frontier || len(got.Snapshot.Observations) != 1 || got.Snapshot.Observations[0].Payload != oldPayload {
			t.Fatalf("knowledge cursor read latest=%+v %v", got, err)
		}
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &selected})
		if err != nil {
			t.Fatal(err)
		}
		defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		if digest != "" && digest != view.ViewDigest {
			t.Fatal("replay changed exact knowledge view identity")
		}
		digest = view.ViewDigest
		if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: newPayload}}); err == nil {
			t.Fatal("knowledge cursor disclosed later evidence")
		}
		if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: oldPayload}}); err != nil {
			t.Fatalf("historical evidence unavailable: %v", err)
		}
	}
	check()
	futureAtOldFence := first.Anchor.Clone()
	futureAtOldFence.Cursor = &model.Cursor{Store: source.Store, Space: "host-space", Revision: latest.Revision.ID}
	wrongExecution := selected.Clone()
	wrongKey := key
	wrongKey.Execution = "two"
	wrongExecution.Knowledge = &wrongKey
	for _, anchor := range []model.SnapshotAnchor{futureAtOldFence, wrongExecution} {
		if _, err = s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor}); !errors.Is(err, model.ErrAnchorMismatch) {
			t.Fatalf("foreign/future cursor accepted: %v", err)
		}
		if _, err = s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "execution_knowledge"}); !errors.Is(err, model.ErrAnchorMismatch) {
			t.Fatalf("foreign/future view accepted: %v", err)
		}
	}
	genesisCursor := first.Anchor.Clone()
	genesisCursor.Cursor = &model.Cursor{Store: source.Store, Space: "host-space", Revision: empty.Revision.ID}
	genesis, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &genesisCursor})
	if err != nil || genesis.Found || genesis.Revision == nil || genesis.Revision.ID != empty.Revision.ID {
		t.Fatalf("deterministic empty cursor=%+v %v", genesis, err)
	}
	if oneAck.Fence != first.Fence {
		t.Fatal("bad fixture")
	}
	s.reopen()
	check()
}

func TestObservationDomainCursorImportsHistoricalValueAndRestrictsEventAncestry(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	task := agentTask("source-cursor-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("source-cursor-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "one", Task: task.ID, Actor: "same-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("source-cursor-exec"), Command: agentprofile.Command{Spawn: &x}})
	old := integrationMutate(t, s.e, integrationRequest("source-cursor-old", integrationPut([]byte("value"), integrationBytes('a'))))
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("source entries=%+v %v", entries, err)
	}
	events, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: view.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil || len(events.Items) != 5 {
		t.Fatalf("source events=%+v %v", events, err)
	}
	// The domain mutation has one core occurrence; Task/Spawn now each also
	// record an attributed Profile occurrence in their own registered stream.
	var sourceEvent model.EventID
	for _, item := range events.Items {
		detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: item.Ref})
		if err != nil {
			t.Fatal(err)
		}
		var event model.Event
		if json.Unmarshal(detail.Data, &event) != nil {
			t.Fatal("invalid typed event")
		}
		if event.Transaction == old.TxID {
			sourceEvent = event.ID
		}
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	graphEntries := []graph.Entry{{Key: entries.Items[0].Key, Value: entries.Items[0].Value}}
	r, err := graph.BuildRevision("host-space", "", nil, old.StateRoot, []model.EventID{sourceEvent}, graphEntries)
	if err != nil {
		t.Fatal(err)
	}
	graphCoreApply(t, s.e, "source-cursor-revision", graph.Command{AddRevision: &graph.AddRevisionCommand{Root: old.StateRoot, Entries: graphEntries, Events: []model.EventID{sourceEvent}}})
	latest := integrationMutate(t, s.e, integrationRequest("source-cursor-new", integrationPut([]byte("value"), integrationBytes('b'))))
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: latest.Fence, Cursor: &model.Cursor{Store: latest.Fence.Store, Space: "host-space", Revision: r.ID}}
	for i, payload := range []model.ObjectID{entries.Items[0].Value, model.ObjectID(sourceEvent)} {
		o := knowledgeObservation(payload, latest.Fence, "one", uint64(i+1))
		o.Source.Anchor = anchor
		o.ID = knowledge.ObservationID(o)
		applyObservation(t, s.e, "source-cursor-adopt-"+string(rune('a'+i)), o)
	}
	// The task event exists at this owner/Fence, but is absent from the selected
	// Revision ancestry and therefore cannot be disclosed through this anchor.
	var unrelated model.EventID
	for _, event := range events.Items {
		if event.Ref.Event != sourceEvent {
			unrelated = event.Ref.Event
			break
		}
	}
	o := knowledgeObservation(model.ObjectID(unrelated), latest.Fence, "one", 3)
	o.Source.Anchor = anchor
	o.ID = knowledge.ObservationID(o)
	if out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("unrelated-event-source-cursor"), Command: knowledge.Command{Observation: &o}}); !errors.Is(err, knowledge.ErrUnknownObservation) || out.Code != api.CodeRejected {
		t.Fatalf("unrelated event imported through domain cursor=%+v %v", out, err)
	}
}

func TestContextPacketAndObservationUseSelectedKnowledgeCursor(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "packet-cursor-one", one)
	state, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := state.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: source.Store, Space: "host-space", Revision: state.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	applyObservation(t, s.e, "packet-cursor-two", knowledgeObservation(payload, source, "one", 2))
	packet := knowledge.ContextPacket{Space: "host-space", Key: &one.Key, Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: view.ViewDigest, Frontier: state.Frontier}, GoalRef: payload, Items: []knowledge.ContextItem{{Observation: one.ID, Content: payload, Label: "old evidence"}}, Renderer: "plain/v1", Tokenizer: "test/v1"}
	packet.ID = knowledge.PacketID(packet)
	req := api.ApplyKnowledgeRequest{Key: agentKey("render-old-knowledge-cursor"), Command: knowledge.Command{Packet: &packet}}
	ack, err := s.e.ApplyKnowledge(ctx, req)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("old-cursor packet rejected=%+v %v", ack, err)
	}
	latest, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil || latest.Revision.Sequence != 2 {
		t.Fatalf("packet advanced knowledge revision=%+v %v", latest, err)
	}
	// Fix the later control fence while selecting the first knowledge revision.
	anchor.Fence = ack.Fence
	past, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil || len(past.Snapshot.Packets) != 1 || len(past.Snapshot.Observations) != 1 {
		t.Fatalf("exact packet source lost=%+v %v", past, err)
	}
	bad := packet
	bad.Items = append([]knowledge.ContextItem(nil), packet.Items...)
	bad.Items[0].Observation = knowledgeObservation(payload, source, "one", 2).ID
	bad.ID = knowledge.PacketID(bad)
	if out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("future-member-old-cursor"), Command: knowledge.Command{Packet: &bad}}); !errors.Is(err, knowledge.ErrUnknownObservation) || out.Code != api.CodeRejected {
		t.Fatalf("later observation leaked through old cursor=%+v %v", out, err)
	}
	s.reopen()
	if retry, err := s.e.ApplyKnowledge(ctx, req); err != nil || retry != ack {
		t.Fatalf("packet cursor retry changed=%+v %v", retry, err)
	}
}
