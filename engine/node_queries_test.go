package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func nodeView(t *testing.T, e *engine.Engine, principal model.PrincipalID, anchor *model.SnapshotAnchor) api.ViewDescriptor {
	t.Helper()
	v, err := e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: principal, Anchor: anchor})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref}) })
	return v
}

func TestKnowledgeNodeContentIncludesOnlyAdoptedHistoricalInputs(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	_, first := applyObservation(t, s.e, "node-adopt-one", one)
	key := one.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: first.Fence, Knowledge: &key}
	open := func(a *model.SnapshotAnchor) api.ViewDescriptor {
		t.Helper()
		v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: a, Knowledge: &key, Mode: "execution_knowledge"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}) })
		return v
	}
	v := open(&anchor)
	ack := integrationMutate(t, s.e, integrationRequest("node-other-input", integrationPut([]byte("other-input"), integrationBytes('b'))))
	global := nodeView(t, s.e, "host-principal", nil)
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: global.Ref})
	if err != nil {
		t.Fatal(err)
	}
	var otherPayload model.ObjectID
	for _, item := range entries.Items {
		if string(item.Key) == "other-input" {
			otherPayload = item.Value
		}
	}
	two := knowledgeObservation(otherPayload, ack.Fence, "two", 1)
	applyObservation(t, s.e, "node-adopt-two", two)
	check := func(view api.ViewDescriptor) {
		t.Helper()
		p, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: view.Ref, Layer: "content"})
		if err != nil || len(p.Items) != 4 {
			t.Fatalf("knowledge nodes=%+v err=%v", p, err)
		}
		for _, id := range []model.ObjectID{payload, one.ID} {
			d, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: id}})
			if err != nil || len(d.Data) == 0 || !reflect.DeepEqual(d.Anchor, view.Anchor) {
				t.Fatalf("adopted=%+v err=%v", d, err)
			}
		}
		for _, id := range []model.ObjectID{otherPayload, two.ID} {
			if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: id}}); !errors.Is(err, engine.ErrNodeNotFound) {
				t.Fatalf("unadopted content leaked: %s %v", id, err)
			}
		}
	}
	check(v)
	// Rendering uses the original adopted cut; the immutable packet becomes
	// visible in a later knowledge view, while this View remains unchanged.
	selected, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &v.Anchor})
	if err != nil {
		t.Fatal(err)
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &key, Source: knowledge.SourceAnchor{Anchor: v.Anchor, ViewDigest: v.ViewDigest, Frontier: selected.Frontier}, GoalRef: payload, Renderer: "plain/v1", Tokenizer: "test/v1", Items: []knowledge.ContextItem{{Ordinal: 0, Observation: one.ID, Content: payload, Label: "evidence"}}}
	packet.ID = knowledge.PacketID(packet)
	if _, err = s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("node-render"), Command: knowledge.Command{Packet: &packet}}); err != nil {
		t.Fatal(err)
	}
	latest := open(nil)
	detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: latest.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: packet.ID}})
	if err != nil || detail.Summary.Kind != "ContextPacket" || len(detail.Sources) != 1 || detail.Sources[0].Object != one.ID {
		t.Fatalf("packet=%+v err=%v", detail, err)
	}
	context, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: latest.Ref, Ref: detail.Ref, Mode: "parents"})
	if err != nil || len(context.Sections["parents"]) != 1 || context.Sections["parents"][0].Ref.Object != one.ID {
		t.Fatalf("packet context=%+v err=%v", context, err)
	}
	if _, err = s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Layer: "entities"}); !errors.Is(err, engine.ErrUnsupportedMethod) {
		t.Fatalf("knowledge view expanded global ledger: %v", err)
	}
	check(v)
	s.reopen()
	check(open(&anchor))
}
func nodeRef(kind, id string) model.NodeRef {
	return model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "agent", Kind: model.EntityKind(kind), ID: model.EntityID(id)}}
}

func TestNodesCannotReadFutureMissingOrOtherPrincipalObjects(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	ctx := context.Background()
	integrationMutate(t, s.e, integrationRequest("n-old", integrationPut([]byte("k"), integrationBytes('a'))))
	old := nodeView(t, s.e, "host-principal", nil)
	a, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: old.Ref})
	if err != nil || len(a.Items) != 1 {
		t.Fatalf("entries=%+v %v", a, err)
	}
	integrationMutate(t, s.e, integrationRequest("n-new", integrationPut([]byte("k"), integrationBytes('b'))))
	latest := nodeView(t, s.e, "host-principal", nil)
	b, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: latest.Ref})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []model.ObjectID{b.Items[0].Value, model.ObjectID(strings.Repeat("f", 64))} {
		if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: old.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: id}}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("nonmember object=%s err=%v", id, err)
		}
	}
	if detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: old.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: a.Items[0].Value}}); err != nil || detail.Anchor.Fence != old.Anchor.Fence || len(detail.Data) == 0 {
		t.Fatalf("old data=%+v %v", detail, err)
	}
	task := agentTask("private")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("n-task"), Command: agentprofile.Command{CreateTask: &task}})
	owner := nodeView(t, s.e, "host-principal", nil)
	other := nodeView(t, s.e, "other-principal", nil)
	own, err := s.e.GetNode(ctx, api.GetNodeRequest{View: owner.Ref, Ref: nodeRef("Task", "private")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: other.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: own.Payload}}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("private snapshot bypass=%v", err)
	}
	if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: other.Ref, Ref: own.Ref}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("private entity bypass=%v", err)
	}
	// Query misses and resource denials must not poison the engine.
	if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: owner.Ref, Ref: own.Ref, Limit: api.QueryLimits{MaxBytes: 1}}); engine.ClassifyError(err) != api.ErrorResourceLimit {
		t.Fatalf("limit err=%v", err)
	}
	if _, err := s.e.Snapshot(ctx); err != nil {
		t.Fatalf("query miss fenced store: %v", err)
	}
}

func TestNodeEntitiesLifecycleDetailContextAndBoundPagination(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	ctx := context.Background()
	task := agentTask("task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("nodes-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "exec/with/slash", Task: "task", Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	ack := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("nodes-exec"), Command: agentprofile.Command{Spawn: &x}})
	before := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
	authority := agentprofile.AuthorityBinding{Deployment: "node-test", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "nodes-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: authority, Status: "active", Evidence: model.ObjectID(strings.Repeat("a", 64))}}})
	step := agentprofile.Step{Execution: x.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: before.Fence}
	recoveryCoreControl(t, s, "nodes-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: authority}})
	inv := agentprofile.Invocation{ID: "call", Execution: x.ID, Step: 1, ProviderCallKey: "c", Tool: "read", Status: agentprofile.InvocationPending, Attempt: 1}
	bindTestInvocation(t, s, &inv, authority, "external")
	commitTestInvocation(t, s, "nodes-call", inv, authority)
	published := integrationSnapshot(t, s.e).Fence
	requestID, request := recoveryValueAt(t, s, published, "invocation-source-"+string(x.ID)+"-input")
	definition := testPureToolDefinition(inv.Tool, "test-tool/v1")
	spec := agentprofile.ActivitySpec{Activity: "activity", Execution: x.ID, Step: 1, Invocation: inv.ID, Role: "external_tool", GoalEpoch: 1, InputFence: published, Request: requestID, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: definition.Contract.Recipient, Effect: "pure", ResourceScope: "workspace", Contract: definition.BindEffect(inv.ToolDefinition)}
	query := agentprofile.Query{ID: "q", Session: "session", Space: "host-space", Owner: "host-principal", Input: []byte("question"), Status: "open", Version: 1}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("node-query"), Command: agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &query}}})
	message := agentprofile.Message{ID: "message", Sender: x.ID, Recipient: x.ID, GoalEpoch: 1, Payload: []byte("hello")}
	for i, command := range []agentprofile.ControlCommand{
		{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: authority}},
		{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "attempt", Sequence: 1, Authority: authority}},
		{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "attempt", Authority: authority, Evidence: inv.ToolDefinition}},
		{CaptureResult: &agentprofile.CaptureResultCommand{Result: agentprofile.ActivityResult{Capture: "full", ID: "result", Activity: spec.Activity, Attempt: "attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("actual tool output")}}},
		{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: "result", ExpectedVersion: 1, Authority: authority, Evidence: inv.ToolDefinition}},
		{SendMessage: &agentprofile.SendMessageCommand{Message: message, Authority: authority}},
	} {
		recoveryCoreControl(t, s, "node-life-"+string(rune('a'+i)), command)
	}
	messageAck := integrationMutate(t, s.e, integrationRequest("node-message-content", integrationPut([]byte("node-message-content"), api.Value{Kind: api.ValueBytes, Data: message.Payload})))
	messageContent, _ := recoveryValueAt(t, s, messageAck.Fence, "node-message-content")
	key := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	messageKnown, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil {
		t.Fatal(err)
	}
	messageWork := api.ApplyWorkRequest{Key: agentKey("nodes-consume-message"), Unit: api.WorkUnit{Kind: "message_consume", Steps: []api.WorkStep{
		workControl(t, agentprofile.ControlCommand{ConsumeMessage: &agentprofile.ConsumeMessageCommand{Consumption: agentprofile.MessageConsumption{Message: message.ID, Recipient: x.ID, Incarnation: 1, GoalEpoch: 1}, Authority: authority}}),
		workSeedObservation(t, key, messageContent, messageAck.Fence, uint64(len(messageKnown.Snapshot.Observations))+1, "received message"),
		workCheckpoint(t, x.ID, authority, messageAck.Fence, "nodes-after-message", 1, 1),
	}}}
	workApply(t, s, messageWork)
	body, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("next-node"), integrationBytes('n'))}})
	if err != nil {
		t.Fatal(err)
	}
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}})
	if err != nil {
		t.Fatal(err)
	}
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "pending", Owner: x.ID, Generation: 1, Request: agentKey("nodes-pending-execute"), CommandDigest: agentprofile.ExactRequestDigest(body), InputFence: published, Status: agentprofile.ActionPending}, Method: api.MethodMutateState, ExactCommand: body, Authority: authority}
	continuation := agentprofile.Continuation{Execution: x.ID, Version: 3, Incarnation: 1, PendingKey: "pending", PendingGeneration: 1, InputFence: published, Phase: "resume", Position: &agentprofile.ContinuationPosition{Space: "host-space", Principal: "host-principal", Step: 1, AdapterVersion: "work/v1", KnowledgeFence: known.Fence, KnowledgeCursor: model.Cursor{Store: known.Fence.Store, Space: "host-space", Revision: known.Revision.ID}, KnowledgeKey: model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}, NextActions: []agentprofile.PendingActionRef{{Key: "pending", Generation: 1}}}}
	recoveryCoreControl(t, s, "node-checkpoint", agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: continuation, ExpectedVersion: 2, Authority: authority}})
	v := nodeView(t, s.e, "host-principal", nil)
	page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Limit: 2})
	if err != nil || len(page.Items) != 2 || !page.Truncated || page.Continuation == "" {
		t.Fatalf("first page=%+v err=%v", page, err)
	}
	seen := map[model.NodeRef]bool{}
	for {
		for _, item := range page.Items {
			if seen[item.Ref] {
				t.Fatal("duplicate pagination item")
			}
			seen[item.Ref] = true
		}
		if !page.Truncated {
			break
		}
		page, err = s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Limit: 2, Continuation: page.Continuation})
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []api.ListNodesRequest{{View: v.Ref, EntityKind: "Task", Continuation: first.Continuation}, {View: v.Ref, Layer: "content", Continuation: first.Continuation}, {View: nodeView(t, s.e, "host-principal", nil).Ref, Continuation: first.Continuation}} {
		if _, err = s.e.ListNodes(ctx, r); !errors.Is(err, api.ErrInvalidRequest) {
			t.Fatalf("cross-query token accepted: %v", err)
		}
	}
	for kind, id := range map[string]string{"Task": "task", "Execution": string(x.ID), "Invocation": "call", "Query": "q", "Activity": "activity", "Attempt": "attempt", "ActivityResult": "result", "Message": "message", "Continuation": string(x.ID)} {
		if !seen[nodeRef(kind, id)] {
			t.Fatalf("paginated result omitted %s/%s", kind, id)
		}
		d, err := s.e.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: nodeRef(kind, id)})
		if err != nil || len(d.Data) == 0 || d.Payload == "" || d.Anchor.Fence != v.Anchor.Fence {
			t.Fatalf("kind=%s detail=%+v err=%v", kind, d, err)
		}
	}
	cr := api.NodeContextRequest{View: v.Ref, Ref: nodeRef("Execution", string(x.ID)), Limits: api.QueryLimits{MaxNodes: 2, MaxDepth: 1}}
	got, err := s.e.GetNodeContext(ctx, cr)
	if err != nil || !got.Truncated {
		t.Fatalf("context=%+v err=%v", got, err)
	}
	contextSeen := map[model.NodeRef]bool{}
	for {
		for section, items := range got.Sections {
			if section == "node" {
				continue
			}
			for _, item := range items {
				if contextSeen[item.Ref] {
					t.Fatal("duplicate context page")
				}
				contextSeen[item.Ref] = true
			}
		}
		if !got.Truncated {
			break
		}
		cr.Continuation = got.Continuation
		got, err = s.e.GetNodeContext(ctx, cr)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !contextSeen[nodeRef("Task", "task")] || !contextSeen[nodeRef("Continuation", string(x.ID))] {
		t.Fatalf("missing relationship %+v", contextSeen)
	}
	historical := nodeView(t, s.e, "host-principal", &before)
	old, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: historical.Ref})
	if err != nil || len(old.Items) != 2 {
		t.Fatalf("historical projected latest=%+v err=%v", old, err)
	}
	if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: historical.Ref, Ref: nodeRef("Activity", "activity")}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("future entity leaked: %v", err)
	}
	// Reopening must reproduce the same historical entity cut, not latest.
	_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: historical.Ref})
	s.reopen()
	reopened := nodeView(t, s.e, "host-principal", &before)
	restored, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: reopened.Ref})
	if err != nil || !reflect.DeepEqual(restored.Items, old.Items) {
		t.Fatalf("reopened history=%+v err=%v", restored, err)
	}
}
