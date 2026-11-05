package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func recoveryValueAt(t *testing.T, s *integrationStore, fence model.CommitFence, key string) (model.ObjectID, []byte) {
	t.Helper()
	ctx := context.Background()
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: fence}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	items, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil {
		t.Fatal(err)
	}
	value, err := s.e.GetViewState(ctx, api.ReadViewStateRequest{View: v.Ref, Key: []byte(key)})
	if err != nil || !value.Found || value.Value.Kind != api.ValueBytes {
		t.Fatalf("missing exact input %s: %+v %v", key, value, err)
	}
	for _, item := range items.Items {
		if string(item.Key) == key {
			return item.Value, value.Value.Data
		}
	}
	t.Fatal("value is not in authenticated entries")
	return "", nil
}

// Every restart below loses all transient render output and acknowledgement
// state. The host recovers the next exact command from Checkpoint, reconciles
// the original RequestKey, and uses the producer's own receipt fence. The test
// intentionally changes latest knowledge before rendering and retries only the
// pure renderer, never a published Observation, recorded input, or Packet.
func TestCheckpointPendingRenderRecoversOriginalKnowledgeAndRecordedInput(t *testing.T) {
	ctx := context.Background()
	s, payload, originalSource := knowledgeFixture(t)
	proof := model.ObjectID(integrationID('a'))
	binding := agentprofile.AuthorityBinding{Deployment: "renderer-host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "render-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Status: "active", Binding: binding, Evidence: proof}}})
	var continuationVersion uint64
	checkpoint := func(name, phase, method string, body []byte, fence model.CommitFence, inputs ...model.ObjectID) model.RequestKey {
		t.Helper()
		key := agentKey(name + "-execute")
		intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: name, Generation: 1, Request: key, CommandDigest: agentprofile.ExactRequestDigest(body), InputFence: fence, Owner: "one", Status: agentprofile.ActionPending}, Method: method, ExactCommand: body, Authority: binding, InputObjects: inputs}
		continuation := agentprofile.Continuation{Execution: "one", Version: continuationVersion + 1, Incarnation: binding.Incarnation, PendingKey: name, PendingGeneration: 1, InputFence: fence, Phase: phase, Position: &agentprofile.ContinuationPosition{Space: "host-space", Principal: "host-principal", Step: 0, AdapterVersion: "render-host/v1", NextActions: []agentprofile.PendingActionRef{{Key: name, Generation: 1}}}}
		bindTestContinuationKnowledge(t, s, &continuation, model.KnowledgeKey{Actor: "same-actor", Execution: "one"}, fence)
		recoveryCoreControl(t, s, name+"-checkpoint", agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: continuation, ExpectedVersion: continuationVersion, Authority: binding}})
		continuationVersion++
		return key
	}
	takeover := func() {
		t.Helper()
		read, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
		if err != nil {
			t.Fatal(err)
		}
		var authority agentprofile.ExecutionAuthority
		for _, current := range read.Snapshot.Lifecycle.Recovery.Authorities {
			if current.Execution == "one" {
				authority = current
			}
		}
		name := fmt.Sprintf("takeover-%d", binding.Incarnation)
		recoveryCoreControl(t, s, name+"-fence", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "one", ExpectedVersion: authority.Version, Evidence: proof}})
		next := binding
		next.OwnerEpoch++
		next.Incarnation++
		next.Holder = name
		recoveryCoreControl(t, s, name+"-resume", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: "one", ExpectedIncarnation: binding.Incarnation, ExpectedAuthorityVersion: authority.Version + 1, Next: next, AdapterVersion: "render-host/v1", Checkpoint: payload, CompatibilityEvidence: proof, ResourceFenceEvidence: proof}})
		binding = next
	}
	recoverApplied := func(name string, expectedBody []byte) api.CommitResult {
		t.Helper()
		read, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
		if err != nil {
			t.Fatal(err)
		}
		var intent agentprofile.ActionIntent
		for _, value := range read.Snapshot.Lifecycle.Recovery.Actions {
			if value.Action.Key == name {
				intent = value
			}
		}
		if intent.Action.Key == "" || !bytes.Equal(intent.ExactCommand, expectedBody) {
			t.Fatal("recovery replaced the original command with latest configuration")
		}
		lookup, err := s.e.LookupRequest(ctx, api.LookupRequestRequest{Space: intent.Action.Request.Space, Principal: intent.Action.Request.Principal, RequestID: intent.Action.Request.RequestID})
		if err != nil || lookup.Code != api.CodeDurableAck {
			t.Fatalf("original producer receipt is unavailable: %+v %v", lookup, err)
		}
		recoveryCoreControl(t, s, name+"-resolved", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: name, Generation: 1, Outcome: "applied", ObservedFence: lookup.ObservedFence, Receipt: lookup.Result.ResultDigest}}})
		return lookup.Result
	}
	execute := func(key model.RequestKey, method string, body []byte) api.CommitResult {
		t.Helper()
		ack, err := s.e.Commit(ctx, api.CommitRequest{Space: key.Space, Principal: key.Principal, RequestID: key.RequestID, Method: method, Body: body})
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatalf("execute exact intent: %+v %v", ack, err)
		}
		return ack
	}
	o := knowledgeObservation(payload, originalSource, "one", 1)
	observeBody, err := knowledge.EncodeCommand(knowledge.Command{Observation: &o})
	if err != nil {
		t.Fatal(err)
	}
	observeKey := checkpoint("observe", "adopt-observation", api.MethodApplyKnowledge, observeBody, originalSource, payload)
	originalAck := execute(observeKey, api.MethodApplyKnowledge, observeBody)
	// No acknowledgement is consumed by the host. New knowledge meanwhile
	// changes the latest frontier and must not enter the original render.
	newSource := integrationMutate(t, s.e, integrationRequest("newer-evidence", integrationPut([]byte("newer-evidence"), integrationBytes('b'))))
	newPayload, _ := recoveryValueAt(t, s, newSource.Fence, "newer-evidence")
	applyObservation(t, s.e, "newer-observation", knowledgeObservation(newPayload, newSource.Fence, "one", 2))
	s.reopen()
	takeover()
	producer := recoverApplied("observe", observeBody)
	if producer != originalAck {
		t.Fatal("original observation receipt changed")
	}
	key := o.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: producer.Fence, Knowledge: &key}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil || len(selected.Snapshot.Observations) != 1 {
		t.Fatalf("source is no longer the original frontier: %+v %v", selected, err)
	}
	source := knowledge.SourceAnchor{Anchor: anchor, ViewDigest: v.ViewDigest, Frontier: selected.Frontier}
	if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	type renderSeed struct {
		Source                        knowledge.SourceAnchor
		Renderer, Tokenizer, Encoding string
	}
	seed := renderSeed{Source: source, Renderer: "plain/v1", Tokenizer: "exact/v1", Encoding: "framed-model-request/v1"}
	seedBytes, _ := json.Marshal(seed)
	seedBody, _ := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("render-seed"), api.Value{Kind: api.ValueBytes, Data: seedBytes})}})
	seedKey := checkpoint("render-seed", "pending-render", api.MethodMutateState, seedBody, producer.Fence, payload)
	execute(seedKey, api.MethodMutateState, seedBody)
	s.reopen()
	takeover()
	seedAck := recoverApplied("render-seed", seedBody)
	seedID, storedSeed := recoveryValueAt(t, s, seedAck.Fence, "render-seed")
	if !bytes.Equal(storedSeed, seedBytes) {
		t.Fatal("pending renderer source or configuration changed")
	}
	activity := agentprofile.ActivitySpec{Activity: "render", Execution: "one", Role: "resource_prepare", GoalEpoch: 1, InputFence: seedAck.Fence, Request: seedID, RequestDigest: agentprofile.ExactRequestDigest(seedBytes), Recipient: "renderer/v1", Effect: "pure", ResourceScope: "memory"}
	create := agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: binding}}}
	createBody, _ := agentprofile.EncodeCommand(create)
	createKey := checkpoint("create-render", "render", api.MethodApplyAgent, createBody, seedAck.Fence, seedID)
	execute(createKey, api.MethodApplyAgent, createBody)
	recoverApplied("create-render", createBody)
	claim := func(id string, sequence uint64) {
		t.Helper()
		recoveryCoreControl(t, s, id+"-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Attempt: id, Activity: activity.Activity, Sequence: sequence, Authority: binding}})
		recoveryCoreControl(t, s, id+"-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: id, Authority: binding, Evidence: proof}})
	}
	render := func() []byte {
		t.Helper()
		var frozen renderSeed
		if err := json.Unmarshal(storedSeed, &frozen); err != nil || frozen.Renderer != "plain/v1" {
			t.Fatal("unsupported frozen renderer")
		}
		read, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &frozen.Source.Anchor})
		if err != nil || len(read.Snapshot.Observations) != 1 || read.Snapshot.Observations[0].ID != o.ID || read.Frontier != frozen.Source.Frontier {
			t.Fatal("renderer consulted latest knowledge")
		}
		_, value := recoveryValueAt(t, s, frozen.Source.Anchor.Fence, "evidence")
		return []byte(fmt.Sprintf("system: summarize exact source\nuser: %s\nsource-lsn: %d\n", value, frozen.Source.Anchor.Fence.LSN))
	}
	claim("render-attempt-1", 1)
	firstRender := render()
	s.reopen() // pure renderer output was never durably captured
	takeover()
	recoveryCoreControl(t, s, "retry-pure-render", agentprofile.ControlCommand{RetryActivity: &agentprofile.RetryActivityCommand{Activity: activity.Activity, ExpectedVersion: 1, Authority: binding}})
	claim("render-attempt-2", 2)
	rendered := render()
	if !bytes.Equal(firstRender, rendered) {
		t.Fatal("pure retry changed the exact source prompt")
	}
	result := agentprofile.ActivityResult{ID: "render-result", Activity: activity.Activity, Attempt: "render-attempt-2", RequestDigest: activity.RequestDigest, Incarnation: binding.Incarnation, Outcome: "success", Payload: rendered}
	recoveryCoreControl(t, s, "render-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreControl(t, s, "render-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: activity.Activity, Result: result.ID, ExpectedVersion: 2, Authority: binding, Evidence: proof}})
	inputBody, _ := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("recorded-input"), api.Value{Kind: api.ValueBytes, Data: rendered})}})
	inputKey := checkpoint("record-input", "store-input", api.MethodMutateState, inputBody, seedAck.Fence, seedID)
	execute(inputKey, api.MethodMutateState, inputBody)
	s.reopen()
	takeover()
	inputAck := recoverApplied("record-input", inputBody)
	inputID, exactInput := recoveryValueAt(t, s, inputAck.Fence, "recorded-input")
	if !bytes.Equal(exactInput, rendered) {
		t.Fatal("recorded input changed after acknowledgement loss")
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &key, Source: source, GoalRef: payload, Renderer: seed.Renderer, Tokenizer: seed.Tokenizer, Items: []knowledge.ContextItem{{Ordinal: 0, Observation: o.ID, Content: payload, Label: "original evidence"}}, Input: &knowledge.RecordedInput{Object: inputID, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: inputAck.Fence}, Encoding: seed.Encoding}}
	packet.ID = knowledge.PacketID(packet)
	packetBody, err := knowledge.EncodeCommand(knowledge.Command{Packet: &packet})
	if err != nil {
		t.Fatal(err)
	}
	packetKey := checkpoint("publish-packet", "publish-context-packet", api.MethodApplyKnowledge, packetBody, inputAck.Fence, inputID)
	execute(packetKey, api.MethodApplyKnowledge, packetBody)
	s.reopen()
	packetAck := recoverApplied("publish-packet", packetBody)
	final, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil || len(final.Snapshot.Observations) != 2 || len(final.Snapshot.Packets) != 1 || final.Snapshot.Packets[0].ID != packet.ID || final.Snapshot.Packets[0].Input == nil || final.Snapshot.Packets[0].Input.Object != inputID || final.Snapshot.Packets[0].Source.Anchor.Fence != producer.Fence || final.Fence.LSN <= packetAck.Fence.LSN {
		t.Fatalf("recovered final context packet=%+v %v", final, err)
	}
}
