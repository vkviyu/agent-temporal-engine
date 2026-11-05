package engine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func goalCoreTask(t *testing.T, s *integrationStore, id model.EntityID) agentprofile.Task {
	t.Helper()
	state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range state.Snapshot.Tasks {
		if task.ID == id {
			return task
		}
	}
	t.Fatal("goal task missing", id)
	return agentprofile.Task{}
}

func goalCoreImage(t *testing.T, s *integrationStore, id string, task agentprofile.Task) (model.ObjectID, model.CommitFence, []byte) {
	t.Helper()
	raw, err := agentprofile.GoalKnowledge(task)
	if err != nil {
		t.Fatal(err)
	}
	ack := integrationMutate(t, s.e, integrationRequest(id, integrationPut([]byte(id), api.Value{Kind: api.ValueBytes, Data: raw})))
	v := nodeView(t, s.e, "host-principal", nil)
	entries, err := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries.Items {
		if string(entry.Key) == id {
			return entry.Value, ack.Fence, raw
		}
	}
	t.Fatal("goal image missing", id)
	return "", model.CommitFence{}, nil
}

func goalCoreObservation(t *testing.T, s *integrationStore, id string, key model.KnowledgeKey, payload model.ObjectID, source model.CommitFence) knowledge.Observation {
	t.Helper()
	state, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil {
		t.Fatal(err)
	}
	var sequence uint64
	for _, o := range state.Snapshot.Observations {
		if o.Sequence > sequence {
			sequence = o.Sequence
		}
	}
	o := knowledge.Observation{Key: key, Sequence: sequence + 1, Payload: payload, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}}, Label: "canonical task goal"}
	o.ID = knowledge.ObservationID(o)
	applyObservation(t, s.e, id, o)
	return o
}

func goalCorePacket(t *testing.T, s *integrationStore, id string, key model.KnowledgeKey, goal knowledge.Observation, items []knowledge.Observation, input model.ObjectID, inputFence model.CommitFence) (knowledge.ContextPacket, api.CommitResult) {
	t.Helper()
	ctx := context.Background()
	state, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &state.Anchor})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
			t.Error(err)
		}
	}()
	packet := knowledge.ContextPacket{Space: "host-space", Key: &key, GoalRef: goal.Payload, Source: knowledge.SourceAnchor{Anchor: state.Anchor, Frontier: state.Frontier, ViewDigest: v.ViewDigest}, Renderer: "goal-test/v1", Tokenizer: "test/v1", Input: &knowledge.RecordedInput{Object: input, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: inputFence}, Encoding: "application/ate-goal-v1"}}
	for i, o := range items {
		packet.Items = append(packet.Items, knowledge.ContextItem{Ordinal: uint64(i), Observation: o.ID, Content: o.Payload, Label: "goal constraint"})
	}
	// Derive structural requirements at the packet cut and match the images
	// against the receiving execution's adopted content. An imported observation
	// carries its origin's anchor, which belongs to a different knowledge view.
	controlCut := model.SnapshotAnchor{Space: state.Anchor.Space, Fence: state.Anchor.Fence}
	agentState, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &controlCut})
	if err != nil {
		t.Fatal(err)
	}
	images, err := agentprofile.ContextRequirements(agentState.Snapshot, key.Execution)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) > 0 {
		facts := make([]knowledge.ContextRequirement, 0, len(images))
		for _, image := range images {
			for _, o := range items {
				if o.Key != key || o.PayloadKind != "" {
					continue
				}
				content, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: v.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: o.Payload}, Projection: "value_bytes"})
				if errors.Is(err, engine.ErrUnsupportedMethod) {
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if content.EOF && bytes.Equal(content.Data, image.Bytes) {
					facts = append(facts, knowledge.ContextRequirement{Kind: image.Kind, ID: image.ID, Observation: o.ID, Content: o.Payload})
					break
				}
			}
		}
		if len(facts) > 0 {
			packet.Requirements = &knowledge.ContextRequirements{RulesVersion: agentprofile.ContextRequirementsVersion, HostPolicyVersion: "goal-test/v1", Facts: facts}
		}
	}
	packet.ID = knowledge.PacketID(packet)
	ack, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey(id), Command: knowledge.Command{Packet: &packet}})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("%s packet=%+v %v", id, ack, err)
	}
	return packet, ack
}

func rejectGoalInput(t *testing.T, s *integrationStore, id string, command agentprofile.ControlCommand) {
	t.Helper()
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey(id), Command: agentprofile.Command{Control: &command}})
	if out.Code != api.CodeRejected || !errors.Is(err, agentprofile.ErrGoalEpoch) || out.Fence != (model.CommitFence{}) {
		t.Fatalf("%s: expected unfenced goal rejection, got %+v %v", id, out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatalf("%s: goal rejection changed publication", id)
	}
}

func TestChangedGoalRequiresActualAdoptionAndNewPacketInput(t *testing.T) {
	ctx := context.Background()
	s, _, _ := knowledgeFixture(t)
	proof := model.ObjectID(integrationID('a'))
	authority := agentprofile.AuthorityBinding{Deployment: "goal-host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "goal-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: authority, Status: "active", Evidence: proof}}})
	key := model.KnowledgeKey{Actor: "same-actor", Execution: "one"}
	task := goalCoreTask(t, s, "kt")
	oldPayload, oldSource, oldRaw := goalCoreImage(t, s, "goal-one-image", task)
	oldObservation := goalCoreObservation(t, s, "goal-one-observe", key, oldPayload, oldSource)
	oldPacket, oldPacketAck := goalCorePacket(t, s, "goal-one-packet", key, oldObservation, []knowledge.Observation{oldObservation}, oldPayload, oldSource)
	oldActivity := agentprofile.ActivitySpec{Activity: "old-goal-model", Execution: "one", Role: "aux_summary", GoalEpoch: 1, InputFence: oldPacketAck.Fence, ContextPacket: oldPacket.ID, Request: oldPayload, RequestDigest: agentprofile.ExactRequestDigest(oldRaw), Recipient: "model", Effect: "pure", ResourceScope: "session"}
	recoveryCoreControl(t, s, "goal-one-model", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: oldActivity, Authority: authority}})
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("goal-rebase"), Command: agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: "kt", ExpectedEpoch: 1, NewGoal: "new goal constraints"}}})
	step := agentprofile.Step{Execution: "one", Sequence: 1, GoalEpoch: 2, Status: agentprofile.StepOpen, InputAnchor: oldPacketAck.Fence}
	rejectGoalInput(t, s, "goal-unobserved-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: authority}})
	observe := agentprofile.ObserveGoalCommand{Execution: "one", Epoch: 2, Evidence: proof, Authority: authority, Observation: model.ObjectID(integrationID('f'))}
	rejectGoalInput(t, s, "goal-undeclared-observation", agentprofile.ControlCommand{ObserveGoal: &observe})
	observe.Observation = oldObservation.ID
	rejectGoalInput(t, s, "goal-old-image", agentprofile.ControlCommand{ObserveGoal: &observe})
	task = goalCoreTask(t, s, "kt")
	payload, source, raw := goalCoreImage(t, s, "goal-two-image", task)
	observe.Observation = payload
	rejectGoalInput(t, s, "goal-content-is-not-adoption", agentprofile.ControlCommand{ObserveGoal: &observe})
	otherKey := key
	otherKey.Execution = "two"
	other := goalCoreObservation(t, s, "goal-two-other-exec", otherKey, payload, source)
	observe.Observation = other.ID
	rejectGoalInput(t, s, "goal-other-execution-adoption", agentprofile.ControlCommand{ObserveGoal: &observe})
	adopted := goalCoreObservation(t, s, "goal-two-adopt", key, payload, source)
	observe.Observation = adopted.ID
	recoveryCoreControl(t, s, "goal-two-acknowledge", agentprofile.ControlCommand{ObserveGoal: &observe})
	// A genuine new-goal observation does not upgrade a declared old input cut.
	rejectGoalInput(t, s, "goal-observed-but-old-step-cut", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: authority}})
	oldActivity.Activity = "old-packet-after-goal-two"
	oldActivity.GoalEpoch = 2
	rejectGoalInput(t, s, "goal-observed-but-old-packet", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: oldActivity, Authority: authority}})
	missing, missingAck := goalCorePacket(t, s, "goal-two-missing-item-packet", key, adopted, []knowledge.Observation{oldObservation}, payload, source)
	activity := oldActivity
	activity.Activity = "new-goal-without-item"
	activity.ContextPacket = missing.ID
	activity.InputFence = missingAck.Fence
	activity.Request = payload
	activity.RequestDigest = agentprofile.ExactRequestDigest(raw)
	rejectGoalInput(t, s, "goal-declared-but-not-in-input", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: authority}})
	packet, packetAck := goalCorePacket(t, s, "goal-two-exact-packet", key, adopted, []knowledge.Observation{adopted}, payload, source)
	step.InputAnchor = packetAck.Fence
	recoveryCoreControl(t, s, "goal-two-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: authority}})
	activity.Activity = "new-goal-model"
	activity.ContextPacket = packet.ID
	activity.InputFence = packetAck.Fence
	command := agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: authority}}
	ack := recoveryCoreControl(t, s, "goal-two-model", command)
	s.reopen()
	if retry, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("goal-two-model"), Command: agentprofile.Command{Control: &command}}); err != nil || retry != ack {
		t.Fatalf("goal input proof replay/receipt changed=%+v %v", retry, err)
	}
}

func goalManagedChild(t *testing.T, s *integrationStore, parent model.ExecutionID, authority agentprofile.AuthorityBinding, taskID model.EntityID, executionID model.ExecutionID, input model.CommitFence) agentprofile.AuthorityBinding {
	t.Helper()
	read, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	var parentExecution agentprofile.Execution
	for _, candidate := range read.Snapshot.Executions {
		if candidate.ID == parent {
			parentExecution = candidate
		}
	}
	inheritedEpoch := uint64(1)
	for _, candidate := range read.Snapshot.Tasks {
		if candidate.ID == parentExecution.Task {
			inheritedEpoch = candidate.GoalEpoch
		}
	}
	step := agentprofile.Step{Execution: parent, Sequence: 1, GoalEpoch: inheritedEpoch, Status: agentprofile.StepOpen, InputAnchor: input}
	recoveryCoreControl(t, s, "goal-spawn-step-"+string(executionID), agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: authority}})
	inv := agentprofile.Invocation{ID: "delegate-" + string(executionID), Execution: parent, Step: 1, ProviderCallKey: "delegate-" + string(executionID), Tool: "delegate", Attempt: 1, Status: agentprofile.InvocationPending, AcceptedResult: "model-result", ToolDefinition: model.ObjectID(integrationID('d')), ToolVersion: "delegate/v1", DispatchClass: "pure"}
	bindTestInvocation(t, s, &inv, authority, "delegate")
	commitTestInvocation(t, s, "goal-spawn-call-"+string(executionID), inv, authority)
	task := agentTask(taskID)
	x := agentprofile.Execution{ID: executionID, Task: taskID, Parent: parent, Actor: model.ActorID(executionID), Incarnation: 1, AcceptedGoalEpoch: inheritedEpoch, Status: agentprofile.ExecutionReady}
	childAuthority := authority
	childAuthority.Holder = string(executionID)
	request := testChildSpawnWork(t, s, "goal-spawn-"+string(executionID), agentprofile.SpawnChildCommand{Parent: parent, Invocation: inv.ID, Task: task, Execution: x, InheritedEpoch: inheritedEpoch, Compatibility: model.ObjectID(integrationID('a')), Authority: authority}, childAuthority, "work/v1")
	workApply(t, s, request)
	settleTestWorkCheckpoints(t, s, request)
	return childAuthority
}

func TestInheritedGoalKnowledgeMustReachChildAndGrandchildModelInputs(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	proof := model.ObjectID(integrationID('a'))
	authority := agentprofile.AuthorityBinding{Deployment: "goal-host", OwnerEpoch: 1, Incarnation: 1, Holder: "root"}
	task := agentTask("goal-root-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("goal-root-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "goal-root", Task: task.ID, Actor: "goal-root", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("goal-root"), Command: agentprofile.Command{Spawn: &x}})
	recoveryCoreControl(t, s, "goal-root-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: authority, Status: "active", Evidence: proof}}})
	_, seed, _ := goalCoreImage(t, s, "goal-root-original", task)
	childAuthority := goalManagedChild(t, s, x.ID, authority, "goal-child-task", "goal-child", seed)
	childKey := model.KnowledgeKey{Actor: "goal-child", Execution: "goal-child"}
	childGoal, childGoalSource, childRaw := goalCoreImage(t, s, "goal-child-image", goalCoreTask(t, s, "goal-child-task"))
	childOwn := goalCoreObservation(t, s, "goal-child-own-adopt", childKey, childGoal, childGoalSource)
	oldPacket, oldPacketAck := goalCorePacket(t, s, "goal-child-old-packet", childKey, childOwn, []knowledge.Observation{childOwn}, childGoal, childGoalSource)
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("goal-parent-rebase"), Command: agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: task.ID, ExpectedEpoch: 1, NewGoal: "new inherited safety constraint"}}})
	parentImage, parentSource, _ := goalCoreImage(t, s, "goal-parent-two-image", goalCoreTask(t, s, task.ID))
	rootKey := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	parentAdoption := goalCoreObservation(t, s, "goal-parent-two-adopt", rootKey, parentImage, parentSource)
	recoveryCoreControl(t, s, "goal-parent-two-observed", agentprofile.ControlCommand{ObserveGoal: &agentprofile.ObserveGoalCommand{Execution: x.ID, Epoch: 2, Observation: parentAdoption.ID, Evidence: proof, Authority: authority}})
	rebase := agentprofile.RebaseInheritedCommand{Execution: "goal-child", SourceTask: task.ID, ExpectedEpoch: 1, Epoch: 2, Compatibility: proof, Authority: childAuthority, Observation: parentAdoption.ID}
	rejectGoalInput(t, s, "goal-child-cannot-use-parent-private-observation", agentprofile.ControlCommand{RebaseInherited: &rebase})
	rebase.Observation = childOwn.ID
	rejectGoalInput(t, s, "goal-child-wrong-constraint", agentprofile.ControlCommand{RebaseInherited: &rebase})
	childConstraint := goalCoreObservation(t, s, "goal-child-parent-constraint", childKey, parentImage, parentSource)
	rebase.Observation = childConstraint.ID
	rebaseAck := recoveryCoreControl(t, s, "goal-child-constraint-rebase", agentprofile.ControlCommand{RebaseInherited: &rebase})
	activity := agentprofile.ActivitySpec{Activity: "child-old-packet", Execution: "goal-child", Role: "aux_summary", GoalEpoch: 1, InputFence: rebaseAck.Fence, ContextPacket: oldPacket.ID, Request: childGoal, RequestDigest: agentprofile.ExactRequestDigest(childRaw), Recipient: "model", Effect: "pure", ResourceScope: "session"}
	rejectGoalInput(t, s, "goal-inherited-new-control-old-packet", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: childAuthority}})
	missing, missingAck := goalCorePacket(t, s, "goal-child-omitted-constraint", childKey, childOwn, []knowledge.Observation{childOwn}, childGoal, childGoalSource)
	activity.Activity = "child-missing-constraint"
	activity.ContextPacket = missing.ID
	activity.InputFence = missingAck.Fence
	rejectGoalInput(t, s, "goal-inherited-missing-input-member", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: childAuthority}})
	packet, packetAck := goalCorePacket(t, s, "goal-child-actual-constraint", childKey, childOwn, []knowledge.Observation{childOwn, childConstraint}, childGoal, childGoalSource)
	activity.Activity = "child-current-constraint"
	activity.ContextPacket = packet.ID
	activity.InputFence = packetAck.Fence
	recoveryCoreControl(t, s, "goal-inherited-correct-model", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: childAuthority}})
	grandAuthority := goalManagedChild(t, s, "goal-child", childAuthority, "goal-grand-task", "goal-grand", packetAck.Fence)
	grandKey := model.KnowledgeKey{Actor: "goal-grand", Execution: "goal-grand"}
	grandGoal, grandSource, grandRaw := goalCoreImage(t, s, "goal-grand-image", goalCoreTask(t, s, "goal-grand-task"))
	grandOwn := goalCoreObservation(t, s, "goal-grand-own", grandKey, grandGoal, grandSource)
	grandOld, grandOldAck := goalCorePacket(t, s, "goal-grand-missing-ancestor", grandKey, grandOwn, []knowledge.Observation{grandOwn}, grandGoal, grandSource)
	activity.Activity = "grand-missing-ancestor"
	activity.Execution = "goal-grand"
	activity.ContextPacket = grandOld.ID
	activity.InputFence = grandOldAck.Fence
	activity.Request = grandGoal
	activity.RequestDigest = agentprofile.ExactRequestDigest(grandRaw)
	rejectGoalInput(t, s, "goal-transitive-constraint-missing-from-input", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: grandAuthority}})
	grandKnowledge, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &grandKey})
	if err != nil {
		t.Fatal(err)
	}
	grandConstraint := byImportedOrigin(grandKnowledge.Snapshot.Observations, childConstraint.ID)
	if grandConstraint.ID == "" || grandConstraint.Key != grandKey || grandConstraint.Payload != parentImage {
		t.Fatalf("grandchild seed did not adopt the ancestor constraint: %+v", grandConstraint)
	}
	grandPacket, grandPacketAck := goalCorePacket(t, s, "goal-grand-actual-ancestor", grandKey, grandOwn, []knowledge.Observation{grandOwn, grandConstraint}, grandGoal, grandSource)
	activity.Activity = "grand-current-ancestor"
	activity.ContextPacket = grandPacket.ID
	activity.InputFence = grandPacketAck.Fence
	command := agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: grandAuthority}}
	ack := recoveryCoreControl(t, s, "goal-transitive-correct-model", command)
	if oldPacketAck.Fence.LSN >= rebaseAck.Fence.LSN {
		t.Fatal("bad inherited-goal fixture")
	}
	s.reopen()
	if retry, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("goal-transitive-correct-model"), Command: agentprofile.Command{Control: &command}}); err != nil || retry != ack {
		t.Fatalf("inherited goal input receipt changed: %+v %v", retry, err)
	}
}
