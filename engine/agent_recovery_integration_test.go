package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func recoveryCoreControl(t *testing.T, s *integrationStore, id string, c agentprofile.ControlCommand) api.CommitResult {
	t.Helper()
	if c.DispatchAttempt != nil {
		return recoveryEmissionDispatch(t, s, id, c)
	}
	return coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey(id), Command: agentprofile.Command{Control: &c}})
}

func TestLegacyControlIsRejectedAtThePublicBoundary(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	ctx := context.Background()
	// Independently encoded pre-V1 ObserveGoal bytes must not be decoded or
	// admitted by the clean V1 public boundary.
	raw, err := hex.DecodeString("4154452d4147454e542d434f4d4d414e4400010d5e4154452d4147454e542d434f4e54524f4c00010f0165024062626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262016401010168")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agentprofile.DecodeCommand(raw); !errors.Is(err, agentprofile.ErrCommandVersion) {
		t.Fatalf("legacy command decoded: %v", err)
	}
	key := agentKey("legacy-control")
	before := integrationSnapshot(t, s.e)
	out, err := s.e.Commit(ctx, api.CommitRequest{Space: key.Space, Principal: key.Principal, RequestID: key.RequestID, Method: api.MethodApplyAgent, Body: raw})
	if !errors.Is(err, agentprofile.ErrCommandVersion) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("legacy control changed or fenced the store: %+v %v", out, err)
	}
	task := agentTask("ready-after-legacy-rejection")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("ready-after-legacy-rejection"), Command: agentprofile.Command{CreateTask: &task}})
}

func TestRecoveryActivityContentAndPacketMustBelongToPublishedOwnerCut(t *testing.T) {
	s, content, source := knowledgeFixture(t)
	proof := model.ObjectID(integrationID('a'))
	authority := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	for _, id := range []model.ExecutionID{"one", "two"} {
		recoveryCoreControl(t, s, string(id)+"-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: id, Binding: authority, Status: "active", Evidence: proof}}})
	}
	a := agentprofile.ActivitySpec{Activity: "content", Execution: "one", Role: "resource_prepare", GoalEpoch: 1, InputFence: source, Request: proof, RequestDigest: agentprofile.ExactRequestDigest([]byte{'a'}), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	recoveryCoreReject(t, s, "guessed-request", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	a.Request = content
	a.InputFence.CommitDigest = model.CommitDigest(integrationID('b'))
	recoveryCoreReject(t, s, "guessed-input-cut", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	a.InputFence = source
	badDigest := a
	badDigest.RequestDigest = proof
	recoveryCoreReject(t, s, "request-byte-digest-mismatch", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: badDigest, Authority: authority}})
	a.ContextPacket = proof
	recoveryCoreReject(t, s, "guessed-packet", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	o := knowledgeObservation(content, source, "one", 1)
	_, adopt := applyObservation(t, s.e, "activity-observe", o)
	key := o.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: adopt.Fence, Knowledge: &key}
	view, err := s.e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &key, GoalRef: content, Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: view.ViewDigest, Frontier: selected.Frontier}, Renderer: "plain/v1", Tokenizer: "test/v1", Items: []knowledge.ContextItem{{Ordinal: 0, Observation: o.ID, Content: content, Label: "exact input"}}}
	if err = s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	packet.ID = knowledge.PacketID(packet)
	packetAck, err := s.e.ApplyKnowledge(context.Background(), api.ApplyKnowledgeRequest{Key: agentKey("activity-packet"), Command: knowledge.Command{Packet: &packet}})
	if err != nil || packetAck.Code != api.CodeDurableAck {
		t.Fatalf("packet=%+v %v", packetAck, err)
	}
	a.ContextPacket = packet.ID
	recoveryCoreReject(t, s, "packet-before-publication", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	a.InputFence = packetAck.Fence
	a.Execution = "two"
	recoveryCoreReject(t, s, "other-execution-packet", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	a.Execution = "one"
	recoveryCoreControl(t, s, "published-exact-input", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	a.Activity, a.Role = "summary", "aux_summary"
	recoveryCoreReject(t, s, "model-with-metadata-only-packet", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	packet.Input = &knowledge.RecordedInput{Object: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Encoding: "test-framed-request/v1"}
	packet.ID = knowledge.PacketID(packet)
	packetAck, err = s.e.ApplyKnowledge(context.Background(), api.ApplyKnowledgeRequest{Key: agentKey("recorded-activity-packet"), Command: knowledge.Command{Packet: &packet}})
	if err != nil || packetAck.Code != api.CodeDurableAck {
		t.Fatalf("recorded packet=%+v %v", packetAck, err)
	}
	a.ContextPacket, a.InputFence, a.Request = packet.ID, packetAck.Fence, proof
	recoveryCoreReject(t, s, "model-packet-request-mismatch", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	a.Request = content
	recoveryCoreControl(t, s, "recorded-model-input", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: authority}})
	s.reopen()
}

func recoveryCoreContent(t *testing.T, s *integrationStore) (model.ObjectID, model.CommitFence) {
	t.Helper()
	ack := integrationMutate(t, s.e, integrationRequest("checkpoint-content", integrationPut([]byte("checkpoint"), api.Value{Kind: api.ValueBytes, Data: []byte("host checkpoint and exact input")})))
	v, err := s.e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("checkpoint=%+v %v", entries, err)
	}
	if err = s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	return entries.Items[0].Value, ack.Fence
}

func recoveryCoreReject(t *testing.T, s *integrationStore, id string, c agentprofile.ControlCommand) {
	t.Helper()
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey(id), Command: agentprofile.Command{Control: &c}})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("expected recovery rejection, got %+v %v", out, err)
	}
	if integrationSnapshot(t, s.e) != before {
		t.Fatal("recovery rejection changed/fenced publication")
	}
}

// This trusted host adapter executes a real file side effect after durable
// dispatch. A simulated crash drops the response before CaptureResult; the
// replacement reconciles the file and imports that result without executing
// the write again. This is host recovery acceptance, not a model benchmark.
func TestManagedParentChildHostRecoveryPreservesEffectAndExactContinuation(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	content, source := recoveryCoreContent(t, s)
	proof := model.ObjectID(integrationID('a')) // opaque trusted-host attestation
	task := agentTask("root-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("root-task"), Command: agentprofile.Command{CreateTask: &task}})
	root := agentprofile.Execution{ID: "root", Task: task.ID, Actor: "root-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("root"), Command: agentprofile.Command{Spawn: &root}})
	rootAuthority := agentprofile.AuthorityBinding{Deployment: "host-one", OwnerEpoch: 1, Incarnation: 1, Holder: "root-worker"}
	recoveryCoreControl(t, s, "root-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: root.ID, Binding: rootAuthority, Status: "active", Evidence: proof}}})
	step := agentprofile.Step{Execution: root.ID, Sequence: 1, GoalEpoch: 1, InputAnchor: source, Status: agentprofile.StepOpen}
	recoveryCoreControl(t, s, "root-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: rootAuthority}})
	inv := agentprofile.Invocation{ID: "spawn", Execution: root.ID, Step: 1, ProviderCallKey: "spawn-child", Tool: "delegate", Attempt: 1, Status: agentprofile.InvocationPending, AcceptedResult: "model-result", ToolDefinition: model.ObjectID(integrationID('d')), ToolVersion: "delegate/v1", DispatchClass: "pure"}
	bindTestInvocation(t, s, &inv, rootAuthority, "delegate")
	commitTestInvocation(t, s, "spawn-invocation", inv, rootAuthority)
	childTask := agentTask("child-task")
	child := agentprofile.Execution{ID: "child", Task: childTask.ID, Parent: root.ID, Actor: "child-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	spawn := agentprofile.ControlCommand{SpawnChild: &agentprofile.SpawnChildCommand{Parent: root.ID, Invocation: inv.ID, Task: childTask, Execution: child, InheritedEpoch: 1, Compatibility: proof, Authority: rootAuthority}}
	childAuthority := rootAuthority
	childAuthority.Holder = "child-worker"
	spawnWork := testChildSpawnWork(t, s, "child-spawn", *spawn.SpawnChild, childAuthority, "work/v1")
	spawnAck := workApply(t, s, spawnWork)
	settleTestWorkCheckpoints(t, s, spawnWork)
	activity := agentprofile.ActivitySpec{Activity: "external-write", Execution: child.ID, Role: "resource_prepare", GoalEpoch: 1, InputFence: source, Request: content, RequestDigest: agentprofile.ExactRequestDigest([]byte("host checkpoint and exact input")), Recipient: "file-executor", Effect: "non_idempotent", ResourceScope: "host-workspace"}
	recoveryCoreControl(t, s, "write-intent", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: childAuthority}})
	recoveryCoreControl(t, s, "write-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Attempt: "write-attempt", Activity: activity.Activity, Sequence: 1, Authority: childAuthority}})
	recoveryCoreControl(t, s, "write-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "write-attempt", Authority: childAuthority, Evidence: proof}})
	effectPath := filepath.Join(t.TempDir(), "external-effect")
	f, err := os.OpenFile(effectPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString("one external write\n"); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	nextBody, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("after-child"), integrationBytes('y'))}})
	if err != nil {
		t.Fatal(err)
	}
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "root-next", Generation: 1, Request: agentKey("root-next-original"), CommandDigest: model.ObjectID(fmt.Sprintf("%x", sha256.Sum256(nextBody))), InputFence: source, Owner: root.ID, Status: agentprofile.ActionPending}, Method: api.MethodMutateState, ExactCommand: nextBody, Authority: rootAuthority, InputObjects: []model.ObjectID{content}}
	continuation := agentprofile.Continuation{Execution: root.ID, Version: 2, Incarnation: 1, PendingKey: intent.Action.Key, PendingGeneration: 1, InputFence: source, Phase: "wait-for-child", Position: &agentprofile.ContinuationPosition{Space: "host-space", Principal: "host-principal", Step: 1, AdapterVersion: "work/v1", NextActions: []agentprofile.PendingActionRef{{Key: intent.Action.Key, Generation: 1}}}}
	bindTestContinuationKnowledge(t, s, &continuation, model.KnowledgeKey{Actor: root.Actor, Execution: root.ID}, integrationSnapshot(t, s.e).Fence)
	checkpoint := agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: continuation, ExpectedVersion: 1, Authority: rootAuthority}}
	checkpointAck := recoveryCoreControl(t, s, "root-checkpoint", checkpoint)
	before, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	beforeImage, err := agentprofile.EncodeRecordImage(before.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	beforeSnapshot, err := beforeImage.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.reopen() // all worker memory is discarded; the external response was lost
	after, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	afterImage, err := agentprofile.EncodeRecordImage(after.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	afterSnapshot, err := afterImage.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) || len(after.Snapshot.Lifecycle.Continuations) != 2 || !reflect.DeepEqual(after.Snapshot.Lifecycle.Continuations[1], continuation) {
		t.Fatalf("restart changed checkpoint: %+v %v", after.Snapshot.Lifecycle.Continuations, err)
	}
	if again := recoveryCoreControl(t, s, "root-checkpoint", checkpoint); again != checkpointAck {
		t.Fatal("recovery re-applied checkpoint instead of original receipt")
	}
	if again := workApply(t, s, spawnWork); again != spawnAck {
		t.Fatal("recovery duplicated child spawn")
	}
	resume := func(id model.ExecutionID, old agentprofile.AuthorityBinding) agentprofile.AuthorityBinding {
		t.Helper()
		recoveryCoreControl(t, s, string(id)+"-fence", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: id, ExpectedVersion: 1, Evidence: proof}})
		next := old
		next.Deployment, next.Holder, next.OwnerEpoch, next.Incarnation = "host-two", string(id)+"-replacement", 2, 2
		recoveryCoreControl(t, s, string(id)+"-resume", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: id, ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: next, AdapterVersion: "work/v1", Checkpoint: content, CompatibilityEvidence: proof, ResourceFenceEvidence: proof}})
		return next
	}
	rootNext, childNext := resume(root.ID, rootAuthority), resume(child.ID, childAuthority)
	recoveryCoreReject(t, s, "unknown-retry", agentprofile.ControlCommand{RetryActivity: &agentprofile.RetryActivityCommand{Activity: activity.Activity, ExpectedVersion: 1, Authority: childNext}})
	result := agentprofile.ActivityResult{Capture: "full", ID: "external-response", Activity: activity.Activity, Attempt: "write-attempt", RequestDigest: activity.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("file exists with expected bytes")}
	recoveryCoreControl(t, s, "late-response", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreReject(t, s, "automatic-stale-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: activity.Activity, Result: result.ID, ExpectedVersion: 1, Authority: childNext, Evidence: proof}})
	actual, err := os.ReadFile(effectPath)
	if err != nil || string(actual) != "one external write\n" {
		t.Fatalf("host cannot reconcile effect: %q %v", actual, err)
	}
	recoveryCoreControl(t, s, "effect-occurred", agentprofile.ControlCommand{ReconcileAttempt: &agentprofile.ReconcileAttemptCommand{Attempt: "write-attempt", Outcome: "occurred", Evidence: proof}})
	recoveryCoreControl(t, s, "import-known-result", agentprofile.ControlCommand{ImportActivityResult: &agentprofile.AcceptActivityCommand{Activity: activity.Activity, Result: result.ID, ExpectedVersion: 1, Authority: childNext, Evidence: proof}})
	recoveryCoreControl(t, s, "settled", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: activity.Activity, Status: "settled", Evidence: proof}})
	recoveryCompleteTask(t, s, "child-done", agentprofile.CompleteTaskCommand{Task: child.Task, Execution: child.ID, Epoch: 1, Result: content, Authority: childNext})
	observed := integrationSnapshot(t, s.e).Fence
	recoveryCoreControl(t, s, "original-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: intent.Action.Key, Generation: 1, Outcome: "absent", ObservedFence: observed}}})
	newKey := agentKey("root-next-rebound")
	recoveryCoreControl(t, s, "rebind-next", agentprofile.ControlCommand{RebindAction: &agentprofile.RebindActionCommand{Key: intent.Action.Key, ExpectedGeneration: 1, NewRequest: newKey, Method: intent.Method, ExactCommand: nextBody, CommandDigest: intent.Action.CommandDigest, Authority: rootNext, FenceEvidence: proof}})
	continuation.Version, continuation.Incarnation, continuation.PendingGeneration, continuation.Phase = 3, 2, 2, "dispatch-next"
	continuation.Position.NextActions[0].Generation = 2
	recoveryCoreControl(t, s, "resume-continuation", agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: continuation, ExpectedVersion: 2, Authority: rootNext}})
	stale, staleErr := s.e.Commit(ctx, api.CommitRequest{Space: intent.Action.Request.Space, Principal: intent.Action.Request.Principal, RequestID: intent.Action.Request.RequestID, Method: intent.Method, Body: nextBody})
	if staleErr == nil || stale.Code == api.CodeDurableAck || !errors.Is(staleErr, engine.ErrActionReservation) {
		t.Fatalf("stale rebound request was admitted: %+v %v", stale, staleErr)
	}
	out, err := s.e.Commit(ctx, api.CommitRequest{Space: newKey.Space, Principal: newKey.Principal, RequestID: newKey.RequestID, Method: intent.Method, Body: nextBody})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("resumed exact next command: %+v %v", out, err)
	}
	recoveryCoreControl(t, s, "next-applied", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: intent.Action.Key, Generation: 2, Outcome: "applied", ObservedFence: out.Fence, Receipt: out.ResultDigest}}})
	finish := agentprofile.ControlCommand{FinishStep: &agentprofile.FinishStepCommand{Execution: root.ID, Sequence: 1, Epoch: 1, Authority: rootNext}}
	closeCheckpoint := workCheckpoint(t, root.ID, rootNext, integrationSnapshot(t, s.e).Fence, "after-root-step", continuation.Version, 1)
	closeWork := api.ApplyWorkRequest{Key: agentKey("finish-root-step"), Unit: api.WorkUnit{Kind: "checkpoint", Steps: []api.WorkStep{workControl(t, finish), closeCheckpoint}}}
	workApply(t, s, closeWork)
	settleTestWorkCheckpoints(t, s, closeWork)
	recoveryCompleteTask(t, s, "root-done", agentprofile.CompleteTaskCommand{Task: root.Task, Execution: root.ID, Epoch: 1, Result: content, Authority: rootNext})
	s.reopen()
	final, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(final.Snapshot.Lifecycle.Recovery.Imports) != 1 || len(final.Snapshot.Lifecycle.Recovery.Outcomes) != 2 ||
		final.Snapshot.Lifecycle.Continuations[1].PendingKey != "after-root-step" || final.Snapshot.Lifecycle.Continuations[1].Version != continuation.Version+1 {
		t.Fatalf("final durable recovery=%+v %v", final, err)
	}
	actual, err = os.ReadFile(effectPath)
	if err != nil || string(actual) != "one external write\n" {
		t.Fatal("recovery repeated or lost external write")
	}
}
