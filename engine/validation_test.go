package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func validationPut(t *testing.T, e *Engine, id string, raw []byte) (model.ObjectID, api.CommitResult) {
	t.Helper()
	body, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte(id), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: raw}}}})
	if err != nil {
		t.Fatal(err)
	}
	request := api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: model.RequestID(id), Method: api.MethodMutateState, Body: body}
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), request)))
	object, err := makeValue(request.Space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		t.Fatal(err)
	}
	objectID, err := codec.ObjectID(object)
	if err != nil {
		t.Fatal(err)
	}
	return objectID, ack
}

func coreValidationFixture(t *testing.T) (*Engine, *coordinatorIO, agentprofile.AuthorityBinding, agentprofile.ValidationRequirement, model.ObjectID) {
	t.Helper()
	e, disk, authority, _ := actionFenceFixture(t)
	result, _ := validationPut(t, e, "validated-artifact", []byte("exact final artifact"))
	command, _ := validationPut(t, e, "validation-command", []byte("checker --artifact exact-artifact --policy accepted/v1"))
	environment, source := validationPut(t, e, "validation-environment", []byte("toolchain=test/v1;workspace=immutable-snapshot"))
	evidence, evidenceACK := validationPut(t, e, "validation-evidence", []byte("checker stdout: pass; exit 0; exact artifact digest"))
	state, err := e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil || len(state.Snapshot.Tasks) != 1 {
		t.Fatal(err)
	}
	goal, err := agentprofile.GoalKnowledgeDigest(state.Snapshot.Tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	r := agentprofile.ValidationRequirement{Version: 1, Source: evidenceACK.Fence, Spec: agentprofile.ValidationSpec{Space: "coordinator-space", Task: "fence-task", Execution: "fence-execution", GoalEpoch: 1, Goal: goal, Result: result, Command: command, Environment: environment, ToolVersion: "checker/v1", PolicyVersion: "accepted/v1"}}
	r.ID = agentprofile.ValidationRequirementID(r)
	if source.Fence.LSN >= evidenceACK.Fence.LSN {
		t.Fatal("invalid validation source fixture")
	}
	bind := profileCoordinatorRequest(t, "bind-validation", agentprofile.Command{Control: &agentprofile.ControlCommand{BindValidation: &agentprofile.BindValidationCommand{Requirement: r, Authority: authority}}})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), bind)))
	return e, disk, authority, r, evidence
}

func validationCompletionRequest(t *testing.T, id string, required agentprofile.ValidationRequirement, authority agentprofile.AuthorityBinding, validation model.ObjectID) api.CommitRequest {
	t.Helper()
	return profileCoordinatorRequest(t, id, agentprofile.Command{Control: &agentprofile.ControlCommand{CompleteTask: &agentprofile.CompleteTaskCommand{Task: required.Spec.Task, Execution: required.Spec.Execution, Epoch: required.Spec.GoalEpoch, Result: required.Spec.Result, Validation: validation, Authority: authority}}})
}

func TestTaskCompletionRejectsOtherArtifactGoalCommandEnvironmentAndRule(t *testing.T) {
	e, disk, authority, required, evidence := coreValidationFixture(t)
	ctx := context.Background()
	base := agentprofile.ValidationEnvelope{Requirement: required.ID, Spec: required.Spec, Passed: true, Evidence: []model.ObjectID{evidence}}
	for name, change := range map[string]func(*agentprofile.ValidationEnvelope){
		"other-artifact":    func(v *agentprofile.ValidationEnvelope) { v.Spec.Result = evidence },
		"old-goal":          func(v *agentprofile.ValidationEnvelope) { v.Spec.GoalEpoch++ },
		"other-goal-image":  func(v *agentprofile.ValidationEnvelope) { v.Spec.Goal = evidence },
		"other-command":     func(v *agentprofile.ValidationEnvelope) { v.Spec.Command = evidence },
		"other-environment": func(v *agentprofile.ValidationEnvelope) { v.Spec.Environment = evidence },
		"other-tool":        func(v *agentprofile.ValidationEnvelope) { v.Spec.ToolVersion = "different-checker/v1" },
		"weaker-policy":     func(v *agentprofile.ValidationEnvelope) { v.Spec.PolicyVersion = "unchecked/v1" },
		"failed-validation": func(v *agentprofile.ValidationEnvelope) { v.Passed = false },
	} {
		changed := base
		change(&changed)
		raw, err := changed.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		validation, _ := validationPut(t, e, "evidence-"+name, raw)
		before, _ := e.Snapshot(ctx)
		out := coordinatorWait(t, coordinatorCommit(e, ctx, validationCompletionRequest(t, "complete-"+name, required, authority, validation)))
		if out.result.Code != api.CodeRejected || !errors.Is(out.err, agentprofile.ErrValidation) {
			t.Fatalf("%s completed task with unrelated evidence: %+v %v", name, out.result, out.err)
		}
		if after, err := e.Snapshot(ctx); err != nil || after != before {
			t.Fatalf("bad evidence changed/fenced store: %+v %v", after, err)
		}
	}
	// Even a genuine previously passed envelope becomes ineligible when the
	// controller binds a new environment. Historical facts remain unchanged.
	raw, _ := base.Canonical()
	oldValidation, _ := validationPut(t, e, "passed-in-old-environment", raw)
	newEnvironment, newSource := validationPut(t, e, "new-environment", []byte("toolchain=test/v2;workspace=new-snapshot"))
	newRequirement := required
	newRequirement.Version, newRequirement.Source, newRequirement.Spec.Environment = 2, newSource.Fence, newEnvironment
	newRequirement.ID = agentprofile.ValidationRequirementID(newRequirement)
	bind := profileCoordinatorRequest(t, "bind-new-environment", agentprofile.Command{Control: &agentprofile.ControlCommand{BindValidation: &agentprofile.BindValidationCommand{Requirement: newRequirement, ExpectedVersion: 1, Authority: authority}}})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, bind)))
	if out := coordinatorWait(t, coordinatorCommit(e, ctx, validationCompletionRequest(t, "old-accepted-envelope", required, authority, oldValidation))); out.result.Code != api.CodeRejected || !errors.Is(out.err, agentprofile.ErrValidation) {
		t.Fatalf("old environment proof remained current: %+v %v", out.result, out.err)
	}
	base.Requirement, base.Spec = newRequirement.ID, newRequirement.Spec
	raw, _ = base.Canonical()
	validation, _ := validationPut(t, e, "current-validation-envelope", raw)
	complete := validationCompletionRequest(t, "complete-with-bound-proof", newRequirement, authority, validation)
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, complete)))
	options := e.options
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, _, err := OpenExisting(disk, options)
	if err != nil {
		t.Fatal("typed validation failed semantic replay", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if retry := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, complete))); retry != ack {
		t.Fatal("completion receipt changed after typed validation replay")
	}
	state, err := e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil || len(state.Snapshot.Lifecycle.Recovery.ValidationRequirements) != 2 || len(state.Snapshot.Lifecycle.Recovery.Outcomes) != 1 || state.Snapshot.Lifecycle.Recovery.Outcomes[0].ValidationRequirement != newRequirement.ID {
		t.Fatalf("completion lost bound requirement: %+v %v", state, err)
	}
}

func TestOpaqueCompletionEvidenceIsReplayOnlyAndOldGoalCannotComplete(t *testing.T) {
	e, _, authority, required, evidence := coreValidationFixture(t)
	ctx := context.Background()
	request := validationCompletionRequest(t, "opaque-completion", required, authority, evidence)
	command, err := agentprofile.DecodeCommand(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	ledger, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, request.Space, request.Principal)
	if err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	legacyErr := e.checkTaskValidation(ctx, request, ledger, command, true)
	e.mu.Unlock()
	if !errors.Is(legacyErr, agentprofile.ErrValidation) {
		t.Fatalf("strict V1 accepted opaque completion evidence: %v", legacyErr)
	}
	if out := coordinatorWait(t, coordinatorCommit(e, ctx, request)); out.result.Code != api.CodeRejected || !errors.Is(out.err, agentprofile.ErrValidation) {
		t.Fatal("new opaque completion bypassed typed policy", out)
	}
	envelope := agentprofile.ValidationEnvelope{Requirement: required.ID, Spec: required.Spec, Passed: true, Evidence: []model.ObjectID{evidence}}
	raw, _ := envelope.Canonical()
	proof, _ := validationPut(t, e, "old-goal-validation", raw)
	rebase := profileCoordinatorRequest(t, "new-validation-goal", agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: required.Spec.Task, ExpectedEpoch: 1, NewGoal: "new constraints after the old validation"}})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, rebase)))
	if out := coordinatorWait(t, coordinatorCommit(e, ctx, validationCompletionRequest(t, "complete-previous-goal", required, authority, proof))); out.result.Code != api.CodeRejected || !errors.Is(out.err, agentprofile.ErrValidation) {
		t.Fatalf("old-goal validation completed changed task: %+v %v", out.result, out.err)
	}
}
