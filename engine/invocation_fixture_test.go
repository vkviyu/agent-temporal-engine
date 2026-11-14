package engine_test

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func testPureToolDefinition(name, recipient string) agentprofile.ToolDefinition {
	return agentprofile.ToolDefinition{Name: name, Version: name + "/v1", SchemaEncoding: "json-schema/v1", Schema: []byte(`{"type":"object"}`), DispatchMode: "external", DispatchClass: "pure", Contract: agentprofile.EffectContract{
		Version: "effect/v1", Recipient: recipient, VerifiedPure: true, DifferentRequest: "unsupported", ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "none",
	}}
}

func publishTestToolDefinition(t *testing.T, s *integrationStore, id string, definition agentprofile.ToolDefinition) (model.ObjectID, model.CommitFence) {
	t.Helper()
	raw, err := definition.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	ack := integrationMutate(t, s.e, integrationRequest(id, integrationPut([]byte(id), api.Value{Kind: api.ValueBytes, Data: raw})))
	object, _ := recoveryValueAt(t, s, ack.Fence, id)
	return object, ack.Fence
}

// bindTestInvocation captures a real primary result without independently
// accepting it. The decision Work must accept the result and publish the whole
// invocation set together with its continuation.
func bindTestInvocation(t *testing.T, s *integrationStore, inv *agentprofile.Invocation, authority agentprofile.AuthorityBinding, mode string) {
	t.Helper()
	ctx := context.Background()
	read, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "invocation-source-" + string(inv.Execution)
	definitionKey := prefix + "-definition"
	if r := read.Snapshot.Lifecycle.Recovery; r != nil {
		for _, spec := range r.ActivitySpecs {
			if spec.Execution != inv.Execution || spec.Step != inv.Step || spec.Role != "primary_decision" {
				continue
			}
			for _, acceptance := range r.ActivityControls {
				if acceptance.Activity == spec.Activity && acceptance.AcceptedResult != "" {
					definition, _ := recoveryValueAt(t, s, spec.InputFence, definitionKey)
					inv.AcceptedResult, inv.ToolDefinition = acceptance.AcceptedResult, definition
					inv.ToolVersion, inv.DispatchClass = inv.Tool+"/v1", "pure"
					return
				}
			}
			t.Fatal("fixture primary decision has no accepted result")
		}
	}
	var execution agentprofile.Execution
	var task agentprofile.Task
	for _, x := range read.Snapshot.Executions {
		if x.ID == inv.Execution {
			execution = x
		}
	}
	for _, candidate := range read.Snapshot.Tasks {
		if candidate.ID == execution.Task {
			task = candidate
		}
	}
	if task.ID == "" {
		t.Fatal("fixture execution/task missing")
	}
	tool := testPureToolDefinition(inv.Tool, "test-tool/v1")
	tool.DispatchMode = mode
	definition, _ := publishTestToolDefinition(t, s, definitionKey, tool)
	input, source, raw := goalCoreImage(t, s, prefix+"-input", task)
	key := model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID}
	o := goalCoreObservation(t, s, prefix+"-observe", key, input, source)
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil {
		t.Fatal(err)
	}
	packet, published := goalCorePacket(t, s, prefix+"-packet", key, o, known.Snapshot.Observations, input, source)
	spec := agentprofile.ActivitySpec{Activity: prefix, Execution: execution.ID, Step: inv.Step, Role: "primary_decision", GoalEpoch: task.GoalEpoch, InputFence: published.Fence, Request: input, RequestDigest: agentprofile.ExactRequestDigest(raw), ContextPacket: packet.ID, Recipient: "test-provider/v1", Effect: "pure", ResourceScope: "session"}
	recoveryCoreControl(t, s, prefix+"-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: authority}})
	attempt := prefix + "-attempt"
	recoveryCoreControl(t, s, prefix+"-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: attempt, Sequence: 1, Authority: authority}})
	recoveryCoreControl(t, s, prefix+"-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: attempt, Evidence: input, Authority: authority}})
	result := agentprofile.ActivityResult{Capture: "full", ID: prefix + "-result", Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest, Incarnation: authority.Incarnation, Outcome: "success", Payload: []byte("recorded delegation decision")}
	recoveryCoreControl(t, s, prefix+"-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	inv.AcceptedResult, inv.ToolDefinition = result.ID, definition
	inv.ToolVersion, inv.DispatchClass = inv.Tool+"/v1", "pure"
}

func TestInvocationAdmissionRequiresAcceptedPrimaryAndItsDefinitionCut(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	task := agentTask("source-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("source-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "source-execution", Task: task.ID, Actor: "source-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	initial := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("source-execution"), Command: agentprofile.Command{Spawn: &x}})
	a := agentprofile.AuthorityBinding{Deployment: "source-host", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	recoveryCoreControl(t, s, "source-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: a, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}})
	recoveryCoreControl(t, s, "source-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: agentprofile.Step{Execution: x.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: initial.Fence}, Authority: a}})
	i := agentprofile.Invocation{ID: "source-call", Execution: x.ID, Step: 1, ProviderCallKey: "call", Tool: "read", Attempt: 1, Status: agentprofile.InvocationPending, AcceptedResult: "invented-result", ToolDefinition: model.ObjectID(integrationID('d')), ToolVersion: "read/v1", DispatchClass: "pure"}
	record := func(i agentprofile.Invocation) agentprofile.ControlCommand {
		return agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: i, Authority: a}}
	}
	recoveryCoreReject(t, s, "source-no-primary", record(i))
	bindTestInvocation(t, s, &i, a, "external")
	request := testInvocationDecision(t, s, "source-real-call", i, a)
	for name, mutate := range map[string]func(*agentprofile.Invocation){
		"forged-result": func(v *agentprofile.Invocation) { v.AcceptedResult = "invented-result" },
		"future-definition": func(v *agentprofile.Invocation) {
			later, _, _ := goalCoreImage(t, s, "definition-after-decision", agentprofile.Task{ID: "other-goal", Space: task.Space, Owner: task.Owner, Goal: "a different definition", GoalEpoch: 1, Status: agentprofile.TaskOpen})
			v.ToolDefinition = later
		},
	} {
		bad := i
		mutate(&bad)
		candidate := request
		candidate.Key = agentKey("source-" + name)
		candidate.Unit.Steps = append([]api.WorkStep(nil), request.Unit.Steps...)
		candidate.Unit.Steps[1] = workControl(t, record(bad))
		before := integrationSnapshot(t, s.e)
		out, err := s.e.ApplyWork(context.Background(), candidate)
		if err == nil || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
			t.Fatalf("invalid invocation decision published: %+v %v", out, err)
		}
	}
	ack := workApply(t, s, request)
	s.reopen()
	if retry := workApply(t, s, request); retry != ack {
		t.Fatal("reopen changed the accepted invocation receipt")
	}
}
