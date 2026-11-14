package engine_test

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// These fixtures exercise the public protected transitions. They never reduce
// protected commands through a test-only production bypass.
func testInvocationDecision(t *testing.T, s *integrationStore, id string, inv agentprofile.Invocation, a agentprofile.AuthorityBinding) api.ApplyWorkRequest {
	t.Helper()
	read, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	var spec agentprofile.ActivitySpec
	for _, result := range read.Snapshot.Lifecycle.Results {
		if result.ID == inv.AcceptedResult {
			for _, candidate := range read.Snapshot.Lifecycle.Recovery.ActivitySpecs {
				if candidate.Activity == result.Activity {
					spec = candidate
				}
			}
		}
	}
	if spec.Activity == "" || spec.Role != "primary_decision" {
		t.Fatal("fixture primary result missing", inv.AcceptedResult)
	}
	var version uint64
	for _, c := range read.Snapshot.Lifecycle.Continuations {
		if c.Execution == inv.Execution {
			version = c.Version
		}
	}
	checkpoint := workCheckpoint(t, inv.Execution, a, read.Fence, id+"-next", version, inv.Step)
	command, err := agentprofile.DecodeCommand(checkpoint.Body)
	if err != nil {
		t.Fatal(err)
	}
	command.Control.Checkpoint.Continuation.Incarnation = a.Incarnation
	execution := model.KnowledgeKey{}
	for _, x := range read.Snapshot.Executions {
		if x.ID == inv.Execution {
			execution = model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
		}
	}
	bindTestContinuationKnowledge(t, s, &command.Control.Checkpoint.Continuation, execution, read.Fence)
	checkpoint, err = api.AgentWorkStep(command)
	if err != nil {
		t.Fatal(err)
	}
	return api.ApplyWorkRequest{Key: agentKey(id), Unit: api.WorkUnit{Kind: "model_decision", Decision: &api.DecisionBinding{Activity: spec.Activity, Result: inv.AcceptedResult, Invocations: []api.DecisionInvocation{{Invocation: inv.ID, Definition: inv.ToolDefinition, Version: inv.ToolVersion, DispatchClass: inv.DispatchClass}}}, Steps: []api.WorkStep{
		workControl(t, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: inv.AcceptedResult, ExpectedVersion: 1, Authority: a, Evidence: inv.ToolDefinition}}),
		workControl(t, agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: inv, Authority: a}}),
		checkpoint,
	}}}
}

func commitTestInvocation(t *testing.T, s *integrationStore, id string, inv agentprofile.Invocation, a agentprofile.AuthorityBinding) api.CommitResult {
	t.Helper()
	request := testInvocationDecision(t, s, id, inv, a)
	ack := workApply(t, s, request)
	settleTestWorkCheckpoints(t, s, request)
	return ack
}

// A fixture's checkpoint carries a real exact state request. Execute and
// reconcile it so unrelated completion tests do not inherit an unknown action.
func settleTestWorkCheckpoints(t *testing.T, s *integrationStore, request api.ApplyWorkRequest) {
	t.Helper()
	for _, step := range request.Unit.Steps {
		if step.Method != api.MethodApplyAgent {
			continue
		}
		command, err := agentprofile.DecodeCommand(step.Body)
		if err != nil {
			t.Fatal(err)
		}
		if command.Control == nil || command.Control.Checkpoint == nil {
			continue
		}
		intent := command.Control.Checkpoint.Intent
		out, err := s.e.Commit(context.Background(), api.CommitRequest{Space: intent.Action.Request.Space, Principal: intent.Action.Request.Principal, RequestID: intent.Action.Request.RequestID, Method: intent.Method, Body: intent.ExactCommand})
		if err != nil || out.Code != api.CodeDurableAck {
			t.Fatalf("fixture next action: %+v %v", out, err)
		}
		recoveryCoreControl(t, s, intent.Action.Key+"-resolved", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: intent.Action.Key, Generation: intent.Action.Generation, Outcome: "applied", ObservedFence: out.Fence, Receipt: out.ResultDigest}}})
	}
}

func testChildSpawnWork(t *testing.T, s *integrationStore, id string, spawn agentprofile.SpawnChildCommand, authority agentprofile.AuthorityBinding, adapter string) api.ApplyWorkRequest {
	t.Helper()
	read, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	var parent agentprofile.Execution
	for _, x := range read.Snapshot.Executions {
		if x.ID == spawn.Parent {
			parent = x
		}
	}
	key := model.KnowledgeKey{Actor: parent.Actor, Execution: parent.ID}
	known, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil || known.Revision == nil || len(known.Snapshot.Observations) == 0 {
		t.Fatalf("fixture child requires explicit parent knowledge: %+v %v", known, err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	var observations []model.ObjectID
	for _, observation := range known.Snapshot.Observations {
		observations = append(observations, observation.ID)
	}
	target := model.KnowledgeKey{Actor: spawn.Execution.Actor, Execution: spawn.Execution.ID}
	seed, err := api.KnowledgeWorkStep(knowledge.Command{Import: &knowledge.ImportCommand{Target: target, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: known.Frontier}, Observations: observations}})
	if err != nil {
		t.Fatal(err)
	}
	goal, source, _ := goalCoreImage(t, s, id+"-goal", spawn.Task)
	checkpoint := workCheckpoint(t, spawn.Execution.ID, authority, source, id+"-seed-next", 0, 0)
	command, err := agentprofile.DecodeCommand(checkpoint.Body)
	if err != nil {
		t.Fatal(err)
	}
	command.Control.Checkpoint.Continuation.Position.AdapterVersion = adapter
	checkpoint, err = api.AgentWorkStep(command)
	if err != nil {
		t.Fatal(err)
	}
	return api.ApplyWorkRequest{Key: agentKey(id), Unit: api.WorkUnit{Kind: "child_spawn", Steps: []api.WorkStep{
		workControl(t, agentprofile.ControlCommand{SpawnChild: &spawn}),
		workControl(t, agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: spawn.Execution.ID, Binding: authority, Status: "active", Evidence: spawn.Compatibility}}}),
		seed, workSeedObservation(t, target, goal, source, uint64(len(observations))+1, "initial child Goal"), checkpoint,
	}}}
}
