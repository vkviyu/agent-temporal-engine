package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestLegacyCompleteStepCannotBypassWorkWithoutAuthority(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	task := agentTask("unmanaged-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("unmanaged-task"), Command: agentprofile.Command{CreateTask: &task}})
	execution := agentprofile.Execution{ID: "unmanaged-execution", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	created := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("unmanaged-execution"), Command: agentprofile.Command{Spawn: &execution}})
	step := agentprofile.Step{Execution: execution.ID, Sequence: 1, GoalEpoch: 1, InputAnchor: created.Fence, Status: agentprofile.StepOpen}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("unmanaged-step"), Command: agentprofile.Command{StartStep: &step}})
	command := agentprofile.Command{CompleteStep: &agentprofile.CompleteStepCommand{Execution: execution.ID, Sequence: 1, Epoch: 1}}
	body, err := agentprofile.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	before := integrationSnapshot(t, s.e)
	for _, raw := range []bool{false, true} {
		key := agentKey("unmanaged-complete-typed")
		var out api.CommitResult
		if raw {
			key = agentKey("unmanaged-complete-raw")
			out, err = s.e.Commit(ctx, api.CommitRequest{Space: key.Space, Principal: key.Principal, RequestID: key.RequestID, Method: api.MethodApplyAgent, Body: body})
		} else {
			out, err = s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key, Command: command})
		}
		lookup := integrationLookup(t, s.e, key)
		if !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before || lookup.Code != api.CodeNotFoundAtFence {
			t.Fatalf("legacy completion changed publication or receipts: %+v %v %+v", out, err, lookup)
		}
	}
	s.reopen()
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Steps) != 1 || state.Snapshot.Steps[0].Status != agentprofile.StepOpen || len(state.Snapshot.Lifecycle.Continuations) != 0 {
		t.Fatalf("legacy rejection changed the recovered step: %+v %v", state, err)
	}
}

func TestFinishStepRequiresAtomicCheckpointAtFinalKnowledgeCut(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	auth := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	for _, execution := range []string{"one", "two"} {
		authority := agentprofile.ExecutionAuthority{Execution: model.ExecutionID(execution), Binding: auth, Status: "active", Evidence: payload}
		recoveryCoreControl(t, s, "authority-"+execution, agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: authority}})
	}
	first := knowledgeObservation(payload, source, "one", 1)
	_, initial := applyObservation(t, s.e, "initial", first)
	step := agentprofile.Step{Execution: "one", Sequence: 1, GoalEpoch: 1, InputAnchor: initial.Fence, Status: agentprofile.StepOpen}
	recoveryCoreControl(t, s, "begin", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: auth}})
	before := integrationSnapshot(t, s.e)
	finish := agentprofile.ControlCommand{FinishStep: &agentprofile.FinishStepCommand{Execution: "one", Sequence: 1, Epoch: 1, Authority: auth}}
	finishStep := workControl(t, finish)
	reject := func(name string, result api.CommitResult, err error) {
		t.Helper()
		lookup := integrationLookup(t, s.e, agentKey(name))
		if !errors.Is(err, engine.ErrWorkBoundary) || result.Code != api.CodeRejected ||
			integrationSnapshot(t, s.e) != before || lookup.Code != api.CodeNotFoundAtFence || lookup.ObservedFence != before.Fence {
			t.Fatalf("%s changed publication or receipts: %+v %v %+v", name, result, err, lookup)
		}
	}
	direct := agentKey("direct")
	out, err := s.e.Commit(ctx, api.CommitRequest{Space: direct.Space, Principal: direct.Principal, RequestID: direct.RequestID, Method: finishStep.Method, Body: finishStep.Body})
	reject("direct", out, err)
	out, err = s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("typed"), Command: agentprofile.Command{Control: &finish}})
	reject("typed", out, err)

	closeCheckpoint := workCheckpoint(t, "one", auth, before.Fence, "after-step", 0, 1)
	foreignCheckpoint := workCheckpoint(t, "two", auth, before.Fence, "foreign-action", 0, 0)
	second := knowledgeObservation(payload, source, "one", 2)
	adopt, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &second})
	if err != nil {
		t.Fatal(err)
	}
	closeWork := func(name string, steps ...api.WorkStep) api.ApplyWorkRequest {
		return api.ApplyWorkRequest{Key: agentKey(name), Unit: api.WorkUnit{Kind: "checkpoint", Steps: steps}}
	}
	out, err = s.e.ApplyWork(ctx, closeWork("foreign-continuation", finishStep, foreignCheckpoint))
	reject("foreign-continuation", out, err)
	out, err = s.e.ApplyWork(ctx, closeWork("stale-knowledge-cut", finishStep, closeCheckpoint, adopt))
	reject("stale-knowledge-cut", out, err)

	request := closeWork("close-step", finishStep, adopt, closeCheckpoint)
	ack := workApply(t, s, request)
	if ack.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("closing the step did not publish one transaction")
	}
	verify := func() {
		t.Helper()
		agent, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: request.Key.Space, Principal: request.Key.Principal})
		if err != nil || len(agent.Snapshot.Steps) != 1 || agent.Snapshot.Steps[0].Status != agentprofile.StepCompleted ||
			len(agent.Snapshot.Lifecycle.Continuations) != 1 || len(agent.Snapshot.Actions) != 1 {
			t.Fatalf("step/action/continuation were not published together: %+v %v", agent, err)
		}
		c := agent.Snapshot.Lifecycle.Continuations[0]
		cut := c.Position.KnowledgeSource
		state, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: request.Key.Space, Principal: request.Key.Principal, Knowledge: &second.Key})
		if err != nil || state.Revision == nil || state.Revision.Sequence != 2 || cut == nil || cut.ProducerTx != ack.TxID ||
			cut.Cursor.Revision != state.Revision.ID || c.PendingKey != agent.Snapshot.Actions[0].Key {
			t.Fatalf("closure lost the adopted revision or next action: %+v %+v %v", c, state, err)
		}
		if replay := workApply(t, s, request); replay != ack {
			t.Fatal("closure receipt changed")
		}
	}
	verify()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	verify()
}
