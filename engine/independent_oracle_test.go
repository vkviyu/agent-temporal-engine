package engine_test

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// This deliberately small map model is independent of Ledger, ControlCommand
// reducers, StateOps and receipt replay. It checks observable task/execution
// status and the emission grant lifecycle for the same bounded sequence.
type controlOracle struct {
	tasks      map[model.EntityID]string
	executions map[model.ExecutionID]string
	grant      string
}

func newControlOracle() *controlOracle {
	return &controlOracle{tasks: map[model.EntityID]string{}, executions: map[model.ExecutionID]string{}}
}
func (o *controlOracle) createTask(id model.EntityID) { o.tasks[id] = "open" }
func (o *controlOracle) spawn(id model.ExecutionID)   { o.executions[id] = "ready" }
func (o *controlOracle) cancel(task model.EntityID, exec model.ExecutionID) {
	o.tasks[task] = "canceled"
	o.executions[exec] = "canceled"
}

func TestIndependentControlOracleMatchesCoreForSpawnCancelAndEmission(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	oracle := newControlOracle()
	task := agentTask("oracle-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("oracle-create"), Command: agentprofile.Command{CreateTask: &task}})
	oracle.createTask(task.ID)
	x := agentprofile.Execution{ID: "oracle-exec", Task: task.ID, Actor: "oracle", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("oracle-spawn"), Command: agentprofile.Command{Spawn: &x}})
	oracle.spawn(x.ID)
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Tasks) != len(oracle.tasks) || len(state.Snapshot.Executions) != len(oracle.executions) {
		t.Fatalf("spawn oracle mismatch %+v %v", state, err)
	}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("oracle-cancel"), Command: agentprofile.Command{CancelTask: &agentprofile.CancelTaskCommand{Task: task.ID}}})
	oracle.cancel(task.ID, x.ID)
	state, err = s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || state.Snapshot.Tasks[0].Status != agentprofile.TaskCanceled || state.Snapshot.Executions[0].Status != agentprofile.ExecutionCanceled || oracle.tasks[task.ID] != "canceled" || oracle.executions[x.ID] != "canceled" {
		t.Fatalf("cancel oracle mismatch %+v %v", state, err)
	}
	// A separate emission fixture uses the production grant API, while the
	// expected transitions are only the three map states below.
	es, content, source, binding := emissionFixture(t)
	grant := api.EmissionSpec{ID: "oracle-grant", Execution: "emission-exec", Task: "emission-task", GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Offset: 0, Length: 1, Destination: "oracle-destination"}
	issue, err := es.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("oracle-issue"), Command: api.EmissionCommand{Issue: &grant}})
	if err != nil || issue.Code != api.CodeDurableAck {
		t.Fatal(issue, err)
	}
	oracle.grant = "issued"
	claim, err := es.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("oracle-claim"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: binding}})
	if err != nil || claim.Code != api.CodeDurableAck {
		t.Fatal(claim, err)
	}
	oracle.grant = "claimed"
	revoke, err := es.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("oracle-revoke"), Command: api.EmissionCommand{RevokeID: grant.ID, RevokeVersion: 2}})
	if err != nil || revoke.Code != api.CodeDurableAck {
		t.Fatal(revoke, err)
	}
	oracle.grant = "revoked"
	got, err := es.e.GetEmission(ctx, api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: grant.ID})
	if err != nil || got.Record.Status != oracle.grant || got.Record.Version != 3 {
		t.Fatalf("emission oracle mismatch %+v %v", got, err)
	}
}
