package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func agentKey(id string) model.RequestKey {
	return model.RequestKey{Space: "host-space", Principal: "host-principal", RequestID: model.RequestID(id)}
}

func agentTask(id model.EntityID) agentprofile.Task {
	return agentprofile.Task{ID: id, Space: "host-space", Owner: "host-principal", Goal: "finish the work", GoalEpoch: 1, Status: agentprofile.TaskOpen}
}

func coreAgentCommit(t *testing.T, e *engine.Engine, r api.ApplyAgentRequest) api.CommitResult {
	t.Helper()
	ack, err := e.ApplyAgent(context.Background(), r)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("agent command did not commit: %+v, %v", ack, err)
	}
	if ack.Fence.Validate() != nil || ack.RootSet == "" || ack.ResultDigest == "" || ack.StateRoot == "" {
		t.Fatalf("agent ACK lacks authenticated proof: %+v", ack)
	}
	return ack
}

func TestCoreAgentLiveControlSharesJournalWithDomainState(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	domainRequest := integrationRequest("host-before", integrationPut([]byte("host-key"), integrationBytes('1')))
	integrationMutate(t, s.e, domainRequest)
	domainBefore := integrationSnapshot(t, s.e)
	task := agentTask("task")
	create := api.ApplyAgentRequest{Key: agentKey("create"), Command: agentprofile.Command{CreateTask: &task}}
	createACK := coreAgentCommit(t, s.e, create)
	afterCreate := integrationSnapshot(t, s.e)
	if afterCreate.Roots.DomainStates != domainBefore.Roots.DomainStates || afterCreate.Roots.LiveControl == "" || afterCreate.Roots.CommandReceipts == domainBefore.Roots.CommandReceipts {
		t.Fatalf("agent commit changed incorrect RootSet slots: before=%+v after=%+v", domainBefore, afterCreate)
	}
	if createACK.Fence.LSN != domainBefore.Fence.LSN+1 || createACK.StateRoot != afterCreate.Roots.LiveControl || createACK.RootSet != afterCreate.RootSet {
		t.Fatalf("agent publication proof is detached from core: ack=%+v state=%+v", createACK, afterCreate)
	}
	read := api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"}
	atCreate, err := s.e.GetAgentState(context.Background(), read)
	if err != nil || !atCreate.Found || atCreate.Fence != createACK.Fence || atCreate.RootSet != createACK.RootSet || atCreate.LiveControlRoot != createACK.StateRoot || len(atCreate.Snapshot.Tasks) != 1 || atCreate.Snapshot.Tasks[0] != task {
		t.Fatalf("published agent state=%+v err=%v", atCreate, err)
	}
	execution := agentprofile.Execution{ID: "root", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	spawn := api.ApplyAgentRequest{Key: agentKey("spawn"), Command: agentprofile.Command{Spawn: &execution}}
	spawnACK := coreAgentCommit(t, s.e, spawn)
	rebase := agentprofile.GoalRebase{Task: task.ID, NewGoal: "revised work", ExpectedEpoch: 1}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("rebase"), Command: agentprofile.Command{RebaseGoal: &rebase}})
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("cancel"), Command: agentprofile.Command{CancelTask: &agentprofile.CancelTaskCommand{Task: task.ID}}})
	controlBefore := integrationSnapshot(t, s.e)
	integrationMutate(t, s.e, integrationRequest("host-after", integrationPut([]byte("host-key"), integrationBytes('2'))))
	final := integrationSnapshot(t, s.e)
	if final.Roots.LiveControl != controlBefore.Roots.LiveControl || final.Roots.DomainStates == controlBefore.Roots.DomainStates {
		t.Fatalf("host mutation overwrote profile root: before=%+v after=%+v", controlBefore, final)
	}
	// Retrying after subsequent goal/cancel transitions must resolve the original
	// core receipt before evaluating the command against today's profile state.
	if got := coreAgentCommit(t, s.e, create); !reflect.DeepEqual(got, createACK) {
		t.Fatalf("create retry proof changed: got=%+v original=%+v", got, createACK)
	}
	if got := coreAgentCommit(t, s.e, spawn); !reflect.DeepEqual(got, spawnACK) {
		t.Fatalf("spawn retry proof changed: got=%+v original=%+v", got, spawnACK)
	}
	if snapshot := integrationSnapshot(t, s.e); snapshot != final {
		t.Fatal("receipt retry published a second transition")
	}
	report := s.reopen()
	if report.ScannedTransactions != 6 {
		t.Fatalf("domain/profile used separate replay prefixes: %+v", report)
	}
	restored, err := s.e.GetAgentState(context.Background(), read)
	if err != nil || !restored.Found || len(restored.Snapshot.Tasks) != 1 || restored.Snapshot.Tasks[0].Status != agentprofile.TaskCanceled || restored.Snapshot.Tasks[0].Goal != rebase.NewGoal || len(restored.Snapshot.Executions) != 1 || restored.Snapshot.Executions[0].Status != agentprofile.ExecutionCanceled {
		t.Fatalf("recovered profile=%+v err=%v", restored, err)
	}
	if restored.Fence != final.Fence || restored.RootSet != final.RootSet || restored.LiveControlRoot != final.Roots.LiveControl {
		t.Fatalf("recovery reconstructed a different bundle: got=%+v expected=%+v", restored, final)
	}
	historicalRequest := read
	historicalRequest.Anchor = &model.SnapshotAnchor{Space: "host-space", Fence: createACK.Fence}
	historical, err := s.e.GetAgentState(context.Background(), historicalRequest)
	if err != nil || !historical.Found || historical.RootSet != createACK.RootSet || historical.Fence != createACK.Fence || historical.LiveControlRoot != createACK.StateRoot || len(historical.Snapshot.Tasks) != 1 || historical.Snapshot.Tasks[0] != task || len(historical.Snapshot.Executions) != 0 {
		t.Fatalf("historical profile silently selected latest: %+v err=%v", historical, err)
	}
	integrationRead(t, s.e, "host-space", []byte("host-key"), true, integrationBytes('2'))
	if got := coreAgentCommit(t, s.e, create); !reflect.DeepEqual(got, createACK) {
		t.Fatalf("post-reopen create retry proof changed: %+v", got)
	}
	lookup := integrationLookup(t, s.e, create.Key)
	if lookup.Code != api.CodeDurableAck || !reflect.DeepEqual(lookup.Result, createACK) {
		t.Fatalf("profile receipt not in common receipt authority: %+v", lookup)
	}
	changed := task
	changed.Goal = "different body under the original request"
	conflict, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: create.Key, Command: agentprofile.Command{CreateTask: &changed}})
	if err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
		t.Fatalf("same key accepted a different profile body: %+v err=%v", conflict, err)
	}
	if after := integrationSnapshot(t, s.e); after != final {
		t.Fatal("idempotency conflict changed publication")
	}
}

func TestCoreAgentSemanticRejectionsLeavePublicationReady(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	before := integrationSnapshot(t, s.e)
	wrongOwner := agentTask("wrong-owner")
	wrongOwner.Owner = "another-principal"
	result, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey("wrong-owner"), Command: agentprofile.Command{CreateTask: &wrongOwner}})
	if err == nil && result.Code == api.CodeDurableAck {
		t.Fatal("caller created a task for another principal")
	}
	if current := integrationSnapshot(t, s.e); current != before {
		t.Fatal("scope rejection changed the core publication")
	}
	task := agentTask("good")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("good"), Command: agentprofile.Command{CreateTask: &task}})
	before = integrationSnapshot(t, s.e)
	missing := agentprofile.Execution{ID: "unknown-task", Task: "missing", Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	result, err = s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey("unknown"), Command: agentprofile.Command{Spawn: &missing}})
	if err == nil && result.Code == api.CodeDurableAck {
		t.Fatal("unknown task accepted")
	}
	if current := integrationSnapshot(t, s.e); current != before {
		t.Fatal("semantic rejection fenced or changed the publication")
	}
	stale := before.Fence
	stale.LSN = 0
	staleTask := agentTask("stale")
	result, err = s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey("stale"), Command: agentprofile.Command{CreateTask: &staleTask}, ExpectedFence: &stale})
	if err == nil && result.Code == api.CodeDurableAck {
		t.Fatal("stale profile fence accepted")
	}
	if current := integrationSnapshot(t, s.e); current != before {
		t.Fatal("stale fence changed or fenced the publication")
	}
	// A different authenticated principal must not obtain another principal's
	// ledger when it selects the same domain Space.
	foreign, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "another-principal"})
	if err != nil || foreign.Found || len(foreign.Snapshot.Tasks) != 0 {
		t.Fatalf("foreign profile state leaked: %+v err=%v", foreign, err)
	}
	second := agentTask("after-rejection")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("after-rejection"), Command: agentprofile.Command{CreateTask: &second}})
}

func TestCoreOnlyRejectsAgentProfileCommand(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	before := integrationSnapshot(t, s.e)
	task := agentTask("disabled")
	result, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey("disabled"), Command: agentprofile.Command{CreateTask: &task}})
	if err == nil && result.Code == api.CodeDurableAck {
		t.Fatal("core-only store accepted profile command")
	}
	if got := integrationSnapshot(t, s.e); got != before {
		t.Fatal("disabled profile command modified or fenced store")
	}
}

func TestCoreAgentStepRequiresPublishedInputFence(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	task := agentTask("step-fence")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("step-task"), Command: agentprofile.Command{CreateTask: &task}})
	execution := agentprofile.Execution{ID: "step-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("step-spawn"), Command: agentprofile.Command{Spawn: &execution}})
	before := integrationSnapshot(t, s.e)
	future := before.Fence
	future.LSN++
	step := agentprofile.Step{Execution: execution.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: future}
	result, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey("future-step"), Command: agentprofile.Command{StartStep: &step}})
	if err == nil && result.Code == api.CodeDurableAck {
		t.Fatal("step consumed an unpublished future fence")
	}
	if current := integrationSnapshot(t, s.e); current != before {
		t.Fatal("future input rejection changed or fenced publication")
	}
	step.InputAnchor = before.Fence
	ack := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("published-step"), Command: agentprofile.Command{StartStep: &step}})
	if ack.Fence.LSN != before.Fence.LSN+1 {
		t.Fatalf("rejected input consumed a publication slot: %+v", ack)
	}
	s.reopen()
	state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Steps) != 1 || state.Snapshot.Steps[0].InputAnchor != before.Fence {
		t.Fatalf("step source did not replay exactly: %+v err=%v", state, err)
	}
}

func TestAgentProfileReplayRequiresEnabledProfile(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	task := agentTask("required-profile")
	ack := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("required-profile"), Command: agentprofile.Command{CreateTask: &task}})
	before := integrationSnapshot(t, s.e)
	if err := s.e.Close(); err != nil {
		t.Fatal(err)
	}
	coreOnly, _, err := engine.OpenExisting(s.d, engine.Options{})
	if coreOnly != nil {
		_ = coreOnly.Close()
		t.Fatal("core-only binary exposed an unvalidated Agent profile")
	}
	if err == nil {
		t.Fatal("required profile replay succeeded without an enabled profile")
	}
	// A rejected open must release its owner and leave the publication intact,
	// so an enabled reopen can authenticate the exact original root and receipt.
	reopened, report, err := engine.OpenExisting(s.d, s.options)
	if err != nil || reopened == nil || !report.Verified {
		t.Fatalf("enabled reopen after rejection: report=%+v err=%v", report, err)
	}
	s.e = reopened
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatalf("rejected open changed the publication: before=%+v after=%+v", before, after)
	}
	if found := integrationLookup(t, s.e, agentKey("required-profile")); found.Code != api.CodeDurableAck || !reflect.DeepEqual(found.Result, ack) {
		t.Fatalf("receipt changed after unsupported replay: %+v", found)
	}
}
