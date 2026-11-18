package agentstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type snapshotCore struct {
	api.AgentCommands
	state   api.AgentStateResult
	request api.ReadAgentStateRequest
}

func (c *snapshotCore) GetAgentState(_ context.Context, request api.ReadAgentStateRequest) (api.AgentStateResult, error) {
	c.request = request
	return c.state, nil
}

func TestFacadeRestoresLargeRecordState(t *testing.T) {
	task := agentprofile.Task{ID: "task", Space: "agent", Owner: "agent-host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	snapshot := agentprofile.Snapshot{Tasks: []agentprofile.Task{task}}
	for i := 0; i < 50; i++ {
		snapshot.Lifecycle.Queries = append(snapshot.Lifecycle.Queries, agentprofile.Query{
			ID: fmt.Sprintf("q-%03d", i), Space: task.Space, Owner: task.Owner,
			Status: "open", Version: 1, Input: bytes.Repeat([]byte("x"), 512<<10),
		})
	}
	// Avoid quadratic durable fixture construction for a large record image.
	image, err := agentprofile.EncodeRecordImage(snapshot)
	if err != nil || image.Size() == 0 {
		t.Fatalf("record fixture size=%d err=%v", image.Size(), err)
	}
	result, err := resultAt(snapshot, agentprofile.Command{CreateTask: &task})
	if err != nil || result.Task == nil || *result.Task != task {
		t.Fatalf("typed ACK result unavailable: %+v %v", result.Task, err)
	}
	fence := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
	core := &snapshotCore{state: api.AgentStateResult{Found: true, Snapshot: snapshot, Fence: fence}}
	request := api.ReadAgentStateRequest{Space: task.Space, Principal: task.Owner}
	ledger, gotFence, err := readCurrentLedger(core, request)
	if err != nil || gotFence != fence || core.request != request {
		t.Fatalf("current ledger fence=%+v request=%+v err=%v", gotFence, core.request, err)
	}
	if got, err := ledger.GetTask(task.ID); err != nil || got != task {
		t.Fatalf("current task=%+v err=%v", got, err)
	}
	// Restoring must preserve all records and own its data, not borrow the core
	// result or merely project the requested task while skipping validation.
	snapshot.Lifecycle.Queries[0].Input[0] = 'z'
	got := ledger.Snapshot()
	if len(got.Lifecycle.Queries) != 50 || got.Lifecycle.Queries[0].Input[0] != 'x' {
		t.Fatal("current ledger lost records or borrowed core state")
	}
}

func TestFacadeRecordRestoreRejectsBrokenRelations(t *testing.T) {
	task := agentprofile.Task{ID: "task", Space: "agent", Owner: "agent-host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	snapshot := agentprofile.Snapshot{
		Tasks: []agentprofile.Task{task},
		Executions: []agentprofile.Execution{{
			ID: "orphan", Task: "missing", Actor: "worker", Incarnation: 1,
			AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
		}},
	}
	if _, err := resultAt(snapshot, agentprofile.Command{CreateTask: &task}); err == nil {
		t.Fatal("typed result bypassed cross-record validation")
	}
	core := &snapshotCore{state: api.AgentStateResult{Found: true, Snapshot: snapshot}}
	if ledger, _, err := readCurrentLedger(core, api.ReadAgentStateRequest{Space: task.Space, Principal: task.Owner}); ledger != nil || !errors.Is(err, ErrProfileState) {
		t.Fatalf("malformed current ledger=%v err=%v", ledger != nil, err)
	}
}

func TestTypedRetriesResolveOriginalLiveControlFenceAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	create := agentprofile.CreateTask{Request: profileRequest("original-task"), Task: agentprofile.Task{ID: "original-task", Space: "agent", Owner: "agent-host", Goal: "first goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}}
	task, err := s.CreateTask(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	createReceipt, err := s.Reconcile(ctx, create.Request)
	if err != nil || createReceipt.Code != api.CodeDurableAck {
		t.Fatalf("create receipt=%+v err=%v", createReceipt, err)
	}
	spawn := agentprofile.SpawnRequest{Request: profileRequest("original-spawn"), Execution: agentprofile.Execution{ID: "execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}}
	execution, err := s.Spawn(ctx, spawn)
	if err != nil {
		t.Fatal(err)
	}
	stepRequest := agentprofile.StepRequest{Request: profileRequest("original-step"), Step: agentprofile.Step{Execution: execution.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen}}
	step, err := s.StartStep(ctx, stepRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteStep(ctx, profileRequest("complete-step"), execution.ID, 1, 1); !errors.Is(err, engine.ErrWorkBoundary) {
		t.Fatalf("facade bypassed the Work completion boundary: %v", err)
	}
	if _, err := s.RebaseGoal(ctx, profileRequest("new-goal"), agentprofile.GoalRebase{Task: task.ID, ExpectedEpoch: 1, NewGoal: "second goal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelTask(ctx, profileRequest("cancel"), task.ID); err != nil {
		t.Fatal(err)
	}
	publication, err := s.Core().Snapshot(ctx)
	if err != nil || publication.Roots.LiveControl == "" || publication.Roots.DomainStates != "" {
		t.Fatalf("facade retained domain snapshot authority: %+v err=%v", publication, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, _, err = OpenExisting(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	latest, err := s.GetTask(task.ID)
	if err != nil || latest.Status != agentprofile.TaskCanceled || latest.Goal != "second goal" {
		t.Fatalf("latest task=%+v err=%v", latest, err)
	}
	if retried, err := s.CreateTask(ctx, create); err != nil || retried != task {
		t.Fatalf("task retry selected latest state: %+v err=%v", retried, err)
	}
	if retried, err := s.Spawn(ctx, spawn); err != nil || retried != execution {
		t.Fatalf("spawn retry selected canceled state: %+v err=%v", retried, err)
	}
	if retried, err := s.StartStep(ctx, stepRequest); err != nil || retried != step {
		t.Fatalf("step retry selected changed state: %+v err=%v", retried, err)
	}
	result, err := s.Apply(ctx, Command{Request: create.Request, CreateTask: &create})
	if err != nil || result.Task == nil || *result.Task != task || result.Fence != createReceipt.Result.Fence {
		t.Fatalf("union retry selected a different fence/result: %+v err=%v", result, err)
	}
	changed := create
	changed.Task.Goal = "different command"
	_, err = s.CreateTask(ctx, changed)
	var conflict *CommandError
	if !errors.As(err, &conflict) || conflict.Key != create.Request || conflict.Result.Code != api.CodeConflict || conflict.Result.Conflict != api.ConflictIdempotency {
		t.Fatalf("same-key changed command did not retain conflict: %v", err)
	}
	if after, err := s.Core().Snapshot(ctx); err != nil || after != publication {
		t.Fatalf("retries or conflict mutated publication: %+v err=%v", after, err)
	}
}

func TestFacadeDoesNotInferTypedSuccessFromUnrelatedReceipt(t *testing.T) {
	s, err := Create(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	create := agentprofile.CreateTask{Request: profileRequest("real-task"), Task: agentprofile.Task{ID: "same-task", Space: "agent", Owner: "agent-host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}}
	if _, err := s.CreateTask(ctx, create); err != nil {
		t.Fatal(err)
	}
	alias := profileRequest("host-receipt")
	_, err = s.Core().Mutate(ctx, api.MutateRequest{Key: alias, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("host"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("value")}}}}})
	if err != nil {
		t.Fatal(err)
	}
	create.Request = alias
	_, err = s.CreateTask(ctx, create)
	var conflict *CommandError
	if !errors.As(err, &conflict) || conflict.Key != alias || conflict.Result.Code != api.CodeConflict || conflict.Result.Conflict != api.ConflictIdempotency {
		t.Fatalf("host receipt was mistaken for profile typed success: %v", err)
	}
}

func TestFacadeReadsCoreLiveControlInsteadOfCachedLedger(t *testing.T) {
	s, err := Create(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	create := agentprofile.CreateTask{Request: profileRequest("read-through"), Task: agentprofile.Task{ID: "read-through", Space: "agent", Owner: "agent-host", Goal: "first", GoalEpoch: 1, Status: agentprofile.TaskOpen}}
	if _, err := s.CreateTask(context.Background(), create); err != nil {
		t.Fatal(err)
	}
	ack, err := s.Core().ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: profileRequest("direct-core-rebase"), Command: agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: create.Task.ID, NewGoal: "from core", ExpectedEpoch: 1}}})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("direct core result=%+v err=%v", ack, err)
	}
	got, err := s.GetTask(create.Task.ID)
	if err != nil || got.Goal != "from core" || got.GoalEpoch != 2 {
		t.Fatalf("facade exposed cached task: %+v err=%v", got, err)
	}
	_, fence, err := s.ProfileSnapshot()
	if err != nil || fence != ack.Fence {
		t.Fatalf("facade exposed cached fence: %+v err=%v", fence, err)
	}
	if original, err := s.CreateTask(context.Background(), create); err != nil || original != create.Task {
		t.Fatalf("retry discarded original historical result: %+v err=%v", original, err)
	}
}

func TestCommandErrorKeepsOriginalKeyAndCoreOutcome(t *testing.T) {
	key := profileRequest("uncertain-request")
	for _, code := range []api.ResultCode{api.CodeWaitInterrupted, api.CodeCommitUnknown} {
		failure := commandFailure(key, api.CommitResult{Code: code, RequestID: key.RequestID}, context.Canceled)
		var typed *CommandError
		if !errors.As(failure, &typed) || typed.Key != key || typed.Result.Code != code || !errors.Is(failure, ErrUncertain) || !errors.Is(failure, context.Canceled) {
			t.Fatalf("lost unknown outcome evidence: %v", failure)
		}
	}
}
