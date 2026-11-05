package agentstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
)

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
	if _, err := s.CompleteStep(ctx, profileRequest("complete-step"), execution.ID, 1, 1); err != nil {
		t.Fatal(err)
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
	legacy, err := s.Core().GetState(ctx, api.ReadStateRequest{Space: "agent", Key: SnapshotKey()})
	if err != nil || legacy.Found {
		t.Fatalf("facade wrote legacy profile key: %+v err=%v", legacy, err)
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
		t.Fatalf("step retry selected completed state: %+v err=%v", retried, err)
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

func TestLegacyProfileSnapshotRequiresExplicitMigration(t *testing.T) {
	for _, value := range []api.Value{{Kind: api.ValueNull}, {Kind: api.ValueBytes, Data: []byte("legacy snapshot bytes")}} {
		t.Run(string(value.Kind), func(t *testing.T) {
			path := t.TempDir()
			core, err := ate.Create(path, ate.Options{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = core.Mutate(context.Background(), api.MutateRequest{Key: profileRequest("legacy"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: SnapshotKey(), Kind: api.MutationPut, Value: value}}}})
			if err != nil {
				t.Fatal(err)
			}
			before, err := core.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := core.Close(); err != nil {
				t.Fatal(err)
			}
			facade, _, err := OpenExisting(path, Options{})
			if facade != nil || !errors.Is(err, ErrLegacyProfileState) {
				if facade != nil {
					_ = facade.Close()
				}
				t.Fatalf("legacy state silently ignored/migrated: facade=%v err=%v", facade, err)
			}
			// Constructor rejection must retire the owner so the host can export
			// the unchanged legacy bytes through its original core API.
			core, _, err = ate.OpenExisting(path, ate.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer core.Close()
			after, err := core.Snapshot(context.Background())
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("legacy rejection modified roots/fence: %+v err=%v", after, err)
			}
			legacy, err := core.GetState(context.Background(), api.ReadStateRequest{Space: "agent", Key: SnapshotKey()})
			if err != nil || !legacy.Found || legacy.Value.Kind != value.Kind || !bytes.Equal(legacy.Value.Data, value.Data) {
				t.Fatalf("legacy bytes changed: %+v err=%v", legacy, err)
			}
		})
	}
}
