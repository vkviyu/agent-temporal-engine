package agentstore

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func profileRequest(id string) model.RequestKey {
	return model.RequestKey{Space: "agent", Principal: "agent-host", RequestID: model.RequestID(id)}
}

func TestCreateReopenProfileSnapshotSharesCoreFence(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, Options{ProfileSpace: "agent", Principal: "agent-host"})
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.CreateTask(context.Background(), agentprofile.CreateTask{Task: agentprofile.Task{ID: "task", Space: "agent", Owner: "agent-host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}, Request: profileRequest("task")})
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	if _, err = s.Spawn(context.Background(), agentprofile.SpawnRequest{Execution: agentprofile.Execution{ID: "exec", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}, Request: profileRequest("spawn")}); err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	_, fence, err := s.ProfileSnapshot()
	if err != nil || fence.LSN != 2 {
		_ = s.Close()
		t.Fatalf("snapshot fence=%+v err=%v", fence, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenExisting(dir, Options{ProfileSpace: "agent", Principal: "agent-host"})
	if err != nil || !report.Verified {
		t.Fatalf("reopen report=%+v err=%v", report, err)
	}
	defer reopened.Close()
	got, err := reopened.GetTask("task")
	if err != nil || got.Goal != "goal" {
		t.Fatalf("reopened task=%+v err=%v", got, err)
	}
	if _, err = reopened.GetExecution("exec"); err != nil {
		t.Fatal(err)
	}
	if again, err := reopened.CreateTask(context.Background(), agentprofile.CreateTask{Task: agentprofile.Task{ID: "task", Space: "agent", Owner: "agent-host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}, Request: profileRequest("task")}); err != nil || again != got {
		t.Fatalf("same task request was not idempotent: %+v %v", again, err)
	}
	if again, err := reopened.Spawn(context.Background(), agentprofile.SpawnRequest{Execution: agentprofile.Execution{ID: "exec", Task: "task", Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}, Request: profileRequest("spawn")}); err != nil || again.ID != "exec" {
		t.Fatalf("same spawn request was not idempotent: %+v %v", again, err)
	}
}

func TestApplyClosedUnionRoutesTypedResult(t *testing.T) {
	s, err := Create(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := profileRequest("apply-task")
	result, err := s.Apply(context.Background(), Command{Request: r, CreateTask: &agentprofile.CreateTask{Request: r, Task: agentprofile.Task{ID: "apply", Space: "agent", Owner: "agent-host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}}})
	if err != nil || result.Task == nil || result.Task.ID != "apply" || result.Fence.Validate() != nil {
		t.Fatalf("apply result=%+v err=%v", result, err)
	}
	if err := (Command{Request: r, CreateTask: resultTask(result)}).Validate(); err == nil {
		t.Fatal("request mismatch accepted")
	}
	if err := (Command{Request: r}).Validate(); err == nil {
		t.Fatal("empty union accepted")
	}
}

func resultTask(r Result) *agentprofile.CreateTask {
	return &agentprofile.CreateTask{Request: profileRequest("different"), Task: *r.Task}
}
