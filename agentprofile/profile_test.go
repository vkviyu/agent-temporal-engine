package agentprofile

import (
	"errors"
	"github.com/vkviyu/agent-temporal-engine/model"
	"strings"
	"testing"
)

func digest(ch byte) model.ObjectID { return model.ObjectID(strings.Repeat(string(ch), 64)) }
func TestTaskExecutionStepInvocationAndGoalEpoch(t *testing.T) {
	l := New()
	task, err := l.CreateTask(CreateTask{Task: Task{ID: "task", Space: "space", Owner: "owner", Goal: "build", GoalEpoch: 1, Status: TaskOpen}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := l.Spawn(SpawnRequest{Execution: Execution{ID: "exec", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.StartStep(StepRequest{Step: Step{Execution: e.ID, Sequence: 1, GoalEpoch: 1, Status: StepOpen}}); err != nil {
		t.Fatal(err)
	}
	authority := grantPrimaryAuthority(t, l, e.ID)
	acceptPrimaryResult(t, l, authority, e.ID, 1, "result")
	if _, err := l.RecordInvocation(InvocationRequest{Invocation: Invocation{ID: "inv", Execution: e.ID, Step: 1, ProviderCallKey: "call", Tool: "grep", Attempt: 1, Status: InvocationPending, AcceptedResult: "result", ToolDefinition: digest('d'), ToolVersion: "grep/v1", DispatchClass: "pure"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.CompleteStep(e.ID, 1, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("pending invocation allowed step complete")
	}
	got, err := l.RebaseGoal(GoalRebase{Task: task.ID, NewGoal: "test", ExpectedEpoch: 1})
	if err != nil || got.GoalEpoch != 2 {
		t.Fatalf("rebase %+v %v", got, err)
	}
	if _, err := l.StartStep(StepRequest{Step: Step{Execution: e.ID, Sequence: 2, GoalEpoch: 1, Status: StepOpen}}); !errors.Is(err, ErrGoalEpoch) {
		t.Fatal("stale goal accepted")
	}
}
func TestCancellationFencesExecutionsAndPendingActions(t *testing.T) {
	l := New()
	task, _ := l.CreateTask(CreateTask{Task: Task{ID: "task", Space: "space", Owner: "owner", Goal: "g", GoalEpoch: 1, Status: TaskOpen}})
	e, _ := l.Spawn(SpawnRequest{Execution: Execution{ID: "exec", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}})
	a, err := l.PutPending(PendingAction{Key: "action", Generation: 1, Request: model.RequestKey{Space: "space", Principal: "owner", RequestID: "r"}, CommandDigest: digest('a'), Owner: e.ID, Status: ActionPending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.CancelTask(task.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := l.GetExecution(e.ID)
	if got.Status != ExecutionCanceled {
		t.Fatalf("execution not canceled: %+v", got)
	}
	if _, err := l.RebindPending(a.Key, 1, model.RequestKey{Space: "space", Principal: "owner", RequestID: "r2"}); err != nil {
		t.Fatalf("fenced action was not explicitly rebound: %v", err)
	}
}
func TestCanonicalProfileCodec(t *testing.T) {
	t1 := Task{ID: "task", Space: "space", Owner: "owner", Goal: "g", GoalEpoch: 1, Status: TaskOpen}
	raw, err := EncodeTask(t1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTask(raw)
	if err != nil || got != t1 {
		t.Fatalf("task codec %+v %v", got, err)
	}
	raw[0] ^= 1
	if _, err := DecodeTask(raw); err == nil {
		t.Fatal("mutated schema accepted")
	}
	e := Execution{ID: "e", Task: "task", Actor: "a", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}
	raw, err = EncodeExecution(e)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeExecution(raw); err != nil || got != e {
		t.Fatalf("execution codec %+v %v", got, err)
	}
}

func TestStepInvocationAndPendingCodecs(t *testing.T) {
	step := Step{Execution: "e", Sequence: 1, GoalEpoch: 1, Status: StepOpen, InputAnchor: model.CommitFence{Store: "s", LSN: 1, CommitDigest: model.CommitDigest(digest('f'))}, PrimaryInvocation: "i"}
	raw, err := EncodeStep(step)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeStep(raw); err != nil || got != step {
		t.Fatalf("step %+v %v", got, err)
	}
	inv := Invocation{ID: "i", Execution: "e", Step: 1, ProviderCallKey: "call", Tool: "tool", Args: []byte("x"), Result: []byte("y"), Status: InvocationPending, Attempt: 1, AcceptedResult: "result", ToolDefinition: digest('d'), ToolVersion: "tool/v1", DispatchClass: "pure"}
	raw, err = EncodeInvocation(inv)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeInvocation(raw); err != nil || got.ID != inv.ID || string(got.Args) != "x" {
		t.Fatalf("inv %+v %v", got, err)
	}
	a := PendingAction{Key: "a", Generation: 1, Request: model.RequestKey{Space: "s", Principal: "p", RequestID: "r"}, CommandDigest: digest('a'), InputFence: step.InputAnchor, Owner: "e", Status: ActionPending}
	raw, err = EncodePendingAction(a)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodePendingAction(raw); err != nil || got.Key != a.Key || got.InputFence != a.InputFence {
		t.Fatalf("action %+v %v", got, err)
	}
}
