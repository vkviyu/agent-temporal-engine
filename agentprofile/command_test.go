package agentprofile

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func commandKey(id string) model.RequestKey {
	return model.RequestKey{Space: "space", Principal: "owner", RequestID: model.RequestID(id)}
}
func commandTask() Task {
	return Task{ID: "task", Space: "space", Owner: "owner", Goal: "finish", GoalEpoch: 1, Status: TaskOpen}
}
func commandExecution() Execution {
	return Execution{ID: "exec", Task: "task", Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}
}
func commandStep() Step { return Step{Execution: "exec", Sequence: 1, GoalEpoch: 1, Status: StepOpen} }
func commandInvocation() Invocation {
	return Invocation{ID: "inv", Execution: "exec", Step: 1, ProviderCallKey: "call", Tool: "read", Args: []byte("args"), Status: InvocationPending, Attempt: 1, AcceptedResult: "model-result", ToolDefinition: digest('d'), ToolVersion: "read/v1", DispatchClass: "pure"}
}

func TestCommandCodecClosedCanonicalUnion(t *testing.T) {
	task, execution, step, invocation := commandTask(), commandExecution(), commandStep(), commandInvocation()
	action := PendingAction{Key: "a", Generation: 1, Request: commandKey("pending"), CommandDigest: digest('a'), Owner: "exec", Status: ActionPending}
	commands := []Command{
		{CreateTask: &task}, {Spawn: &execution}, {StartStep: &step}, {RecordInvocation: &invocation},
		{CompleteStep: &CompleteStepCommand{Execution: "exec", Sequence: 1, Epoch: 1}},
		{CancelTask: &CancelTaskCommand{Task: "task"}},
		{RebaseGoal: &GoalRebase{Task: "task", NewGoal: "changed", ExpectedEpoch: 1}},
		{PutPending: &action}, {RebindPending: &RebindPendingCommand{Key: "a", Generation: 1}},
		{SettleInvocation: &SettleInvocationCommand{ID: "inv", Result: []byte("result")}},
	}
	for _, c := range commands {
		raw, err := EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := EncodeCommand(decoded)
		if err != nil || !bytes.Equal(raw, canonical) {
			t.Fatalf("roundtrip: %v", err)
		}
		for end := 0; end < len(raw); end++ {
			if _, err := DecodeCommand(raw[:end]); err == nil {
				t.Fatalf("truncated at %d accepted", end)
			}
		}
		if _, err := DecodeCommand(append(raw, 0)); err == nil {
			t.Fatal("trailing command bytes accepted")
		}
	}
	if _, err := EncodeCommand(Command{}); !errors.Is(err, ErrCommandShape) {
		t.Fatal(err)
	}
	if _, err := EncodeCommand(Command{CreateTask: &task, Spawn: &execution}); !errors.Is(err, ErrCommandShape) {
		t.Fatal(err)
	}
	for _, op := range []uint64{0, 10, math.MaxUint64} {
		raw := append([]byte(nil), commandDomain...)
		raw = codec.AppendUint(raw, CommandSchema)
		raw = codec.AppendUint(raw, op)
		raw, _ = codec.AppendBytes(raw, nil)
		if _, err := DecodeCommand(raw); !errors.Is(err, ErrCommandVersion) {
			t.Fatal(err)
		}
	}
	raw := append([]byte(nil), commandDomain...)
	raw = codec.AppendUint(raw, 2)
	if _, err := DecodeCommand(raw); !errors.Is(err, ErrCommandVersion) {
		t.Fatal(err)
	}
	oversize := commandInvocation()
	oversize.Args = make([]byte, MaxCommandBytes+1)
	if _, err := EncodeCommand(Command{RecordInvocation: &oversize}); err == nil {
		t.Fatal("oversize command accepted")
	}
}

func TestApplyCommandScopePrivateCandidateAndOwnership(t *testing.T) {
	l := New()
	task, execution, step := commandTask(), commandExecution(), commandStep()
	for i, c := range []Command{{CreateTask: &task}, {Spawn: &execution}, {StartStep: &step}} {
		if _, err := l.ApplyCommand(commandKey(fmt.Sprint(i)), c); err != nil {
			t.Fatal(err)
		}
	}
	authority := grantPrimaryAuthority(t, l, execution.ID)
	acceptPrimaryResult(t, l, authority, execution.ID, 1, "model-result")
	before := recordCanonical(t, l.Snapshot())
	i := commandInvocation()
	badKey := commandKey("bad")
	badKey.Principal = "other"
	if _, err := l.ApplyCommand(badKey, Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: i, Authority: authority}}}); !errors.Is(err, ErrCommandScope) {
		t.Fatal(err)
	}
	after := recordCanonical(t, l.Snapshot())
	if !bytes.Equal(before, after) {
		t.Fatal("rejected scope changed state")
	}
	result, err := l.ApplyCommand(commandKey("record"), Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: i, Authority: authority}}})
	if err != nil || result.Invocation == nil {
		t.Fatal(err)
	}
	i.Args[0] = 'X'
	result.Invocation.Args[0] = 'Y'
	got, err := l.GetInvocation("inv")
	if err != nil || string(got.Args) != "args" {
		t.Fatalf("input/result aliased ledger: %+v %v", got, err)
	}
	changed := commandInvocation()
	changed.Args = []byte("new args")
	before = recordCanonical(t, l.Snapshot())
	if _, err := l.ApplyCommand(commandKey("changed-slot"), Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: changed, Authority: authority}}}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	after = recordCanonical(t, l.Snapshot())
	if !bytes.Equal(before, after) {
		t.Fatal("rejected unique slot changed state")
	}
	complete := FinishStepCommand{Execution: "exec", Sequence: 1, Epoch: 1, Authority: authority}
	if _, err := l.ApplyCommand(commandKey("complete"), Command{Control: &ControlCommand{FinishStep: &complete}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending invocation allowed step completion: %v", err)
	}
	if _, err := l.ApplyCommand(commandKey("late"), Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: changed, Authority: authority}}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("completed step accepted invocation: %v", err)
	}
	if _, err := l.ApplyCommand(commandKey("cancel"), Command{CancelTask: &CancelTaskCommand{Task: "task"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ApplyCommand(commandKey("late-complete"), Command{Control: &ControlCommand{FinishStep: &complete}}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled task completed step: %v", err)
	}
}

func TestApplyCommandPendingGoalAndConcurrentTasks(t *testing.T) {
	l := New()
	task, execution := commandTask(), commandExecution()
	for i, c := range []Command{{CreateTask: &task}, {Spawn: &execution}} {
		if _, err := l.ApplyCommand(commandKey(fmt.Sprint(i)), c); err != nil {
			t.Fatal(err)
		}
	}
	action := PendingAction{Key: "a", Generation: 1, Request: commandKey("a"), CommandDigest: digest('b'), Owner: "exec", Status: ActionPending}
	if _, err := l.ApplyCommand(commandKey("mismatch"), Command{PutPending: &action}); !errors.Is(err, ErrCommandScope) {
		t.Fatal(err)
	}
	if _, err := l.ApplyCommand(action.Request, Command{PutPending: &action}); err != nil {
		t.Fatal(err)
	}
	result, err := l.ApplyCommand(commandKey("rebind"), Command{RebindPending: &RebindPendingCommand{Key: "a", Generation: 1}})
	if err != nil || result.Pending == nil || result.Pending.Generation != 2 || result.Pending.Request != commandKey("rebind") {
		t.Fatalf("rebind %+v %v", result, err)
	}
	if _, err := l.ApplyCommand(commandKey("goal"), Command{RebaseGoal: &GoalRebase{Task: "task", ExpectedEpoch: 1, NewGoal: "new goal"}}); err != nil {
		t.Fatal(err)
	}
	step := commandStep()
	if _, err := l.ApplyCommand(commandKey("stale"), Command{StartStep: &step}); !errors.Is(err, ErrGoalEpoch) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errorsCh := make(chan error, 32)
	for n := 0; n < 32; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			task := commandTask()
			task.ID = model.EntityID(fmt.Sprintf("task-%d", n))
			_, err := l.ApplyCommand(commandKey(string(task.ID)), Command{CreateTask: &task})
			errorsCh <- err
		}(n)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(l.Snapshot().Tasks) != 33 {
		t.Fatal("concurrent candidates lost a task")
	}
}

func TestLifecycleCommandsRoundTripAndRestore(t *testing.T) {
	l := New()
	task, execution := commandTask(), commandExecution()
	if _, err := l.ApplyCommand(commandKey("lc-task"), Command{CreateTask: &task}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ApplyCommand(commandKey("lc-exec"), Command{Spawn: &execution}); err != nil {
		t.Fatal(err)
	}
	q := Query{ID: "q1", Session: "s1", Space: "space", Owner: "owner", Input: []byte("hello"), Status: "open", Version: 1}
	cmds := []Command{
		{Lifecycle: &LifecycleCommand{PutQuery: &q}},
		{Lifecycle: &LifecycleCommand{PutActivity: &Activity{ID: "a1", Execution: "exec", Kind: "tool", Step: 1, Status: "running", Version: 1}}},
		{Lifecycle: &LifecycleCommand{PutAttempt: &Attempt{ID: "at1", Activity: "a1", Sequence: 1, Incarnation: 1, Status: "running"}}},
		{Lifecycle: &LifecycleCommand{PutResult: &ActivityResult{Capture: "full", ID: "r1", Activity: "a1", Attempt: "at1", Incarnation: 1, Outcome: "success", Payload: []byte("ok")}}},
		{Lifecycle: &LifecycleCommand{PutMessage: &Message{ID: "m1", Sender: "exec", Recipient: "exec", GoalEpoch: 1, Payload: []byte("msg")}}},
		{Lifecycle: &LifecycleCommand{ConsumeMessage: &MessageConsumption{Message: "m1", Recipient: "exec", Incarnation: 1, GoalEpoch: 1}}},
	}
	for i, c := range cmds {
		raw, err := EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = l.ApplyCommand(commandKey(fmt.Sprintf("lc-%d", i)), decoded); err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
		if l.Snapshot().Lifecycle.Recovery != nil {
			t.Fatal("compatibility command changed historical snapshot bytes by creating recovery metadata")
		}
	}
	legacyContinuation := Command{Lifecycle: &LifecycleCommand{PutContinuation: &Continuation{Execution: "exec", Version: 1, Incarnation: 1, PendingKey: "next", Phase: "resume"}}}
	if _, err := EncodeCommand(legacyContinuation); !errors.Is(err, ErrCommandShape) {
		t.Fatalf("untyped continuation admitted: %v", err)
	}
	restored := restoreRecord(t, l.Snapshot())
	if _, err := restored.GetQuery("q1"); err != nil {
		t.Fatal(err)
	}
	if got, err := restored.GetActivityResult("r1"); err != nil || string(got.Payload) != "ok" {
		t.Fatalf("result=%+v err=%v", got, err)
	}
}
