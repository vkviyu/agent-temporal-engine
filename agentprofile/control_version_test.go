package agentprofile

import (
	"testing"
)

func TestControlV6WorkStartSlotBindsImmutableOriginalIntent(t *testing.T) {
	l := New()
	q := Query{ID: "query", Space: "space", Owner: "owner", Version: 1, Status: "open"}
	recoveryApply(t, l, "query", Command{Lifecycle: &LifecycleCommand{PutQuery: &q}})
	start := StartWorkCommand{Query: q.ID, Slot: "work", ExpectedQueryVersion: 1, Task: commandTask(), Execution: commandExecution()}
	command := ControlCommand{StartWork: &start}
	recoveryControl(t, l, "start", command)
	if command.SchemaVersion() != 6 || l.Snapshot().Lifecycle.Recovery.Starts[0].IntentDigest == "" {
		t.Fatal("new start slot did not retain original command commitment")
	}
	changed := start
	changed.Task.Goal = "different intent under the same task ID"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{StartWork: &changed}}, ErrUniqueSlot)
	changed = start
	changed.Execution.Actor = "different-worker"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{StartWork: &changed}}, ErrUniqueSlot)
	recoveryApply(t, l, "rebase", Command{RebaseGoal: &GoalRebase{Task: "task", NewGoal: "later goal", ExpectedEpoch: 1}})
	recoveryControl(t, l, "original-start-new-request", command)
	if len(l.Snapshot().Tasks) != 1 || len(l.Snapshot().Executions) != 1 {
		t.Fatal("slot retry spawned another execution after later task mutation")
	}
}

func TestControlV6ChildSlotRejectsChangedBodyAfterGoalRebase(t *testing.T) {
	l, authority := recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: authority}})
	inv := commandInvocation()
	inv.Status, inv.Result = InvocationPending, nil
	acceptPrimaryResult(t, l, authority, "exec", 1, inv.AcceptedResult)
	recoveryControl(t, l, "invocation", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: authority}})
	task := commandTask()
	task.ID = "child-task"
	e := commandExecution()
	e.ID, e.Task, e.Parent = "child", task.ID, "exec"
	spawn := SpawnChildCommand{Parent: "exec", Invocation: inv.ID, Task: task, Execution: e, InheritedEpoch: 1, Compatibility: digest('a'), Authority: authority}
	recoveryControl(t, l, "spawn", ControlCommand{SpawnChild: &spawn})
	changed := spawn
	changed.Task.Goal = "changed child goal"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{SpawnChild: &changed}}, ErrUniqueSlot)
	changed = spawn
	changed.Execution.Actor = "other-actor"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{SpawnChild: &changed}}, ErrUniqueSlot)
	recoveryApply(t, l, "later-child-goal", Command{RebaseGoal: &GoalRebase{Task: task.ID, ExpectedEpoch: 1, NewGoal: "revised child goal"}})
	recoveryControl(t, l, "original-spawn-new-request", ControlCommand{SpawnChild: &spawn})
	if len(l.Snapshot().Lifecycle.Recovery.TaskLinks) != 1 {
		t.Fatal("original child binding changed")
	}
}
