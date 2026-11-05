package agentprofile

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func recoveryFence() model.CommitFence {
	return model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digest('f'))}
}

func recoveryApply(t *testing.T, l *Ledger, id string, c Command) Result {
	t.Helper()
	v, err := l.ApplyCommand(commandKey(id), c)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return v
}

func recoveryControl(t *testing.T, l *Ledger, id string, c ControlCommand) Result {
	t.Helper()
	return recoveryApply(t, l, id, Command{Control: &c})
}

func recoveryFixture(t *testing.T) (*Ledger, AuthorityBinding) {
	t.Helper()
	l := New()
	task, execution := commandTask(), commandExecution()
	recoveryApply(t, l, "task", Command{CreateTask: &task})
	recoveryApply(t, l, "exec", Command{Spawn: &execution})
	b := AuthorityBinding{Deployment: "deployment", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryControl(t, l, "authority", ControlCommand{GrantAuthority: &GrantAuthorityCommand{Authority: ExecutionAuthority{Execution: execution.ID, Binding: b, Status: "active", Evidence: digest('a')}}})
	return l, b
}

func recoveryActivity(t *testing.T, l *Ledger, b AuthorityBinding, id, effect string) ActivitySpec {
	t.Helper()
	a := ActivitySpec{Activity: id, Execution: "exec", Role: "resource_prepare", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('b'), RequestDigest: digest('c'), Recipient: "executor", Effect: effect, ResourceScope: "workspace"}
	recoveryControl(t, l, "activity-"+id, ControlCommand{CreateActivity: &CreateActivityCommand{Spec: a, Authority: b}})
	return a
}

func recoveryTakeover(t *testing.T, l *Ledger, old AuthorityBinding) AuthorityBinding {
	t.Helper()
	_, a, _ := l.authority("exec")
	recoveryControl(t, l, "fence", ControlCommand{FenceAuthority: &FenceAuthorityCommand{Execution: "exec", ExpectedVersion: a.Version, Evidence: digest('d')}})
	next := old
	next.OwnerEpoch++
	next.Incarnation++
	next.Holder = "replacement"
	recoveryControl(t, l, "resume", ControlCommand{Resume: &ResumePlan{Execution: "exec", ExpectedIncarnation: old.Incarnation, ExpectedAuthorityVersion: a.Version + 1, Next: next, AdapterVersion: "host/v1", Checkpoint: digest('e'), CompatibilityEvidence: digest('f'), ResourceFenceEvidence: digest('a')}})
	return next
}

func assertRecoveryRejected(t *testing.T, l *Ledger, c Command, want error) {
	t.Helper()
	before, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.ApplyCommand(commandKey("rejected"), c); !errors.Is(err, want) {
		t.Fatalf("got %v; want %v", err, want)
	}
	after, _ := EncodeSnapshot(l.Snapshot())
	if !bytes.Equal(before, after) {
		t.Fatal("rejected recovery command changed state")
	}
}

func TestParentFencePropagatesToDescendantAuthority(t *testing.T) {
	l, parentBinding := recoveryFixture(t)
	step := Step{Execution: "exec", Sequence: 1, GoalEpoch: 1, Status: StepOpen, InputAnchor: recoveryFence()}
	recoveryControl(t, l, "parent-step", ControlCommand{BeginStep: &BeginStepCommand{Step: step, Authority: parentBinding}})
	inv := Invocation{ID: "delegate-child", Execution: "exec", Step: 1, ProviderCallKey: "delegate-child", Tool: "delegate", Attempt: 1, Status: InvocationPending, AcceptedResult: "result", ToolDefinition: digest('d'), ToolVersion: "delegate/v1", DispatchClass: "pure"}
	acceptPrimaryResult(t, l, parentBinding, "exec", 1, inv.AcceptedResult)
	recoveryControl(t, l, "delegate-child", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: parentBinding}})
	childTask := Task{ID: "child-task", Space: "space", Owner: "owner", Goal: "child", GoalEpoch: 1, Status: TaskOpen}
	child := Execution{ID: "child-exec", Task: childTask.ID, Parent: "exec", Actor: "child", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}
	recoveryControl(t, l, "child-spawn", ControlCommand{SpawnChild: &SpawnChildCommand{Parent: "exec", Invocation: inv.ID, Task: childTask, Execution: child, InheritedEpoch: 1, Compatibility: digest('a'), Authority: parentBinding}})
	childBinding := parentBinding
	childBinding.Holder = "child-worker"
	recoveryControl(t, l, "child-authority", ControlCommand{GrantAuthority: &GrantAuthorityCommand{Authority: ExecutionAuthority{Execution: child.ID, Binding: childBinding, Status: "active", Evidence: digest('b')}}})
	recoveryControl(t, l, "parent-fence", ControlCommand{FenceAuthority: &FenceAuthorityCommand{Execution: "exec", ExpectedVersion: 1, Evidence: digest('c')}})
	state := l.Snapshot().Lifecycle.Recovery
	var childAuthority ExecutionAuthority
	for _, authority := range state.Authorities {
		if authority.Execution == child.ID {
			childAuthority = authority
		}
	}
	if childAuthority.Status != "fenced" || childAuthority.Version != 2 {
		t.Fatalf("descendant authority was not fenced: %+v", childAuthority)
	}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{BeginStep: &BeginStepCommand{Step: Step{Execution: child.ID, Sequence: 1, GoalEpoch: 1, Status: StepOpen, InputAnchor: recoveryFence()}, Authority: childBinding}}}, ErrConflict)
	parentNext := parentBinding
	parentNext.Deployment, parentNext.OwnerEpoch, parentNext.Incarnation, parentNext.Holder = "replacement", 2, 2, "parent-replacement"
	recoveryControl(t, l, "parent-resume", ControlCommand{Resume: &ResumePlan{Execution: "exec", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: parentNext, AdapterVersion: "host/v1", Checkpoint: digest('d'), CompatibilityEvidence: digest('e'), ResourceFenceEvidence: digest('f')}})
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{BeginStep: &BeginStepCommand{Step: Step{Execution: child.ID, Sequence: 1, GoalEpoch: 1, Status: StepOpen, InputAnchor: recoveryFence()}, Authority: childBinding}}}, ErrConflict)
	childNext := childBinding
	childNext.Deployment, childNext.OwnerEpoch, childNext.Incarnation, childNext.Holder = "replacement", 2, 2, "child-replacement"
	// The explicit child fence is idempotent after inherited propagation.
	childFence := ControlCommand{FenceAuthority: &FenceAuthorityCommand{Execution: child.ID, ExpectedVersion: 1, Evidence: digest('a')}}
	if raw, err := EncodeCommand(Command{Control: &childFence}); err != nil {
		t.Fatalf("child fence encode: %v", err)
	} else if _, err := DecodeCommand(raw); err != nil {
		t.Fatalf("child fence decode: %v", err)
	}
	recoveryControl(t, l, "child-fence", childFence)
	recoveryControl(t, l, "child-resume", ControlCommand{Resume: &ResumePlan{Execution: child.ID, ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: childNext, AdapterVersion: "host/v1", Checkpoint: digest('a'), CompatibilityEvidence: digest('b'), ResourceFenceEvidence: digest('c')}})
}

func TestRecoveryUnknownEffectsQuarantineAndNoEffectRetry(t *testing.T) {
	l, old := recoveryFixture(t)
	a := recoveryActivity(t, l, old, "write", "non_idempotent")
	recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "attempt-1", Activity: a.Activity, Sequence: 1, Authority: old}})
	recoveryControl(t, l, "dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "attempt-1", Authority: old, Evidence: digest('d')}})
	next := recoveryTakeover(t, l, old)
	late := ActivityResult{ID: "late", Activity: a.Activity, Attempt: "attempt-1", RequestDigest: a.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("old worker")}
	recoveryControl(t, l, "late-capture", ControlCommand{CaptureResult: &CaptureResultCommand{Result: late}})
	s := l.Snapshot().Lifecycle.Recovery
	if len(s.Captures) != 1 || !s.Captures[0].Quarantined {
		t.Fatal("stale worker result was not quarantined")
	}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: a.Activity, Result: late.ID, ExpectedVersion: 1, Authority: next, Evidence: digest('e')}}}, ErrConflict)
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RetryActivity: &RetryActivityCommand{Activity: a.Activity, ExpectedVersion: 1, Authority: next}}}, ErrActionUnknown)
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{ReconcileAttempt: &ReconcileAttemptCommand{Attempt: "attempt-1", Outcome: "no_effect", Evidence: digest('f')}}}, ErrCommandShape)
	recoveryControl(t, l, "no-effect", ControlCommand{ReconcileAttempt: &ReconcileAttemptCommand{Attempt: "attempt-1", Outcome: "no_effect", FutureEffectFenced: true, Evidence: digest('f')}})
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{ReconcileAttempt: &ReconcileAttemptCommand{Attempt: "attempt-1", ExpectedVersion: 1, Outcome: "unresolved", Evidence: digest('f')}}}, ErrConflict)
	recoveryControl(t, l, "retry", ControlCommand{RetryActivity: &RetryActivityCommand{Activity: a.Activity, ExpectedVersion: 1, Authority: next}})
	recoveryControl(t, l, "claim-2", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "attempt-2", Activity: a.Activity, Sequence: 2, Authority: next}})
	recoveryControl(t, l, "dispatch-2", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "attempt-2", Authority: next, Evidence: digest('d')}})
	result := ActivityResult{ID: "current", Activity: a.Activity, Attempt: "attempt-2", RequestDigest: a.RequestDigest, Incarnation: 2, Outcome: "success", Payload: []byte("replacement")}
	recoveryControl(t, l, "capture-2", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
	duplicate := result
	duplicate.ID = "second-result"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CaptureResult: &CaptureResultCommand{Result: duplicate}}}, ErrUniqueSlot)
	recoveryControl(t, l, "accept-2", ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: a.Activity, Result: result.ID, ExpectedVersion: 2, Authority: next, Evidence: digest('e')}})
	raw, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, ctl, err := restored.activityControl(a.Activity)
	if err != nil || ctl.AcceptedResult != result.ID || ctl.Incarnation != 2 {
		t.Fatalf("recovery lost accepted result: %+v %v", ctl, err)
	}
}

func TestRecoveryFencedUndispatchedAttemptCanRetry(t *testing.T) {
	l, old := recoveryFixture(t)
	a := recoveryActivity(t, l, old, "not-sent", "non_idempotent")
	recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "not-dispatched", Activity: a.Activity, Sequence: 1, Authority: old}})
	next := recoveryTakeover(t, l, old)
	_, at, _ := l.attemptControl("not-dispatched")
	if at.Status != "fenced_before_dispatch" {
		t.Fatalf("unattempted outbox status: %s", at.Status)
	}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: at.Attempt, Authority: old, Evidence: digest('d')}}}, ErrConflict)
	recoveryControl(t, l, "retry", ControlCommand{RetryActivity: &RetryActivityCommand{Activity: a.Activity, ExpectedVersion: 1, Authority: next}})
	recoveryControl(t, l, "new-attempt", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "new-attempt", Activity: a.Activity, Sequence: 2, Authority: next}})
}

func TestRecoveryAtomicCheckpointAndRebindRequiresFencedGeneration(t *testing.T) {
	l, old := recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: old}})
	body := []byte("exact canonical host command")
	intent := ActionIntent{Action: PendingAction{Key: "next", Generation: 1, Request: commandKey("original-action"), CommandDigest: commandHash(body), InputFence: recoveryFence(), Owner: "exec", Status: ActionPending}, Method: "host/v1", ExactCommand: body, Authority: old, InputObjects: []model.ObjectID{digest('b')}}
	continuation := Continuation{Execution: "exec", Version: 1, Incarnation: 1, PendingKey: "next", PendingGeneration: 1, InputFence: recoveryFence(), Phase: "dispatch"}
	bad := CheckpointCommand{Intent: intent, Continuation: continuation, Authority: old, ExpectedVersion: 0}
	bad.Continuation.Version = 2
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{Checkpoint: &bad}}, ErrCommandShape)
	if _, err := l.GetPending("next"); !errors.Is(err, ErrUnknown) {
		t.Fatal("failed continuation leaked its pending intent")
	}
	continuation.Position = &ContinuationPosition{Space: "space", Principal: "owner", Step: 1, KnowledgeFence: recoveryFence(), KnowledgeCursor: model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(digest('b'))}, KnowledgeKey: model.KnowledgeKey{Actor: "actor", Execution: "exec"}, AdapterVersion: "host/v1", NextActions: []PendingActionRef{{Key: "next", Generation: 1}}}
	recoveryControl(t, l, "checkpoint", ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: continuation, Authority: old}})
	assertRecoveryRejected(t, l, Command{RebindPending: &RebindPendingCommand{Key: "next", Generation: 1}}, ErrConflict)
	recoveryControl(t, l, "absent-live", ControlCommand{ResolveAction: &ResolveActionCommand{Resolution: ActionResolution{Key: "next", Generation: 1, Outcome: "absent", ObservedFence: recoveryFence()}}})
	rebind := RebindActionCommand{Key: "next", ExpectedGeneration: 1, NewRequest: commandKey("new-action"), Method: intent.Method, ExactCommand: body, CommandDigest: commandHash(body), Authority: old, FenceEvidence: digest('a')}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RebindAction: &rebind}}, ErrConflict)
	next := recoveryTakeover(t, l, old)
	recoveryControl(t, l, "unknown", ControlCommand{ResolveAction: &ResolveActionCommand{Resolution: ActionResolution{Key: "next", Generation: 1, Outcome: "unknown", ObservedFence: recoveryFence()}}})
	rebind.Authority = next
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RebindAction: &rebind}}, ErrConflict)
	recoveryControl(t, l, "absent", ControlCommand{ResolveAction: &ResolveActionCommand{Resolution: ActionResolution{Key: "next", Generation: 1, Outcome: "absent", ObservedFence: recoveryFence()}}})
	r := recoveryControl(t, l, "rebind", ControlCommand{RebindAction: &rebind})
	if r.Pending == nil || r.Pending.Generation != 2 || r.Pending.Request != rebind.NewRequest {
		t.Fatalf("wrong rebound action: %+v", r.Pending)
	}
	checkpoint, err := l.GetContinuation("exec")
	if err != nil || checkpoint.PendingGeneration != 1 {
		t.Fatal("rebind silently rewrote historical continuation")
	}
}

func TestRecoveryManagedCompatibilityCommandsCannotBypassAuthority(t *testing.T) {
	l, b := recoveryFixture(t)
	a := recoveryActivity(t, l, b, "activity", "pure")
	child := commandExecution()
	child.ID, child.Parent = "bypass-child", "exec"
	for _, c := range []Command{
		{Spawn: &child},
		{StartStep: &Step{Execution: "exec", Sequence: 1, GoalEpoch: 1, Status: StepOpen}},
		{Lifecycle: &LifecycleCommand{PutActivity: &Activity{ID: "legacy", Execution: "exec", Kind: "tool", Version: 1, Status: "running"}}},
		{Lifecycle: &LifecycleCommand{PutAttempt: &Attempt{ID: "legacy", Activity: a.Activity, Sequence: 1, Incarnation: 1, Status: "running"}}},
		{Lifecycle: &LifecycleCommand{PutResult: &ActivityResult{ID: "legacy", Activity: a.Activity, Attempt: "missing", Incarnation: 1, Outcome: "success"}}},
		{Lifecycle: &LifecycleCommand{PutMessage: &Message{ID: "legacy", Sender: "exec", Recipient: "exec", GoalEpoch: 1}}},
	} {
		assertRecoveryRejected(t, l, c, ErrConflict)
	}
	assertRecoveryRejected(t, l, Command{Lifecycle: &LifecycleCommand{PutContinuation: &Continuation{Execution: "exec", Version: 1, Incarnation: 1, PendingKey: "missing", Phase: "dispatch"}}}, ErrCommandShape)
	message := Message{ID: "message", Sender: "exec", Recipient: "exec", GoalEpoch: 1, Payload: []byte("same frozen message")}
	recoveryControl(t, l, "send", ControlCommand{SendMessage: &SendMessageCommand{Message: message, Authority: b}})
	consumption := MessageConsumption{Message: message.ID, Recipient: "exec", Incarnation: 1, GoalEpoch: 1}
	recoveryControl(t, l, "consume", ControlCommand{ConsumeMessage: &ConsumeMessageCommand{Consumption: consumption, Authority: b}})
	assertRecoveryRejected(t, l, Command{Lifecycle: &LifecycleCommand{ConsumeMessage: &consumption}}, ErrConflict)
	next := recoveryTakeover(t, l, b)
	consumption.Incarnation = next.Incarnation
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{ConsumeMessage: &ConsumeMessageCommand{Consumption: consumption, Authority: next}}}, ErrConflict)
}

func TestRecoveryChildCancellationRetainsUnknownEffects(t *testing.T) {
	l, parent := recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: parent}})
	inv := commandInvocation()
	inv.Status, inv.Result = InvocationPending, nil
	acceptPrimaryResult(t, l, parent, "exec", 1, inv.AcceptedResult)
	recoveryControl(t, l, "invocation", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: parent}})
	task := commandTask()
	task.ID = "child-task"
	child := commandExecution()
	child.ID, child.Task, child.Parent, child.Actor = "child", task.ID, "exec", "child-actor"
	recoveryControl(t, l, "spawn-child", ControlCommand{SpawnChild: &SpawnChildCommand{Parent: "exec", Invocation: inv.ID, Task: task, Execution: child, InheritedEpoch: 1, Compatibility: digest('b'), Authority: parent}})
	b := parent
	b.Holder = "child-worker"
	recoveryControl(t, l, "child-authority", ControlCommand{GrantAuthority: &GrantAuthorityCommand{Authority: ExecutionAuthority{Execution: child.ID, Status: "active", Binding: b, Evidence: digest('b')}}})
	a := ActivitySpec{Activity: "child-write", Execution: child.ID, Role: "resource_prepare", GoalEpoch: 1, InputFence: recoveryFence(), RequestDigest: digest('c'), Request: digest('b'), Recipient: "executor", Effect: "non_idempotent", ResourceScope: "workspace"}
	recoveryControl(t, l, "child-activity", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: a, Authority: b}})
	recoveryControl(t, l, "child-claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "child-attempt", Activity: a.Activity, Sequence: 1, Authority: b}})
	recoveryControl(t, l, "child-dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "child-attempt", Authority: b, Evidence: digest('d')}})
	recoveryControl(t, l, "cancel-child", ControlCommand{CancelExecution: &CancelExecutionCommand{Execution: child.ID, ExpectedIncarnation: 1}})
	recoveryControl(t, l, "finish-step", ControlCommand{FinishStep: &FinishStepCommand{Execution: "exec", Sequence: 1, Epoch: 1, Authority: parent}})
	completion := CompleteTaskCommand{Task: "task", Execution: "exec", Epoch: 1, Result: digest('e'), Validation: digest('f'), Authority: parent}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CompleteTask: &completion}}, ErrActionUnknown)
	recoveryControl(t, l, "reconcile-child", ControlCommand{ReconcileAttempt: &ReconcileAttemptCommand{Attempt: "child-attempt", Outcome: "no_effect", FutureEffectFenced: true, Evidence: digest('f')}})
	recoveryControl(t, l, "complete-parent", ControlCommand{CompleteTask: &completion})

	// A Task-level cancellation must cross Task boundaries along Parent edges.
	l, parent = recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: parent}})
	acceptPrimaryResult(t, l, parent, "exec", 1, inv.AcceptedResult)
	recoveryControl(t, l, "invocation", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: parent}})
	recoveryControl(t, l, "child", ControlCommand{SpawnChild: &SpawnChildCommand{Parent: "exec", Invocation: inv.ID, Task: task, Execution: child, InheritedEpoch: 1, Compatibility: digest('b'), Authority: parent}})
	recoveryApply(t, l, "cancel-task", Command{CancelTask: &CancelTaskCommand{Task: "task"}})
	got, _ := l.GetExecution(child.ID)
	if got.Status != ExecutionCanceled {
		t.Fatalf("cross-task child survived cancellation: %+v", got)
	}
}

func TestRecoveryGoalBarrierIncludesGrandchildren(t *testing.T) {
	l, root := recoveryFixture(t)
	spawn := func(parent model.ExecutionID, authority AuthorityBinding, name string) AuthorityBinding {
		t.Helper()
		step := Step{Execution: parent, Sequence: 1, GoalEpoch: 1, Status: StepOpen}
		recoveryControl(t, l, name+"-step", ControlCommand{BeginStep: &BeginStepCommand{Step: step, Authority: authority}})
		inv := Invocation{ID: name + "-inv", Execution: parent, Step: 1, ProviderCallKey: name, Tool: "delegate", Status: InvocationPending, Attempt: 1, AcceptedResult: "result", ToolDefinition: digest('d'), ToolVersion: "delegate/v1", DispatchClass: "pure"}
		inv.AcceptedResult = name + "-result"
		acceptPrimaryResult(t, l, authority, parent, 1, inv.AcceptedResult)
		recoveryControl(t, l, name+"-inv", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: authority}})
		task := commandTask()
		task.ID = model.EntityID(name + "-task")
		e := commandExecution()
		e.ID, e.Task, e.Parent = model.ExecutionID(name), task.ID, parent
		recoveryControl(t, l, name+"-spawn", ControlCommand{SpawnChild: &SpawnChildCommand{Parent: parent, Invocation: inv.ID, Task: task, Execution: e, InheritedEpoch: 1, Compatibility: digest('a'), Authority: authority}})
		next := authority
		next.Holder = name
		recoveryControl(t, l, name+"-grant", ControlCommand{GrantAuthority: &GrantAuthorityCommand{Authority: ExecutionAuthority{Execution: e.ID, Binding: next, Status: "active", Evidence: digest('a')}}})
		return next
	}
	child := spawn("exec", root, "child")
	grandchild := spawn("child", child, "grandchild")
	recoveryApply(t, l, "rebase-root", Command{RebaseGoal: &GoalRebase{Task: "task", NewGoal: "revised objective", ExpectedEpoch: 1}})
	spec := ActivitySpec{Activity: "grandchild-work", Execution: "grandchild", Role: "resource_prepare", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('a'), RequestDigest: digest('b'), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CreateActivity: &CreateActivityCommand{Spec: spec, Authority: grandchild}}}, ErrGoalEpoch)
	recoveryControl(t, l, "compatible-child-rebase", ControlCommand{RebaseInherited: &RebaseInheritedCommand{Execution: "child", SourceTask: "task", ExpectedEpoch: 1, Epoch: 2, Compatibility: digest('c'), Authority: child, Observation: digest('d')}})
	recoveryControl(t, l, "compatible-grandchild-work", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: spec, Authority: grandchild}})
}

func TestRecoveryPendingIntentBlocksTaskCompletion(t *testing.T) {
	l, b := recoveryFixture(t)
	body := []byte("exact command")
	intent := ActionIntent{Action: PendingAction{Key: "pending", Generation: 1, Request: commandKey("original"), CommandDigest: commandHash(body), InputFence: recoveryFence(), Owner: "exec", Status: ActionPending}, Method: "host/v1", ExactCommand: body, Authority: b}
	recoveryControl(t, l, "intent", ControlCommand{PutAction: &PutActionCommand{Intent: intent, Authority: b}})
	completion := CompleteTaskCommand{Task: "task", Execution: "exec", Epoch: 1, Result: digest('c'), Validation: digest('d'), Authority: b}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CompleteTask: &completion}}, ErrActionUnknown)
}

func TestRecoverySnapshotRejectsBrokenContinuationAndConsumption(t *testing.T) {
	l, b := recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: b}})
	body := []byte("exact command")
	intent := ActionIntent{Action: PendingAction{Key: "pending", Generation: 1, Request: commandKey("original"), CommandDigest: commandHash(body), InputFence: recoveryFence(), Owner: "exec", Status: ActionPending}, Method: "host/v1", ExactCommand: body, Authority: b}
	continuation := Continuation{Execution: "exec", Version: 1, Incarnation: 1, PendingKey: "pending", PendingGeneration: 1, InputFence: recoveryFence(), Phase: "resume", Position: &ContinuationPosition{Space: "space", Principal: "owner", Step: 1, KnowledgeFence: recoveryFence(), KnowledgeCursor: model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(digest('b'))}, KnowledgeKey: model.KnowledgeKey{Actor: "actor", Execution: "exec"}, AdapterVersion: "host/v1", NextActions: []PendingActionRef{{Key: "pending", Generation: 1}}}}
	recoveryControl(t, l, "checkpoint", ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: continuation, Authority: b}})
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.Lifecycle.Continuations[0].PendingGeneration = 2 },
		func(s *Snapshot) { s.Lifecycle.Continuations[0].Incarnation = 2 },
		func(s *Snapshot) { s.Lifecycle.Recovery.Actions[0].ExactCommand = []byte("changed") },
	} {
		s := l.Snapshot()
		mutate(&s)
		if _, err := EncodeSnapshot(s); err == nil {
			t.Fatal("broken recovery checkpoint accepted by snapshot encoder")
		}
	}
	message := Message{ID: "message", Sender: "exec", Recipient: "exec", GoalEpoch: 1}
	recoveryControl(t, l, "send", ControlCommand{SendMessage: &SendMessageCommand{Message: message, Authority: b}})
	consumption := MessageConsumption{Message: message.ID, Recipient: "exec", GoalEpoch: 1, Incarnation: 1}
	recoveryControl(t, l, "consume", ControlCommand{ConsumeMessage: &ConsumeMessageCommand{Consumption: consumption, Authority: b}})
	s := l.Snapshot()
	s.Lifecycle.Consumptions[0].GoalEpoch = 2
	if _, err := EncodeSnapshot(s); err == nil {
		t.Fatal("message consumption forged across goal epochs")
	}
}

func TestRecoveryRawOrSettledResultCannotCompleteTaskOrStep(t *testing.T) {
	for _, withStep := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound-activity", true: "step-activity"}[withStep], func(t *testing.T) {
			l, b := recoveryFixture(t)
			if withStep {
				recoveryControl(t, l, "begin", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: b}})
			}
			a := ActivitySpec{Activity: "tool", Execution: "exec", Role: "resource_prepare", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('b'), RequestDigest: digest('c'), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
			if withStep {
				a.Step = 1
			}
			recoveryControl(t, l, "activity", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: a, Authority: b}})
			recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "attempt", Activity: a.Activity, Sequence: 1, Authority: b}})
			recoveryControl(t, l, "dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "attempt", Authority: b, Evidence: digest('a')}})
			result := ActivityResult{ID: "failed-result", Activity: a.Activity, Attempt: "attempt", RequestDigest: a.RequestDigest, Incarnation: 1, Outcome: "failure", Payload: []byte("known counterexample")}
			recoveryControl(t, l, "capture", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
			completion := CompleteTaskCommand{Task: "task", Execution: "exec", Epoch: 1, Result: digest('a'), Validation: digest('b'), Authority: b}
			assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CompleteTask: &completion}}, ErrConflict)
			recoveryControl(t, l, "settle-without-accept", ControlCommand{SettleActivity: &SettleActivityCommand{Activity: a.Activity, Status: "settled", Evidence: digest('a')}})
			assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CompleteTask: &completion}}, ErrConflict)
			finish := FinishStepCommand{Execution: "exec", Sequence: 1, Epoch: 1, Authority: b}
			if withStep {
				assertRecoveryRejected(t, l, Command{Control: &ControlCommand{FinishStep: &finish}}, ErrConflict)
			}
			recoveryControl(t, l, "accept-known-failure", ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: a.Activity, Result: result.ID, ExpectedVersion: 1, Authority: b, Evidence: digest('a')}})
			if withStep {
				recoveryControl(t, l, "finish", ControlCommand{FinishStep: &finish})
			}
			recoveryControl(t, l, "complete-after-acceptance", ControlCommand{CompleteTask: &completion})
		})
	}
}

func TestRecoveryCannotInsertInvocationAfterFinishedStep(t *testing.T) {
	l, b := recoveryFixture(t)
	recoveryControl(t, l, "begin", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: b}})
	recoveryControl(t, l, "finish", ControlCommand{FinishStep: &FinishStepCommand{Execution: "exec", Sequence: 1, Epoch: 1, Authority: b}})
	inv := commandInvocation()
	inv.Status, inv.Result = InvocationPending, nil
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: b}}}, ErrConflict)
	step := commandStep()
	step.Sequence = 2
	recoveryControl(t, l, "next-step", ControlCommand{BeginStep: &BeginStepCommand{Step: step, Authority: b}})
}

func TestRecoveryJoinCannotObserveRawSettledChildAsCompleted(t *testing.T) {
	l, parent := recoveryFixture(t)
	recoveryControl(t, l, "parent-step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: parent}})
	inv := commandInvocation()
	inv.Status, inv.Result = InvocationPending, nil
	acceptPrimaryResult(t, l, parent, "exec", 1, inv.AcceptedResult)
	recoveryControl(t, l, "parent-invocation", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: parent}})
	task := commandTask()
	task.ID = "child-task"
	child := commandExecution()
	child.ID, child.Task, child.Parent = "child", task.ID, "exec"
	recoveryControl(t, l, "child-spawn", ControlCommand{SpawnChild: &SpawnChildCommand{Parent: "exec", Invocation: inv.ID, Task: task, Execution: child, InheritedEpoch: 1, Compatibility: digest('a'), Authority: parent}})
	b := parent
	b.Holder = "child-worker"
	recoveryControl(t, l, "child-authority", ControlCommand{GrantAuthority: &GrantAuthorityCommand{Authority: ExecutionAuthority{Execution: child.ID, Status: "active", Binding: b, Evidence: digest('a')}}})
	a := ActivitySpec{Activity: "child-tool", Execution: child.ID, Role: "resource_prepare", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('a'), RequestDigest: digest('b'), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	recoveryControl(t, l, "child-activity", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: a, Authority: b}})
	recoveryControl(t, l, "child-claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Attempt: "child-attempt", Activity: a.Activity, Sequence: 1, Authority: b}})
	recoveryControl(t, l, "child-dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "child-attempt", Authority: b, Evidence: digest('a')}})
	result := ActivityResult{ID: "child-result", Activity: a.Activity, Attempt: "child-attempt", RequestDigest: a.RequestDigest, Incarnation: 1, Outcome: "success"}
	recoveryControl(t, l, "raw-result", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
	recoveryControl(t, l, "raw-settled", ControlCommand{SettleActivity: &SettleActivityCommand{Activity: a.Activity, Status: "settled", Evidence: digest('a')}})
	completion := CompleteTaskCommand{Task: task.ID, Execution: child.ID, Epoch: 1, Result: digest('a'), Validation: digest('b'), Authority: b}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CompleteTask: &completion}}, ErrConflict)
	join := JoinExecutionsCommand{Join: ExecutionJoin{ID: "join", Parent: "exec", Members: []model.ExecutionID{child.ID}, Mode: "any"}, Authority: parent}
	recoveryControl(t, l, "join-before-accept", ControlCommand{JoinExecutions: &join})
	if state := l.Snapshot().Lifecycle.Recovery.Joins[0]; state.Status != "pending" || len(state.Winners) != 0 {
		t.Fatalf("raw child became winner: %+v", state)
	}
	recoveryControl(t, l, "child-accept", ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: a.Activity, Result: result.ID, ExpectedVersion: 1, Authority: b, Evidence: digest('a')}})
	recoveryControl(t, l, "child-complete", ControlCommand{CompleteTask: &completion})
	join.ExpectedVersion = 1
	recoveryControl(t, l, "join-after-accept", ControlCommand{JoinExecutions: &join})
	if state := l.Snapshot().Lifecycle.Recovery.Joins[0]; state.Status != "completed" || len(state.Winners) != 1 || state.Winners[0] != child.ID {
		t.Fatalf("accepted child did not become winner: %+v", state)
	}
}

func TestChildSpawnAcceptanceDoesNotSkipOtherStepObligations(t *testing.T) {
	l, authority := recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: authority}})
	inv := commandInvocation()
	acceptPrimaryResult(t, l, authority, "exec", 1, inv.AcceptedResult)
	recoveryControl(t, l, "invocation", ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: inv, Authority: authority}})
	task := commandTask()
	task.ID = "child-task"
	child := commandExecution()
	child.ID, child.Task, child.Parent = "child", task.ID, "exec"
	spawned := recoveryControl(t, l, "spawn", ControlCommand{SpawnChild: &SpawnChildCommand{Parent: "exec", Invocation: inv.ID, Task: task, Execution: child, InheritedEpoch: 1, Compatibility: digest('a'), Authority: authority}})
	if spawned.Invocation == nil || spawned.Invocation.Status != InvocationAccepted || string(spawned.Invocation.Result) != string(child.ID) {
		t.Fatalf("spawn did not accept its child identity result: %+v", spawned.Invocation)
	}
	a := ActivitySpec{Activity: "other-step-obligation", Execution: "exec", Step: 1, Role: "resource_prepare", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('b'), RequestDigest: digest('c'), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	recoveryControl(t, l, "other-create", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: a, Authority: authority}})
	finish := FinishStepCommand{Execution: "exec", Sequence: 1, Epoch: 1, Authority: authority}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{FinishStep: &finish}}, ErrConflict)
	recoveryControl(t, l, "other-claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Activity: a.Activity, Attempt: "other-attempt", Sequence: 1, Authority: authority}})
	recoveryControl(t, l, "other-dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "other-attempt", Authority: authority, Evidence: digest('a')}})
	result := ActivityResult{ID: "other-result", Activity: a.Activity, Attempt: "other-attempt", RequestDigest: a.RequestDigest, Incarnation: 1, Outcome: "success"}
	recoveryControl(t, l, "other-capture", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
	recoveryControl(t, l, "other-accept", ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: a.Activity, Result: result.ID, ExpectedVersion: 1, Authority: authority, Evidence: digest('a')}})
	recoveryControl(t, l, "finish", ControlCommand{FinishStep: &finish})
}
