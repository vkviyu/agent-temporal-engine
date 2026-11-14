package agentprofile

import (
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func continuationPositionFixture(t *testing.T) (*Ledger, AuthorityBinding, Continuation, ActionIntent) {
	t.Helper()
	l, binding := recoveryFixture(t)
	step := Step{Execution: "exec", Sequence: 1, GoalEpoch: 1, Status: StepOpen, InputAnchor: recoveryFence()}
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: step, Authority: binding}})
	body := []byte("exact next command")
	intent := ActionIntent{Action: PendingAction{Key: "next", Generation: 1, Request: commandKey("next-command"), CommandDigest: commandHash(body), InputFence: recoveryFence(), Owner: "exec", Status: ActionPending}, Method: "host/v1", ExactCommand: body, Authority: binding}
	x, _ := l.GetExecution("exec")
	c := Continuation{Execution: "exec", Version: 1, Incarnation: 1, PendingKey: "next", PendingGeneration: 1, InputFence: recoveryFence(), Phase: "dispatch", Position: &ContinuationPosition{
		Space: "space", Principal: "owner", Step: 1, KnowledgeFence: recoveryFence(), KnowledgeCursor: model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(digest('b'))}, KnowledgeKey: model.KnowledgeKey{Actor: x.Actor, Execution: "exec"}, AdapterVersion: "host/v1", NextActions: []PendingActionRef{{Key: "next", Generation: 1}},
	}}
	return l, binding, c, intent
}

func TestContinuationPositionRoundTripRestoreAndOwnership(t *testing.T) {
	l, binding, c, intent := continuationPositionFixture(t)
	branch := intent
	branch.Action.Key = "branch"
	branch.Action.Request = commandKey("branch-command")
	recoveryControl(t, l, "branch-intent", ControlCommand{PutAction: &PutActionCommand{Intent: branch, Authority: binding}})
	c.Position.NextActions = append([]PendingActionRef{{Key: "branch", Generation: 1}}, c.Position.NextActions...)
	command := ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: c, Authority: binding}}
	if command.SchemaVersion() != 6 {
		t.Fatal("recovery position did not select schema 6")
	}
	raw, err := EncodeCommand(Command{Control: &command})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCommand(raw)
	if err != nil || !reflect.DeepEqual(decoded.Control.Checkpoint.Continuation, c) {
		t.Fatalf("position lost by command codec: %+v %v", decoded, err)
	}
	recoveryControl(t, l, "checkpoint", command)
	c.Position.AdapterVersion = "caller-mutated"
	c.Position.NextActions[0].Generation = 9
	got, err := l.GetContinuation("exec")
	if err != nil || got.Position.AdapterVersion != "host/v1" || got.Position.NextActions[0].Generation != 1 {
		t.Fatalf("caller retained mutable checkpoint aliases: %+v %v", got, err)
	}
	got.Position.AdapterVersion = "read-mutated"
	got.Position.NextActions[0].Generation = 10
	image, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSnapshot(image)
	if err != nil {
		t.Fatal(err)
	}
	got, err = restored.GetContinuation("exec")
	if err != nil || got.Position.AdapterVersion != "host/v1" || got.Position.Step != 1 || got.Position.NextActions[0].Generation != 1 {
		t.Fatalf("restore lost exact recovery position: %+v %v", got, err)
	}
	downgraded := cloneContinuation(got)
	downgraded.Version++
	downgraded.Position = nil
	assertRecoveryRejected(t, restored, Command{Control: &ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: downgraded, ExpectedVersion: 1, Authority: binding}}}, ErrCommandShape)
	clearedKnowledge := cloneContinuation(got)
	clearedKnowledge.Version++
	clearedKnowledge.Position.KnowledgeFence = model.CommitFence{}
	clearedKnowledge.Position.KnowledgeCursor = model.Cursor{}
	clearedKnowledge.Position.KnowledgeKey = model.KnowledgeKey{}
	assertRecoveryRejected(t, restored, Command{Control: &ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: clearedKnowledge, ExpectedVersion: 1, Authority: binding}}}, ErrConflict)
	changedAdapter := cloneContinuation(got)
	changedAdapter.Version++
	changedAdapter.Position.AdapterVersion = "host/v2"
	assertRecoveryRejected(t, restored, Command{Control: &ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: changedAdapter, ExpectedVersion: 1, Authority: binding}}}, ErrConflict)
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.Lifecycle.Continuations[0].Position.Principal = "other" },
		func(s *Snapshot) { s.Lifecycle.Continuations[0].Position.Step = 99 },
		func(s *Snapshot) { s.Lifecycle.Continuations[0].Position.KnowledgeKey.Actor = "other" },
		func(s *Snapshot) {
			s.Lifecycle.Continuations[0].Position.NextActions = append([]PendingActionRef{{Key: "missing", Generation: 1}}, s.Lifecycle.Continuations[0].Position.NextActions...)
		},
	} {
		corrupt := restored.Snapshot()
		mutate(&corrupt)
		if _, err := EncodeSnapshot(corrupt); err == nil {
			t.Fatal("accepted structurally valid but unrelated recovery location")
		}
	}
}

func TestContinuationPositionResumeRequiresMatchingAdapter(t *testing.T) {
	l, binding, c, intent := continuationPositionFixture(t)
	recoveryControl(t, l, "checkpoint", ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: c, Authority: binding}})
	recoveryControl(t, l, "fence", ControlCommand{FenceAuthority: &FenceAuthorityCommand{Execution: "exec", ExpectedVersion: 1, Evidence: digest('a')}})
	next := binding
	next.Incarnation++
	next.OwnerEpoch++
	next.Holder = "replacement"
	resume := ResumePlan{Execution: "exec", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: next, AdapterVersion: "host/v2", Checkpoint: digest('e'), CompatibilityEvidence: digest('f'), ResourceFenceEvidence: digest('a')}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{Resume: &resume}}, ErrConflict)
	resume.AdapterVersion = "host/v1"
	recoveryControl(t, l, "matching-resume", ControlCommand{Resume: &resume})
}

func TestContinuationPositionNewCheckpointRequiresLatestStep(t *testing.T) {
	l, binding, c, intent := continuationPositionFixture(t)
	c.Position.Step = 0
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: c, Authority: binding}}}, ErrConflict)
}

func TestContinuationPositionRejectsMissingNextActionAtomically(t *testing.T) {
	l, binding, c, intent := continuationPositionFixture(t)
	c.Position.NextActions = append([]PendingActionRef{{Key: "missing", Generation: 1}}, c.Position.NextActions...)
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: c, Authority: binding}}}, ErrInvalid)
	if _, err := l.GetPending("next"); err == nil {
		t.Fatal("failed checkpoint published only its primary intent")
	}
}

func TestContinuationPositionRequiresTypedV1Encoding(t *testing.T) {
	_, binding, c, intent := continuationPositionFixture(t)
	legacy := c
	legacy.Position = nil
	if _, err := EncodeCommand(Command{Control: &ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: legacy, Authority: binding}}}); err == nil {
		t.Fatal("untyped continuation accepted by V1 codec")
	}
	raw, err := EncodeCommand(Command{Lifecycle: &LifecycleCommand{PutContinuation: &c}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCommand(raw)
	if err != nil || !reflect.DeepEqual(*decoded.Lifecycle.PutContinuation, c) {
		t.Fatalf("typed continuation round trip: %v", err)
	}
}

func TestContinuationPositionAllowsInitialNoKnowledgeCutThenBindsIt(t *testing.T) {
	l, binding, c, intent := continuationPositionFixture(t)
	initial := cloneContinuation(c)
	initial.Position.KnowledgeFence = model.CommitFence{}
	initial.Position.KnowledgeCursor = model.Cursor{}
	initial.Position.KnowledgeKey = model.KnowledgeKey{}
	recoveryControl(t, l, "initial-checkpoint", ControlCommand{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: initial, Authority: binding}})
	c.Version = 2
	recoveryControl(t, l, "bind-knowledge", ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: c, ExpectedVersion: 1, Authority: binding}})
	initial.Version = 3
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: initial, ExpectedVersion: 2, Authority: binding}}}, ErrConflict)
}
