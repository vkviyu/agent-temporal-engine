package agentprofile

import (
	"bytes"
	"math"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func commandPrecondition(l *Ledger, c Command) error {
	if c.Control != nil {
		return nil
	}
	if c.CancelTask != nil {
		t, err := l.GetTask(c.CancelTask.Task)
		if err != nil {
			return err
		}
		if t.Status != TaskCanceled && t.CancelEpoch == math.MaxUint64 {
			return ErrConflict
		}
		if t.Status == TaskCompleted {
			return ErrConflict
		}
	}
	var execution model.ExecutionID
	switch {
	case c.Spawn != nil:
		execution = c.Spawn.Parent
	case c.StartStep != nil:
		execution = c.StartStep.Execution
	case c.RecordInvocation != nil:
		execution = c.RecordInvocation.Execution
	case c.CompleteStep != nil:
		execution = c.CompleteStep.Execution
	case c.PutPending != nil:
		execution = c.PutPending.Owner
	case c.RebindPending != nil:
		a, err := l.GetPending(c.RebindPending.Key)
		if err != nil {
			return err
		}
		execution = a.Owner
	case c.SettleInvocation != nil:
		inv, getErr := l.GetInvocation(c.SettleInvocation.ID)
		if getErr != nil {
			return getErr
		}
		execution = inv.Execution
	case c.Lifecycle != nil:
		if err := l.lifecycleAuthorityGuard(*c.Lifecycle); err != nil {
			return err
		}
	}
	if execution != "" {
		if _, _, managed := l.authority(execution); managed {
			return ErrConflict
		}
	}
	if execution != "" {
		x, err := l.GetExecution(execution)
		if err != nil {
			return err
		}
		t, err := l.GetTask(x.Task)
		if err != nil {
			return err
		}
		if t.Status == TaskCanceled || x.Status == ExecutionCanceled || x.CancelEpoch != t.CancelEpoch {
			return ErrCanceled
		}
		if t.Status != TaskOpen || (x.Status != ExecutionReady && x.Status != ExecutionRunning) {
			return ErrConflict
		}
		if x.AcceptedGoalEpoch != t.GoalEpoch {
			return ErrGoalEpoch
		}
	}
	if c.StartStep != nil {
		if old, err := l.GetStep(c.StartStep.Execution, c.StartStep.Sequence); err == nil && old != *c.StartStep {
			return ErrConflict
		}
	}
	if c.CompleteStep != nil {
		old, err := l.GetStep(c.CompleteStep.Execution, c.CompleteStep.Sequence)
		if err != nil {
			return err
		}
		if old.GoalEpoch != c.CompleteStep.Epoch {
			return ErrGoalEpoch
		}
		if old.Status != StepOpen && old.Status != StepAccepted && old.Status != StepCompleted {
			return ErrConflict
		}
	}
	if c.RecordInvocation != nil {
		i := c.RecordInvocation
		step, err := l.GetStep(i.Execution, i.Step)
		if err != nil {
			return err
		}
		if step.Status != StepOpen && step.Status != StepAccepted {
			return ErrConflict
		}
		if old, err := l.GetInvocation(i.ID); err == nil {
			x, e := EncodeInvocation(old)
			y, f := EncodeInvocation(*i)
			if e != nil || f != nil || !bytes.Equal(x, y) {
				return ErrConflict
			}
		}
	}
	if c.SettleInvocation != nil {
		i, err := l.GetInvocation(c.SettleInvocation.ID)
		if err != nil {
			return err
		}
		if i.Status == InvocationSettled {
			if !bytes.Equal(i.Result, c.SettleInvocation.Result) {
				return ErrConflict
			}
		} else if i.Status == InvocationPending || i.Status == InvocationUnknown {
			return ErrConflict
		}
	}
	return nil
}
