package agentprofile

import (
	"bytes"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func applyCommand(l *Ledger, key model.RequestKey, c Command) (Result, error) {
	if c.Control != nil {
		return l.applyControl(key, *c.Control)
	}
	result := Result{}
	switch {
	case c.CreateTask != nil:
		v, err := l.CreateTask(CreateTask{Task: *c.CreateTask, Request: key})
		if err != nil {
			return Result{}, err
		}
		result.Task = &v
	case c.Spawn != nil:
		v, err := l.Spawn(SpawnRequest{Execution: *c.Spawn, Request: key})
		if err != nil {
			return Result{}, err
		}
		result.Execution = &v
	case c.StartStep != nil:
		v, err := l.StartStep(StepRequest{Step: *c.StartStep, Request: key})
		if err != nil {
			return Result{}, err
		}
		result.Step = &v
	case c.RecordInvocation != nil:
		v, err := l.RecordInvocation(InvocationRequest{Invocation: *c.RecordInvocation, Request: key})
		if err != nil {
			return Result{}, err
		}
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		result.Invocation = &v
	case c.CompleteStep != nil:
		v, err := l.CompleteStep(c.CompleteStep.Execution, c.CompleteStep.Sequence, c.CompleteStep.Epoch)
		if err != nil {
			return Result{}, err
		}
		result.Step = &v
	case c.CancelTask != nil:
		v, err := l.CancelTask(c.CancelTask.Task)
		if err != nil {
			return Result{}, err
		}
		result.Task = &v
	case c.RebaseGoal != nil:
		v, err := l.RebaseGoal(*c.RebaseGoal)
		if err != nil {
			return Result{}, err
		}
		result.Task = &v
	case c.PutPending != nil:
		v, err := l.PutPending(*c.PutPending)
		if err != nil {
			return Result{}, err
		}
		result.Pending = &v
	case c.RebindPending != nil:
		v, err := l.RebindPending(c.RebindPending.Key, c.RebindPending.Generation, key)
		if err != nil {
			return Result{}, err
		}
		result.Pending = &v
	case c.SettleInvocation != nil:
		v, err := l.SettleInvocation(c.SettleInvocation.ID, c.SettleInvocation.Result)
		if err != nil {
			return Result{}, err
		}
		result.Invocation = &v
	case c.Lifecycle != nil:
		if _, err := l.applyLifecycle(*c.Lifecycle); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}
