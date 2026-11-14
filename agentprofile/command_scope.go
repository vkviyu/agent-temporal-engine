package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

func commandScope(l *Ledger, key model.RequestKey, c Command) error {
	if c.Control != nil {
		return l.controlScope(key, *c.Control)
	}
	if c.CreateTask != nil {
		if c.CreateTask.Space != key.Space || c.CreateTask.Owner != key.Principal {
			return ErrCommandScope
		}
		return nil
	}
	var task Task
	var err error
	taskForExecution := func(id model.ExecutionID) (Task, error) {
		x, e := l.GetExecution(id)
		if e != nil {
			return Task{}, e
		}
		return l.GetTask(x.Task)
	}
	switch {
	case c.Spawn != nil:
		task, err = l.GetTask(c.Spawn.Task)
	case c.StartStep != nil:
		task, err = taskForExecution(c.StartStep.Execution)
	case c.RecordInvocation != nil:
		task, err = taskForExecution(c.RecordInvocation.Execution)
	case c.CompleteStep != nil:
		task, err = taskForExecution(c.CompleteStep.Execution)
	case c.CancelTask != nil:
		task, err = l.GetTask(c.CancelTask.Task)
	case c.RebaseGoal != nil:
		task, err = l.GetTask(c.RebaseGoal.Task)
	case c.PutPending != nil:
		if c.PutPending.Request != key {
			return ErrCommandScope
		}
		task, err = taskForExecution(c.PutPending.Owner)
	case c.RebindPending != nil:
		var a PendingAction
		a, err = l.GetPending(c.RebindPending.Key)
		if err == nil {
			task, err = taskForExecution(a.Owner)
		}
	case c.SettleInvocation != nil:
		var i Invocation
		i, err = l.GetInvocation(c.SettleInvocation.ID)
		if err == nil {
			task, err = taskForExecution(i.Execution)
		}
	case c.Lifecycle != nil:
		// Lifecycle records carry their own execution identity. Scope is
		// checked against the authenticated task/owner when an execution is
		// referenced; query records carry Space/Owner directly.
		if q := c.Lifecycle.PutQuery; q != nil {
			if q.Space != key.Space || q.Owner != key.Principal {
				return ErrCommandScope
			}
			return nil
		}
		if a := c.Lifecycle.PutActivity; a != nil {
			task, err = taskForExecution(a.Execution)
		}
		if a := c.Lifecycle.PutAttempt; a != nil {
			act, e := l.GetActivity(a.Activity)
			if e != nil {
				err = e
			} else {
				task, err = taskForExecution(act.Execution)
			}
		}
		if r := c.Lifecycle.PutResult; r != nil {
			act, e := l.GetActivity(r.Activity)
			if e != nil {
				err = e
			} else {
				task, err = taskForExecution(act.Execution)
			}
		}
		if m := c.Lifecycle.PutMessage; m != nil {
			task, err = taskForExecution(m.Sender)
		}
		if m := c.Lifecycle.ConsumeMessage; m != nil {
			msg, e := l.GetMessage(m.Message)
			if e != nil {
				err = e
			} else {
				task, err = taskForExecution(msg.Recipient)
			}
		}
		if x := c.Lifecycle.PutContinuation; x != nil {
			task, err = taskForExecution(x.Execution)
		}
	}
	if err != nil {
		return err
	}
	if task.Space != key.Space || task.Owner != key.Principal {
		return ErrCommandScope
	}
	return nil
}
