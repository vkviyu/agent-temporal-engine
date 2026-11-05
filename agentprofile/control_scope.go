package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

func (l *Ledger) controlScope(key model.RequestKey, c ControlCommand) error {
	var eid model.ExecutionID
	queryID := ""
	switch {
	case c.AnswerQuery != nil:
		queryID = c.AnswerQuery.Query
	case c.StartWork != nil:
		queryID = c.StartWork.Query
		if c.StartWork.Task.Space != key.Space || c.StartWork.Task.Owner != key.Principal {
			return ErrCommandScope
		}
	case c.GrantAuthority != nil:
		eid = c.GrantAuthority.Authority.Execution
	case c.FenceAuthority != nil:
		eid = c.FenceAuthority.Execution
	case c.Resume != nil:
		eid = c.Resume.Execution
	case c.PutIdempotencyGuarantee != nil:
		s, err := l.activitySpec(c.PutIdempotencyGuarantee.Guarantee.Activity)
		if err != nil {
			return err
		}
		eid, queryID = s.Execution, s.Query
	case c.AuthorizeRetry != nil:
		s, err := l.activitySpec(c.AuthorizeRetry.Authorization.Activity)
		if err != nil {
			return err
		}
		eid, queryID = s.Execution, s.Query
	case c.CreateActivity != nil:
		eid = c.CreateActivity.Spec.Execution
		queryID = c.CreateActivity.Spec.Query
	case c.ClaimAttempt != nil:
		s, err := l.activitySpec(c.ClaimAttempt.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.DispatchAttempt != nil:
		_, a, err := l.attemptControl(c.DispatchAttempt.Attempt)
		if err != nil {
			return err
		}
		s, err := l.activitySpec(a.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.CaptureResult != nil:
		s, err := l.activitySpec(c.CaptureResult.Result.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.AcceptActivity != nil:
		s, err := l.activitySpec(c.AcceptActivity.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.SettleActivity != nil:
		s, err := l.activitySpec(c.SettleActivity.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.ReconcileAttempt != nil:
		_, a, err := l.attemptControl(c.ReconcileAttempt.Attempt)
		if err != nil {
			return err
		}
		s, err := l.activitySpec(a.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.RetryActivity != nil:
		s, err := l.activitySpec(c.RetryActivity.Activity)
		if err != nil {
			return err
		}
		eid = s.Execution
		queryID = s.Query
	case c.SaveContinuation != nil:
		eid = c.SaveContinuation.Value.Execution
	case c.ObserveGoal != nil:
		eid = c.ObserveGoal.Execution
	case c.CompleteTask != nil:
		eid = c.CompleteTask.Execution
	case c.CancelExecution != nil:
		eid = c.CancelExecution.Execution
	case c.SpawnChild != nil:
		eid = c.SpawnChild.Parent
		if c.SpawnChild.Task.Space != key.Space || c.SpawnChild.Task.Owner != key.Principal {
			return ErrCommandScope
		}
	case c.PutAction != nil:
		eid = c.PutAction.Intent.Action.Owner
		if c.PutAction.Intent.Action.Request.Space != key.Space || c.PutAction.Intent.Action.Request.Principal != key.Principal {
			return ErrCommandScope
		}
	case c.ResolveAction != nil:
		a, ok := l.actions[c.ResolveAction.Resolution.Key]
		if !ok {
			return ErrUnknown
		}
		eid = a.Owner
	case c.RebindAction != nil:
		a, ok := l.actions[c.RebindAction.Key]
		if !ok {
			return ErrUnknown
		}
		eid = a.Owner
		if c.RebindAction.NewRequest.Space != key.Space || c.RebindAction.NewRequest.Principal != key.Principal {
			return ErrCommandScope
		}
	case c.BeginStep != nil:
		eid = c.BeginStep.Step.Execution
	case c.RecordInvocation != nil:
		eid = c.RecordInvocation.Invocation.Execution
	case c.FinishStep != nil:
		eid = c.FinishStep.Execution
	case c.JoinExecutions != nil:
		eid = c.JoinExecutions.Join.Parent
	case c.RebaseInherited != nil:
		eid = c.RebaseInherited.Execution
	case c.Checkpoint != nil:
		eid = c.Checkpoint.Continuation.Execution
		if c.Checkpoint.Intent.Action.Request.Space != key.Space || c.Checkpoint.Intent.Action.Request.Principal != key.Principal {
			return ErrCommandScope
		}
	case c.SendMessage != nil:
		eid = c.SendMessage.Message.Sender
	case c.ConsumeMessage != nil:
		eid = c.ConsumeMessage.Consumption.Recipient
	case c.ImportActivityResult != nil:
		s, err := l.activitySpec(c.ImportActivityResult.Activity)
		if err != nil {
			return err
		}
		eid, queryID = s.Execution, s.Query
	case c.BindValidation != nil:
		eid = c.BindValidation.Requirement.Spec.Execution
	}
	if queryID != "" {
		_, q, err := l.query(queryID)
		if err != nil {
			return err
		}
		if q.Space != key.Space || q.Owner != key.Principal {
			return ErrCommandScope
		}
		return nil
	}
	x, ok := l.executions[eid]
	if !ok {
		return ErrUnknown
	}
	t, ok := l.tasks[x.Task]
	if !ok {
		return ErrUnknown
	}
	if t.Space != key.Space || t.Owner != key.Principal {
		return ErrCommandScope
	}
	return nil
}
