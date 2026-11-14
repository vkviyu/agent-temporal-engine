package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

func (l *Ledger) applyControl(key model.RequestKey, c ControlCommand) (Result, error) {
	s := l.recovery()
	switch {
	case c.AnswerQuery != nil:
		return l.applyAnswerQuery(s, c.AnswerQuery)
	case c.StartWork != nil:
		return l.applyStartWork(s, c, c.StartWork)
	case c.GrantAuthority != nil:
		return l.applyGrantAuthority(s, c.GrantAuthority)
	case c.FenceAuthority != nil:
		return l.applyFenceAuthority(c.FenceAuthority)
	case c.Resume != nil:
		return l.applyResume(s, c.Resume)
	case c.PutIdempotencyGuarantee != nil:
		return l.putIdempotencyGuarantee(c.PutIdempotencyGuarantee.Guarantee)
	case c.AuthorizeRetry != nil:
		return l.authorizeRetry(c.AuthorizeRetry.Authorization)
	case c.CreateActivity != nil:
		return l.applyCreateActivity(s, c.CreateActivity)
	case c.ClaimAttempt != nil:
		return l.applyClaimAttempt(s, c.ClaimAttempt)
	case c.DispatchAttempt != nil:
		return l.applyDispatchAttempt(s, c.DispatchAttempt)
	case c.CaptureOutputChunk != nil:
		return Result{}, l.captureOutputChunk(c.CaptureOutputChunk.Chunk)
	case c.CaptureResult != nil:
		return l.applyCaptureResult(s, c.CaptureResult.Result)
	case c.AcceptActivity != nil:
		return l.applyAcceptActivity(s, c.AcceptActivity)
	case c.SettleActivity != nil:
		return l.applySettleActivity(s, c.SettleActivity)
	case c.ReconcileAttempt != nil:
		return l.applyReconcileAttempt(s, c.ReconcileAttempt)
	case c.RetryActivity != nil:
		return l.applyRetryActivity(s, c.RetryActivity)
	case c.SaveContinuation != nil:
		return l.applySaveContinuation(s, c.SaveContinuation)
	case c.ObserveGoal != nil:
		return l.applyObserveGoal(s, c.ObserveGoal)
	case c.CompleteTask != nil:
		return l.applyCompleteTask(s, c.CompleteTask)
	case c.CancelExecution != nil:
		return l.applyCancelExecution(c.CancelExecution)
	case c.SpawnChild != nil:
		return l.applySpawnChild(s, c, c.SpawnChild)
	case c.PutAction != nil:
		return l.applyPutAction(s, c.PutAction)
	case c.ResolveAction != nil:
		return l.applyResolveAction(s, c.ResolveAction)
	case c.RebindAction != nil:
		return l.applyRebindAction(s, c.RebindAction)
	case c.BeginStep != nil:
		return l.applyBeginStep(key, c.BeginStep)
	case c.RecordInvocation != nil:
		return l.applyRecordInvocation(key, c.RecordInvocation)
	case c.FinishStep != nil:
		return l.applyFinishStep(s, c.FinishStep)
	case c.JoinExecutions != nil:
		return l.applyExecutionJoin(*c.JoinExecutions)
	case c.RebaseInherited != nil:
		return l.applyRebaseInherited(s, c.RebaseInherited)
	case c.Checkpoint != nil:
		return l.applyCheckpoint(key, c.Checkpoint)
	case c.SendMessage != nil:
		return l.applySendMessage(c.SendMessage)
	case c.ConsumeMessage != nil:
		return l.applyConsumeMessage(c.ConsumeMessage)
	case c.ImportActivityResult != nil:
		return l.applyImportActivityResult(s, c.ImportActivityResult)
	case c.BindValidation != nil:
		return l.applyBindValidation(s, c.BindValidation)
	}
	return Result{}, ErrCommandShape
}
