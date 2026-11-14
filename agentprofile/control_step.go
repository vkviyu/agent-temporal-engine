package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

func (l *Ledger) applyBeginStep(key model.RequestKey, x *BeginStepCommand) (Result, error) {
	if _, _, err := l.checkExecution(x.Step.Execution, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	v, err := l.StartStep(StepRequest{Step: x.Step, Request: key})
	if err != nil {
		return Result{}, err
	}
	return Result{Step: &v}, nil
}

func (l *Ledger) applyRecordInvocation(key model.RequestKey, x *RecordInvocationCommand) (Result, error) {
	if _, _, err := l.checkExecution(x.Invocation.Execution, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	step, ok := l.steps[stepKey(x.Invocation.Execution, x.Invocation.Step)]
	if !ok || (step.Status != StepOpen && step.Status != StepAccepted) {
		return Result{}, ErrConflict
	}
	v, err := l.RecordInvocation(InvocationRequest{Invocation: x.Invocation, Request: key})
	if err != nil {
		return Result{}, err
	}
	return Result{Invocation: &v}, nil
}

func (l *Ledger) applyFinishStep(s *RecoverySnapshot, x *FinishStepCommand) (Result, error) {
	if _, _, err := l.checkExecution(x.Execution, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	if err := l.requireActivityAcceptance(x.Execution, x.Sequence); err != nil {
		return Result{}, err
	}
	for _, a := range s.ActivitySpecs {
		if a.Execution == x.Execution && a.Step == x.Sequence && a.Role == "primary_decision" {
			_, ctl, err := l.activityControl(a.Activity)
			if err != nil || ctl.AcceptedResult == "" {
				return Result{}, ErrConflict
			}
		}
	}
	v, err := l.CompleteStep(x.Execution, x.Sequence, x.Epoch)
	if err != nil {
		return Result{}, err
	}
	return Result{Step: &v}, nil
}
