package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

func (l *Ledger) applyCheckpoint(key model.RequestKey, x *CheckpointCommand) (Result, error) {
	if _, err := l.applyControl(key, ControlCommand{
		PutAction: &PutActionCommand{
			Intent:    x.Intent,
			Authority: x.Authority,
		},
	}); err != nil {
		return Result{}, err
	}
	if _, err := l.applyControl(key, ControlCommand{
		SaveContinuation: &SaveContinuationCommand{
			Value:           x.Continuation,
			ExpectedVersion: x.ExpectedVersion,
			Authority:       x.Authority,
		},
	}); err != nil {
		return Result{}, err
	}
	a := l.actions[x.Intent.Action.Key]
	return Result{Pending: &a}, nil
}
