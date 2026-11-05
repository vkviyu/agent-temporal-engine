package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

// lifecycleAuthorityGuard prevents generic lifecycle records from bypassing
// the authority and incarnation checks once an execution is managed by the
// typed recovery controller.
func (l *Ledger) lifecycleAuthorityGuard(c LifecycleCommand) error {
	var executions []model.ExecutionID
	activity := ""
	switch {
	case c.PutActivity != nil:
		executions = append(executions, c.PutActivity.Execution)
	case c.PutAttempt != nil:
		activity = c.PutAttempt.Activity
	case c.PutResult != nil:
		activity = c.PutResult.Activity
	case c.PutContinuation != nil:
		executions = append(executions, c.PutContinuation.Execution)
	case c.PutMessage != nil:
		executions = append(executions, c.PutMessage.Sender, c.PutMessage.Recipient)
	case c.ConsumeMessage != nil:
		executions = append(executions, c.ConsumeMessage.Recipient)
	}
	if activity != "" {
		if _, err := l.activitySpec(activity); err == nil {
			return ErrConflict
		}
		v, err := l.GetActivity(activity)
		if err != nil {
			return err
		}
		executions = append(executions, v.Execution)
	}
	for _, id := range executions {
		if _, _, managed := l.authority(id); managed {
			return ErrConflict
		}
	}
	return nil
}
