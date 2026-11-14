package agentprofile

func (l *Ledger) applySendMessage(x *SendMessageCommand) (Result, error) {
	_, sender, err := l.checkExecution(x.Message.Sender, x.Authority, true, true)
	if err != nil {
		return Result{}, err
	}
	recipient, ok := l.executions[x.Message.Recipient]
	if !ok {
		return Result{}, ErrUnknown
	}
	task := l.tasks[recipient.Task]
	if task.Owner != sender.Owner || task.Space != sender.Space || task.GoalEpoch != x.Message.GoalEpoch || task.Status != TaskOpen || recipient.Status == ExecutionCanceled || recipient.Status == ExecutionCompleted {
		return Result{}, ErrConflict
	}
	return l.applyLifecycle(LifecycleCommand{PutMessage: &x.Message})
}

func (l *Ledger) applyConsumeMessage(x *ConsumeMessageCommand) (Result, error) {
	_, task, err := l.checkExecution(x.Consumption.Recipient, x.Authority, true, true)
	if err != nil {
		return Result{}, err
	}
	message, err := l.GetMessage(x.Consumption.Message)
	if err != nil {
		return Result{}, err
	}
	if message.Recipient != x.Consumption.Recipient || message.GoalEpoch != task.GoalEpoch || x.Consumption.GoalEpoch != message.GoalEpoch || x.Consumption.Incarnation != x.Authority.Incarnation {
		return Result{}, ErrConflict
	}
	return l.applyLifecycle(LifecycleCommand{ConsumeMessage: &x.Consumption})
}
