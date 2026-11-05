package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

// ValidateActionDispatch checks the current durable reservation before a host
// submits its exact core command. It never changes the ledger, dispatches work,
// or treats a historical read anchor as current execution authority.
func (l *Ledger) ValidateActionDispatch(actionKey string, generation uint64, request model.RequestKey) error {
	l.mu.RLock()
	candidate := l.commandCloneLocked()
	l.mu.RUnlock()
	action, ok := candidate.actions[actionKey]
	if !ok {
		return ErrUnknown
	}
	if action.Generation != generation || action.Request != request || (action.Status != ActionPending && action.Status != ActionRebound) {
		return ErrConflict
	}
	if candidate.lifecycle.Recovery == nil {
		return ErrConflict
	}
	for _, intent := range candidate.lifecycle.Recovery.Actions {
		if intent.Action.Key == actionKey && intent.Action.Generation == generation {
			_, _, err := candidate.checkExecution(action.Owner, intent.Authority, true, true)
			return err
		}
	}
	return ErrUnknown
}
