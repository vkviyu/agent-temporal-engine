package agentprofile

import (
	"bytes"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// ApplyCommand atomically applies a validated pure transition. It first builds
// and validates a private candidate, then swaps its owned maps while holding
// the ledger mutex. A rejected command never changes visible state. No IO,
// callback, publication or external side effect occurs here.
func (l *Ledger) ApplyCommand(key model.RequestKey, command Command) (Result, error) {
	if err := key.Validate(); err != nil {
		return Result{}, err
	}
	raw, err := EncodeCommand(command)
	if err != nil {
		return Result{}, err
	}
	command, err = DecodeCommand(raw)
	if err != nil {
		return Result{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	candidate := l.commandCloneLocked()
	if err = commandScope(candidate, key, command); err != nil {
		return Result{}, err
	}
	if err = commandPrecondition(candidate, command); err != nil {
		return Result{}, err
	}
	result, err := applyCommand(candidate, key, command)
	if err != nil {
		return Result{}, err
	}
	image, err := EncodeSnapshot(candidate.Snapshot())
	if err != nil {
		return Result{}, err
	}
	candidate, err = RestoreSnapshot(image)
	if err != nil {
		return Result{}, err
	}
	l.tasks, l.executions, l.steps, l.invocations, l.actions, l.slots, l.lifecycle = candidate.tasks, candidate.executions, candidate.steps, candidate.invocations, candidate.actions, candidate.slots, candidate.lifecycle
	return result, nil
}

func (l *Ledger) commandCloneLocked() *Ledger {
	out := New()
	out.lifecycle = cloneLifecycle(l.lifecycle)
	for k, v := range l.tasks {
		out.tasks[k] = v
	}
	for k, v := range l.executions {
		out.executions[k] = v
	}
	for k, v := range l.steps {
		out.steps[k] = v
	}
	for k, v := range l.invocations {
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		out.invocations[k] = v
	}
	for k, v := range l.actions {
		out.actions[k] = v
	}
	for k, v := range l.slots {
		out.slots[k] = v
	}
	return out
}
