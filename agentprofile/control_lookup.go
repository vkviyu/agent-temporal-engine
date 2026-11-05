package agentprofile

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (l *Ledger) recovery() *RecoverySnapshot {
	if l.lifecycle.Recovery == nil {
		l.lifecycle.Recovery = &RecoverySnapshot{}
	}
	return l.lifecycle.Recovery
}
func (l *Ledger) authority(id model.ExecutionID) (int, ExecutionAuthority, bool) {
	s := l.lifecycle.Recovery
	if s == nil {
		return 0, ExecutionAuthority{}, false
	}
	i, ok := findLifecycle(s.Authorities, func(a ExecutionAuthority) bool { return a.Execution == id })
	if !ok {
		return 0, ExecutionAuthority{}, false
	}
	return i, s.Authorities[i], true
}
func (l *Ledger) activitySpec(id string) (ActivitySpec, error) {
	if l.lifecycle.Recovery == nil {
		return ActivitySpec{}, ErrUnknown
	}
	for _, s := range l.lifecycle.Recovery.ActivitySpecs {
		if s.Activity == id {
			return s, nil
		}
	}
	return ActivitySpec{}, ErrUnknown
}
func (l *Ledger) activityControl(id string) (int, ActivityControl, error) {
	s := l.lifecycle.Recovery
	if s == nil {
		return 0, ActivityControl{}, ErrUnknown
	}
	i, ok := findLifecycle(s.ActivityControls, func(a ActivityControl) bool { return a.Activity == id })
	if !ok {
		return 0, ActivityControl{}, ErrUnknown
	}
	return i, s.ActivityControls[i], nil
}
func (l *Ledger) attemptControl(id string) (int, AttemptControl, error) {
	s := l.lifecycle.Recovery
	if s == nil {
		return 0, AttemptControl{}, ErrUnknown
	}
	i, ok := findLifecycle(s.AttemptControls, func(a AttemptControl) bool { return a.Attempt == id })
	if !ok {
		return 0, AttemptControl{}, ErrUnknown
	}
	return i, s.AttemptControls[i], nil
}
func (l *Ledger) query(id string) (int, Query, error) {
	i, ok := findLifecycle(l.lifecycle.Queries, func(q Query) bool { return q.ID == id })
	if !ok {
		return 0, Query{}, ErrUnknown
	}
	return i, l.lifecycle.Queries[i], nil
}

func (l *Ledger) checkExecution(eid model.ExecutionID, b AuthorityBinding, goal, inherited bool) (Execution, Task, error) {
	x, ok := l.executions[eid]
	if !ok {
		return Execution{}, Task{}, ErrUnknown
	}
	t, ok := l.tasks[x.Task]
	if !ok {
		return Execution{}, Task{}, ErrUnknown
	}
	if t.Status == TaskCanceled || x.Status == ExecutionCanceled || x.CancelEpoch != t.CancelEpoch {
		return x, t, ErrCanceled
	}
	if t.Status != TaskOpen || x.Status == ExecutionCompleted {
		return x, t, ErrConflict
	}
	_, a, ok := l.authority(eid)
	if !ok || a.Status != "active" || a.Binding != b || x.Incarnation != b.Incarnation {
		return x, t, ErrConflict
	}
	seen := map[model.ExecutionID]bool{}
	for p := x.Parent; p != ""; {
		if seen[p] {
			return x, t, ErrConflict
		}
		seen[p] = true
		parent, ok := l.executions[p]
		if !ok {
			return x, t, ErrUnknown
		}
		pt := l.tasks[parent.Task]
		if parent.Status == ExecutionCanceled || pt.Status == TaskCanceled {
			return x, t, ErrCanceled
		}
		if _, authority, managed := l.authority(parent.ID); managed && authority.Status != "active" {
			return x, t, ErrConflict
		}
		if inherited {
			if err := l.checkInheritedTask(pt.ID); err != nil {
				return x, t, err
			}
		}
		p = parent.Parent
	}
	if goal {
		if t.GoalEpoch != x.AcceptedGoalEpoch {
			return x, t, ErrGoalEpoch
		}
		if t.GoalEpoch > 1 {
			found := false
			for _, o := range l.recovery().Goals {
				found = found || (o.Execution == eid && o.Epoch == t.GoalEpoch && o.Observation != "")
			}
			if !found {
				return x, t, ErrGoalEpoch
			}
		}
	}
	if inherited {
		if err := l.checkInheritedTask(x.Task); err != nil {
			return x, t, err
		}
	}
	return x, t, nil
}

// Request namespaces remain reserved across all generations. Reusing an old
// key would let an earlier durable receipt stand in for a different action.
func (l *Ledger) actionRequestReserved(request model.RequestKey) bool {
	if l.lifecycle.Recovery == nil {
		return false
	}
	for _, intent := range l.lifecycle.Recovery.Actions {
		if intent.Action.Request == request {
			return true
		}
	}
	return false
}

func (l *Ledger) checkInheritedTask(task model.EntityID) error {
	for _, link := range l.recovery().TaskLinks {
		if link.Task == task {
			source, ok := l.tasks[link.InheritedTask]
			if !ok || source.Status != TaskOpen || source.GoalEpoch != link.InheritedEpoch {
				return ErrGoalEpoch
			}
			if link.InheritedEpoch > 1 && link.GoalObservation == "" {
				return ErrGoalEpoch
			}
		}
	}
	return nil
}
func (l *Ledger) checkActivity(spec ActivitySpec, b AuthorityBinding) error {
	if spec.Execution != "" {
		_, t, err := l.checkExecution(spec.Execution, b, true, true)
		if err != nil {
			return err
		}
		if spec.GoalEpoch != t.GoalEpoch {
			return ErrGoalEpoch
		}
		if spec.Invocation != "" {
			inv, ok := l.invocations[spec.Invocation]
			if !ok {
				return ErrUnknown
			}
			if !activityMatchesInvocation(spec, inv) {
				return ErrConflict
			}
			return validateInvocationSource(inv, l.lifecycle)
		}
		return nil
	}
	_, q, err := l.query(spec.Query)
	if err != nil {
		return err
	}
	if !zeroBinding(b) || q.Status != "open" || q.Version != spec.GoalEpoch {
		return ErrConflict
	}
	return nil
}

func activityMatchesInvocation(spec ActivitySpec, inv Invocation) bool {
	return spec.Execution == inv.Execution && spec.Step == inv.Step &&
		spec.Effect == inv.DispatchClass && spec.Contract.Definition == inv.ToolDefinition
}
func commandHash(b []byte) model.ObjectID {
	s := sha256.Sum256(b)
	return model.ObjectID(hex.EncodeToString(s[:]))
}

func cloneExpectedFence(fence *model.CommitFence) *model.CommitFence {
	if fence == nil {
		return nil
	}
	copy := *fence
	return &copy
}
