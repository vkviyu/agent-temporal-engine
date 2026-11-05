package agentprofile

import (
	"math"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (l *Ledger) setAttemptStatus(id, status string) {
	for i, v := range l.lifecycle.Attempts {
		if v.ID == id {
			v.Status = status
			l.lifecycle.Attempts[i] = v
			return
		}
	}
}
func (l *Ledger) setActivityStatus(id, status string, version uint64) {
	for i, v := range l.lifecycle.Activities {
		if v.ID == id {
			v.Status = status
			v.Version = version
			l.lifecycle.Activities[i] = v
			return
		}
	}
}
func (l *Ledger) fenceExecutionWork(id model.ExecutionID) {
	s := l.recovery()
	for i, a := range s.AttemptControls {
		spec, err := l.activitySpec(a.Activity)
		if err == nil && spec.Execution == id && (a.Status == "dispatch_recorded" || a.Status == "created") {
			if a.Status == "created" {
				a.Status = "fenced_before_dispatch"
			} else {
				a.Status = "effect_unknown"
			}
			s.AttemptControls[i] = a
			l.setAttemptStatus(a.Attempt, a.Status)
		}
	}
	for key, a := range l.actions {
		if a.Owner == id && (a.Status == ActionPending || a.Status == ActionRebound) {
			a.Status = ActionFenced
			l.actions[key] = a
		}
	}
}

// fenceExecutionTree propagates an authority loss through the execution
// hierarchy. A child worker must not remain active under a parent that has
// been fenced and later resumed with a different incarnation; each child
// needs its own explicit resume and fresh authority evidence.
func (l *Ledger) fenceExecutionTree(id model.ExecutionID, evidence model.ObjectID) {
	pending := []model.ExecutionID{id}
	seen := map[model.ExecutionID]bool{}
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[current] {
			continue
		}
		seen[current] = true
		l.fenceExecutionWork(current)
		if i, authority, ok := l.authority(current); ok && authority.Status == "active" {
			if authority.Version != math.MaxUint64 {
				authority.Version++
			}
			authority.Status = "fenced"
			authority.Evidence = evidence
			l.recovery().Authorities[i] = authority
		}
		if execution, ok := l.executions[current]; ok && execution.Status != ExecutionCanceled && execution.Status != ExecutionCompleted {
			execution.Status = ExecutionBlocked
			l.executions[current] = execution
		}
		for _, child := range l.executions {
			if child.Parent == current {
				pending = append(pending, child.ID)
			}
		}
	}
}

func (l *Ledger) cancelExecutionTree(id model.ExecutionID) {
	pending := []model.ExecutionID{id}
	seen := map[model.ExecutionID]bool{}
	for len(pending) > 0 {
		eid := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[eid] {
			continue
		}
		seen[eid] = true
		e, ok := l.executions[eid]
		if !ok {
			continue
		}
		if e.Status != ExecutionCompleted {
			e.Status = ExecutionCanceled
			l.executions[eid] = e
		}
		l.fenceExecutionWork(eid)
		if i, a, ok := l.authority(eid); ok {
			a.Status = "canceled"
			l.recovery().Authorities[i] = a
		}
		for _, child := range l.executions {
			if child.Parent == eid {
				pending = append(pending, child.ID)
			}
		}
	}
}
func (l *Ledger) hasUnknownEffects(id model.ExecutionID) bool {
	for _, action := range l.actions {
		if action.Owner != id || action.Status == ActionApplied {
			continue
		}
		if action.Status != ActionFenced {
			return true
		}
		resolvedAbsent := false
		for _, resolution := range l.recovery().Resolutions {
			if resolution.Key == action.Key && resolution.Generation == action.Generation {
				resolvedAbsent = resolvedAbsent || resolution.Outcome == "absent"
			}
		}
		if !resolvedAbsent {
			return true
		}
	}
	for _, a := range l.recovery().AttemptControls {
		spec, err := l.activitySpec(a.Activity)
		if err != nil || spec.Execution != id {
			continue
		}
		if a.Status == "dispatch_recorded" || a.Status == "effect_unknown" {
			return true
		}
	}
	return false
}

func (l *Ledger) hasUnknownEffectsTree(id model.ExecutionID) bool {
	pending := []model.ExecutionID{id}
	seen := map[model.ExecutionID]bool{}
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[current] {
			continue
		}
		seen[current] = true
		if l.hasUnknownEffects(current) {
			return true
		}
		for _, child := range l.executions {
			if child.Parent == current {
				pending = append(pending, child.ID)
			}
		}
	}
	return false
}

// A captured raw result or a settled charge does not accept an activity's
// logical result. Every live task/step obligation needs its unique acceptance;
// an accepted failure is a known terminal counterexample and may be handled
// by the host's validation policy without pretending it succeeded.
func (l *Ledger) requireActivityAcceptance(id model.ExecutionID, step uint64) error {
	for _, invocation := range l.invocations {
		if invocation.Execution == id && (step == 0 || invocation.Step == step) && (invocation.Status == InvocationPending || invocation.Status == InvocationUnknown) {
			return ErrConflict
		}
	}
	for _, spec := range l.recovery().ActivitySpecs {
		if spec.Execution != id || (step != 0 && spec.Step != step) {
			continue
		}
		_, control, err := l.activityControl(spec.Activity)
		if err != nil {
			return err
		}
		if control.AcceptedResult == "" {
			return ErrConflict
		}
	}
	return nil
}
