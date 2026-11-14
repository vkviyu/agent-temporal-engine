package agentprofile

import "math"

func (l *Ledger) applyBindValidation(s *RecoverySnapshot, x *BindValidationCommand) (Result, error) {
	r := x.Requirement
	e, task, err := l.checkExecution(r.Spec.Execution, x.Authority, true, true)
	if err != nil {
		return Result{}, err
	}
	goal, err := GoalKnowledgeDigest(task)
	if err != nil || r.Spec.Task != task.ID || e.Task != task.ID || r.Spec.Space != task.Space || r.Spec.GoalEpoch != task.GoalEpoch || r.Spec.Goal != goal {
		return Result{}, ErrValidation
	}
	var previous uint64
	for _, old := range s.ValidationRequirements {
		if old.Spec.Task == task.ID && old.Spec.Execution == e.ID && old.Spec.GoalEpoch == task.GoalEpoch && old.Version > previous {
			previous = old.Version
		}
	}
	if previous != x.ExpectedVersion || previous == math.MaxUint64 || r.Version != previous+1 {
		return Result{}, ErrConflict
	}
	s.ValidationRequirements = append(s.ValidationRequirements, r)
	return Result{}, nil
}
