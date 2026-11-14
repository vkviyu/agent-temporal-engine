package agentprofile

func (l *Ledger) applySaveContinuation(s *RecoverySnapshot, x *SaveContinuationCommand) (Result, error) {
	v := cloneContinuation(x.Value)
	if _, _, err := l.checkExecution(v.Execution, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	if v.Incarnation != x.Authority.Incarnation {
		return Result{}, ErrConflict
	}
	a, ok := l.actions[v.PendingKey]
	if !ok || a.Owner != v.Execution || a.Generation != v.PendingGeneration || a.InputFence != v.InputFence {
		return Result{}, ErrConflict
	}
	if err := validateContinuationRelations(v, l.Snapshot()); err != nil {
		return Result{}, err
	}
	if v.Position != nil {
		var latestStep uint64
		for _, step := range l.steps {
			if step.Execution == v.Execution && step.Sequence > latestStep {
				latestStep = step.Sequence
			}
		}
		if v.Position.Step != latestStep {
			return Result{}, ErrConflict
		}
		for _, ref := range v.Position.NextActions {
			action, found := l.actions[ref.Key]
			if !found || action.Owner != v.Execution || action.Generation != ref.Generation {
				return Result{}, ErrConflict
			}
		}
	}
	i, found := findLifecycle(l.lifecycle.Continuations, func(c Continuation) bool { return c.Execution == v.Execution })
	var old uint64
	if found {
		old = l.lifecycle.Continuations[i].Version
		if l.lifecycle.Continuations[i].Position != nil && v.Position == nil {
			return Result{}, ErrConflict
		}
		if oldPosition := l.lifecycle.Continuations[i].Position; oldPosition != nil {
			if oldPosition.AdapterVersion != v.Position.AdapterVersion || (!continuationKnowledgeEmpty(oldPosition) && continuationKnowledgeEmpty(v.Position)) {
				return Result{}, ErrConflict
			}
		}
	}
	if old != x.ExpectedVersion {
		return Result{}, ErrConflict
	}
	if found {
		l.lifecycle.Continuations[i] = v
	} else {
		l.lifecycle.Continuations = append(l.lifecycle.Continuations, v)
	}
	return Result{}, nil
}
