package agentprofile

import "math"

func (l *Ledger) applyGrantAuthority(s *RecoverySnapshot, x *GrantAuthorityCommand) (Result, error) {
	a := x.Authority
	e, ok := l.executions[a.Execution]
	if !ok {
		return Result{}, ErrUnknown
	}
	t := l.tasks[e.Task]
	if t.Status != TaskOpen || e.Status == ExecutionCanceled || e.Status == ExecutionCompleted || a.Binding.Incarnation != e.Incarnation {
		return Result{}, ErrCanceled
	}
	if _, old, ok := l.authority(a.Execution); ok {
		if old.Binding == a.Binding && old.Evidence == a.Evidence && old.Status == "active" {
			return Result{Execution: &e}, nil
		}
		return Result{}, ErrConflict
	}
	if x.ExpectedVersion != 0 {
		return Result{}, ErrConflict
	}
	a.Version = 1
	s.Authorities = append(s.Authorities, a)
	return Result{Execution: &e}, nil
}

func (l *Ledger) applyFenceAuthority(x *FenceAuthorityCommand) (Result, error) {
	_, a, ok := l.authority(x.Execution)
	if !ok {
		return Result{}, ErrUnknown
	}
	if a.Status != "active" && a.Status != "fenced" {
		return Result{}, ErrConflict
	}
	// A parent fence propagates to descendants. A worker may still issue
	// its own fence command after that propagation; treat the already
	// fenced version as an idempotent acknowledgement so Resume can use
	// the version produced by the inherited barrier.
	if a.Status == "fenced" && (a.Version == x.ExpectedVersion || (x.ExpectedVersion != math.MaxUint64 && a.Version == x.ExpectedVersion+1)) {
		e := l.executions[x.Execution]
		return Result{Execution: &e}, nil
	}
	if a.Version != x.ExpectedVersion || a.Version == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	l.fenceExecutionTree(x.Execution, x.Evidence)
	e := l.executions[x.Execution]
	return Result{Execution: &e}, nil
}

func (l *Ledger) applyResume(s *RecoverySnapshot, x *ResumePlan) (Result, error) {
	for _, continuation := range l.lifecycle.Continuations {
		if continuation.Execution == x.Execution && continuation.Position != nil && continuation.Position.AdapterVersion != x.AdapterVersion {
			// Adapter migration requires a separate typed contract.
			return Result{}, ErrConflict
		}
	}
	i, a, ok := l.authority(x.Execution)
	if !ok {
		return Result{}, ErrUnknown
	}
	e := l.executions[x.Execution]
	t := l.tasks[e.Task]
	if a.Status != "fenced" || a.Version != x.ExpectedAuthorityVersion || a.Version == math.MaxUint64 || e.Incarnation != x.ExpectedIncarnation || x.Next.Incarnation != e.Incarnation+1 || t.Status != TaskOpen || e.Status == ExecutionCanceled || e.Status == ExecutionCompleted || x.Next.OwnerEpoch <= a.Binding.OwnerEpoch {
		return Result{}, ErrConflict
	}
	e.Incarnation++
	e.Status = ExecutionReady
	l.executions[e.ID] = e
	a.Version++
	a.Status = "active"
	a.Binding = x.Next
	a.Evidence = x.ResourceFenceEvidence
	s.Authorities[i] = a
	s.Resumes = append(s.Resumes, *x)
	return Result{Execution: &e}, nil
}
