package agentprofile

import "math"

func (l *Ledger) applyObserveGoal(s *RecoverySnapshot, x *ObserveGoalCommand) (Result, error) {
	e, t, err := l.checkExecution(x.Execution, x.Authority, false, true)
	if err != nil {
		return Result{}, err
	}
	if t.GoalEpoch != x.Epoch {
		return Result{}, ErrGoalEpoch
	}
	for i, v := range s.Goals {
		if v.Execution == x.Execution && v.Epoch == x.Epoch {
			if v.Observation == "" {
				v.Observation, v.Evidence = x.Observation, x.Evidence
				s.Goals[i] = v
				e.AcceptedGoalEpoch = x.Epoch
				l.executions[e.ID] = e
				return Result{Execution: &e}, nil
			}
			if v.Evidence != x.Evidence || v.Observation != x.Observation {
				return Result{}, ErrConflict
			}
			return Result{Execution: &e}, nil
		}
	}
	e.AcceptedGoalEpoch = x.Epoch
	l.executions[e.ID] = e
	s.Goals = append(s.Goals, GoalObservation{Execution: e.ID, Epoch: x.Epoch, Evidence: x.Evidence, Observation: x.Observation})
	return Result{Execution: &e}, nil
}

func (l *Ledger) applyCompleteTask(s *RecoverySnapshot, x *CompleteTaskCommand) (Result, error) {
	e, t, err := l.checkExecution(x.Execution, x.Authority, true, true)
	if err != nil {
		return Result{}, err
	}
	if e.Task != x.Task || t.GoalEpoch != x.Epoch {
		return Result{}, ErrGoalEpoch
	}
	for _, v := range s.Outcomes {
		if v.Task == x.Task {
			return Result{}, ErrUniqueSlot
		}
	}
	for _, child := range l.executions {
		if child.Parent == e.ID && child.Status != ExecutionCompleted && child.Status != ExecutionCanceled {
			return Result{}, ErrConflict
		}
		if child.Task == t.ID && child.ID != e.ID && child.Status != ExecutionCompleted && child.Status != ExecutionCanceled {
			return Result{}, ErrConflict
		}
	}
	for _, step := range l.steps {
		if step.Execution == e.ID && step.Status != StepCompleted && step.Status != StepCanceled {
			return Result{}, ErrConflict
		}
	}
	if l.hasUnknownEffectsTree(e.ID) {
		return Result{}, ErrActionUnknown
	}
	if err := l.requireActivityAcceptance(e.ID, 0); err != nil {
		return Result{}, err
	}
	t.Status = TaskCompleted
	e.Status = ExecutionCompleted
	l.tasks[t.ID] = t
	l.executions[e.ID] = e
	var sequence uint64
	for _, v := range s.Outcomes {
		if v.Sequence > sequence {
			sequence = v.Sequence
		}
	}
	if sequence == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	outcome := TaskOutcome{Task: t.ID, Execution: e.ID, GoalEpoch: x.Epoch, Result: x.Result, Validation: x.Validation, Sequence: sequence + 1}
	if requirement, err := LatestValidationRequirement(l.Snapshot(), t.ID, e.ID, x.Epoch); err == nil {
		outcome.ValidationRequirement = requirement.ID
	}
	s.Outcomes = append(s.Outcomes, outcome)
	return Result{Task: &t, Execution: &e}, nil
}

func (l *Ledger) applyCancelExecution(x *CancelExecutionCommand) (Result, error) {
	e, ok := l.executions[x.Execution]
	if !ok {
		return Result{}, ErrUnknown
	}
	if e.Incarnation != x.ExpectedIncarnation {
		return Result{}, ErrConflict
	}
	l.cancelExecutionTree(e.ID)
	e = l.executions[e.ID]
	return Result{Execution: &e}, nil
}

func (l *Ledger) applySpawnChild(s *RecoverySnapshot, c ControlCommand, x *SpawnChildCommand) (Result, error) {
	parent, t, err := l.checkExecution(x.Parent, x.Authority, true, true)
	if err != nil {
		return Result{}, err
	}
	if x.InheritedEpoch != t.GoalEpoch {
		return Result{}, ErrGoalEpoch
	}
	raw, err := encodeControlCommand(c)
	if err != nil {
		return Result{}, err
	}
	intentDigest := commandHash(raw)
	for _, link := range s.TaskLinks {
		if link.ParentExecution == x.Parent && link.Invocation == x.Invocation {
			if link.Task != x.Task.ID || (link.IntentDigest == "" || link.IntentDigest != intentDigest) {
				return Result{}, ErrUniqueSlot
			}
			e, ok := l.executions[x.Execution.ID]
			if !ok || e.Task != link.Task {
				return Result{}, ErrUniqueSlot
			}
			task := l.tasks[link.Task]
			return Result{Task: &task, Execution: &e}, nil
		}
	}
	inv, ok := l.invocations[x.Invocation]
	if !ok || inv.Execution != parent.ID || inv.Status != InvocationPending {
		return Result{}, ErrConflict
	}
	if _, ok = l.tasks[x.Task.ID]; ok {
		return Result{}, ErrConflict
	}
	if _, ok = l.executions[x.Execution.ID]; ok {
		return Result{}, ErrConflict
	}
	l.tasks[x.Task.ID] = x.Task
	l.executions[x.Execution.ID] = x.Execution
	s.TaskLinks = append(s.TaskLinks, TaskLink{Task: x.Task.ID, ParentExecution: parent.ID, Invocation: inv.ID, InheritedTask: t.ID, InheritedEpoch: t.GoalEpoch, Compatibility: x.Compatibility, IntentDigest: intentDigest})
	task, e := x.Task, x.Execution
	// The accepted result of the Spawn command is the child identity. The
	// parent remains responsible for joining or canceling the child tree.
	inv.Status = InvocationAccepted
	inv.Result = []byte(e.ID)
	l.invocations[inv.ID] = inv
	return Result{Task: &task, Execution: &e, Invocation: &inv}, nil
}

func (l *Ledger) applyRebaseInherited(s *RecoverySnapshot, x *RebaseInheritedCommand) (Result, error) {
	e, _, err := l.checkExecution(x.Execution, x.Authority, false, false)
	if err != nil {
		return Result{}, err
	}
	i, ok := findLifecycle(s.TaskLinks, func(v TaskLink) bool { return v.Task == e.Task && v.InheritedTask == x.SourceTask })
	if !ok {
		return Result{}, ErrUnknown
	}
	v := s.TaskLinks[i]
	source, ok := l.tasks[x.SourceTask]
	if !ok || source.Status != TaskOpen || source.GoalEpoch != x.Epoch || v.InheritedEpoch != x.ExpectedEpoch {
		return Result{}, ErrGoalEpoch
	}
	if x.Epoch == x.ExpectedEpoch && v.GoalObservation != "" {
		if v.GoalObservation == x.Observation && v.Compatibility == x.Compatibility {
			return Result{Execution: &e}, nil
		}
		return Result{}, ErrConflict
	}
	v.InheritedEpoch = x.Epoch
	v.Compatibility = x.Compatibility
	v.GoalObservation = x.Observation
	s.TaskLinks[i] = v
	return Result{Execution: &e}, nil
}
