package agentprofile

import (
	"bytes"
	"math"
	"slices"
)

func (l *Ledger) applyAnswerQuery(s *RecoverySnapshot, x *AnswerQueryCommand) (Result, error) {
	i, q, err := l.query(x.Query)
	if err != nil {
		return Result{}, err
	}
	for _, a := range s.Answers {
		if a.Query == x.Query && a.Slot == x.Slot {
			if a.Kind == x.Kind && a.Final == x.Final && bytes.Equal(a.Body, x.Body) && a.GeneratedFrom == x.GeneratedFrom && slices.Equal(a.Outcomes, x.Outcomes) && a.InputEvidence == x.InputEvidence {
				return Result{}, nil
			}
			return Result{}, ErrUniqueSlot
		}
	}
	if q.Version != x.ExpectedVersion || q.Status != "open" || q.Version == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	if x.Final {
		if err := validateFinalAnswer(l.Snapshot(), x.Query, x.GeneratedFrom, x.Body, x.Outcomes, x.InputEvidence); err != nil {
			return Result{}, err
		}
	}
	q.Version++
	if x.Final {
		q.Status = "answered"
		q.Answer = bytes.Clone(x.Body)
	}
	l.lifecycle.Queries[i] = q
	s.Answers = append(s.Answers, QueryAnswer{Query: x.Query, Slot: x.Slot, Kind: x.Kind, Version: q.Version, Body: bytes.Clone(x.Body), Final: x.Final, GeneratedFrom: x.GeneratedFrom, Outcomes: append([]TaskOutcome(nil), x.Outcomes...), InputEvidence: x.InputEvidence})
	return Result{}, nil
}

func (l *Ledger) applyStartWork(s *RecoverySnapshot, c ControlCommand, x *StartWorkCommand) (Result, error) {
	raw, err := encodeControlCommand(c)
	if err != nil {
		return Result{}, err
	}
	intentDigest := commandHash(raw)
	for _, a := range s.Starts {
		if a.Query == x.Query && a.Slot == x.Slot {
			if a.Task != x.Task.ID || a.Execution != x.Execution.ID || a.IntentDigest == "" || a.IntentDigest != intentDigest {
				return Result{}, ErrUniqueSlot
			}
			t := l.tasks[a.Task]
			e := l.executions[a.Execution]
			return Result{Task: &t, Execution: &e}, nil
		}
	}
	_, q, err := l.query(x.Query)
	if err != nil {
		return Result{}, err
	}
	if q.Status != "open" || q.Version != x.ExpectedQueryVersion {
		return Result{}, ErrConflict
	}
	if _, ok := l.tasks[x.Task.ID]; ok {
		return Result{}, ErrConflict
	}
	if _, ok := l.executions[x.Execution.ID]; ok {
		return Result{}, ErrConflict
	}
	l.tasks[x.Task.ID] = x.Task
	l.executions[x.Execution.ID] = x.Execution
	s.Starts = append(s.Starts, WorkStart{Query: x.Query, Slot: x.Slot, Task: x.Task.ID, Execution: x.Execution.ID, IntentDigest: intentDigest})
	t, e := x.Task, x.Execution
	return Result{Task: &t, Execution: &e}, nil
}
