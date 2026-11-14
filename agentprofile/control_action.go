package agentprofile

import (
	"bytes"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (l *Ledger) applyPutAction(s *RecoverySnapshot, x *PutActionCommand) (Result, error) {
	v := x.Intent
	if _, _, err := l.checkExecution(v.Action.Owner, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	if v.Action.CommandDigest != commandHash(v.ExactCommand) {
		return Result{}, ErrConflict
	}
	for _, id := range v.InputObjects {
		if !validDigest(id) {
			return Result{}, ErrInvalid
		}
	}
	if _, ok := l.actions[v.Action.Key]; ok {
		return Result{}, ErrUniqueSlot
	}
	if l.actionRequestReserved(v.Action.Request) {
		return Result{}, ErrUniqueSlot
	}
	v.ExactCommand = bytes.Clone(v.ExactCommand)
	v.InputObjects = append([]model.ObjectID(nil), v.InputObjects...)
	v.ExpectedFence = cloneExpectedFence(v.ExpectedFence)
	l.actions[v.Action.Key] = v.Action
	s.Actions = append(s.Actions, v)
	a := v.Action
	return Result{Pending: &a}, nil
}

func (l *Ledger) applyResolveAction(s *RecoverySnapshot, x *ResolveActionCommand) (Result, error) {
	v := x.Resolution
	a, ok := l.actions[v.Key]
	if !ok {
		return Result{}, ErrUnknown
	}
	if a.Generation != v.Generation {
		return Result{}, ErrConflict
	}
	for _, r := range s.Resolutions {
		if r.Key == v.Key && r.Generation == v.Generation && r.Outcome == "applied" {
			if r == v {
				return Result{Pending: &a}, nil
			}
			return Result{}, ErrConflict
		}
	}
	if v.Outcome == "applied" {
		a.Status = ActionApplied
	} else if v.Outcome == "unknown" {
		a.Status = ActionUnknown
	} else {
		a.Status = ActionFenced
	}
	l.actions[a.Key] = a
	s.Resolutions = append(s.Resolutions, v)
	return Result{Pending: &a}, nil
}

func (l *Ledger) applyRebindAction(s *RecoverySnapshot, x *RebindActionCommand) (Result, error) {
	a, ok := l.actions[x.Key]
	if !ok {
		return Result{}, ErrUnknown
	}
	if _, _, err := l.checkExecution(a.Owner, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	if a.Generation != x.ExpectedGeneration || a.Status != ActionFenced || x.NewRequest == a.Request || x.CommandDigest != commandHash(x.ExactCommand) {
		return Result{}, ErrConflict
	}
	if l.actionRequestReserved(x.NewRequest) {
		return Result{}, ErrUniqueSlot
	}
	absent := false
	for _, v := range s.Resolutions {
		if v.Key == a.Key && v.Generation == a.Generation && v.Outcome == "absent" {
			absent = true
		}
	}
	if !absent {
		return Result{}, ErrActionUnknown
	}
	i, ok := findLifecycle(s.Actions, func(v ActionIntent) bool { return v.Action.Key == a.Key && v.Action.Generation == a.Generation })
	if !ok {
		return Result{}, ErrUnknown
	}
	a.Generation++
	a.Request = x.NewRequest
	a.CommandDigest = x.CommandDigest
	a.Status = ActionRebound
	a.Supersedes = a.Key
	l.actions[a.Key] = a
	v := s.Actions[i]
	_, authority, ok := l.authority(a.Owner)
	if !ok || v.Authority == x.Authority || v.Authority.Incarnation >= x.Authority.Incarnation || authority.Evidence != x.FenceEvidence {
		return Result{}, ErrConflict
	}
	v.Action = a
	v.Method = x.Method
	v.ExactCommand = bytes.Clone(x.ExactCommand)
	v.Authority = x.Authority
	v.ExpectedFence = cloneExpectedFence(x.ExpectedFence)
	s.Actions = append(s.Actions, v)
	return Result{Pending: &a}, nil
}
