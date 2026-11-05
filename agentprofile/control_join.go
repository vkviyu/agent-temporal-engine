package agentprofile

import (
	"math"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (l *Ledger) applyExecutionJoin(x JoinExecutionsCommand) (Result, error) {
	if _, _, err := l.checkExecution(x.Join.Parent, x.Authority, true, true); err != nil {
		return Result{}, err
	}
	s := l.recovery()
	j := x.Join
	seen := map[model.ExecutionID]bool{}
	for _, id := range j.Members {
		e, ok := l.executions[id]
		if !ok || e.Parent != j.Parent || seen[id] {
			return Result{}, ErrConflict
		}
		seen[id] = true
	}
	i, found := findLifecycle(s.Joins, func(v ExecutionJoin) bool { return v.ID == j.ID })
	var previous uint64
	if found {
		old := s.Joins[i]
		if old.Parent != j.Parent || old.Mode != j.Mode || old.Quorum != j.Quorum || !reflect.DeepEqual(old.Members, j.Members) {
			return Result{}, ErrConflict
		}
		if old.Status != "pending" {
			return Result{}, nil
		}
		previous = old.Version
	}
	if previous != x.ExpectedVersion || previous == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	type winner struct {
		id  model.ExecutionID
		seq uint64
	}
	var candidates []winner
	for _, o := range s.Outcomes {
		if seen[o.Execution] {
			if err := l.requireActivityAcceptance(o.Execution, 0); err != nil {
				return Result{}, err
			}
			candidates = append(candidates, winner{o.Execution, o.Sequence})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].seq < candidates[j].seq })
	need := len(j.Members)
	if j.Mode == "any" {
		need = 1
	} else if j.Mode == "quorum" {
		need = int(j.Quorum)
	}
	j.Status = "pending"
	if len(candidates) >= need {
		j.Status = "completed"
		for _, v := range candidates[:need] {
			j.Winners = append(j.Winners, v.id)
		}
	} else if x.TimedOut {
		j.Status = "timed_out"
	}
	j.Version = previous + 1
	j.Members = append([]model.ExecutionID(nil), j.Members...)
	if found {
		s.Joins[i] = j
	} else {
		s.Joins = append(s.Joins, j)
	}
	return Result{}, nil
}
