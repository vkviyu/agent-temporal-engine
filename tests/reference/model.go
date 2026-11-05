// Package reference holds deliberately small executable specifications used by
// differential tests. It imports no production reducer, codec, or profile DTO.
// It models logical transitions, not physical durability or external effects.
package reference

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

var ErrRejected = errors.New("reference transition rejected")

type Task struct {
	Epoch  uint64
	Status string
}
type Execution struct {
	Task, Parent, Status string
	Incarnation, Goal    uint64
}
type Authority struct {
	OwnerEpoch, Incarnation, Version uint64
	Holder, Status                   string
}
type Join struct {
	Parent, Mode, Status string
	Members, Winners     []string
	Quorum, Version      uint64
}
type Profile struct {
	Tasks       map[string]Task
	Executions  map[string]Execution
	Authorities map[string]Authority
	Outcomes    []string
	Joins       map[string]Join
}

func NewProfile() *Profile {
	return &Profile{Tasks: map[string]Task{}, Executions: map[string]Execution{}, Authorities: map[string]Authority{}, Joins: map[string]Join{}}
}
func (p *Profile) Clone() *Profile {
	c := *p
	c.Tasks, c.Executions, c.Authorities, c.Joins = maps.Clone(p.Tasks), maps.Clone(p.Executions), maps.Clone(p.Authorities), maps.Clone(p.Joins)
	c.Outcomes = slices.Clone(p.Outcomes)
	for id, join := range c.Joins {
		join.Members, join.Winners = slices.Clone(join.Members), slices.Clone(join.Winners)
		c.Joins[id] = join
	}
	return &c
}

// Batch publishes no facts unless every transition accepts. This specification
// is intentionally oblivious to production Work framing or private reducers.
func (p *Profile) Batch(steps ...func(*Profile) error) error {
	candidate := p.Clone()
	for _, apply := range steps {
		if err := apply(candidate); err != nil {
			return err
		}
	}
	*p = *candidate
	return nil
}
func (p *Profile) CreateTask(id string) error {
	if id == "" || p.Tasks[id].Status != "" {
		return ErrRejected
	}
	p.Tasks[id] = Task{1, "open"}
	return nil
}
func (p *Profile) Spawn(id, task, parent string) error {
	if id == "" || p.Executions[id].Task != "" || p.Tasks[task].Status != "open" {
		return ErrRejected
	}
	if parent != "" && p.Executions[parent].Status == "" {
		return ErrRejected
	}
	p.Executions[id] = Execution{Task: task, Parent: parent, Status: "ready", Incarnation: 1, Goal: p.Tasks[task].Epoch}
	return nil
}
func (p *Profile) Grant(id, holder string) error {
	x, ok := p.Executions[id]
	if !ok || p.Authorities[id].Version != 0 {
		return ErrRejected
	}
	p.Authorities[id] = Authority{1, x.Incarnation, 1, holder, "active"}
	return nil
}
func (p *Profile) CanRun(id string, auth Authority, goal bool) bool {
	x, ok := p.Executions[id]
	if !ok || x.Status == "completed" || x.Status == "canceled" || p.Tasks[x.Task].Status != "open" || p.Authorities[id] != auth || auth.Status != "active" || auth.Incarnation != x.Incarnation {
		return false
	}
	for parent := x.Parent; parent != ""; parent = p.Executions[parent].Parent {
		ancestor := p.Executions[parent]
		if ancestor.Status == "canceled" || p.Tasks[ancestor.Task].Status != "open" {
			return false
		}
		if a := p.Authorities[parent]; a.Version != 0 && a.Status != "active" {
			return false
		}
	}
	return !goal || x.Goal == p.Tasks[x.Task].Epoch
}
func (p *Profile) Rebase(task string, expected uint64) error {
	t, ok := p.Tasks[task]
	if !ok || t.Status != "open" || t.Epoch != expected {
		return ErrRejected
	}
	t.Epoch++
	p.Tasks[task] = t
	return nil
}
func (p *Profile) Observe(id string, epoch uint64, auth Authority) error {
	if !p.CanRun(id, auth, false) {
		return ErrRejected
	}
	x := p.Executions[id]
	if p.Tasks[x.Task].Epoch != epoch {
		return ErrRejected
	}
	x.Goal = epoch
	p.Executions[id] = x
	return nil
}
func (p *Profile) BeginStep(id string, auth Authority) error {
	if !p.CanRun(id, auth, true) {
		return ErrRejected
	}
	x := p.Executions[id]
	x.Status = "running"
	p.Executions[id] = x
	return nil
}
func (p *Profile) descendants(id string) []string {
	set := map[string]bool{id: true}
	for changed := true; changed; {
		changed = false
		for key, x := range p.Executions {
			if set[x.Parent] && !set[key] {
				set[key], changed = true, true
			}
		}
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
func (p *Profile) Fence(id string, expected uint64) error {
	if a := p.Authorities[id]; a.Status == "fenced" && (a.Version == expected || a.Version == expected+1) {
		return nil
	}
	if p.Authorities[id].Version != expected || p.Executions[id].Task == "" {
		return ErrRejected
	}
	for _, key := range p.descendants(id) {
		a := p.Authorities[key]
		if a.Status == "active" {
			a.Status = "fenced"
			a.Version++
			p.Authorities[key] = a
		}
		x := p.Executions[key]
		if x.Status != "completed" && x.Status != "canceled" {
			x.Status = "blocked"
			p.Executions[key] = x
		}
	}
	return nil
}
func (p *Profile) Resume(id string, expected uint64, next Authority) error {
	x, a := p.Executions[id], p.Authorities[id]
	if x.Task == "" || a.Status != "fenced" || a.Version != expected || x.Status == "canceled" || x.Status == "completed" || p.Tasks[x.Task].Status != "open" || next.OwnerEpoch <= a.OwnerEpoch || next.Incarnation != x.Incarnation+1 {
		return ErrRejected
	}
	x.Status, x.Incarnation = "ready", next.Incarnation
	next.Status, next.Version = "active", a.Version+1
	p.Executions[id], p.Authorities[id] = x, next
	return nil
}
func (p *Profile) CancelExecution(id string, expectedIncarnation uint64) error {
	if p.Executions[id].Task == "" || p.Executions[id].Incarnation != expectedIncarnation {
		return ErrRejected
	}
	for _, key := range p.descendants(id) {
		x := p.Executions[key]
		x.Status = "canceled"
		p.Executions[key] = x
		if a := p.Authorities[key]; a.Version != 0 {
			a.Status = "canceled"
			p.Authorities[key] = a
		}
	}
	return nil
}
func (p *Profile) Complete(id string, auth Authority) error {
	if !p.CanRun(id, auth, true) {
		return ErrRejected
	}
	for otherID, other := range p.Executions {
		if other.Parent == id && other.Status != "completed" && other.Status != "canceled" {
			return ErrRejected
		}
		if other.Task == p.Executions[id].Task && otherID != id && other.Status != "completed" && other.Status != "canceled" {
			return ErrRejected
		}
	}
	x := p.Executions[id]
	x.Status = "completed"
	p.Executions[id] = x
	t := p.Tasks[x.Task]
	t.Status = "completed"
	p.Tasks[x.Task] = t
	p.Outcomes = append(p.Outcomes, id)
	return nil
}
func (p *Profile) Join(id, parent, mode string, members []string, quorum, expected uint64, timedOut bool, auth Authority) error {
	if !p.CanRun(parent, auth, true) || len(members) == 0 {
		return ErrRejected
	}
	seen := map[string]bool{}
	for _, child := range members {
		if seen[child] || p.Executions[child].Parent != parent {
			return ErrRejected
		}
		seen[child] = true
	}
	old := p.Joins[id]
	if old.Version != 0 {
		if old.Parent != parent || old.Mode != mode || old.Quorum != quorum || !slices.Equal(old.Members, members) {
			return ErrRejected
		}
		if old.Status != "pending" {
			return nil
		}
	}
	if old.Version != expected {
		return ErrRejected
	}
	need := len(members)
	switch mode {
	case "all":
	case "any":
		need = 1
	case "quorum":
		if quorum == 0 || quorum > uint64(need) {
			return ErrRejected
		}
		need = int(quorum)
	default:
		return ErrRejected
	}
	var ready []string
	for _, complete := range p.Outcomes {
		if seen[complete] {
			ready = append(ready, complete)
		}
	}
	status := "pending"
	var winners []string
	if len(ready) >= need {
		status, winners = "completed", slices.Clone(ready[:need])
	} else if timedOut {
		status = "timed_out"
	}
	p.Joins[id] = Join{parent, mode, status, slices.Clone(members), winners, quorum, old.Version + 1}
	return nil
}

// Scope is a knowledge/security partition. One Actor's independent execution
// does not share adopted knowledge implicitly.
type Scope struct{ Space, Principal, Actor, Execution string }
type Knowledge struct{ Facts map[Scope]map[string]uint64 }

func NewKnowledge() *Knowledge { return &Knowledge{Facts: map[Scope]map[string]uint64{}} }
func (k *Knowledge) Adopt(scope Scope, id string, publication uint64) {
	if k.Facts[scope] == nil {
		k.Facts[scope] = map[string]uint64{}
	}
	k.Facts[scope][id] = publication
}
func (k *Knowledge) Visible(scope Scope, id string, fence uint64) bool {
	lsn, ok := k.Facts[scope][id]
	return ok && lsn <= fence
}

type Policy struct {
	Sources      map[string][]string
	Restrictions map[string]string
}

func NewPolicy() *Policy {
	return &Policy{Sources: map[string][]string{}, Restrictions: map[string]string{}}
}
func (p *Policy) Withdraw(id, kind string) {
	old := p.Restrictions[id]
	if old != "privacy_erasure" && old != "acl_revocation" {
		p.Restrictions[id] = kind
	}
}
func (p *Policy) Check(ids []string, reuse bool) error {
	pending := slices.Clone(ids)
	visited := map[string]bool{}
	for len(pending) != 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if visited[id] {
			continue
		}
		visited[id] = true
		kind := p.Restrictions[id]
		if kind != "" && (reuse || kind == "privacy_erasure" || kind == "acl_revocation") {
			return fmt.Errorf("%w: %s", ErrRejected, id)
		}
		pending = append(pending, p.Sources[id]...)
	}
	return nil
}
