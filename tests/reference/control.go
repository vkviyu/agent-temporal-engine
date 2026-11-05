package reference

import (
	"maps"
	"slices"
)

// Control specifies observable contracts using names and immutable values. It
// has no codecs, production DTOs, roots, or reducer implementation dependencies.
type PrimaryDecision struct {
	Execution, Result string
	Step              uint64
}
type InvocationContract struct {
	Execution, Result, Call, Tool, Definition, Version, Effect, Arguments, Status, Outcome string
	Step                                                                                   uint64
}
type ReservedAction struct {
	Execution, Request, Method, Body, ExpectedFence, Authority string
	Generation                                                 uint64
}
type ResumePosition struct {
	Execution, Space, Principal, Adapter, Knowledge string
	Step, Version                                   uint64
	Next                                            map[string]uint64
}
type Control struct {
	Decisions     map[string]PrimaryDecision
	Invocations   map[string]InvocationContract
	Actions       map[string]ReservedAction
	Reservations  map[string]string
	Continuations map[string]ResumePosition
}

func NewControl() *Control {
	return &Control{map[string]PrimaryDecision{}, map[string]InvocationContract{}, map[string]ReservedAction{}, map[string]string{}, map[string]ResumePosition{}}
}
func (c *Control) Clone() *Control {
	out := *c
	out.Decisions = maps.Clone(c.Decisions)
	out.Invocations = maps.Clone(c.Invocations)
	out.Actions = maps.Clone(c.Actions)
	out.Reservations = maps.Clone(c.Reservations)
	out.Continuations = maps.Clone(c.Continuations)
	for id, p := range out.Continuations {
		p.Next = maps.Clone(p.Next)
		out.Continuations[id] = p
	}
	return &out
}
func (c *Control) Batch(apply func(*Control) error) error {
	next := c.Clone()
	if err := apply(next); err != nil {
		return err
	}
	*c = *next
	return nil
}
func (c *Control) AcceptPrimary(execution string, step uint64, result string) {
	c.Decisions[result] = PrimaryDecision{execution, result, step}
}
func (c *Control) Invoke(id string, i InvocationContract) error {
	d, known := c.Decisions[i.Result]
	if !known || d.Execution != i.Execution || d.Step != i.Step || i.Call == "" || i.Tool == "" || i.Definition == "" || i.Version == "" || (i.Effect != "pure" && i.Effect != "idempotent" && i.Effect != "non_idempotent") {
		return ErrRejected
	}
	for oldID, old := range c.Invocations {
		if oldID == id || old.Execution == i.Execution && old.Step == i.Step && old.Call == i.Call {
			if oldID == id && old == i {
				return nil
			}
			return ErrRejected
		}
	}
	c.Invocations[id] = i
	return nil
}
func (c *Control) AcceptSpawn(id, child string) error {
	i, ok := c.Invocations[id]
	if !ok || i.Status != "pending" {
		return ErrRejected
	}
	i.Status, i.Outcome = "accepted", child
	c.Invocations[id] = i
	return nil
}
func (c *Control) Reserve(id string, a ReservedAction) error {
	if _, ok := c.Actions[id]; ok {
		return ErrRejected
	}
	if a.Generation != 1 || a.Execution == "" || a.Request == "" || a.Method == "" || a.Body == "" || a.Authority == "" {
		return ErrRejected
	}
	if _, used := c.Reservations[a.Request]; used {
		return ErrRejected
	}
	c.Actions[id] = a
	c.Reservations[a.Request] = id
	return nil
}
func (c *Control) Matches(request, method, body, fence string) bool {
	for _, a := range c.Actions {
		if a.Request == request {
			return a.Method == method && a.Body == body && a.ExpectedFence == fence
		}
	}
	return true
}
func (c *Control) Save(p ResumePosition, expected uint64) error {
	old, exists := c.Continuations[p.Execution]
	if p.Version != expected+1 || exists && old.Version != expected || !exists && expected != 0 || p.Adapter == "" || p.Space == "" || p.Principal == "" || len(p.Next) == 0 {
		return ErrRejected
	}
	if exists && (old.Adapter != p.Adapter || old.Knowledge != "" && p.Knowledge == "") {
		return ErrRejected
	}
	for id, generation := range p.Next {
		a, exists := c.Actions[id]
		if !exists || a.Generation != generation || a.Execution != p.Execution {
			return ErrRejected
		}
	}
	p.Next = maps.Clone(p.Next)
	c.Continuations[p.Execution] = p
	return nil
}
func (c *Control) PendingInvocations(execution string, step uint64) []string {
	out := []string{}
	for id, i := range c.Invocations {
		if i.Execution == execution && i.Step == step && (i.Status == "pending" || i.Status == "unknown") {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}
