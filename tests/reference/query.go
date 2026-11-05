package reference

import "slices"

type QueryDecision struct {
	Sequence         uint64
	Activity, Result string
	Invocations      []QueryInvocation
}
type QueryProgress struct {
	Query, Adapter, Phase, Source, Next string
	Version, Sequence                   uint64
	Completed                           bool
	Decisions                           []QueryDecision
}
type QueryFact struct{ Operation string }

type QueryController struct {
	Progress QueryProgress
	Reserved map[string]bool
	Facts    []QueryFact
}

func NewQueryController(query string) *QueryController {
	return &QueryController{Progress: QueryProgress{Query: query}, Reserved: map[string]bool{}}
}

// Advance is the host-visible controller contract: each next action is exact,
// the previous one must be receipted, accepted decisions are never regenerated,
// and the adapter cannot change silently across recovery.
func (q *QueryController) Advance(kind string, expected, sequence uint64, adapter, phase, source, next, activity, result string, previousApplied bool) error {
	p := q.Progress
	if p.Version != expected || p.Completed || adapter == "" || p.Version > 0 && p.Adapter != adapter {
		return ErrRejected
	}
	if p.Version == 0 {
		if kind != "open" || sequence != 0 {
			return ErrRejected
		}
	} else if !previousApplied {
		return ErrRejected
	}
	switch kind {
	case "open":
		if p.Version != 0 {
			return ErrRejected
		}
	case "decision":
		if sequence != p.Sequence+1 || activity == "" || result == "" {
			return ErrRejected
		}
		for _, d := range p.Decisions {
			if d.Activity == activity || d.Result == result {
				return ErrRejected
			}
		}
		p.Decisions = append(slices.Clone(p.Decisions), QueryDecision{Sequence: sequence, Activity: activity, Result: result})
	case "answer":
		if sequence != p.Sequence || len(p.Decisions) == 0 || next != "" || !q.toolsDone() {
			return ErrRejected
		}
		p.Completed = true
	default:
		return ErrRejected
	}
	if kind != "answer" && next == "" {
		return ErrRejected
	}
	if next != "" && q.Reserved[next] {
		return ErrRejected
	}
	if next != "" {
		q.Reserved[next] = true
	}
	p.Version++
	p.Sequence = sequence
	p.Adapter, p.Phase, p.Source, p.Next = adapter, phase, source, next
	q.Progress = p
	return nil
}

// Observe records the logical fact expected after the host has submitted its
// concrete Activity transition; it does not inspect generated Event objects.
func (q *QueryController) Observe(operation string) { q.Facts = append(q.Facts, QueryFact{operation}) }

// QueryInvocation is a model-decision contract, separate from a tool result.
// The accepted tool result is appended later without changing its arguments.
type QueryInvocation struct {
	ID, Activity, Call, Tool, Definition, ToolVersion, Arguments, Effect, AcceptedResult string
}
type QueryInvocationBinding struct{ Definition, ToolVersion, Effect string }

func (q *QueryController) toolsDone() bool {
	for _, d := range q.Progress.Decisions {
		for _, i := range d.Invocations {
			if i.AcceptedResult == "" {
				return false
			}
		}
	}
	return true
}
func (q *QueryController) clone() *QueryController {
	c := *q
	c.Reserved = map[string]bool{}
	for k, v := range q.Reserved {
		c.Reserved[k] = v
	}
	c.Progress.Decisions = slices.Clone(q.Progress.Decisions)
	for i := range c.Progress.Decisions {
		c.Progress.Decisions[i].Invocations = slices.Clone(c.Progress.Decisions[i].Invocations)
	}
	c.Facts = slices.Clone(q.Facts)
	return &c
}
func (q *QueryController) Decide(expected, sequence uint64, adapter, phase, source, next, activity, result string, previousApplied bool, calls []QueryInvocation, bindings map[string]QueryInvocationBinding) error {
	if !q.toolsDone() || len(calls) != len(bindings) {
		return ErrRejected
	}
	ids, activities, keys := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, d := range q.Progress.Decisions {
		for _, i := range d.Invocations {
			ids[i.ID] = true
			activities[i.Activity] = true
		}
	}
	for _, i := range calls {
		b, ok := bindings[i.ID]
		if !ok || i.ID == "" || i.Activity == "" || i.Call == "" || i.Tool == "" || i.Definition == "" || i.ToolVersion == "" || i.AcceptedResult != "" || ids[i.ID] || activities[i.Activity] || keys[i.Call] || b.Definition != i.Definition || b.ToolVersion != i.ToolVersion || b.Effect != i.Effect || (i.Effect != "pure" && i.Effect != "idempotent" && i.Effect != "non_idempotent") {
			return ErrRejected
		}
		ids[i.ID], activities[i.Activity], keys[i.Call] = true, true, true
	}
	candidate := q.clone()
	if err := candidate.Advance("decision", expected, sequence, adapter, phase, source, next, activity, result, previousApplied); err != nil {
		return err
	}
	last := len(candidate.Progress.Decisions) - 1
	candidate.Progress.Decisions[last].Invocations = slices.Clone(calls)
	*q = *candidate
	return nil
}
func (q *QueryController) ToolResult(expected, sequence uint64, adapter, phase, source, next, activity, result string, previousApplied bool) error {
	p := q.Progress
	if p.Version != expected || p.Sequence != sequence || p.Adapter != adapter || p.Completed || !previousApplied || next == "" || q.Reserved[next] || result == "" {
		return ErrRejected
	}
	candidate := q.clone()
	matched := false
	for di := range candidate.Progress.Decisions {
		for ii := range candidate.Progress.Decisions[di].Invocations {
			i := &candidate.Progress.Decisions[di].Invocations[ii]
			if i.AcceptedResult == result {
				return ErrRejected
			}
			if i.Activity == activity {
				if i.AcceptedResult != "" {
					return ErrRejected
				}
				i.AcceptedResult = result
				matched = true
			}
		}
	}
	if !matched {
		return ErrRejected
	}
	candidate.Progress.Version++
	candidate.Progress.Phase, candidate.Progress.Source, candidate.Progress.Next = phase, source, next
	candidate.Reserved[next] = true
	*q = *candidate
	return nil
}
