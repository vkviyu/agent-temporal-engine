package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type traceProvenance struct {
	roots    model.RootSet
	specs    map[string]agentprofile.ActivitySpec
	results  map[string]string
	attempts map[string]string
	outcomes map[model.EntityID]agentprofile.TaskOutcome
	answers  map[string]agentprofile.QueryAnswer
}

func newTraceProvenance(roots model.RootSet, snapshot agentprofile.Snapshot) traceProvenance {
	p := traceProvenance{
		roots: roots,
		specs: map[string]agentprofile.ActivitySpec{}, results: map[string]string{},
		attempts: map[string]string{}, outcomes: map[model.EntityID]agentprofile.TaskOutcome{},
		answers: map[string]agentprofile.QueryAnswer{},
	}
	for _, result := range snapshot.Lifecycle.Results {
		p.results[result.ID] = result.Activity
	}
	for _, attempt := range snapshot.Lifecycle.Attempts {
		p.attempts[attempt.ID] = attempt.Activity
	}
	if r := snapshot.Lifecycle.Recovery; r != nil {
		for _, spec := range r.ActivitySpecs {
			p.specs[spec.Activity] = spec
		}
		for _, outcome := range r.Outcomes {
			p.outcomes[outcome.Task] = outcome
		}
		for _, answer := range r.Answers {
			p.answers[recoveryNodeID(answer.Query, answer.Slot)] = answer
		}
	}
	return p
}

func (e *Engine) taskTraceDependencies(ctx context.Context, req api.TaskTraceRequest, r taskTraceRead, ids []model.EventID) ([]model.ObjectID, error) {
	deps := []model.ObjectID{r.record}
	activities := map[string][]model.ObjectID{}
	for _, id := range ids {
		more, err := e.traceOccurrenceDependencies(ctx, req, r.provenance, activities, r.occurrence(id))
		if err != nil {
			return nil, e.nodeReadError(ctx, err)
		}
		deps = append(deps, more...)
	}
	return deps, nil
}

// Text about a recorded result can disclose inputs that were withdrawn.
// Resolve the same Activity and outcome provenance used by control reads.
func (e *Engine) traceOccurrenceDependencies(ctx context.Context, req api.TaskTraceRequest, provenance traceProvenance, activities map[string][]model.ObjectID, event model.Event) ([]model.ObjectID, error) {
	ids := []model.ObjectID{model.ObjectID(event.ID), event.Command, event.Payload}
	a := event.Attribution
	if a == nil {
		return ids, nil
	}
	switch a.Subject.Profile {
	case "graph":
		if a.Subject.Kind != "Revision" {
			return ids, nil
		}
		revision, err := graphRevisionAt(ctx, e.reader(ctx), provenance.roots, req.Space, model.RevisionID(a.Subject.ID))
		if err != nil {
			return nil, err
		}
		return append(append(ids, model.ObjectID(revision.ID)), graph.ContentReferences(revision)...), nil
	case "knowledge":
		sources, err := e.traceKnowledgeDependencies(ctx, req, provenance.roots, a.Subject)
		return append(ids, sources...), err
	case "agent":
	default:
		return ids, nil
	}
	id := string(a.Subject.ID)
	var activity string
	switch a.Subject.Kind {
	case "Activity", "ActivityAcceptance":
		activity = id
	case "Attempt":
		activity = provenance.attempts[id]
	case "ActivityResult":
		activity = provenance.results[id]
	case "QueryAnswer":
		answer := provenance.answers[id]
		activity = provenance.results[answer.GeneratedFrom]
		ids = append(ids, answer.InputEvidence)
		for _, outcome := range answer.Outcomes {
			ids = append(ids, outcome.Result, outcome.Validation, outcome.ValidationRequirement)
		}
	case "TaskOutcome":
		outcome := provenance.outcomes[a.Subject.ID]
		ids = append(ids, outcome.Result, outcome.Validation, outcome.ValidationRequirement)
	}
	if spec, ok := provenance.specs[activity]; ok {
		sources, cached := activities[activity]
		if !cached {
			var err error
			sources, err = e.activityInputDisclosureIDs(ctx, req.Space, req.Principal, spec)
			if err != nil {
				return nil, err
			}
			activities[activity] = sources
		}
		ids = append(ids, sources...)
	}
	return ids, nil
}

func (e *Engine) traceKnowledgeDependencies(ctx context.Context, req api.TaskTraceRequest, roots model.RootSet, subject model.EntityKey) ([]model.ObjectID, error) {
	id := model.ObjectID(subject.ID)
	switch subject.Kind {
	case "Observation":
		index, _, err := e.knowledgeIndexAt(ctx, roots, req.Space, req.Principal)
		if err != nil {
			return nil, err
		}
		observation, found, err := index.observation(ctx, id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrStoredObject
		}
		ids := []model.ObjectID{id, observation.Payload, observation.ImportedFrom}
		more, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, observationSummaryIDs([]knowledge.Observation{observation}))
		return append(ids, more...), err
	case "ContextPacket":
		record, err := e.packetAt(ctx, roots, req.Space, req.Principal, id)
		if err != nil {
			return nil, err
		}
		return e.packetDependencyIDs(ctx, roots, req.Space, req.Principal, record.packet, record.selected)
	case "Summary":
		return e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, []model.ObjectID{id})
	}
	return nil, nil
}
