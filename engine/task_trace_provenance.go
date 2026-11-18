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
		p.results[traceProfileString(result.ID)] = traceProfileString(result.Activity)
	}
	for _, attempt := range snapshot.Lifecycle.Attempts {
		p.attempts[traceProfileString(attempt.ID)] = traceProfileString(attempt.Activity)
	}
	if r := snapshot.Lifecycle.Recovery; r != nil {
		for _, spec := range r.ActivitySpecs {
			// Only these fields participate in activityInputDisclosureIDs.
			// Own strings so a small ID cannot retain a decoded large payload.
			p.specs[traceProfileString(spec.Activity)] = agentprofile.ActivitySpec{
				Request: traceProfileString(spec.Request), ContextPacket: traceProfileString(spec.ContextPacket),
				FinalAnswerInput: traceProfileString(spec.FinalAnswerInput),
				InputFence: model.CommitFence{Store: traceProfileString(spec.InputFence.Store),
					LSN: spec.InputFence.LSN, CommitDigest: traceProfileString(spec.InputFence.CommitDigest)},
				Contract: agentprofile.EffectContract{Definition: traceProfileString(spec.Contract.Definition)},
			}
		}
		for _, outcome := range r.Outcomes {
			p.outcomes[traceProfileString(outcome.Task)] = traceProfileOutcome(outcome)
		}
		for _, answer := range r.Answers {
			projected := agentprofile.QueryAnswer{
				GeneratedFrom: traceProfileString(answer.GeneratedFrom),
				InputEvidence: traceProfileString(answer.InputEvidence),
			}
			if len(answer.Outcomes) > 0 {
				projected.Outcomes = make([]agentprofile.TaskOutcome, len(answer.Outcomes))
				for i, outcome := range answer.Outcomes {
					projected.Outcomes[i] = traceProfileOutcome(outcome)
				}
			}
			p.answers[recoveryNodeID(answer.Query, answer.Slot)] = projected
		}
	}
	return p
}

func traceProfileOutcome(outcome agentprofile.TaskOutcome) agentprofile.TaskOutcome {
	return agentprofile.TaskOutcome{
		Result: traceProfileString(outcome.Result), Validation: traceProfileString(outcome.Validation),
		ValidationRequirement: traceProfileString(outcome.ValidationRequirement),
	}
}

func (e *Engine) taskTraceDependencies(ctx context.Context, req api.TaskTraceRequest, r taskTraceRead, ids []model.EventID) ([]model.ObjectID, error) {
	var deps []model.ObjectID
	seen := map[model.ObjectID]bool{}
	add := func(id model.ObjectID) {
		if id != "" && !seen[id] {
			seen[id] = true
			deps = append(deps, id)
		}
	}
	add(r.record)
	activities := map[string][]model.ObjectID{}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		more, err := e.traceOccurrenceDependencies(ctx, req, r.provenance, activities, r.occurrence(id))
		if err != nil {
			return nil, e.nodeReadError(ctx, err)
		}
		for _, dependency := range more {
			add(dependency)
		}
	}
	return deps, nil
}

// Text about a recorded result can disclose inputs that were withdrawn.
// Resolve the same Activity and outcome provenance used by control reads.
func (e *Engine) traceOccurrenceDependencies(ctx context.Context, req api.TaskTraceRequest, provenance traceProvenance, activities map[string][]model.ObjectID, event model.Event) ([]model.ObjectID, error) {
	dependencies := objectPolicyDependencies(model.ObjectID(event.ID), event.Command, event.Payload)
	a := event.Attribution
	if a == nil {
		return policyIdentities(dependencies), nil
	}
	switch a.Subject.Profile {
	case "graph":
		if a.Subject.Kind != "Revision" {
			return policyIdentities(dependencies), nil
		}
		revision, err := graphRevisionAt(ctx, e.reader(ctx), provenance.roots, req.Space, model.RevisionID(a.Subject.ID))
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, traceRevisionPolicyDependencies(revision)...)
		return policyIdentities(dependencies), nil
	case "knowledge":
		sources, err := e.traceKnowledgeDependencies(ctx, req, provenance.roots, a.Subject)
		// These sources have already crossed their policy identity boundary.
		return append(policyIdentities(dependencies), sources...), err
	case "agent":
	default:
		return policyIdentities(dependencies), nil
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
		dependencies = append(dependencies, policyDependency{kind: policyObject, id: answer.InputEvidence})
		for _, outcome := range answer.Outcomes {
			dependencies = append(dependencies, traceOutcomePolicyDependencies(outcome)...)
		}
	case "TaskOutcome":
		outcome := provenance.outcomes[a.Subject.ID]
		dependencies = append(dependencies, traceOutcomePolicyDependencies(outcome)...)
	}
	ids := policyIdentities(dependencies)
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
		dependencies := []policyDependency{
			{kind: policyObservation, id: id},
			payloadPolicyDependency(observation.Payload, observation.PayloadKind),
			{kind: policyObservation, id: observation.ImportedFrom},
		}
		more, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, observationSummaryIDs([]knowledge.Observation{observation}))
		return append(policyIdentities(dependencies), more...), err
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

func traceRevisionPolicyDependencies(revision graph.Revision) []policyDependency {
	dependencies := []policyDependency{{kind: policyRevision, id: model.ObjectID(revision.ID)}}
	return append(dependencies, objectPolicyDependencies(graph.ContentReferences(revision)...)...)
}

func traceOutcomePolicyDependencies(outcome agentprofile.TaskOutcome) []policyDependency {
	return []policyDependency{
		{kind: policyObject, id: outcome.Result},
		{kind: policyObject, id: outcome.Validation},
		{kind: policyControl, id: outcome.ValidationRequirement},
	}
}
