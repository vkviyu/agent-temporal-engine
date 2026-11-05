package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func domainSubject(profile, kind, id string) model.EntityKey {
	return model.EntityKey{Profile: model.ProfileID(profile), Kind: model.EntityKind(kind), ID: model.EntityID(id)}
}

func domainDraft(operation, profile, kind, id string) profileEventDraft {
	return profileEventDraft{attribution: model.EventAttribution{Operation: operation, Subject: domainSubject(profile, kind, id)}}
}

// domainEventDrafts describes the accepted domain transition at its actual
// subject. The core command occurrence is still produced separately. Reading
// the private post-state binds generated IDs (immutable memory versions and
// merge revisions) without inventing a request-shaped entity or publishing an
// intermediate result.
func (e *Engine) domainEventDrafts(ctx context.Context, req api.CommitRequest, reader state.ObjectReader, roots model.RootSet) ([]profileEventDraft, error) {
	switch req.Method {
	case api.MethodMutateState:
		return nil, nil // the generic StateOps occurrence is already complete
	case api.MethodApplyGraph:
		return graphEventDrafts(ctx, req, reader, roots)
	case api.MethodApplyMemory:
		return memoryEventDrafts(ctx, req, reader, roots)
	case api.MethodApplyKnowledge:
		c, err := knowledge.DecodeCommand(req.Body)
		if err != nil {
			return nil, err
		}
		snapshot, err := eventAgentSnapshot(ctx, reader, roots, req)
		if err != nil {
			return nil, err
		}
		return []profileEventDraft{knowledgeWorkEventDraft(c, snapshot)}, nil
	case api.MethodApplyAnnotation:
		r, err := annotation.DecodeRecord(req.Body)
		if err != nil {
			return nil, err
		}
		operation := "Annotation.Apply"
		if r.Retracts != "" {
			operation = "Annotation.Retract"
		}
		d := domainDraft(operation, "annotation", "Annotation", string(r.ID))
		if r.Retracts != "" {
			d.causes = append(d.causes, profileEventCause{"Annotation.Apply", string(r.Retracts)})
		}
		if r.Source.Knowledge != nil {
			d.attribution.Execution = r.Source.Knowledge.Execution
		}
		return []profileEventDraft{d}, nil
	case api.MethodApplySummary:
		s, err := knowledge.DecodeSummary(req.Body)
		if err != nil {
			return nil, err
		}
		d := domainDraft("Summary.Apply", "knowledge", "Summary", string(s.ID))
		if s.Source.Anchor.Knowledge == nil {
			return nil, ErrInvalidStateData
		}
		d.attribution.Execution = s.Source.Anchor.Knowledge.Execution
		d.causes = append(d.causes, profileEventCause{"Control.AcceptActivity", s.Generation.Activity})
		if s.Replaces != "" {
			d.causes = append(d.causes, profileEventCause{"Summary.Apply", string(s.Replaces)})
		}
		for _, src := range s.Sources {
			d.causes = append(d.causes, profileEventCause{"Knowledge.Observation", string(src.Observation)})
		}
		snapshot, err := eventAgentSnapshot(ctx, reader, roots, req)
		if err != nil {
			return nil, err
		}
		bindProfileOwner(&d, snapshot)
		return []profileEventDraft{d}, nil
	case api.MethodApplyEmission:
		return emissionEventDrafts(ctx, req, reader, roots)
	case api.MethodImportTransfer:
		c, err := api.DecodeImportTransfer(req.Body)
		if err != nil {
			return nil, err
		}
		return []profileEventDraft{domainDraft("Transfer.Import", "transfer", "ImportRecord", string(c.Manifest.ID))}, nil
	default:
		return nil, ErrUnsupportedSchema
	}
}

func eventAgentSnapshot(ctx context.Context, reader state.ObjectReader, roots model.RootSet, req api.CommitRequest) (snapshot agentprofile.Snapshot, err error) {
	l, _, _, err := loadAgentLedger(ctx, reader, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return snapshot, err
	}
	return l.Snapshot(), nil
}

func graphEventDrafts(ctx context.Context, req api.CommitRequest, reader state.ObjectReader, roots model.RootSet) ([]profileEventDraft, error) {
	space, c, err := graph.DecodeCommand(req.Body)
	if err != nil || space != req.Space {
		return nil, ErrInvalidStateData
	}
	var d profileEventDraft
	var revisions []model.RevisionID
	switch {
	case c.AddRevision != nil:
		x := c.AddRevision
		r, err := graph.BuildRevision(string(space), x.Parent, x.Joins, x.Root, x.Events, x.Entries)
		if err != nil {
			return nil, err
		}
		d = domainDraft("Graph.AddRevision", "graph", "Revision", string(r.ID))
		revisions = append(append(revisions, x.Parent), x.Joins...)
	case c.Fork != nil:
		d = domainDraft("Graph.Fork", "graph", "Branch", string(c.Fork.Branch))
		revisions = append(revisions, c.Fork.Source)
	case c.Advance != nil:
		d = domainDraft("Graph.Advance", "graph", "Branch", string(c.Advance.Branch))
		revisions = append(revisions, c.Advance.Head)
	case c.Join != nil:
		branch, _, err := graphBranchAt(ctx, reader, roots, space, c.Join.Branch)
		if err != nil {
			return nil, err
		}
		r := domainDraft("Graph.JoinRevision", "graph", "Revision", string(branch.Head))
		r.causes = graphRevisionCauses(c.Join.Left, c.Join.Right)
		d = domainDraft("Graph.Join", "graph", "Branch", string(branch.ID))
		d.causes = []profileEventCause{{"Graph.JoinRevision", string(branch.Head)}}
		return []profileEventDraft{r, d}, nil
	case c.Prune != nil:
		d = domainDraft("Graph.Prune", "graph", "Branch", string(c.Prune.Branch))
		revisions = append(revisions, c.Prune.Head)
	default:
		return nil, ErrUnsupportedSchema
	}
	d.causes = graphRevisionCauses(revisions...)
	return []profileEventDraft{d}, nil
}

func graphRevisionCauses(ids ...model.RevisionID) []profileEventCause {
	var causes []profileEventCause
	for _, id := range ids {
		if id != "" {
			causes = append(causes, profileEventCause{"Graph.AddRevision", string(id)}, profileEventCause{"Graph.JoinRevision", string(id)})
		}
	}
	return causes
}

func memoryEventDrafts(ctx context.Context, req api.CommitRequest, reader state.ObjectReader, roots model.RootSet) ([]profileEventDraft, error) {
	c, err := memory.DecodeCommand(req.Body)
	if err != nil {
		return nil, err
	}
	raw, _, found, err := domainEventValue(ctx, reader, roots.ExecutionKnowledge, req.Space, memoryStateKey(req.Space))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrInvalidStateData
	}
	s, err := memory.DecodeSnapshot(raw)
	if err != nil {
		return nil, err
	}
	var id, prior model.ObjectID
	kind := "Memory"
	operation := "Memory." + selectedProfileOperation(c)
	switch {
	case c.PutCandidate != nil:
		id = memory.IDFor(*c.PutCandidate)
	case c.Validate != nil:
		prior = c.Validate.ID
	case c.Promote != nil:
		prior = *c.Promote
	case c.Transition != nil:
		prior = c.Transition.ID
	case c.RetractSource != nil:
		kind, id = "Source", c.RetractSource.Source
	case c.BuildPath != nil:
		x := c.BuildPath
		kind = "ExperiencePath"
		id = memory.PathIDFor(memory.ExperiencePath{Space: x.Space, SourceAnchor: x.Anchor, Steps: x.Steps, Omitted: x.Omitted, Validation: x.Validation, Applicability: x.Applicability, DerivedFrom: x.DerivedFrom, Status: memory.StatusPromoted})
	case c.RetractPath != nil:
		kind, prior = "ExperiencePath", c.RetractPath.ID
	}
	if prior != "" {
		links := s.Successors
		if kind == "ExperiencePath" {
			links = s.PathSuccessors
		}
		for _, link := range links {
			if link.Before == prior {
				id = link.After
				break
			}
		}
	}
	if !model.ValidDigest(string(id)) {
		return nil, ErrInvalidStateData
	}
	present := false
	switch kind {
	case "Memory":
		for _, m := range s.Memories {
			present = present || m.ID == id
		}
	case "ExperiencePath":
		for _, p := range s.Paths {
			present = present || p.ID == id
		}
	case "Source":
		for _, ban := range s.Bans {
			present = present || ban.Source == id
		}
	}
	if !present {
		return nil, ErrInvalidStateData
	}
	d := domainDraft(operation, "memory", kind, string(id))
	if prior != "" {
		for _, op := range []string{"Memory.PutCandidate", "Memory.Validate", "Memory.Promote", "Memory.Transition", "Memory.BuildPath", "Memory.RetractPath"} {
			d.causes = append(d.causes, profileEventCause{op, string(prior)})
		}
	}
	return []profileEventDraft{d}, nil
}

func emissionEventDrafts(ctx context.Context, req api.CommitRequest, reader state.ObjectReader, roots model.RootSet) ([]profileEventDraft, error) {
	c, err := api.DecodeEmissionCommand(req.Body)
	if err != nil {
		return nil, err
	}
	id, op := c.ClaimID, "Emission.Claim"
	if c.Issue != nil {
		id, op = c.Issue.ID, "Emission.Issue"
	} else if c.RevokeID != "" {
		id, op = c.RevokeID, "Emission.Revoke"
	}
	raw, _, found, err := eventValueAtObjectSpace(ctx, reader, systemSpace, req.Space, roots.LiveControl, emissionStorageKey(req.Space, req.Principal, id))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrInvalidStateData
	}
	r, err := decodeEmissionRecord(raw)
	if err != nil || r.Spec.ID != id {
		return nil, ErrInvalidStateData
	}
	d := domainDraft(op, "agent", "Emission", id)
	d.attribution.Execution, d.attribution.Query = r.Spec.Execution, r.Spec.Query
	d.actorAuthorized = c.Issue != nil || c.ClaimID != ""
	if c.ClaimID != "" || c.RevokeID != "" {
		d.causes = append(d.causes, profileEventCause{"Emission.Issue", id})
	}
	if c.RevokeID != "" {
		d.causes = append(d.causes, profileEventCause{"Emission.Claim", id})
	}
	snapshot, err := eventAgentSnapshot(ctx, reader, roots, req)
	if err != nil {
		return nil, err
	}
	bindProfileOwner(&d, snapshot)
	return []profileEventDraft{d}, nil
}

func domainEventValue(ctx context.Context, reader state.ObjectReader, catalog model.ObjectID, space model.SpaceID, key []byte) ([]byte, model.ObjectID, bool, error) {
	root, err := slotSpaceRoot(ctx, reader, catalog, space)
	if err != nil {
		return nil, "", false, err
	}
	return eventValueAt(ctx, reader, space, root, key)
}

func eventValueAt(ctx context.Context, reader state.ObjectReader, space model.SpaceID, root model.ObjectID, key []byte) ([]byte, model.ObjectID, bool, error) {
	return eventValueAtObjectSpace(ctx, reader, space, space, root, key)
}

func eventValueAtObjectSpace(ctx context.Context, reader state.ObjectReader, treeSpace, objectSpace model.SpaceID, root model.ObjectID, key []byte) ([]byte, model.ObjectID, bool, error) {
	id, found, err := state.Get(ctx, reader, treeSpace, root, key)
	if err != nil || !found {
		return nil, "", found, err
	}
	o, err := readObject(ctx, reader, id)
	if err != nil {
		return nil, "", false, err
	}
	v, err := readValue(o)
	if err != nil || o.Space != objectSpace || v.Kind != api.ValueBytes {
		return nil, "", false, ErrInvalidStateData
	}
	return v.Data, id, true, nil
}
