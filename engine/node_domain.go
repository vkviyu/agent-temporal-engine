package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

// domainSubjectNode resolves registry-owned subjects from the selected roots.
// It does not turn an arbitrary digest into an object read capability. An
// execution-knowledge View continues to use its adopted-content projection.
func domainSubjectNode(ctx context.Context, reader state.ObjectReader, entry *viewEntry, ref model.NodeRef) (nodeProjection, bool, error) {
	if entry.descriptor.Anchor.Knowledge != nil {
		return nodeProjection{}, false, nil
	}
	space, principal := entry.descriptor.Anchor.Space, entry.principal
	id := model.ObjectID(ref.Entity.ID)
	var value any
	var payload model.ObjectID
	var encoding string
	var sources []model.NodeRef
	found := false
	entity := ref.Entity
	switch {
	case ref.Kind == model.NodeEntity && entity.Profile == "graph" && entity.Kind == "Branch":
		branch, pruned, err := graphBranchAt(ctx, reader, entry.roots, space, model.BranchID(entity.ID))
		if err != nil {
			if errors.Is(err, graph.ErrUnknownBranch) {
				return nodeProjection{}, true, ErrNodeNotFound
			}
			return nodeProjection{}, true, err
		}
		if entry.mode != "audit_snapshot" {
			if err := graphAllowedAtCursor(ctx, &cursorReader{source: reader}, entry.roots, entry.descriptor.Anchor, branch.Head); err != nil {
				if errors.Is(err, model.ErrAnchorMismatch) {
					return nodeProjection{}, true, ErrNodeNotFound
				}
				return nodeProjection{}, true, err
			}
		}
		_, record, exists, err := domainEventValue(ctx, reader, entry.roots.BranchRefs, space, graphBranchKey(branch.ID))
		if err != nil || !exists {
			return nodeProjection{}, true, missingDomainNode(exists, err)
		}
		payload = record
		value = struct {
			Branch graph.Branch       `json:"branch"`
			Prune  *graph.PruneRecord `json:"prune,omitempty"`
		}{branch, pruned}
		encoding, found = "graph/Branch/v1+json", true
		sources = append(sources, model.NodeRef{Kind: model.NodeRevision, Revision: branch.Head})
		if branch.Scope != nil {
			seen := map[model.RevisionID]bool{branch.Head: true}
			for _, revision := range []model.RevisionID{branch.Scope.Base, branch.PublishedHead} {
				if revision != "" && !seen[revision] {
					seen[revision] = true
					sources = append(sources, model.NodeRef{Kind: model.NodeRevision, Revision: revision})
				}
			}
			if branch.Scope.Execution != "" && branch.Scope.Principal == principal {
				sources = append(sources, model.NodeRef{Kind: model.NodeEntity, Entity: eventSubject("Execution", string(branch.Scope.Execution))})
			}
		}
	case ref.Kind == model.NodeEntity && entity.Profile == "memory":
		raw, _, exists, err := domainEventValue(ctx, reader, entry.roots.ExecutionKnowledge, space, memoryStateKey(space))
		if err != nil || !exists {
			return nodeProjection{}, true, missingDomainNode(exists, err)
		}
		s, err := memory.DecodeSnapshot(raw)
		if err != nil {
			return nodeProjection{}, true, err
		}
		switch entity.Kind {
		case "memory":
			for _, m := range s.Memories {
				if m.ID == id {
					value, payload, encoding, found = m, m.ID, "memory/Memory/v1+json", true
					for _, src := range m.Sources {
						sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: src.SourceID})
					}
				}
			}
		case "experience_path":
			for _, p := range s.Paths {
				if p.ID == id {
					value, payload, encoding, found = p, p.ID, "memory/ExperiencePath/v1+json", true
					for _, step := range p.Steps {
						sources = append(sources, model.NodeRef{Kind: model.NodeEntity, Entity: domainSubject("memory", "memory", string(step.Memory))}, step.Node)
					}
					sources = append(sources, p.Omitted...)
				}
			}
		case "Source":
			for _, ban := range s.Bans {
				if ban.Source == id {
					value, payload, encoding, found = ban, ban.Source, "memory/SourceBan/v1+json", true
				}
			}
		default:
			return nodeProjection{}, false, nil
		}
	case ref.Kind == model.NodeEntity && entity.Profile == "annotation" && entity.Kind == "Annotation":
		raw, record, exists, err := domainEventValue(ctx, reader, entry.roots.ExecutionKnowledge, space, annotationStateKey(space, principal))
		if err != nil || !exists {
			return nodeProjection{}, true, missingDomainNode(exists, err)
		}
		snapshot, err := annotation.DecodeSnapshot(raw)
		if err != nil {
			return nodeProjection{}, true, ErrInvalidStateData
		}
		for _, r := range snapshot.Records {
			if r.ID != id {
				continue
			}
			value, payload, encoding, found = r, record, "annotation/Record/v1+json", true
			sources = append(sources, r.Target)
			sources = append(sources, r.Evidence...)
			break
		}
	case ref.Kind == model.NodeEntity && entity.Profile == "knowledge" && entity.Kind == "summary":
		raw, record, exists, err := domainEventValue(ctx, reader, entry.roots.ExecutionKnowledge, space, summaryStateKey(space, principal, id))
		if err != nil || !exists {
			return nodeProjection{}, true, missingDomainNode(exists, err)
		}
		s, err := knowledge.DecodeSummary(raw)
		if err != nil || s.ID != id || s.Space != space {
			return nodeProjection{}, true, ErrInvalidStateData
		}
		value, payload, encoding, found = s, record, "knowledge/Summary/v1+json", true
	case ref.Kind == model.NodeEntity && entity.Profile == "agent" && entity.Kind == "Emission":
		raw, record, exists, err := eventValueAtObjectSpace(ctx, reader, systemSpace, space, entry.roots.LiveControl, emissionStorageKey(space, principal, string(entity.ID)))
		if err != nil || !exists {
			return nodeProjection{}, true, missingDomainNode(exists, err)
		}
		r, err := decodeEmissionRecord(raw)
		if err != nil || r.Spec.ID != string(entity.ID) || r.Spec.Anchor.Space != space || r.Proof == nil || r.Proof.Principal != principal {
			return nodeProjection{}, true, ErrInvalidStateData
		}
		value, payload, encoding, found = r, record, "agent/Emission/v1+json", true
		for _, id := range r.Proof.Dependencies {
			sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: id})
		}
	case ref.Kind == model.NodeEntity && entity.Profile == "transfer" && entity.Kind == "ImportRecord":
		raw, record, exists, err := domainEventValue(ctx, reader, entry.roots.ExecutionKnowledge, space, transferStateKey(principal, id))
		if err != nil || !exists {
			return nodeProjection{}, true, missingDomainNode(exists, err)
		}
		r, err := transfer.DecodeImportRecord(raw)
		if err != nil || r.Space != space || r.Principal != principal || r.Manifest.ID != id {
			return nodeProjection{}, true, ErrInvalidStateData
		}
		value, payload, encoding, found = r, record, "transfer/ImportRecord/v1+json", true
		for _, mapping := range r.Mappings {
			sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: mapping.Target})
		}
	case ref.Kind == model.NodeObject:
		root, err := slotSpaceRoot(ctx, reader, entry.roots.ExecutionKnowledge, space)
		if err != nil {
			return nodeProjection{}, true, err
		}
		index, err := openKnowledgeIndex(ctx, reader, root, space, principal)
		if err != nil {
			return nodeProjection{}, true, err
		}
		if o, ok, err := index.observation(ctx, ref.Object); err != nil {
			return nodeProjection{}, true, err
		} else if ok {
			value, encoding, found = o, "knowledge/Observation/v1+json", true
			sources = append(sources, knowledgePayloadNodeRef(o))
		}
		if p, ok, err := index.packet(ctx, ref.Object); err != nil {
			return nodeProjection{}, true, err
		} else if ok {
			value, encoding, found = p, "knowledge/ContextPacket/v1+json", true
			for _, item := range p.Items {
				sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: item.Observation})
			}
		}
		if !found {
			return nodeProjection{}, false, nil
		}
	default:
		return nodeProjection{}, false, nil
	}
	if !found {
		return nodeProjection{}, true, ErrNodeNotFound
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nodeProjection{}, true, err
	}
	kind := entity.Kind
	if ref.Kind == model.NodeObject {
		kind = "KnowledgeRecord"
	}
	return nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Kind: kind, Label: string(kind)}, Payload: payload, Encoding: encoding, Data: raw, Sources: sources, Anchor: entry.descriptor.Anchor.Clone()}, parents: sources}, true, nil
}

func missingDomainNode(found bool, err error) error {
	if err != nil {
		return err
	}
	return ErrNodeNotFound
}

func addRequestedDomainContext(ctx context.Context, reader state.ObjectReader, entry *viewEntry, c *nodeCatalog, ref model.NodeRef) error {
	if _, exists := c.nodes[ref]; !exists {
		if n, handled, err := domainSubjectNode(ctx, reader, entry, ref); err != nil {
			return err
		} else if handled {
			c.nodes[ref] = n
		}
	}
	n, exists := c.nodes[ref]
	if !exists {
		return nil
	}
	for _, source := range n.detail.Sources {
		if _, exists := c.nodes[source]; exists {
			continue
		}
		if p, handled, err := domainSubjectNode(ctx, reader, entry, source); err != nil {
			// A source relation may be an opaque identifier (including an old
			// withdrawn source). It is not an obligation to disclose its payload.
			if err == ErrNodeNotFound {
				continue
			}
			return err
		} else if handled {
			c.nodes[source] = p
		}
		if len(c.nodes) > maxNodeQueryRecords {
			return storage.ErrLimit
		}
	}
	return nil
}
