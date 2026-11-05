package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) addEventNodes(ctx context.Context, r *nodeQueryReader, entry *viewEntry, c *nodeCatalog) error {
	if !e.options.EventProfile {
		return ErrUnsupportedMethod
	}
	ids, err := e.selectedEventIDs(ctx, entry)
	if err != nil {
		return err
	}
	var allowed map[model.EventID]bool
	if entry.descriptor.Anchor.Knowledge != nil {
		allowed = map[model.EventID]bool{}
		snap, _, _, _, err := e.selectedKnowledge(ctx, entry.roots, entry.descriptor.Anchor, entry.principal)
		if err != nil {
			return err
		}
		for _, o := range snap.Observations {
			if o.PayloadKind == "" {
				allowed[model.EventID(o.Payload)] = true
			}
		}
	} else if entry.descriptor.Anchor.Cursor != nil && entry.mode != "audit_snapshot" {
		allowed = map[model.EventID]bool{}
		// Use the same authenticated ancestry projection as revision browsing.
		revisions := &nodeCatalog{nodes: map[model.NodeRef]nodeProjection{}, objects: c.objects}
		if err = addRevisionNodes(ctx, r, entry, revisions); err != nil {
			return err
		}
		for ref, n := range revisions.nodes {
			if ref.Kind == model.NodeRevision {
				var revision graph.Revision
				if json.Unmarshal(n.detail.Data, &revision) != nil {
					return ErrInvalidStateData
				}
				for _, id := range revision.Events {
					allowed[id] = true
				}
			}
		}
	}
	for _, id := range ids {
		if allowed != nil && !allowed[id] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		obj, err := readObject(ctx, r, model.ObjectID(id))
		if err != nil {
			return err
		}
		event, err := eventlog.Decode(obj)
		if err != nil {
			return ErrStoredObject
		}
		if event.Space != entry.descriptor.Anchor.Space || event.RecordedBy != entry.principal || event.ID != id {
			return ErrInvalidStateData
		}
		if err = addEventProjection(ctx, r, entry, c, event); err != nil {
			return err
		}
		c.objects[model.ObjectID(id)] = obj
		if len(c.nodes) > maxNodeQueryRecords {
			return storage.ErrLimit
		}
	}
	return nil
}

func addEventProjection(ctx context.Context, r *nodeQueryReader, entry *viewEntry, c *nodeCatalog, event model.Event) error {
	parents := make([]model.NodeRef, 0, len(event.CausalParents)+2)
	for _, id := range event.CausalParents {
		parents = append(parents, model.NodeRef{Kind: model.NodeEvent, Event: id})
	}
	for _, id := range []model.ObjectID{event.Command, event.Payload} {
		ref := model.NodeRef{Kind: model.NodeObject, Object: id}
		parents = append(parents, ref)
		// Adopting an Event makes that occurrence known. Its evidence and
		// causal identifiers remain opaque unless independently adopted;
		// strong storage references are not execution-knowledge grants.
		if entry.descriptor.Anchor.Knowledge != nil {
			continue
		}
		obj, err := readObject(ctx, r, id)
		if err != nil {
			return err
		}
		if obj.Space != event.Space {
			return ErrInvalidStateData
		}
		if id == event.Command {
			cmd, err := readCommand(obj)
			if err != nil {
				return err
			}
			if cmd.Principal != event.RecordedBy || cmd.Method != event.Method {
				return ErrInvalidStateData
			}
		} else {
			if _, err = readOperations(obj); err != nil {
				return err
			}
		}
		c.objects[id] = obj
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Label: obj.Type, Kind: "event_evidence"}, Payload: id, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}
	}
	ref := model.NodeRef{Kind: model.NodeEvent, Event: event.ID}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	label := event.Method
	if event.Attribution != nil {
		label = event.Attribution.Operation
	}
	sources := append([]model.NodeRef(nil), parents...)
	if event.Attribution != nil {
		subject, err := eventlog.SubjectRef(*event.Attribution)
		if err != nil {
			return err
		}
		sources = append(sources, subject)
		// Attribution is a navigable reference, not an implicit grant to the
		// subject's payload. Inspect resolves it independently at the same
		// View; strict knowledge Views still require explicit adoption.
	}
	if event.PreviousInStream != "" {
		sources = append(sources, model.NodeRef{Kind: model.NodeEvent, Event: event.PreviousInStream})
	}
	c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Kind: "Event", Label: label}, Payload: model.ObjectID(event.ID), Data: raw, Encoding: fmt.Sprintf("event/%s/v%d+json", event.Kind, event.SchemaVersion), Sources: sources, Anchor: entry.descriptor.Anchor.Clone()}, parents: parents, streamPrevious: event.PreviousInStream}
	return nil
}
