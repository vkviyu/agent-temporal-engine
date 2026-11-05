package engine

import (
	"context"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type nodeProjection struct {
	detail         api.NodeDetail
	parents        []model.NodeRef
	streamPrevious model.EventID
}
type nodeCatalog struct {
	nodes   map[model.NodeRef]nodeProjection
	objects map[model.ObjectID]codec.Object
}

// knowledgePayloadNodeRef keeps typed memory identities in the logical node
// namespace. They are not codec objects and therefore must not be exposed as
// NodeObject refs to ReadContent.
func knowledgePayloadNodeRef(o knowledge.Observation) model.NodeRef {
	if o.PayloadKind == "" {
		return model.NodeRef{Kind: model.NodeObject, Object: o.Payload}
	}
	if o.PayloadKind == "summary" {
		return model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "knowledge", Kind: "summary", ID: model.EntityID(o.Payload)}}
	}
	return model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "memory", Kind: model.EntityKind(o.PayloadKind), ID: model.EntityID(o.Payload)}}
}

func (e *Engine) nodeCatalog(ctx context.Context, entry *viewEntry, layer string) (nodeCatalog, error) {
	c := nodeCatalog{nodes: map[model.NodeRef]nodeProjection{}, objects: map[model.ObjectID]codec.Object{}}
	if layer == "events" {
		err := e.addEventNodes(ctx, &nodeQueryReader{source: e.index}, entry, &c)
		return c, err
	}
	if entry.descriptor.Anchor.Knowledge != nil {
		// A knowledge view expands adopted content only. It must not fall
		// back to the whole space or the principal's complete Agent ledger.
		if layer != "content" {
			return c, ErrUnsupportedMethod
		}
		snapshot, _, _, _, err := e.selectedKnowledge(ctx, entry.roots, entry.descriptor.Anchor, entry.principal)
		if err != nil {
			return c, err
		}
		if len(snapshot.Observations)+len(snapshot.Packets) > maxNodeQueryRecords {
			return c, storage.ErrLimit
		}
		r := &nodeQueryReader{source: e.index}
		for _, o := range snapshot.Observations {
			ref := knowledgePayloadNodeRef(o)
			if o.PayloadKind != "" {
				if _, exists := c.nodes[ref]; !exists {
					store, _, _, err := e.loadMemory(ctx, entry.roots, entry.descriptor.Anchor.Space)
					if err != nil {
						return c, err
					}
					var value any
					var encoding string
					var sources []model.NodeRef
					switch o.PayloadKind {
					case "memory":
						value, err = store.Get(o.Payload)
						encoding = "memory/Memory/v1+json"
					case "experience_path":
						value, err = store.GetPath(o.Payload)
						encoding = "memory/ExperiencePath/v1+json"
					case "summary":
						var summary knowledge.Summary
						var found bool
						summary, _, found, err = e.loadSummary(ctx, entry.roots, entry.descriptor.Anchor.Space, entry.principal, o.Payload)
						if err == nil && !found {
							return c, ErrInvalidStateData
						}
						value = summary
						encoding = "knowledge/Summary/v1+json"
						for _, id := range knowledge.SummaryContentReferences(summary) {
							sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: id})
						}
					default:
						return c, knowledge.ErrInvalid
					}
					if err != nil {
						return c, err
					}
					data, err := json.Marshal(value)
					if err != nil {
						return c, err
					}
					n := api.NodeSummary{Ref: ref, Label: o.Label, Kind: model.EntityKind(o.PayloadKind)}
					c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: n, Payload: o.Payload, Data: data, Encoding: encoding, Anchor: entry.descriptor.Anchor.Clone(), Sources: sources}, parents: append([]model.NodeRef(nil), sources...)}
				}
			} else if _, exists := c.objects[o.Payload]; !exists {
				obj, err := readObject(ctx, r, o.Payload)
				if err != nil {
					return c, err
				}
				if obj.Space != entry.descriptor.Anchor.Space {
					return c, ErrInvalidStateData
				}
				if obj.Type == eventlog.Type || obj.Type == eventlog.ProfileType {
					if _, err = eventlog.Decode(obj); err != nil {
						return c, err
					}
				} else if _, err = readValue(obj); err != nil {
					return c, err
				}
				n := api.NodeSummary{Ref: ref, Label: o.Label, Kind: "adopted_content"}
				c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: n, Payload: o.Payload, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}
				c.objects[o.Payload] = obj
			}
			observationRef := model.NodeRef{Kind: model.NodeObject, Object: o.ID}
			raw, err := json.Marshal(o)
			if err != nil {
				return c, err
			}
			c.nodes[observationRef] = nodeProjection{detail: api.NodeDetail{Ref: observationRef, Summary: api.NodeSummary{Ref: observationRef, Label: o.Label, Kind: "Observation"}, Data: raw, Encoding: "knowledge/Observation/v1+json", Anchor: entry.descriptor.Anchor.Clone(), Sources: []model.NodeRef{ref}}, parents: []model.NodeRef{ref}}
		}
		for _, p := range snapshot.Packets {
			ref := model.NodeRef{Kind: model.NodeObject, Object: p.ID}
			raw, err := json.Marshal(p)
			if err != nil {
				return c, err
			}
			parents := make([]model.NodeRef, 0, len(p.Items))
			for _, item := range p.Items {
				parents = append(parents, model.NodeRef{Kind: model.NodeObject, Object: item.Observation})
			}
			if p.Input != nil {
				inputRef := model.NodeRef{Kind: model.NodeObject, Object: p.Input.Object}
				parents = append(parents, inputRef)
				obj, exists := c.objects[p.Input.Object]
				if !exists {
					obj, err = readObject(ctx, r, p.Input.Object)
					if err != nil {
						return c, err
					}
					if obj.Space != entry.descriptor.Anchor.Space {
						return c, ErrInvalidStateData
					}
					c.objects[p.Input.Object] = obj
				}
				value, err := readValue(obj)
				if err != nil {
					return c, err
				}
				if value.Kind != api.ValueBytes {
					return c, ErrInvalidStateData
				}
				if _, exists := c.nodes[inputRef]; !exists {
					c.nodes[inputRef] = nodeProjection{detail: api.NodeDetail{Ref: inputRef, Summary: api.NodeSummary{Ref: inputRef, Label: p.Input.Encoding, Kind: "recorded_input"}, Payload: p.Input.Object, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}
				}
			}
			c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Label: p.Renderer, Kind: "ContextPacket"}, Data: raw, Encoding: "knowledge/ContextPacket/v1+json", Anchor: entry.descriptor.Anchor.Clone(), Sources: parents}, parents: parents}
		}
		chain, err := knowledge.Revisions(snapshot, entry.descriptor.Anchor.Space, *entry.descriptor.Anchor.Knowledge)
		if err != nil {
			return c, err
		}
		for _, revision := range chain {
			ref := model.NodeRef{Kind: model.NodeRevision, Revision: revision.ID}
			parents := []model.NodeRef{}
			if revision.Parent != "" {
				parents = append(parents, model.NodeRef{Kind: model.NodeRevision, Revision: revision.Parent})
			}
			if revision.Observation != "" {
				parents = append(parents, model.NodeRef{Kind: model.NodeObject, Object: revision.Observation})
			}
			raw, err := json.Marshal(revision)
			if err != nil {
				return c, err
			}
			c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Kind: "KnowledgeRevision", Label: string(revision.ID)}, Data: raw, Encoding: "knowledge/Revision/v1+json", Anchor: entry.descriptor.Anchor.Clone(), Sources: parents}, parents: parents}
		}
		if len(c.nodes) > maxNodeQueryRecords {
			return c, storage.ErrLimit
		}
		if e.options.EventProfile {
			if err := e.addEventNodes(ctx, r, entry, &c); err != nil {
				return c, err
			}
		}
		return c, nil
	}
	r := &nodeQueryReader{source: e.index}
	if layer == "entities" {
		if !e.options.AgentProfile {
			return c, ErrUnsupportedMethod
		}
		ledger, id, found, err := loadAgentLedger(ctx, r, entry.roots.LiveControl, entry.descriptor.Anchor.Space, entry.principal)
		if err != nil {
			return c, err
		}
		if !found {
			return c, nil
		}
		if err = addProfileNodes(&c, ledger.Snapshot(), id, entry.descriptor.Anchor); err != nil {
			return c, err
		}
		if err = addQueryProgressNodes(ctx, r, entry, &c, ledger.Snapshot()); err != nil {
			return c, err
		}
		obj, err := readObject(ctx, r, id)
		if err != nil {
			return c, err
		}
		c.objects[id] = obj
		return c, nil
	}
	if layer != "content" {
		return c, ErrUnsupportedMethod
	}
	var root model.ObjectID
	var err error
	if entry.cursorRoot != nil {
		root = *entry.cursorRoot
	} else {
		root, err = domainRoot(ctx, r, entry.roots, entry.descriptor.Anchor.Space)
		if err != nil {
			return c, err
		}
	}
	items, err := state.Entries(ctx, r, entry.descriptor.Anchor.Space, root, state.ScanLimits{MaxEntries: maxNodeQueryRecords, MaxBytes: 16 << 20, MaxReadBytes: 64 << 20})
	if err != nil {
		return c, err
	}
	for _, item := range items {
		if err = ctx.Err(); err != nil {
			return c, err
		}
		if _, exists := c.objects[item.Value]; exists {
			continue
		}
		obj, err := readObject(ctx, r, item.Value)
		if err != nil {
			return c, err
		}
		if obj.Space != entry.descriptor.Anchor.Space {
			return c, ErrInvalidStateData
		}
		if _, err = readValue(obj); err != nil {
			return c, err
		}
		ref := model.NodeRef{Kind: model.NodeObject, Object: item.Value}
		if _, exists := c.nodes[ref]; !exists {
			n := api.NodeSummary{Ref: ref, Label: string(item.Key), Kind: "state"}
			c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: n, Payload: item.Value, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}
		}
		c.objects[item.Value] = obj
	}
	if err := addRevisionNodes(ctx, r, entry, &c); err != nil {
		return c, err
	}
	if entry.descriptor.Anchor.Cursor == nil {
		if err := e.addTransferArchiveNodes(ctx, r, entry, &c); err != nil {
			return c, err
		}
	}
	if e.options.EventProfile {
		if err := e.addEventNodes(ctx, r, entry, &c); err != nil {
			return c, err
		}
	}
	return c, nil
}
