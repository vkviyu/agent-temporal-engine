package engine

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Query progress belongs to the same authenticated LiveControl partition as
// its Query, not to a synthetic Execution. Historical Views use only the
// progress value at that exact root and never fall back to latest.
func addQueryProgressNodes(ctx context.Context, r *nodeQueryReader, entry *viewEntry, c *nodeCatalog, snapshot agentprofile.Snapshot) error {
	anchor := entry.descriptor.Anchor
	for _, query := range snapshot.Lifecycle.Queries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if query.Space != anchor.Space || query.Owner != entry.principal {
			return ErrInvalidStateData
		}
		recordID, found, err := state.Get(ctx, r, systemSpace, entry.roots.LiveControl, queryProgressKey(anchor.Space, entry.principal, query.ID))
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		o, err := readObject(ctx, r, recordID)
		if err != nil {
			return err
		}
		p, err := decodeQueryProgressValue(o)
		if err != nil {
			return err
		}
		if o.Space != anchor.Space || p.Query != query.ID || (p.Next != nil && (p.Next.Space != anchor.Space || p.Next.Principal != entry.principal)) {
			return ErrInvalidStateData
		}
		add := func(kind, id, label string, value any, parents, sources []model.NodeRef) error {
			if len(c.nodes) >= maxNodeQueryRecords {
				return storage.ErrLimit
			}
			ref := agentEntity(model.EntityKind(kind), id)
			raw, err := json.Marshal(value)
			if err != nil {
				return err
			}
			if _, exists := c.nodes[ref]; exists {
				return ErrInvalidStateData
			}
			c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Kind: model.EntityKind(kind), Label: label}, Payload: recordID, Data: raw, Encoding: "agent/" + kind + "/v1+json", Sources: sources, Anchor: anchor.Clone()}, parents: parents}
			return nil
		}
		queryRef := agentEntity("Query", query.ID)
		if err := add("QueryProgress", query.ID, p.Phase, p, []model.NodeRef{queryRef}, nil); err != nil {
			return err
		}
		for _, decision := range p.Decisions {
			decisionID := recoveryNodeID(query.ID, strconv.FormatUint(decision.Sequence, 10))
			decisionRef := agentEntity("QueryDecision", decisionID)
			parents := []model.NodeRef{queryRef, agentEntity("Activity", decision.Activity), agentEntity("ActivityResult", decision.AcceptedResult)}
			if err := add("QueryDecision", decisionID, "accepted Query decision", decision, parents, nil); err != nil {
				return err
			}
			for _, invocation := range decision.Invocations {
				parents := []model.NodeRef{queryRef, decisionRef, agentEntity("Activity", invocation.Activity), agentEntity("ActivityResult", decision.AcceptedResult)}
				if invocation.AcceptedResult != "" {
					parents = append(parents, agentEntity("ActivityResult", invocation.AcceptedResult))
				}
				sources := []model.NodeRef{{Kind: model.NodeObject, Object: invocation.Definition}}
				if err := add("QueryInvocation", recoveryNodeID(query.ID, invocation.ID), invocation.Tool, invocation, parents, sources); err != nil {
					return err
				}
			}
		}
		c.objects[recordID] = o
	}
	return nil
}
