package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Revision membership comes from the View's BranchRefs, never today's graph.
// A state View with a Cursor may navigate its selected ancestry. Looking at
// other histories at that Fence is an explicit audit_snapshot operation.
func addRevisionNodes(ctx context.Context, r *nodeQueryReader, entry *viewEntry, c *nodeCatalog) error {
	space := entry.descriptor.Anchor.Space
	root, err := slotSpaceRoot(ctx, r, entry.roots.BranchRefs, space)
	if err != nil {
		return err
	}
	items, err := state.Entries(ctx, r, space, root, state.ScanLimits{MaxEntries: maxNodeQueryRecords, MaxBytes: 16 << 20, MaxReadBytes: 64 << 20})
	if err != nil {
		return err
	}
	records := map[model.RevisionID]model.ObjectID{}
	for _, item := range items {
		if !bytes.HasPrefix(item.Key, []byte("ate/graph/revision/")) {
			continue
		}
		id := model.RevisionID(item.Key[len("ate/graph/revision/"):])
		if !model.ValidDigest(string(id)) {
			return ErrInvalidStateData
		}
		records[id] = item.Value
	}
	selected := map[model.RevisionID]graph.Revision{}
	load := func(id model.RevisionID) (graph.Revision, error) {
		objectID, ok := records[id]
		if !ok {
			return graph.Revision{}, fmt.Errorf("%w: missing revision parent", ErrInvalidStateData)
		}
		obj, err := readObject(ctx, r, objectID)
		if err != nil {
			return graph.Revision{}, err
		}
		if obj.Space != space {
			return graph.Revision{}, ErrInvalidStateData
		}
		value, err := readValue(obj)
		if err != nil {
			return graph.Revision{}, err
		}
		if value.Kind != api.ValueBytes {
			return graph.Revision{}, ErrInvalidStateData
		}
		revision, err := graph.DecodeRevision(value.Data)
		if err != nil {
			return graph.Revision{}, fmt.Errorf("%w: %w", ErrStoredObject, err)
		}
		if revision.Space != space || revision.ID != id {
			return graph.Revision{}, ErrInvalidStateData
		}
		c.objects[objectID] = obj
		return revision, nil
	}
	var pending []model.RevisionID
	if cursor := entry.descriptor.Anchor.Cursor; cursor != nil && entry.mode != "audit_snapshot" {
		pending = []model.RevisionID{cursor.Revision}
	} else {
		for id := range records {
			pending = append(pending, id)
		}
	}
	expanded := 0
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, done := selected[id]; done {
			continue
		}
		if len(selected) >= maxNodeQueryRecords {
			return storage.ErrLimit
		}
		revision, err := load(id)
		if err != nil {
			return err
		}
		selected[id] = revision
		expanded += 1 + len(revision.Entries) + len(revision.JoinParents)
		if expanded > maxNodeQueryRecords {
			return storage.ErrLimit
		}
		if revision.StateParent != "" {
			pending = append(pending, revision.StateParent)
		}
		pending = append(pending, revision.JoinParents...)
	}
	ids := make([]model.RevisionID, 0, len(selected))
	for id := range selected {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		revision := selected[id]
		entries, err := state.Entries(ctx, r, space, revision.StateRoot, state.ScanLimits{MaxEntries: maxNodeQueryRecords, MaxBytes: 16 << 20, MaxReadBytes: 64 << 20})
		if err != nil {
			return err
		}
		if len(entries) != len(revision.Entries) {
			return ErrInvalidStateData
		}
		ref := model.NodeRef{Kind: model.NodeRevision, Revision: id}
		parents := make([]model.NodeRef, 0, len(revision.Entries)+len(revision.JoinParents)+1)
		if revision.StateParent != "" {
			parents = append(parents, model.NodeRef{Kind: model.NodeRevision, Revision: revision.StateParent})
		}
		for _, parent := range revision.JoinParents {
			parents = append(parents, model.NodeRef{Kind: model.NodeRevision, Revision: parent})
		}
		for _, id := range revision.Events {
			parents = append(parents, model.NodeRef{Kind: model.NodeEvent, Event: id})
		}
		for i, v := range entries {
			if !bytes.Equal(v.Key, revision.Entries[i].Key) || v.Value != revision.Entries[i].Value {
				return ErrInvalidStateData
			}
			content := model.NodeRef{Kind: model.NodeObject, Object: v.Value}
			parents = append(parents, content)
			o, exists := c.objects[v.Value]
			if !exists {
				o, err = readObject(ctx, r, v.Value)
				if err != nil {
					return err
				}
				if o.Space != space {
					return ErrInvalidStateData
				}
				if _, err = readValue(o); err != nil {
					return err
				}
				c.objects[v.Value] = o
			}
			if _, exists = c.nodes[content]; !exists {
				c.nodes[content] = nodeProjection{detail: api.NodeDetail{Ref: content, Summary: api.NodeSummary{Ref: content, Label: string(v.Key), Kind: "revision_content"}, Payload: v.Value, Data: bytes.Clone(o.Payload), Encoding: o.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}
			}
			if len(c.nodes) > maxNodeQueryRecords {
				return storage.ErrLimit
			}
		}
		raw, err := json.Marshal(revision)
		if err != nil {
			return err
		}
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Label: string(id), Kind: "Revision"}, Payload: records[id], Data: raw, Encoding: "graph/Revision/v1+json", Anchor: entry.descriptor.Anchor.Clone(), Sources: parents}, parents: parents}
		if len(c.nodes) > maxNodeQueryRecords {
			return storage.ErrLimit
		}
	}
	return nil
}
