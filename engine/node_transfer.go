package engine

import (
	"bytes"
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func (e *Engine) addTransferArchiveNodes(ctx context.Context, r *nodeQueryReader, entry *viewEntry, c *nodeCatalog) error {
	space := entry.descriptor.Anchor.Space
	root, err := slotSpaceRoot(ctx, r, entry.roots.ExecutionKnowledge, space)
	if err != nil {
		return err
	}
	items, err := state.Entries(ctx, r, space, root, state.ScanLimits{MaxEntries: maxNodeQueryRecords, MaxBytes: 16 << 20, MaxReadBytes: 64 << 20})
	if err != nil {
		return err
	}
	prefix := transferArchivePrefix(entry.principal)
	for _, item := range items {
		if !bytes.HasPrefix(item.Key, prefix) {
			continue
		}
		o, err := readObject(ctx, r, item.Value)
		if err != nil {
			return err
		}
		v, err := readValue(o)
		if err != nil || o.Space != space || v.Kind != api.ValueBytes {
			return ErrInvalidStateData
		}
		archive, err := transfer.DecodeArchiveEntry(v.Data)
		if err != nil {
			return ErrInvalidStateData
		}
		if !bytes.Equal(item.Key, transferArchiveKey(entry.principal, item.Value)) {
			return ErrInvalidStateData
		}
		record, found, err := e.loadTransfer(ctx, entry.roots, space, entry.principal, archive.Manifest)
		if err != nil {
			return err
		}
		if !found {
			return ErrInvalidStateData
		}
		targets := map[string]model.ObjectID{}
		for _, mapping := range record.Mappings {
			targets[mapping.Source.Key()] = mapping.Target
		}
		var sources []model.NodeRef
		for _, dep := range archive.Source.Dependencies {
			if target := targets[dep.Key()]; target != "" {
				sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: target})
			}
		}
		ref := model.NodeRef{Kind: model.NodeObject, Object: item.Value}
		summary := api.NodeSummary{Ref: ref, Label: "imported foreign evidence", Kind: "TransferEvidence"}
		// Foreign anchors remain inside the typed ArchiveEntry data. They are
		// not local NodeAnchor positions and cannot be used to restore control.
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: summary, Payload: item.Value, Data: bytes.Clone(v.Data), Encoding: "transfer/ArchiveEntry/v1+json", Anchor: entry.descriptor.Anchor.Clone(), Sources: sources}, parents: sources}
		c.objects[item.Value] = o
	}
	if len(c.nodes) > maxNodeQueryRecords {
		return transfer.ErrInvalid
	}
	return nil
}
