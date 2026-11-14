package engine

import (
	"context"
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func (e *Engine) ExportTransfer(ctx context.Context, req api.ExportTransferRequest) (transfer.Manifest, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "export_transfer", req.Principal, req, func(scoped context.Context) (transfer.Manifest, error) {
			return e.ExportTransfer(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return transfer.Manifest{}, err
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return transfer.Manifest{}, err
	}
	if len(req.Roots) == 0 || len(req.Roots) > transfer.MaxEntries || len(req.Omit) > transfer.MaxEntries || req.Policy.ID == "" || req.Policy.Version == "" {
		return transfer.Manifest{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return transfer.Manifest{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return transfer.Manifest{}, err
	}
	publication := e.readPublication(ctx)
	rootsNow := e.readRoots(ctx)
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: publication.Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
		return transfer.Manifest{}, model.ErrAnchorMismatch
	}
	if _, _, err := e.rootsAtFence(ctx, anchor.Fence, false); err != nil {
		return transfer.Manifest{}, err
	}
	current, _, _, err := e.loadMemory(ctx, rootsNow, req.Space)
	if err != nil {
		return transfer.Manifest{}, err
	}
	omit := map[model.ObjectID]bool{}
	for _, id := range req.Omit {
		if !model.ValidDigest(string(id)) || omit[id] {
			return transfer.Manifest{}, api.ErrInvalidRequest
		}
		omit[id] = true
	}
	roots := append([]transfer.Reference(nil), req.Roots...)
	roots = transfer.SortReferences(roots)
	if len(roots) != len(req.Roots) {
		return transfer.Manifest{}, api.ErrInvalidRequest
	}
	for _, root := range roots {
		if root.Kind != "memory" && root.Kind != "experience_path" {
			return transfer.Manifest{}, ErrUnsupportedMethod
		}
		if root.Kind == "memory" {
			if _, err := current.ReusableMemory(root.ID, model.SnapshotAnchor{Space: req.Space, Fence: publication.Fence()}); err != nil {
				return transfer.Manifest{}, err
			}
		} else {
			if _, err := current.ReusablePath(root.ID, model.SnapshotAnchor{Space: req.Space, Fence: publication.Fence()}); err != nil {
				return transfer.Manifest{}, err
			}
		}
	}
	m := transfer.Manifest{Schema: transfer.Schema, Source: anchor, Principal: req.Principal, Policy: req.Policy, PolicyFence: publication.Fence(), PolicyRoot: rootsNow.ExecutionKnowledge, Roots: roots}
	queue := append([]transfer.Reference(nil), roots...)
	seen := map[string]bool{}
	var disclosed []model.ObjectID
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return transfer.Manifest{}, err
		}
		ref := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		if len(seen) > transfer.MaxEntries {
			return transfer.Manifest{}, transfer.ErrInvalid
		}
		if ref.Anchor.Validate() != nil || ref.Anchor.Space != req.Space || ref.Anchor.Fence.Store != anchor.Fence.Store || ref.Anchor.Fence.LSN > anchor.Fence.LSN {
			return transfer.Manifest{}, model.ErrAnchorMismatch
		}
		if err := e.checkPolicyIDs(ctx, req.Space, []model.ObjectID{ref.ID}, memory.AccessReuse); err != nil {
			return transfer.Manifest{}, err
		}
		disclosed = append(disclosed, ref.ID)
		entry, reason, err := e.transferEntry(ctx, req, ref)
		if err != nil {
			return transfer.Manifest{}, err
		}
		if omit[ref.ID] {
			reason = "whole-record-redaction"
		}
		if reason != "" {
			m.Gaps = append(m.Gaps, transfer.Gap{Source: ref, Reason: reason})
			continue
		}
		m.Entries = append(m.Entries, entry)
		queue = append(queue, entry.Dependencies...)
	}
	sort.Slice(m.Entries, func(i, j int) bool { return m.Entries[i].Source.Key() < m.Entries[j].Source.Key() })
	sort.Slice(m.Gaps, func(i, j int) bool { return m.Gaps[i].Source.Key() < m.Gaps[j].Source.Key() })
	m.ID = m.Digest()
	canonical, err := m.Canonical()
	if err != nil {
		return transfer.Manifest{}, err
	}
	manifest, err := transfer.DecodeManifest(canonical)
	if err != nil {
		return transfer.Manifest{}, err
	}
	return finishReadDelivery(ctx, e, "export_transfer", req.Principal, manifest, anchor, "", "", disclosed)
}

func (e *Engine) transferEntry(ctx context.Context, req api.ExportTransferRequest, ref transfer.Reference) (transfer.Entry, string, error) {
	commit := api.CommitRequest{Space: req.Space, Principal: req.Principal}
	roots, err := e.memoryAnchor(ctx, commit, ref.Anchor)
	if err != nil {
		return transfer.Entry{}, "", err
	}
	entry := transfer.Entry{Source: ref}
	prior, _, _, err := e.loadMemory(ctx, roots, req.Space)
	if err != nil {
		return entry, "", err
	}
	if ref.Kind == "memory" || ref.Kind == "source" {
		if m, err := prior.Get(ref.ID); err == nil {
			entry.Encoding = "memory"
			entry.Data, err = memory.Canonical(m)
			if err != nil {
				return entry, "", err
			}
			entry.Dependencies, err = transfer.Dependencies(ref, entry.Encoding, entry.Data)
			return entry, "", err
		}
	}
	if ref.Kind == "experience_path" || ref.Kind == "source" {
		if p, err := prior.GetPath(ref.ID); err == nil {
			entry.Encoding = "experience_path"
			entry.Data, err = memory.CanonicalPath(p)
			if err != nil {
				return entry, "", err
			}
			entry.Dependencies, err = transfer.Dependencies(ref, entry.Encoding, entry.Data)
			return entry, "", err
		}
	}
	if ref.Kind != "source" && ref.Kind != "object" && ref.Kind != "event" {
		return entry, "unsupported-schema-relation", nil
	}
	if ref.Anchor.Knowledge != nil {
		snap, _, _, _, err := e.selectedKnowledge(ctx, roots, ref.Anchor, req.Principal)
		if err != nil {
			return entry, "", err
		}
		for _, o := range snap.Observations {
			if o.ID == ref.ID {
				return entry, "embedded-knowledge-record-not-portable", nil
			}
		}
		for _, p := range snap.Packets {
			if p.ID == ref.ID {
				return entry, "embedded-knowledge-record-not-portable", nil
			}
		}
	}
	if err := e.visibleSourceObject(ctx, roots, ref.Anchor, req.Principal, ref.ID); err != nil {
		if errors.Is(err, knowledge.ErrUnknownObservation) {
			return entry, "unexported-control-or-missing-source", nil
		}
		return entry, "", err
	}
	o, err := readObject(ctx, e.reader(ctx), ref.ID)
	if err != nil {
		return entry, "", err
	}
	if o.Type != valueType && o.Type != eventlog.Type && o.Type != eventlog.ProfileType {
		return entry, "unsupported-raw-object-schema", nil
	}
	entry.Encoding = "object"
	if o.Type == eventlog.Type || o.Type == eventlog.ProfileType {
		entry.Encoding = "event"
	}
	entry.Data, err = codec.EncodeObject(o)
	if err != nil {
		return entry, "", err
	}
	entry.Dependencies, err = transfer.Dependencies(ref, entry.Encoding, entry.Data)
	return entry, "", err
}
