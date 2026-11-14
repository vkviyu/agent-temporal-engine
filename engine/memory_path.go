package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Path steps reference both a promoted statement and the exact historical
// record that supports it. Navigation edges, time order, and an entity's
// shared backing snapshot cannot establish that support.
func pathStepSupported(node nodeProjection, value memory.Memory) bool {
	support := value.SupportingSources()
	matches := func(id model.ObjectID) bool {
		if id == "" {
			return false
		}
		for _, source := range support {
			if id == source.SourceID {
				return true
			}
		}
		return false
	}
	ref := node.detail.Ref
	if ref.Kind != model.NodeEntity || ref.Entity.Profile == "memory" || (ref.Entity.Profile == "knowledge" && ref.Entity.Kind == "summary") {
		if matches(nodeReadRefID(ref)) {
			return true
		}
	}
	for _, source := range node.detail.Sources {
		// An event's predecessor is a different occurrence. A revision's
		// contents and attached events are explicit parts of that revision.
		if ref.Kind == model.NodeRevision && source.Kind == model.NodeEvent && matches(model.ObjectID(source.Event)) {
			return true
		}
		if source.Kind == model.NodeObject && source.Object != node.detail.Payload &&
			(ref.Kind != model.NodeEvent || node.detail.Anchor.Knowledge == nil) && matches(source.Object) {
			return true
		}
	}
	return false
}

func (e *Engine) checkExperiencePathReferences(ctx context.Context, req api.CommitRequest, path *memory.BuildPathCommand, prior *memory.Store) ([]model.ObjectID, error) {
	entry, err := e.historicalSourceEntry(ctx, req.Space, req.Principal, path.Anchor, "state")
	if err != nil {
		return nil, err
	}
	refs := make(map[model.NodeRef]nodeProjection)
	catalogs := make(map[string]nodeCatalog)
	dependencies := append([]model.ObjectID(nil), path.DerivedFrom...)
	agentEvidenceCollected := false
	resolve := func(ref model.NodeRef) (nodeProjection, error) {
		if node, found := refs[ref]; found {
			return node, nil
		}
		node, err := e.pathNodeAt(ctx, entry, ref, catalogs)
		if errors.Is(err, ErrNodeNotFound) {
			return nodeProjection{}, memory.ErrSourceUnavailable
		}
		if err != nil {
			return nodeProjection{}, err
		}
		refs[ref] = node
		dependencies = append(dependencies, nodeReadRefID(ref))
		dependencies = append(dependencies, nodeDisclosureIDs(node)...)
		if node.detail.Encoding == "annotation/Record/v1+json" {
			var record annotation.Record
			if err := json.Unmarshal(node.detail.Data, &record); err != nil {
				return nodeProjection{}, ErrStoredObject
			}
			_, ids, err := e.checkAnnotationReferences(ctx, req.Space, req.Principal, record)
			if err != nil {
				return nodeProjection{}, err
			}
			dependencies = append(dependencies, ids...)
		}
		if !agentEvidenceCollected && ref.Kind == model.NodeEntity && ref.Entity.Profile == "agent" {
			ids, err := e.pathAgentDependencies(ctx, entry)
			if err != nil {
				return nodeProjection{}, err
			}
			dependencies = append(dependencies, ids...)
			agentEvidenceCollected = true
		}
		if len(dependencies) > maxDerivationEdges {
			return nodeProjection{}, storage.ErrLimit
		}
		return node, nil
	}
	if path.Anchor.Knowledge != nil {
		snapshot, _, _, _, err := e.selectedKnowledge(ctx, entry.roots, path.Anchor, req.Principal)
		if err != nil {
			return nil, err
		}
		adopted := make(map[model.ObjectID]bool)
		for _, observation := range snapshot.Observations {
			if observation.PayloadKind == "memory" || observation.PayloadKind == "experience_path" {
				adopted[observation.Payload] = true
			}
		}
		for _, step := range path.Steps {
			if !adopted[step.Memory] {
				return nil, memory.ErrSourceUnavailable
			}
		}
		for _, parent := range path.DerivedFrom {
			if !adopted[parent] {
				return nil, memory.ErrSourceUnavailable
			}
		}
	}
	for i, step := range path.Steps {
		node, err := resolve(step.Node)
		if err != nil {
			return nil, fmt.Errorf("experience path step %d: %w", i, err)
		}
		member, err := prior.Get(step.Memory)
		if err != nil {
			return nil, memory.ErrSourceUnavailable
		}
		if !pathStepSupported(node, member) {
			return nil, fmt.Errorf("experience path step %d: %w", i, memory.ErrStepEvidenceMismatch)
		}
		dependencies = append(dependencies, step.Memory)
	}
	for i, ref := range path.Omitted {
		if _, err := resolve(ref); err != nil {
			return nil, fmt.Errorf("experience path omitted node %d: %w", i, err)
		}
	}
	for _, source := range append(append([]memory.Source(nil), path.Validation.Evidence...), path.Validation.Counterexamples...) {
		dependencies = append(dependencies, source.SourceID)
	}
	return dependencies, e.checkPolicyIDs(ctx, req.Space, dependencies, memory.AccessReuse)
}

// pathNodeAt resolves explicit historical refs. A single path can cite many
// nodes in one view, so it builds each bounded layer at most once. Unfiltered
// occurrence reads need only their exact record.
func (e *Engine) pathNodeAt(ctx context.Context, entry *viewEntry, ref model.NodeRef, catalogs map[string]nodeCatalog) (nodeProjection, error) {
	anchor := entry.descriptor.Anchor
	if ref.Kind == model.NodeEvent && anchor.Knowledge == nil && anchor.Cursor == nil {
		return e.projectUnfilteredEvent(ctx, entry, ref)
	}
	if ref.Kind == model.NodeEntity || ref.Kind == model.NodeObject {
		if node, handled, err := domainSubjectNode(ctx, &nodeQueryReader{source: e.reader(ctx)}, entry, ref); handled {
			return node, err
		}
	}
	layer := "content"
	if ref.Kind == model.NodeEntity && anchor.Knowledge == nil {
		layer = "entities"
	} else if ref.Kind == model.NodeEvent {
		layer = "events"
	}
	catalog, found := catalogs[layer]
	if !found {
		var err error
		catalog, err = e.nodeCatalog(ctx, entry, layer)
		if err != nil {
			return nodeProjection{}, err
		}
		catalogs[layer] = catalog
	}
	if node, found := catalog.nodes[ref]; found {
		return node, nil
	}
	if ref.Kind == model.NodeObject {
		if _, found := catalog.objects[ref.Object]; found {
			return nodeProjection{detail: api.NodeDetail{Ref: ref, Payload: ref.Object, Anchor: anchor}}, nil
		}
		if anchor.Knowledge == nil && e.options.AgentProfile {
			// Agent projections may declare exact historical input/receipt
			// object refs outside the current domain tree.
			node, _, err := e.projectedNode(ctx, entry, ref)
			return node, err
		}
	}
	return nodeProjection{}, ErrNodeNotFound
}

func (e *Engine) pathAgentDependencies(ctx context.Context, entry *viewEntry) ([]model.ObjectID, error) {
	ledger, _, found, err := loadAgentLedger(ctx, e.reader(ctx), entry.roots.LiveControl, entry.descriptor.Anchor.Space, entry.principal)
	if err != nil || !found {
		return nil, err
	}
	return e.checkAgentSnapshotDisclosure(ctx, entry.roots, entry.descriptor.Anchor.Space, entry.principal, ledger.Snapshot(), true)
}

func (e *Engine) prepareExperiencePathValue(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, raw []byte, path memory.ExperiencePath, dependencies []model.ObjectID) (*commitPlan, error) {
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := codec.ObjectID(command)
	if err != nil {
		return nil, err
	}
	return e.prepareDerivedValue(ctx, req, digest, txID, lsn, recorded, memoryStateKey(req.Space), raw, map[model.ObjectID][]model.ObjectID{
		path.ID:   dependencies,
		commandID: {path.ID},
	})
}
