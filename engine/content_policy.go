package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func packetPolicyIDs(packet knowledge.ContextPacket, snapshot knowledge.Snapshot) []model.ObjectID {
	ids := []model.ObjectID{packet.ID, packet.GoalRef}
	if packet.Input != nil {
		ids = append(ids, packet.Input.Object)
	}
	adopted := make(map[model.ObjectID]knowledge.Observation, len(snapshot.Observations))
	for _, observation := range snapshot.Observations {
		adopted[observation.ID] = observation
	}
	for _, item := range packet.Items {
		ids = append(ids, item.Observation, item.Content)
		if observation, ok := adopted[item.Observation]; ok {
			ids = append(ids, observation.Payload, observation.ImportedFrom)
		}
	}
	return ids
}

func knowledgePolicyIDs(snapshot knowledge.Snapshot) []model.ObjectID {
	ids := make([]model.ObjectID, 0, len(snapshot.Observations)*3)
	for _, observation := range snapshot.Observations {
		ids = append(ids, observation.ID, observation.Payload, observation.ImportedFrom)
	}
	for _, packet := range snapshot.Packets {
		ids = append(ids, packetPolicyIDs(packet, snapshot)...)
	}
	return ids
}

// summaryDependencyIDs follows only explicit immutable Summary provenance.
// It never searches for copied text or grants access to a dependency. Limits
// are cumulative, including shared/nested summaries, so audit/use gates cannot
// be made unbounded by a long summary chain.
func (e *Engine) summaryDependencyIDs(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, summaries []model.ObjectID) ([]model.ObjectID, error) {
	queue := append([]model.ObjectID(nil), summaries...)
	seen := map[model.ObjectID]bool{}
	var ids []model.ObjectID
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		if len(seen) > 4096 || len(ids) > 16384 {
			return nil, storage.ErrLimit
		}
		s, _, found, err := e.loadSummary(ctx, roots, space, principal, id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, knowledge.ErrInvalidSummary
		}
		ids = append(ids, summaryPolicyIDs(s)...)
		for _, source := range s.Sources {
			if source.PayloadKind == "summary" {
				queue = append(queue, source.Content)
			}
		}
	}
	return ids, nil
}

func observationSummaryIDs(observations []knowledge.Observation) []model.ObjectID {
	var ids []model.ObjectID
	for _, o := range observations {
		if o.PayloadKind == "summary" {
			ids = append(ids, o.Payload)
		}
	}
	return ids
}

// checkDisclosureIDs is called while the engine lock protects the current
// control roots. It never rewrites historical content after a withdrawal.
func (e *Engine) checkDisclosureIDs(ctx context.Context, space model.SpaceID, ids []model.ObjectID) error {
	current, _, _, err := e.loadMemory(ctx, e.roots, space)
	if err != nil {
		return err
	}
	return current.AccessPolicy().Check(ids, memory.AccessAudit)
}

// checkViewDisclosure conservatively rejects a fixed View containing explicit
// protected provenance instead of returning a silently edited historical view.
// This is an output check under the core lock, not a transferable emission grant.
func (e *Engine) checkViewDisclosure(ctx context.Context, entry *viewEntry) error {
	// This gate is called with the core lock held. Catalog expansion may read
	// historical event metadata; its private cut must not reacquire that lock.
	lockedEntry := *entry
	lockedEntry.coreLocked = true
	entry = &lockedEntry
	space := entry.descriptor.Anchor.Space
	current, _, _, err := e.loadMemory(ctx, e.roots, space)
	if err != nil {
		return err
	}
	policy := current.AccessPolicy()
	if !policy.HasDisclosureDenials() {
		return nil
	}
	// Published annotation commands and events retain judgment text. Their
	// sources remain part of the owner's disclosure closure after live keys are
	// removed, so hiding/deleting navigation entries cannot bypass a later ban.
	if entry.descriptor.Anchor.Knowledge == nil {
		reviews, err := e.loadAnnotations(ctx, entry.roots, space, entry.principal)
		if err != nil {
			return err
		}
		for _, record := range reviews.Records {
			if _, _, err := e.checkAnnotationReferences(ctx, space, entry.principal, record); err != nil {
				return err
			}
		}
	}
	if e.options.AgentProfile && entry.descriptor.Anchor.Knowledge == nil {
		ledger, _, found, err := loadAgentLedger(ctx, e.index, entry.roots.LiveControl, space, entry.principal)
		if err != nil {
			return err
		}
		if found {
			if _, err := e.checkAgentSnapshotDisclosure(ctx, entry.roots, space, entry.principal, ledger.Snapshot(), false); err != nil {
				return err
			}
		}
	}
	if entry.descriptor.Anchor.Knowledge != nil {
		snap, _, _, _, err := e.selectedKnowledge(ctx, entry.roots, entry.descriptor.Anchor, entry.principal)
		if err != nil {
			return err
		}
		ids, err := e.summaryDependencyIDs(ctx, entry.roots, space, entry.principal, observationSummaryIDs(snap.Observations))
		if err != nil {
			return err
		}
		if err = policy.Check(append(knowledgePolicyIDs(snap), ids...), memory.AccessAudit); err != nil {
			return err
		}
	}
	catalog, err := e.nodeCatalog(ctx, entry, "content")
	if err != nil {
		return err
	}
	ids := make([]model.ObjectID, 0, len(catalog.nodes)+len(catalog.objects))
	for id := range catalog.objects {
		ids = append(ids, id)
	}
	for ref, node := range catalog.nodes {
		ids = append(ids, node.detail.Payload)
		if ref.Kind == model.NodeEntity && ref.Entity.Profile == "memory" {
			ids = append(ids, model.ObjectID(ref.Entity.ID))
		}
	}
	return policy.Check(ids, memory.AccessAudit)
}

// checkCurrentActivityUse runs against the current candidate roots at each
// managed dispatch stage. A packet's historical input cut does not allow a
// later claim or dispatch to ignore a published source withdrawal. This checks
// explicitly recorded provenance; it cannot detect arbitrary copied bytes and
// is not an out-of-process network grant/revoke protocol.
func (e *Engine) checkCurrentActivityUse(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, spec agentprofile.ActivitySpec) error {
	current, _, _, err := e.loadMemory(ctx, e.roots, req.Space)
	if err != nil {
		return err
	}
	policy := current.AccessPolicy()
	ids := []model.ObjectID{spec.Request}
	if spec.Contract.Definition != "" {
		ids = append(ids, spec.Contract.Definition)
	}
	if spec.Query != "" && spec.Execution == "" {
		p, found, err := e.loadQueryProgress(ctx, req, spec.Query)
		if err != nil {
			return err
		}
		if found {
			if inv, exists := queryInvocation(p, spec.Activity); exists {
				if !matchesQueryToolEffect(inv, spec) {
					return agentprofile.ErrConflict
				}
				ids = append(ids, inv.Definition)
			}
		}
	}
	if spec.Invocation != "" {
		invocation, err := ledger.GetInvocation(spec.Invocation)
		if err != nil {
			return err
		}
		if invocation.ToolDefinition != "" {
			ids = append(ids, invocation.ToolDefinition)
		}
	}
	if err := policy.Check(ids, memory.AccessReuse); err != nil {
		return err
	}
	if spec.ContextPacket == "" {
		return nil
	}
	roots, _, err := e.rootsAtFence(ctx, spec.InputFence, true)
	if err != nil {
		return err
	}
	snapshot, _, _, err := e.loadKnowledge(ctx, roots, req.Space, req.Principal)
	if err != nil {
		return err
	}
	for _, packet := range snapshot.Packets {
		if packet.ID == spec.ContextPacket {
			selected := map[model.ObjectID]bool{}
			for _, item := range packet.Items {
				selected[item.Observation] = true
			}
			var summaries []model.ObjectID
			for _, o := range snapshot.Observations {
				if selected[o.ID] && o.PayloadKind == "summary" {
					summaries = append(summaries, o.Payload)
				}
			}
			dependencies, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, summaries)
			if err != nil {
				return err
			}
			return policy.Check(append(packetPolicyIDs(packet, snapshot), dependencies...), memory.AccessReuse)
		}
	}
	return agentprofile.ErrUnknown
}

func activitySpecInSnapshot(snapshot agentprofile.Snapshot, id string) (agentprofile.ActivitySpec, error) {
	if snapshot.Lifecycle.Recovery != nil {
		for _, spec := range snapshot.Lifecycle.Recovery.ActivitySpecs {
			if spec.Activity == id {
				return spec, nil
			}
		}
	}
	return agentprofile.ActivitySpec{}, agentprofile.ErrUnknown
}

func (e *Engine) checkCurrentAttemptUse(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, activity, attempt string) error {
	snapshot := ledger.Snapshot()
	if attempt != "" && snapshot.Lifecycle.Recovery != nil {
		for _, control := range snapshot.Lifecycle.Recovery.AttemptControls {
			if control.Attempt == attempt {
				activity = control.Activity
				break
			}
		}
	}
	spec, err := activitySpecInSnapshot(snapshot, activity)
	if err != nil {
		return err
	}
	return e.checkCurrentActivityUse(ctx, req, ledger, spec)
}
