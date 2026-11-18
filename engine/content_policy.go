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
	return policyIdentities(packetPolicyDependencies(packet, snapshot))
}

func observationPolicyDependencies(observation knowledge.Observation) []policyDependency {
	return []policyDependency{
		{kind: policyObservation, id: observation.ID},
		payloadPolicyDependency(observation.Payload, observation.PayloadKind),
		{kind: policyObservation, id: observation.ImportedFrom},
	}
}

func packetPolicyDependencies(packet knowledge.ContextPacket, snapshot knowledge.Snapshot) []policyDependency {
	adopted := make(map[model.ObjectID]knowledge.Observation, len(snapshot.Observations))
	goal := policyDependency{kind: policyNode, id: packet.GoalRef}
	goalFound := false
	for _, observation := range snapshot.Observations {
		adopted[observation.ID] = observation
		if packet.GoalRef != "" && observation.Payload == packet.GoalRef {
			candidate := payloadPolicyDependency(observation.Payload, observation.PayloadKind)
			if !goalFound {
				goal = candidate
				goalFound = true
			} else if goal.kind != candidate.kind {
				// A digest alone cannot choose between payload namespaces.
				goal.kind = policyNode
			}
		}
	}
	dependencies := []policyDependency{{kind: policyPacket, id: packet.ID}, goal}
	if packet.Input != nil {
		dependencies = append(dependencies, policyDependency{kind: policyObject, id: packet.Input.Object})
	}
	for _, item := range packet.Items {
		content := policyDependency{kind: policyNode, id: item.Content}
		observation, ok := adopted[item.Observation]
		if ok && item.Content != "" && observation.Payload == item.Content {
			content = payloadPolicyDependency(item.Content, observation.PayloadKind)
		}
		dependencies = append(dependencies, policyDependency{kind: policyObservation, id: item.Observation}, content)
		if ok {
			dependencies = append(dependencies, observationPolicyDependencies(observation)[1:]...)
		}
	}
	return dependencies
}

func knowledgePolicyIDs(snapshot knowledge.Snapshot) []model.ObjectID {
	return policyIdentities(knowledgePolicyDependencies(snapshot))
}

func knowledgePolicyDependencies(snapshot knowledge.Snapshot) []policyDependency {
	dependencies := make([]policyDependency, 0, len(snapshot.Observations)*3)
	for _, observation := range snapshot.Observations {
		dependencies = append(dependencies, observationPolicyDependencies(observation)...)
	}
	for _, packet := range snapshot.Packets {
		dependencies = append(dependencies, packetPolicyDependencies(packet, snapshot)...)
	}
	return dependencies
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
	return e.checkPolicyIDs(ctx, space, ids, memory.AccessAudit)
}

// checkViewDisclosure conservatively rejects a fixed View containing explicit
// protected provenance instead of returning a silently edited historical view.
// This is an output check under the core lock, not a transferable emission grant.
func (e *Engine) checkViewDisclosure(ctx context.Context, entry *viewEntry) error {
	// This gate is called with the core lock held. Catalog expansion may read
	// historical event metadata; its private cut must not reacquire that lock.
	lockedEntry := *entry
	lockedEntry.coreLocked = e.readCut(ctx) == nil
	entry = &lockedEntry
	space := entry.descriptor.Anchor.Space
	policy, err := e.readPolicy(ctx, space)
	if err != nil {
		return err
	}
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
		ledger, _, found, err := loadAgentLedger(ctx, e.reader(ctx), entry.roots.LiveControl, space, entry.principal)
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
		if err = e.checkPolicyIDs(ctx, space, append(knowledgePolicyIDs(snap), ids...), memory.AccessAudit); err != nil {
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
	return e.checkPolicyIDs(ctx, space, ids, memory.AccessAudit)
}

// checkCurrentActivityUse runs against the current candidate roots at each
// managed dispatch stage. A packet's historical input cut does not allow a
// later claim or dispatch to ignore a source withdrawal or a superseded
// memory. This checks
// explicitly recorded provenance; it cannot detect arbitrary copied bytes and
// is not an out-of-process network grant/revoke protocol.
func (e *Engine) checkCurrentActivityUse(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, spec agentprofile.ActivitySpec) error {
	ids := []model.ObjectID{spec.Request}
	answerIDs, err := e.finalAnswerInputDisclosureIDs(ctx, spec.FinalAnswerInput)
	if err != nil {
		return err
	}
	ids = append(ids, answerIDs...)
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
	if err := e.checkPolicyIDs(ctx, req.Space, ids, memory.AccessReuse); err != nil {
		return err
	}
	if spec.ContextPacket == "" {
		return nil
	}
	record, err := e.activityPacketAt(ctx, req.Space, req.Principal, spec.InputFence, spec.ContextPacket)
	if err != nil {
		return err
	}
	dependencies, err := e.packetDependencyIDs(ctx, record.roots, req.Space, req.Principal, record.packet, record.selected)
	if err != nil {
		return err
	}
	if err = e.checkPolicyIDs(ctx, req.Space, dependencies, memory.AccessReuse); err != nil {
		return err
	}
	if isModelActivityRole(spec.Role) {
		return e.validateCurrentTypedPacket(ctx, record.packet, record.selected, req.Space, req.Principal)
	}
	return nil
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
