package engine

import (
	"context"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// A View position and an original input position are different relations. In
// particular, a control record does not acquire an executable graph Revision.
func (e *Engine) nodeDeclaredAnchors(_ context.Context, entry *viewEntry, n nodeProjection) ([]api.NodeAnchor, error) {
	out := []api.NodeAnchor{{Relation: "selected_view", Anchor: entry.descriptor.Anchor.Clone()}}
	switch n.detail.Encoding {
	case "knowledge/ContextPacket/v1+json":
		var p knowledge.ContextPacket
		if json.Unmarshal(n.detail.Data, &p) != nil {
			return nil, ErrInvalidStateData
		}
		out = append(out, api.NodeAnchor{Relation: "knowledge_source", Anchor: p.Source.Anchor.Clone()})
		if p.Input != nil {
			out = append(out, api.NodeAnchor{Relation: "recorded_input", Anchor: p.Input.Anchor.Clone()})
		}
	case "knowledge/Observation/v1+json":
		var o knowledge.Observation
		if json.Unmarshal(n.detail.Data, &o) != nil {
			return nil, ErrInvalidStateData
		}
		out = append(out, api.NodeAnchor{Relation: "observation_source", Anchor: o.Source.Anchor.Clone()})
	case "knowledge/Summary/v1+json":
		var s knowledge.Summary
		if json.Unmarshal(n.detail.Data, &s) != nil {
			return nil, ErrInvalidStateData
		}
		out = append(out, api.NodeAnchor{Relation: "summary_source", Anchor: s.Source.Anchor.Clone()})
	case "agent/ActivitySpec/v1+json":
		var a agentprofile.ActivitySpec
		if json.Unmarshal(n.detail.Data, &a) != nil {
			return nil, ErrInvalidStateData
		}
		out = append(out, api.NodeAnchor{Relation: "activity_input", Anchor: model.SnapshotAnchor{Space: entry.descriptor.Anchor.Space, Fence: a.InputFence}})
	}
	return out, nil
}

// addDeclaredNodeInputs follows only explicit references from the owner's
// authenticated ActivitySpecs. It is not a scan of other executions' knowledge
// and must never run for an execution_knowledge View.
func (e *Engine) addDeclaredNodeInputs(ctx context.Context, entry *viewEntry, c *nodeCatalog) error {
	if entry.descriptor.Anchor.Knowledge != nil {
		return nil
	}
	var specs []agentprofile.ActivitySpec
	for _, n := range c.nodes {
		if n.detail.Encoding == "agent/ActivitySpec/v1+json" {
			var a agentprofile.ActivitySpec
			if json.Unmarshal(n.detail.Data, &a) != nil {
				return ErrInvalidStateData
			}
			specs = append(specs, a)
		}
	}
	r := &nodeQueryReader{source: e.reader(ctx)}
	space := entry.descriptor.Anchor.Space
	root, err := slotSpaceRoot(ctx, r, entry.roots.ExecutionKnowledge, space)
	if err != nil {
		return err
	}
	index, err := openKnowledgeIndex(ctx, r, root, space, entry.principal)
	if err != nil {
		return err
	}
	addObject := func(id model.ObjectID, label string, kind model.EntityKind, requireBytes bool) error {
		if id == "" {
			return nil
		}
		ref := model.NodeRef{Kind: model.NodeObject, Object: id}
		if _, ok := c.nodes[ref]; ok {
			return nil
		}
		obj, err := readObject(ctx, r, id)
		if err != nil {
			return err
		}
		if obj.Space != entry.descriptor.Anchor.Space {
			return ErrInvalidStateData
		}
		if requireBytes {
			v, err := readValue(obj)
			if err != nil || v.Kind != api.ValueBytes {
				return ErrInvalidStateData
			}
		}
		c.objects[id] = obj
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Kind: kind, Label: label}, Payload: id, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}
		if len(c.nodes) > maxNodeQueryRecords {
			return storage.ErrLimit
		}
		return nil
	}
	for _, a := range specs {
		if err := addObject(a.Request, "request envelope", "recorded_input", true); err != nil {
			return err
		}
		if a.ContextPacket == "" {
			continue
		}
		p, ok, err := index.packet(ctx, a.ContextPacket)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		// Publication validation binds model packets to the Activity execution;
		// retain the check here so catalog composition cannot widen that scope.
		if p.Space != entry.descriptor.Anchor.Space || (a.Execution == "" && p.Key != nil) || (a.Execution != "" && (p.Key == nil || p.Key.Execution != a.Execution)) {
			return ErrInvalidStateData
		}
		if p.Input != nil {
			if err := addObject(p.Input.Object, p.Input.Encoding, "recorded_input", true); err != nil {
				return err
			}
		}
		ref := model.NodeRef{Kind: model.NodeObject, Object: p.ID}
		data, err := json.Marshal(p)
		if err != nil {
			return err
		}
		parents := []model.NodeRef{}
		for _, item := range p.Items {
			parents = append(parents, model.NodeRef{Kind: model.NodeObject, Object: item.Observation})
		}
		if p.Input != nil {
			parents = append(parents, model.NodeRef{Kind: model.NodeObject, Object: p.Input.Object})
		}
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Kind: "ContextPacket", Label: p.Renderer}, Data: data, Encoding: "knowledge/ContextPacket/v1+json", Anchor: entry.descriptor.Anchor.Clone(), Sources: parents}, parents: parents}
		if len(c.nodes) > maxNodeQueryRecords {
			return storage.ErrLimit
		}
	}
	// These source fields are authenticated raw-object relationships, unlike
	// opaque evidence/goal digests which must never become read grants.
	declared := []nodeProjection{}
	for _, n := range c.nodes {
		switch n.detail.Encoding {
		case "agent/TaskOutcome/v1+json", "agent/ResumePlan/v1+json", "agent/ActionIntent/v1+json", "agent/ActionResolution/v1+json", "agent/ValidationRequirement/v1+json":
			declared = append(declared, n)
		}
	}
	for _, n := range declared {
		for _, source := range n.detail.Sources {
			if source.Kind != model.NodeObject || source.Object == n.detail.Payload {
				continue
			}
			kind := model.EntityKind("artifact")
			if n.detail.Summary.Kind == "ActionIntent" {
				kind = "declared_input"
			}
			if n.detail.Summary.Kind == "ActionResolution" {
				kind = "receipt"
			}
			if err := addObject(source.Object, string(n.detail.Summary.Kind), kind, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) nodeContextCatalog(ctx context.Context, entry *viewEntry) (nodeCatalog, error) {
	c, err := e.nodeCatalog(ctx, entry, "content")
	if err != nil {
		return c, err
	}
	if entry.descriptor.Anchor.Knowledge != nil || !e.options.AgentProfile {
		return c, nil
	}
	p, err := e.nodeCatalog(ctx, entry, "entities")
	if err != nil {
		return c, err
	}
	for ref, n := range p.nodes {
		c.nodes[ref] = n
	}
	for id, obj := range p.objects {
		c.objects[id] = obj
	}
	if len(c.nodes) > maxNodeQueryRecords {
		return c, storage.ErrLimit
	}
	return c, e.addDeclaredNodeInputs(ctx, entry, &c)
}

// Additional historical envelopes are authorized by the explicit publication
// chain, but must still respect current audit restrictions at output time.
func (e *Engine) nodeContextOutputGate(ctx context.Context, entry *viewEntry, ids []model.ObjectID, details ...nodeProjection) error {
	// Annotation text can describe a source no longer present in the selected
	// navigation View. Recheck its own historical provenance at output time.
	for _, detail := range details {
		if detail.detail.Encoding != "annotation/Record/v1+json" {
			continue
		}
		var record annotation.Record
		if json.Unmarshal(detail.detail.Data, &record) != nil {
			return e.readFailureAt(ctx, ErrStoredObject)
		}
		if _, _, err := e.checkAnnotationProvenance(ctx, entry.descriptor.Anchor.Space, entry.principal, record); err != nil {
			return e.annotationReadFailure(ctx, err)
		}
	}
	if key := entry.descriptor.Anchor.Knowledge; key != nil {
		if err := e.authorizeKnowledge(ctx, e.readRoots(ctx), entry.descriptor.Anchor.Space, entry.principal, *key); err != nil {
			return e.knowledgeReadFailure(ctx, err)
		}
	}
	if err := e.checkDisclosureIDs(ctx, entry.descriptor.Anchor.Space, ids); err != nil {
		return err
	}
	if err := e.checkViewDisclosure(ctx, entry); err != nil {
		return err
	}
	return e.viewOutputGate()
}

func nodeDisclosureIDs(n nodeProjection) []model.ObjectID {
	return policyIdentities(nodeDisclosureDependencies(n))
}

func nodeDisclosureDependencies(n nodeProjection) []policyDependency {
	payload := policyDependency{kind: policyObject, id: n.detail.Payload}
	switch n.detail.Encoding {
	case "memory/Memory/v1+json":
		payload.kind = policyMemory
	case "memory/ExperiencePath/v1+json":
		payload.kind = policyPath
	case "memory/SourceBan/v1+json":
		payload.kind = policyNode
	case "knowledge/Summary/v1+json":
		// Knowledge catalogs carry the logical Summary ID; domain catalogs
		// carry the physical record. Digest equality cannot distinguish them.
		if n.detail.Anchor.Knowledge != nil {
			payload.kind = policySummary
		}
	}
	dependencies := []policyDependency{payload}
	if n.detail.Encoding == "annotation/Record/v1+json" {
		var record annotation.Record
		if json.Unmarshal(n.detail.Data, &record) == nil {
			dependencies = append(dependencies,
				policyDependency{kind: policyAnnotation, id: record.ID},
				policyDependency{kind: policyAnnotation, id: record.Retracts})
			for _, ref := range append([]model.NodeRef{record.Target}, record.Evidence...) {
				switch ref.Kind {
				case model.NodeObject, model.NodeRevision, model.NodeEvent:
					dependencies = append(dependencies, nodePolicyDependency(ref))
				case model.NodeEntity:
					if model.ValidDigest(string(ref.Entity.ID)) {
						dependencies = append(dependencies, nodePolicyDependency(ref))
					}
				}
			}
		}
	}
	for _, source := range n.detail.Sources {
		if source.Kind == model.NodeObject {
			// NodeObject is a catalog selector, also used by embedded
			// Knowledge records; it does not promise a physical object.
			dependencies = append(dependencies, nodePolicyDependency(source))
		}
	}
	if n.detail.Encoding == "knowledge/ContextPacket/v1+json" {
		var p knowledge.ContextPacket
		if json.Unmarshal(n.detail.Data, &p) == nil {
			dependencies = append(dependencies, policyDependency{kind: policyNode, id: p.GoalRef})
			for _, item := range p.Items {
				// A packet alone does not carry the adopted payload kind.
				dependencies = append(dependencies, policyDependency{kind: policyNode, id: item.Content})
			}
		}
	}
	if n.detail.Encoding == "knowledge/Summary/v1+json" {
		var s knowledge.Summary
		if json.Unmarshal(n.detail.Data, &s) == nil {
			dependencies = append(dependencies, summaryPolicyDependencies(s)...)
		}
	}
	return dependencies
}
