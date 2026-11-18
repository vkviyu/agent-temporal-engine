package engine

import "github.com/vkviyu/agent-temporal-engine/model"

// policyDependency records why an identity participates in disclosure or reuse.
// It is never an address accepted by the physical content reader.
type policyDependency struct {
	kind policyDependencyKind
	id   model.ObjectID
}

type policyDependencyKind string

const (
	policyObject      policyDependencyKind = "object"
	policyObservation policyDependencyKind = "knowledge.observation"
	policyPacket      policyDependencyKind = "knowledge.packet"
	policySummary     policyDependencyKind = "knowledge.summary"
	policyMemory      policyDependencyKind = "memory"
	policyPath        policyDependencyKind = "experience_path"
	policyAnnotation  policyDependencyKind = "annotation"
	// A node selector can name an embedded record even when Kind is NodeObject.
	// Resolution/authorization belongs to the selected catalog, not this tag.
	policyNode     policyDependencyKind = "node"
	policyRevision policyDependencyKind = "revision"
	policyControl  policyDependencyKind = "control"
)

// policyIdentities is the deliberate type-erasure boundary for the existing
// policy graph and persisted derivations. Within a Space, equal digests share
// withdrawals across namespaces and authorized owners. The caller must first
// resolve and authorize each dependency; this conversion grants no access.
// Keep order, duplicates and empty IDs for existing caller/codec semantics.
func policyIdentities(dependencies []policyDependency) []model.ObjectID {
	ids := make([]model.ObjectID, len(dependencies))
	for i, dependency := range dependencies {
		ids[i] = dependency.id
	}
	return ids
}

func objectPolicyDependencies(ids ...model.ObjectID) []policyDependency {
	out := make([]policyDependency, len(ids))
	for i, id := range ids {
		out[i] = policyDependency{kind: policyObject, id: id}
	}
	return out
}

// payloadPolicyDependency uses the authenticated observation/summary schema.
// An empty payload kind denotes physical content; typed payloads are records.
func payloadPolicyDependency(id model.ObjectID, kind string) policyDependency {
	dependency := policyDependency{id: id}
	switch kind {
	case "":
		dependency.kind = policyObject
	case "memory":
		dependency.kind = policyMemory
	case "experience_path":
		dependency.kind = policyPath
	case "summary":
		dependency.kind = policySummary
	default:
		// Unresolved schema identities remain logical, never physical.
		dependency.kind = policyNode
	}
	return dependency
}

func nodePolicyDependency(ref model.NodeRef) policyDependency {
	switch ref.Kind {
	case model.NodeObject:
		return policyDependency{kind: policyNode, id: ref.Object}
	case model.NodeEvent:
		return policyDependency{kind: policyObject, id: model.ObjectID(ref.Event)}
	case model.NodeRevision:
		return policyDependency{kind: policyRevision, id: model.ObjectID(ref.Revision)}
	case model.NodeEntity:
		if model.ValidDigest(string(ref.Entity.ID)) {
			return policyDependency{kind: policyNode, id: model.ObjectID(ref.Entity.ID)}
		}
	}
	return policyDependency{kind: policyNode}
}
