package memory

import (
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

var (
	ErrHistoryAccessDenied = errors.New("content access denied by current privacy or ACL policy")
	ErrUseDenied           = errors.New("content is withdrawn from new use")
)

type AccessPurpose string

const (
	AccessAudit AccessPurpose = "audit"
	AccessReuse AccessPurpose = "reuse"
)

// AccessPolicy is an owned immutable projection of the current recorded
// provenance and withdrawals. It detects references, not arbitrary copies of
// bytes. It does not erase physical history or grant access to any object.
type AccessPolicy struct {
	restrictions map[model.ObjectID]RetractionKind
	dependencies map[model.ObjectID][]model.ObjectID
	disclosure   bool
}

func disclosureBan(kind RetractionKind) bool {
	return kind == RetractionPrivacyErasure || kind == RetractionACLRevocation
}

// AccessPolicy captures policy under the Store lock, including retained
// historical Retraction records. A later weak source-ban projection cannot
// conceal a retained privacy/ACL withdrawal of a dependency. Historical logs
// which discarded that withdrawal without any derived record cannot be
// reconstructed by this projection alone.
func (s *Store) AccessPolicy() AccessPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p := AccessPolicy{restrictions: make(map[model.ObjectID]RetractionKind), dependencies: make(map[model.ObjectID][]model.ObjectID)}
	addBan := func(id model.ObjectID, kind RetractionKind) {
		if id == "" || !validRetractionKind(kind) {
			return
		}
		if !disclosureBan(p.restrictions[id]) {
			p.restrictions[id] = kind
		}
		p.disclosure = p.disclosure || disclosureBan(kind)
	}
	for id, retraction := range s.sourceBan {
		addBan(id, retraction.Kind)
	}
	for id, m := range s.memories {
		dependencies := append(append([]model.ObjectID(nil), m.Supersedes...), m.Retracts...)
		for _, source := range allSources(m) {
			dependencies = append(dependencies, source.SourceID)
		}
		if m.Retraction != nil {
			addBan(id, m.Retraction.Kind)
			addBan(m.Retraction.SourceID, m.Retraction.Kind)
			for _, old := range m.Retracts {
				addBan(old, m.Retraction.Kind)
			}
			dependencies = append(dependencies, m.Retraction.SourceID)
		}
		p.dependencies[id] = dependencies
	}
	nodeID := func(ref model.NodeRef) model.ObjectID {
		switch ref.Kind {
		case model.NodeObject:
			return ref.Object
		case model.NodeEvent:
			return model.ObjectID(ref.Event)
		case model.NodeRevision:
			return model.ObjectID(ref.Revision)
		case model.NodeEntity:
			if ref.Entity.Profile == "memory" {
				return model.ObjectID(ref.Entity.ID)
			}
		}
		return ""
	}
	for id, path := range s.paths {
		dependencies := append([]model.ObjectID(nil), path.DerivedFrom...)
		for _, step := range path.Steps {
			dependencies = append(dependencies, step.Memory, nodeID(step.Node))
		}
		for _, omitted := range path.Omitted {
			dependencies = append(dependencies, nodeID(omitted))
		}
		for _, source := range append(append([]Source(nil), path.Validation.Evidence...), path.Validation.Counterexamples...) {
			dependencies = append(dependencies, source.SourceID)
		}
		if path.Retraction != nil {
			addBan(id, path.Retraction.Kind)
			addBan(path.Retraction.SourceID, path.Retraction.Kind)
			dependencies = append(dependencies, path.Retraction.SourceID)
		}
		p.dependencies[id] = dependencies
	}
	return p
}

func (p AccessPolicy) HasDisclosureDenials() bool { return p.disclosure }

// DependencyClosure returns canonical identities reachable through explicitly
// recorded Memory/ExperiencePath provenance, including the supplied roots. It
// neither detects copied bytes nor grants access. Callers may persist this cut
// as audit evidence, but must still Check current policy before any new use.
func (p AccessPolicy) DependencyClosure(ids []model.ObjectID) ([]model.ObjectID, error) {
	seen := make(map[model.ObjectID]bool)
	pending := append([]model.ObjectID(nil), ids...)
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id == "" || seen[id] {
			continue
		}
		if !model.ValidDigest(string(id)) {
			return nil, ErrInvalidMemory
		}
		seen[id] = true
		pending = append(pending, p.dependencies[id]...)
	}
	out := make([]model.ObjectID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Check applies today's withdrawal policy to explicitly recorded identities.
// FactualCorrection and UseBan preserve immutable audit reads but prohibit new
// reuse. PrivacyErasure and ACLRevocation also prohibit historical disclosure.
// Denials intentionally do not contain identifying content or object IDs.
func (p AccessPolicy) Check(ids []model.ObjectID, purpose AccessPurpose) error {
	if purpose != AccessAudit && purpose != AccessReuse {
		return ErrInvalidMemory
	}
	seen := make(map[model.ObjectID]bool)
	pending := append([]model.ObjectID(nil), ids...)
	useDenied := false
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id == "" || seen[id] {
			continue
		}
		if !model.ValidDigest(string(id)) {
			return ErrInvalidMemory
		}
		seen[id] = true
		if kind := p.restrictions[id]; kind != "" {
			if disclosureBan(kind) {
				return ErrHistoryAccessDenied
			}
			useDenied = true
		}
		pending = append(pending, p.dependencies[id]...)
	}
	if purpose == AccessReuse && useDenied {
		return ErrUseDenied
	}
	return nil
}
