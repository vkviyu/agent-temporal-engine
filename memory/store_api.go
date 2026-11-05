package memory

import "github.com/vkviyu/agent-temporal-engine/model"

func (s *Store) PutCandidate(m Memory) (Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putCandidate(m)
}
func (s *Store) Get(id model.ObjectID) (Memory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.get(id)
}

// ReusableMemory returns the current promoted, non-superseded version only
// when its source cut and source bans are still valid at the requested anchor.
// Get intentionally remains an immutable audit read and may return retracted
// historical versions.
func (s *Store) ReusableMemory(id model.ObjectID, at model.SnapshotAnchor) (Memory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, err := s.get(id)
	if err != nil {
		return Memory{}, err
	}
	if m.Status != StatusPromoted || s.successors[id] != "" || !validAnchor(at) || at.Space != m.Space || !s.eligible(m, Query{Space: m.Space, At: at}) {
		return Memory{}, ErrNotEligible
	}
	return cloneMemory(m), nil
}
func (s *Store) Transition(id model.ObjectID, to Status, why string) (Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if to == StatusValidated || to == StatusPromoted {
		return Memory{}, ErrInvalidTransition
	}
	return s.transition(id, to, why)
}
func (s *Store) Validate(id model.ObjectID, v Validation) (Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.validate(id, v)
}
func (s *Store) Promote(id model.ObjectID) (Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.promote(id)
}
func (s *Store) RetractSource(id model.ObjectID, r Retraction) ([]model.ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retractSource(id, r)
}
func (s *Store) Search(q Query, limit int) ([]Result, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.search(q, limit)
}
func (s *Store) BuildExperiencePath(space model.SpaceID, a model.SnapshotAnchor, steps []Step, omitted []model.NodeRef, v Validation, app Applicability, from []model.ObjectID) (ExperiencePath, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildExperiencePath(space, a, steps, omitted, v, app, from)
}
func (s *Store) GetPath(id model.ObjectID) (ExperiencePath, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getPath(id)
}
func (s *Store) RetractPath(id model.ObjectID, r Retraction) (ExperiencePath, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retractPath(id, r)
}

// ReusablePath checks current source bans and derived-version status. GetPath
// remains an immutable audit read and does not silently rewrite historical IDs.
func (s *Store) ReusablePath(id model.ObjectID, at model.SnapshotAnchor) (ExperiencePath, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, err := s.getPath(id)
	if err != nil {
		return ExperiencePath{}, err
	}
	if p.Status != StatusPromoted || s.pathSuccessors[id] != "" || !validAnchor(at) || at.Space != p.Space || at.Fence.Store != p.SourceAnchor.Fence.Store || at.Fence.LSN < p.SourceAnchor.Fence.LSN {
		return ExperiencePath{}, ErrNotEligible
	}
	for _, step := range p.Steps {
		m, ok := s.memories[step.Memory]
		if !ok || s.successors[step.Memory] != "" || !s.eligible(m, Query{Space: p.Space, At: at}) {
			return ExperiencePath{}, ErrNotEligible
		}
	}
	for _, src := range append(append([]Source(nil), p.Validation.Evidence...), p.Validation.Counterexamples...) {
		if src.Anchor.Fence.Store != at.Fence.Store || src.Anchor.Space != p.Space || src.Anchor.Fence.LSN > at.Fence.LSN {
			return ExperiencePath{}, ErrNotEligible
		}
		if _, banned := s.sourceBan[src.SourceID]; banned {
			return ExperiencePath{}, ErrNotEligible
		}
	}
	return p, nil
}
