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

// ReusableMemory checks the assertion and its direct source eligibility at
// the supplied anchor. ReusableMemoryFor additionally traverses typed
// dependencies and accepts an explicit task scope and evaluation time.
// Get remains an immutable audit read.
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
	return m, nil
}

// ReusableMemoryFor also applies the caller's explicit applicability and time
// selectors. Its Text field is relevant to Search only.
func (s *Store) ReusableMemoryFor(id model.ObjectID, q Query) (Memory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, err := s.get(id)
	if err != nil {
		return Memory{}, err
	}
	if !matchesApplicability(m.Applicability, q) || !s.reusableDependencies(id, q) {
		return Memory{}, ErrNotEligible
	}
	return m, nil
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

// ReusablePath checks the path and its direct members at the supplied anchor.
// ReusablePathFor additionally traverses typed derivations and accepts an
// explicit task scope and evaluation time. GetPath remains an audit read.
func (s *Store) ReusablePath(id model.ObjectID, at model.SnapshotAnchor) (ExperiencePath, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, err := s.getPath(id)
	if err != nil {
		return ExperiencePath{}, err
	}
	if !validAnchor(at) || !s.pathReusableAt(p, Query{Space: at.Space, At: at}) {
		return ExperiencePath{}, ErrNotEligible
	}
	for _, step := range p.Steps {
		m, ok := s.memories[step.Memory]
		if !ok || s.successors[step.Memory] != "" || !s.eligible(m, Query{Space: p.Space, At: at}) {
			return ExperiencePath{}, ErrNotEligible
		}
	}
	for _, source := range append(append([]Source(nil), p.Validation.Evidence...), p.Validation.Counterexamples...) {
		if _, banned := s.sourceBan[source.SourceID]; banned {
			return ExperiencePath{}, ErrNotEligible
		}
	}
	return p, nil
}

// ReusablePathFor applies the caller's explicit scope to the path as a whole
// and the evaluation time to its members and other reusable dependencies.
func (s *Store) ReusablePathFor(id model.ObjectID, q Query) (ExperiencePath, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, err := s.getPath(id)
	if err != nil {
		return ExperiencePath{}, err
	}
	if !matchesApplicability(p.Applicability, q) || !s.reusableDependencies(id, q) {
		return ExperiencePath{}, ErrNotEligible
	}
	return p, nil
}
