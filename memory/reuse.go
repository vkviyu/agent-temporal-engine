package memory

import "github.com/vkviyu/agent-temporal-engine/model"

// reusableDependencies checks typed provenance recursively. The caller must
// hold s.mu. Only promoted heads can participate in new reuse. A historical
// predecessor may remain readable without being eligible for a new dispatch.
func (s *Store) reusableDependencies(root model.ObjectID, q Query) bool {
	if !validAnchor(q.At) || q.Space != q.At.Space || !validTime(q.AtTime) {
		return false
	}
	// A procedure can contain steps for different sub-goals. The caller checks
	// applicability on the record being used, while this walk checks current
	// status and time on every supporting record.
	at := Query{Space: q.Space, At: q.At, AtTime: q.AtTime}
	pending := []model.ObjectID{root}
	seen := map[model.ObjectID]bool{}
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		if _, banned := s.sourceBan[id]; banned {
			return false
		}
		if value, found := s.memories[id]; found {
			if value.Status != StatusPromoted || s.successors[id] != "" || !s.eligible(value, at) {
				return false
			}
			for _, source := range allSources(value) {
				pending = append(pending, source.SourceID)
			}
			continue
		}
		if path, found := s.paths[id]; found {
			if !s.pathReusableAt(path, at) {
				return false
			}
			for _, step := range path.Steps {
				if _, found := s.memories[step.Memory]; !found {
					return false
				}
				pending = append(pending, step.Memory, pathNodeID(step.Node))
			}
			for _, ref := range path.Omitted {
				pending = append(pending, pathNodeID(ref))
			}
			for _, parent := range path.DerivedFrom {
				if _, memoryFound := s.memories[parent]; !memoryFound {
					if _, pathFound := s.paths[parent]; !pathFound {
						return false
					}
				}
				pending = append(pending, parent)
			}
			for _, evidence := range path.Validation.Evidence {
				pending = append(pending, evidence.SourceID)
			}
			for _, counterexample := range path.Validation.Counterexamples {
				pending = append(pending, counterexample.SourceID)
			}
		}
	}
	return true
}

func (s *Store) pathReusableAt(path ExperiencePath, q Query) bool {
	at := q.At
	if path.Status != StatusPromoted || s.pathSuccessors[path.ID] != "" || path.Space != at.Space ||
		path.SourceAnchor.Fence.Store != at.Fence.Store || path.SourceAnchor.Fence.LSN > at.Fence.LSN {
		return false
	}
	for _, source := range append(append([]Source(nil), path.Validation.Evidence...), path.Validation.Counterexamples...) {
		if source.Anchor.Fence.Store != at.Fence.Store || source.Anchor.Space != path.Space || source.Anchor.Fence.LSN > at.Fence.LSN {
			return false
		}
	}
	return true
}
