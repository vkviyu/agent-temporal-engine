// Package memory implements deterministic, source-aware memory and experience
// derivation semantics. Store is an in-memory semantic index; Durable adds a
// prepared Journal projection for crash-safe replay and idempotent lifecycle
// commands. Neither is the core Engine's five-root authority.
package memory

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func validStatus(s Status) bool {
	switch s {
	case StatusCandidate, StatusValidated, StatusPromoted, StatusSuperseded, StatusRetracted, StatusRejected:
		return true
	}
	return false
}
func validKind(k Kind) bool {
	switch k {
	case KindFact, KindDecision, KindProcedure, KindFailurePattern, KindPreference:
		return true
	}
	return false
}
func validRetractionKind(k RetractionKind) bool {
	switch k {
	case RetractionFactualCorrection, RetractionUseBan, RetractionPrivacyErasure, RetractionACLRevocation:
		return true
	}
	return false
}
func cloneSource(s Source) Source { return Source{SourceID: s.SourceID, Anchor: s.Anchor.Clone()} }
func cloneMemory(m Memory) Memory {
	m.Claim = bytes.Clone(m.Claim)
	m.Sources = append([]Source(nil), m.Sources...)
	for i := range m.Sources {
		m.Sources[i] = cloneSource(m.Sources[i])
	}
	m.Evidence = append([]Evidence(nil), m.Evidence...)
	for i := range m.Evidence {
		m.Evidence[i].Source = cloneSource(m.Evidence[i].Source)
	}
	m.Applicability.Tags = append([]string(nil), m.Applicability.Tags...)
	m.Supersedes = append([]model.ObjectID(nil), m.Supersedes...)
	m.Retracts = append([]model.ObjectID(nil), m.Retracts...)
	if m.Retraction != nil {
		x := *m.Retraction
		m.Retraction = &x
	}
	if m.ValidUntil != nil {
		x := *m.ValidUntil
		m.ValidUntil = &x
	}
	if m.Validation != nil {
		x := cloneValidation(*m.Validation)
		m.Validation = &x
	}
	return m
}
func validateSource(s Source) error {
	if !model.ValidDigest(string(s.SourceID)) {
		return ErrInvalidMemory
	}
	if !validAnchor(s.Anchor) {
		return ErrInvalidMemory
	}
	return nil
}
func Validate(m Memory) error {
	if err := validateFields(m); err != nil {
		return err
	}
	raw := canonicalParts(m)
	if len(raw) > MaxRecordBytes {
		return ErrInvalidMemory
	}
	if m.ID != "" && m.ID != hashBytes(raw) {
		return ErrInvalidMemory
	}
	return nil
}
func transition(from, to Status) bool {
	switch from {
	case StatusCandidate:
		return to == StatusValidated || to == StatusRejected || to == StatusRetracted
	case StatusValidated:
		return to == StatusPromoted || to == StatusRejected || to == StatusRetracted
	case StatusPromoted:
		return to == StatusSuperseded || to == StatusRetracted
	case StatusSuperseded, StatusRejected, StatusRetracted:
		return false
	}
	return false
}
func (s *Store) putCandidate(m Memory) (Memory, error) {
	m.Status = StatusCandidate
	if m.VisibleAt.IsZero() {
		m.VisibleAt = m.RecordedAt
	}
	m.ID = IDFor(m)
	if err := Validate(m); err != nil {
		return Memory{}, err
	}
	if _, ok := s.memories[m.ID]; ok {
		return Memory{}, ErrAlreadyExists
	}
	s.memories[m.ID] = cloneMemory(m)
	s.index(m)
	return cloneMemory(m), nil
}
func (s *Store) index(m Memory) {
	for _, token := range tokenize(string(m.Claim) + " " + strings.Join(m.Applicability.Tags, " ")) {
		if s.tokens[token] == nil {
			s.tokens[token] = make(map[model.ObjectID]struct{})
		}
		s.tokens[token][m.ID] = struct{}{}
	}
}
func tokenize(text string) []string {
	parts := strings.Fields(strings.ToLower(text))
	sort.Strings(parts)
	out := parts[:0]
	for _, p := range parts {
		if len(out) == 0 || out[len(out)-1] != p {
			out = append(out, p)
		}
	}
	return out
}
func (s *Store) get(id model.ObjectID) (Memory, error) {
	m, ok := s.memories[id]
	if !ok {
		return Memory{}, ErrNotFound
	}
	return cloneMemory(m), nil
}
func (s *Store) transition(id model.ObjectID, to Status, why string) (Memory, error) {
	m, err := s.get(id)
	if err != nil {
		return Memory{}, err
	}
	if s.successors[id] != "" || !transition(m.Status, to) {
		return Memory{}, fmt.Errorf("%w: %s→%s", ErrInvalidTransition, m.Status, to)
	}
	if why == "" {
		return Memory{}, ErrInvalidTransition
	}
	m.Status = to
	m.Supersedes = append(m.Supersedes, id)
	if to == StatusRetracted {
		m.Retraction = &Retraction{Kind: RetractionFactualCorrection, Reason: why, RecordedAt: time.Time{}}
	}
	m.ID = IDFor(m) // lifecycle is an immutable derived record; ID changes
	if err := Validate(m); err != nil {
		return Memory{}, err
	}
	s.memories[m.ID] = cloneMemory(m)
	s.successors[id] = m.ID
	s.index(m)
	return cloneMemory(m), nil
}
func (s *Store) validate(id model.ObjectID, v Validation) (Memory, error) {
	m, err := s.get(id)
	if err != nil {
		return Memory{}, err
	}
	if m.Status != StatusCandidate || s.successors[id] != "" || !v.Passed || len(v.Evidence) == 0 || !validValidation(v, m.Space) {
		return Memory{}, ErrInvalidTransition
	}
	m.Status = StatusValidated
	m.Supersedes = append(m.Supersedes, id)
	v = cloneValidation(v)
	m.Validation = &v
	m.StrategyVersion = v.StrategyVersion
	m.ID = IDFor(m)
	if err := Validate(m); err != nil || m.ID == "" {
		return Memory{}, ErrInvalidMemory
	}
	s.memories[m.ID] = cloneMemory(m)
	s.successors[id] = m.ID
	s.index(m)
	return cloneMemory(m), nil
}
func (s *Store) promote(id model.ObjectID) (Memory, error) {
	m, err := s.get(id)
	if err != nil {
		return Memory{}, err
	}
	if m.Status != StatusValidated || len(m.Sources) == 0 || m.Validation == nil || !m.Validation.Passed {
		return Memory{}, ErrInvalidTransition
	}
	for _, src := range allSources(m) {
		if _, banned := s.sourceBan[src.SourceID]; banned {
			return Memory{}, ErrNotEligible
		}
	}
	return s.transition(id, StatusPromoted, "promoted")
}

// SupportingSources returns the sources and positive evidence the caller
// supplied for this assertion. Counterexamples remain provenance for audit
// and withdrawal, but do not establish support for a path step.
func (m Memory) SupportingSources() []Source {
	out := append([]Source(nil), m.Sources...)
	for _, evidence := range m.Evidence {
		if !evidence.Counter {
			out = append(out, evidence.Source)
		}
	}
	if m.Validation != nil {
		out = append(out, m.Validation.Evidence...)
	}
	for i := range out {
		out[i] = cloneSource(out[i])
	}
	return out
}

func allSources(m Memory) []Source {
	out := append([]Source(nil), m.Sources...)
	for _, e := range m.Evidence {
		out = append(out, e.Source)
	}
	if m.Validation != nil {
		out = append(out, m.Validation.Evidence...)
		out = append(out, m.Validation.Counterexamples...)
	}
	return out
}

// withdraw computes the dependency closure before installing any ban/version.
// Memory Sources and validation evidence, path members and DerivedFrom links
// all participate. Historical records remain available for exact audit reads.
func (s *Store) withdraw(seed model.ObjectID, r Retraction) ([]model.ObjectID, error) {
	if !model.ValidDigest(string(seed)) {
		return nil, ErrInvalidMemory
	}
	r.SourceID = seed
	if !validRetraction(&r) {
		return nil, ErrInvalidMemory
	}
	reverse := map[model.ObjectID][]model.ObjectID{}
	for id, m := range s.memories {
		for _, src := range allSources(m) {
			reverse[src.SourceID] = append(reverse[src.SourceID], id)
		}
	}
	for id, p := range s.paths {
		for _, step := range p.Steps {
			reverse[step.Memory] = append(reverse[step.Memory], id)
		}
		for _, parent := range p.DerivedFrom {
			reverse[parent] = append(reverse[parent], id)
		}
		for _, src := range append(append([]Source(nil), p.Validation.Evidence...), p.Validation.Counterexamples...) {
			reverse[src.SourceID] = append(reverse[src.SourceID], id)
		}
	}
	affected := map[model.ObjectID]bool{seed: true}
	queue := []model.ObjectID{seed}
	for at := 0; at < len(queue); at++ {
		for _, id := range reverse[queue[at]] {
			if !affected[id] {
				affected[id] = true
				queue = append(queue, id)
			}
		}
	}
	keys := make([]model.ObjectID, 0, len(affected))
	for id := range affected {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	changed := []model.ObjectID{}
	newMemories := map[model.ObjectID]Memory{}
	newPaths := map[model.ObjectID]ExperiencePath{}
	for _, id := range keys {
		if original, ok := s.memories[id]; ok && s.successors[id] == "" && original.Status != StatusRetracted && original.Status != StatusRejected && original.Status != StatusSuperseded {
			m := cloneMemory(original)
			m.Status = StatusRetracted
			m.Retraction = &r
			m.Supersedes = append(m.Supersedes, id)
			m.Retracts = append(m.Retracts, id)
			m.ID = IDFor(m)
			if err := Validate(m); err != nil || m.ID == "" {
				return nil, ErrInvalidMemory
			}
			newMemories[id] = m
			changed = append(changed, m.ID)
		}
		if original, ok := s.paths[id]; ok && s.pathSuccessors[id] == "" && original.Status != StatusRetracted && original.Status != StatusRejected && original.Status != StatusSuperseded {
			p := clonePath(original)
			p.Status = StatusRetracted
			p.Retraction = &r
			p.DerivedFrom = append(p.DerivedFrom, id)
			p.ID = PathIDFor(p)
			if p.ID == "" {
				return nil, ErrPathInvalid
			}
			newPaths[id] = p
		}
	}
	for id := range affected {
		s.sourceBan[id] = r
	}
	for old, m := range newMemories {
		s.memories[m.ID] = m
		s.successors[old] = m.ID
		s.index(m)
	}
	for old, p := range newPaths {
		s.paths[p.ID] = p
		s.pathSuccessors[old] = p.ID
	}
	sort.Slice(changed, func(i, j int) bool { return changed[i] < changed[j] })
	return changed, nil
}
func (s *Store) retractSource(source model.ObjectID, r Retraction) ([]model.ObjectID, error) {
	return s.withdraw(source, r)
}
func (s *Store) search(q Query, limit int) ([]Result, error) {
	if q.Space == "" || limit < 0 || limit > 1024 || !validAnchor(q.At) || q.At.Space != q.Space || !validTime(q.AtTime) {
		return nil, ErrInvalidMemory
	}
	tokens := tokenize(q.Text)
	cand := map[model.ObjectID]struct{}{}
	if len(tokens) == 0 {
		for id := range s.memories {
			cand[id] = struct{}{}
		}
	} else {
		for id := range s.tokens[tokens[0]] {
			cand[id] = struct{}{}
		}
		for _, t := range tokens[1:] {
			for id := range cand {
				if _, ok := s.tokens[t][id]; !ok {
					delete(cand, id)
				}
			}
		}
	}
	out := []Result{}
	for id := range cand {
		m := s.memories[id]
		if m.Space != q.Space || m.Status != StatusPromoted || s.successors[id] != "" || !s.eligible(m, q) {
			continue
		}
		score := uint32(0)
		for _, t := range tokens {
			if _, ok := s.tokens[t][id]; ok {
				score++
			}
		}
		out = append(out, Result{Memory: cloneMemory(m), Score: score})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Memory.ID < out[j].Memory.ID
	})
	if limit == 0 {
		limit = 100
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *Store) eligible(m Memory, q Query) bool {
	if len(m.Sources) == 0 || !validAnchor(q.At) || q.At.Space != m.Space {
		return false
	}
	for _, src := range allSources(m) {
		if src.Anchor.Space != m.Space || src.Anchor.Fence.Store != q.At.Fence.Store || src.Anchor.Fence.LSN > q.At.Fence.LSN {
			return false
		}
		if _, banned := s.sourceBan[src.SourceID]; banned {
			return false
		}
	}
	if (!m.ValidFrom.IsZero() || m.ValidUntil != nil) && q.AtTime.IsZero() {
		return false
	}
	if !q.AtTime.IsZero() {
		if q.AtTime.Before(m.VisibleAt) || q.AtTime.Before(m.ValidFrom) || (m.ValidUntil != nil && !q.AtTime.Before(*m.ValidUntil)) {
			return false
		}
	}
	return matchesApplicability(m.Applicability, q)
}

func matchesApplicability(applicability Applicability, q Query) bool {
	if q.Goal != "" && applicability.Goal != "" && q.Goal != applicability.Goal {
		return false
	}
	if q.Environment != "" && applicability.Environment != "" && q.Environment != applicability.Environment {
		return false
	}
	if q.SchemaVersion != 0 && applicability.SchemaVersion != 0 && q.SchemaVersion != applicability.SchemaVersion {
		return false
	}
	tags := make(map[string]bool, len(applicability.Tags))
	for _, tag := range applicability.Tags {
		tags[tag] = true
	}
	for _, tag := range q.Tags {
		if !tags[tag] {
			return false
		}
	}
	return true
}

func (s *Store) buildExperiencePath(space model.SpaceID, anchor model.SnapshotAnchor, steps []Step, omitted []model.NodeRef, validation Validation, applicability Applicability, derivedFrom []model.ObjectID) (ExperiencePath, error) {
	if space == "" || !validAnchor(anchor) || anchor.Space != space || len(steps) == 0 || len(steps) > MaxItems || len(omitted) > MaxItems || !validation.Passed || len(validation.Evidence) == 0 || !validValidation(validation, space) || !validApplicability(applicability) || !validIDs(derivedFrom) {
		return ExperiencePath{}, ErrPathInvalid
	}
	for _, id := range derivedFrom {
		if _, banned := s.sourceBan[id]; banned {
			return ExperiencePath{}, ErrNotEligible
		}
		if m, ok := s.memories[id]; ok {
			if m.Status != StatusPromoted || s.successors[id] != "" || !s.eligible(m, Query{Space: space, At: anchor}) {
				return ExperiencePath{}, ErrNotEligible
			}
			continue
		}
		if p, ok := s.paths[id]; ok {
			if p.Space != space || p.Status != StatusPromoted || s.pathSuccessors[id] != "" || p.SourceAnchor.Fence.Store != anchor.Fence.Store || p.SourceAnchor.Fence.LSN > anchor.Fence.LSN {
				return ExperiencePath{}, ErrNotEligible
			}
			continue
		}
		return ExperiencePath{}, ErrSourceUnavailable
	}
	for i, step := range steps {
		if step.Order != uint32(i) || step.Memory == "" || step.Node.Validate() != nil {
			return ExperiencePath{}, ErrPathInvalid
		}
		m, err := s.get(step.Memory)
		if err != nil || m.Status != StatusPromoted || s.successors[m.ID] != "" {
			return ExperiencePath{}, ErrNotEligible
		}
		if len(m.Sources) == 0 {
			return ExperiencePath{}, ErrSourceUnavailable
		}
		for _, src := range allSources(m) {
			if src.Anchor.Space != space || src.Anchor.Fence.Store != anchor.Fence.Store || src.Anchor.Fence.LSN > anchor.Fence.LSN {
				return ExperiencePath{}, ErrSourceUnavailable
			}
			if _, banned := s.sourceBan[src.SourceID]; banned {
				return ExperiencePath{}, ErrNotEligible
			}
		}
	}
	for _, src := range append(append([]Source(nil), validation.Evidence...), validation.Counterexamples...) {
		if validateSource(src) != nil || src.Anchor.Space != space || src.Anchor.Fence.Store != anchor.Fence.Store || src.Anchor.Fence.LSN > anchor.Fence.LSN {
			return ExperiencePath{}, ErrPathInvalid
		}
	}
	p := ExperiencePath{Space: space, SourceAnchor: anchor.Clone(), Steps: append([]Step(nil), steps...), Omitted: append([]model.NodeRef(nil), omitted...), Validation: cloneValidation(validation), Applicability: cloneApplicability(applicability), Status: StatusPromoted, DerivedFrom: append([]model.ObjectID(nil), derivedFrom...)}
	p.ID = PathIDFor(p)
	if p.ID == "" {
		return ExperiencePath{}, ErrPathInvalid
	}
	if _, ok := s.paths[p.ID]; ok {
		return ExperiencePath{}, ErrAlreadyExists
	}
	s.paths[p.ID] = clonePath(p)
	return clonePath(p), nil
}
func (s *Store) getPath(id model.ObjectID) (ExperiencePath, error) {
	p, ok := s.paths[id]
	if !ok {
		return ExperiencePath{}, ErrNotFound
	}
	return clonePath(p), nil
}
func (s *Store) retractPath(id model.ObjectID, r Retraction) (ExperiencePath, error) {
	if _, err := s.getPath(id); err != nil {
		return ExperiencePath{}, err
	}
	if s.pathSuccessors[id] != "" {
		return ExperiencePath{}, ErrPathInvalid
	}
	if _, err := s.withdraw(id, r); err != nil {
		return ExperiencePath{}, err
	}
	next := s.pathSuccessors[id]
	if next == "" {
		return ExperiencePath{}, ErrPathInvalid
	}
	return clonePath(s.paths[next]), nil
}

func cloneValidation(v Validation) Validation {
	v.Evidence = append([]Source(nil), v.Evidence...)
	for i := range v.Evidence {
		v.Evidence[i] = cloneSource(v.Evidence[i])
	}
	v.Counterexamples = append([]Source(nil), v.Counterexamples...)
	for i := range v.Counterexamples {
		v.Counterexamples[i] = cloneSource(v.Counterexamples[i])
	}
	return v
}
func cloneApplicability(a Applicability) Applicability {
	a.Tags = append([]string(nil), a.Tags...)
	return a
}
func clonePath(p ExperiencePath) ExperiencePath {
	p.SourceAnchor = p.SourceAnchor.Clone()
	p.Steps = append([]Step(nil), p.Steps...)
	p.Omitted = append([]model.NodeRef(nil), p.Omitted...)
	p.DerivedFrom = append([]model.ObjectID(nil), p.DerivedFrom...)
	p.Validation = cloneValidation(p.Validation)
	p.Applicability = cloneApplicability(p.Applicability)
	if p.Retraction != nil {
		r := *p.Retraction
		p.Retraction = &r
	}
	return p
}
