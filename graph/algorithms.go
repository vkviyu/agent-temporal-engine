package graph

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const MaxWalkEdges = 4 * MaxWalkNodes

func parents(r Revision, visit func(model.RevisionID)) {
	if r.StateParent != "" {
		visit(r.StateParent)
	}
	for _, p := range r.JoinParents {
		visit(p)
	}
}

// ancestorSet is an iterative, bounded DFS. Gray nodes are on the active path;
// encountering one proves a cycle rather than treating it as a visited cache hit.
func (s *Store) ancestorSet(id model.RevisionID) (map[model.RevisionID]struct{}, error) {
	type frame struct {
		id   model.RevisionID
		exit bool
	}
	stack := []frame{{id: id}}
	color := make(map[model.RevisionID]uint8)
	out := make(map[model.RevisionID]struct{})
	edges := 0
	for len(stack) > 0 {
		next := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if next.exit {
			color[next.id] = 2
			continue
		}
		if color[next.id] == 2 {
			continue
		}
		if color[next.id] == 1 {
			return nil, ErrRevisionCycle
		}
		r, ok := s.revisions[next.id]
		if !ok {
			return nil, ErrUnknownRevision
		}
		if len(out) >= MaxWalkNodes {
			return nil, ErrLimit
		}
		out[next.id] = struct{}{}
		color[next.id] = 1
		stack = append(stack, frame{id: next.id, exit: true})
		count := len(r.JoinParents)
		if r.StateParent != "" {
			count++
		}
		if count > MaxWalkEdges-edges {
			return nil, ErrLimit
		}
		edges += count
		for i := len(r.JoinParents) - 1; i >= 0; i-- {
			stack = append(stack, frame{id: r.JoinParents[i]})
		}
		if r.StateParent != "" {
			stack = append(stack, frame{id: r.StateParent})
		}
	}
	return out, nil
}
func (s *Store) mergeBaseLocked(left, right model.RevisionID) (model.RevisionID, error) {
	l, err := s.getLocked(left)
	if err != nil {
		return "", err
	}
	r, err := s.getLocked(right)
	if err != nil {
		return "", err
	}
	if l.Space != r.Space {
		return "", ErrSpaceMismatch
	}
	if left == right {
		return left, nil
	}
	a, err := s.ancestorSet(left)
	if err != nil {
		return "", err
	}
	b, err := s.ancestorSet(right)
	if err != nil {
		return "", err
	}
	common := make(map[model.RevisionID]struct{})
	for id := range a {
		if _, ok := b[id]; ok {
			common[id] = struct{}{}
		}
	}
	if len(common) == 0 {
		return "", ErrNoMergeBase
	}
	// Common ancestors form an ancestry-closed set. Its maximal elements are
	// exactly those not appearing as a direct parent of another common element.
	inferior := make(map[model.RevisionID]struct{})
	for id := range common {
		parents(s.revisions[id], func(p model.RevisionID) {
			if _, ok := common[p]; ok {
				inferior[p] = struct{}{}
			}
		})
	}
	best := make([]model.RevisionID, 0)
	for id := range common {
		if _, ok := inferior[id]; !ok {
			best = append(best, id)
		}
	}
	sort.Slice(best, func(i, j int) bool { return best[i] < best[j] })
	if len(best) != 1 {
		return "", fmt.Errorf("%w: %v", ErrAmbiguousMergeBase, best)
	}
	return best[0], nil
}
func (s *Store) MergeBase(left, right model.RevisionID) (model.RevisionID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mergeBaseLocked(left, right)
}
func (s *Store) IsAncestor(ancestor, descendant model.RevisionID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, err := s.getLocked(ancestor)
	if err != nil {
		return false, err
	}
	b, err := s.getLocked(descendant)
	if err != nil {
		return false, err
	}
	if a.Space != b.Space {
		return false, ErrSpaceMismatch
	}
	set, err := s.ancestorSet(descendant)
	if err != nil {
		return false, err
	}
	_, ok := set[ancestor]
	return ok, nil
}
func change(key []byte, before, after model.ObjectID) (Change, bool) {
	if before == after {
		return Change{}, false
	}
	kind := ChangeUpdate
	if before == "" {
		kind = ChangeAdd
	}
	if after == "" {
		kind = ChangeRemove
	}
	return Change{Key: bytes.Clone(key), Kind: kind, Before: before, After: after}, true
}
func diffEntries(a, b []Entry) []Change {
	var changes []Change
	for i, j := 0, 0; i < len(a) || j < len(b); {
		if j == len(b) || (i < len(a) && bytes.Compare(a[i].Key, b[j].Key) < 0) {
			c, _ := change(a[i].Key, a[i].Value, "")
			changes = append(changes, c)
			i++
			continue
		}
		if i == len(a) || bytes.Compare(a[i].Key, b[j].Key) > 0 {
			c, _ := change(b[j].Key, "", b[j].Value)
			changes = append(changes, c)
			j++
			continue
		}
		if c, ok := change(a[i].Key, a[i].Value, b[j].Value); ok {
			changes = append(changes, c)
		}
		i++
		j++
	}
	return changes
}
func (s *Store) Diff(from, to model.RevisionID) (Diff, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, err := s.getLocked(from)
	if err != nil {
		return Diff{}, err
	}
	b, err := s.getLocked(to)
	if err != nil {
		return Diff{}, err
	}
	if a.Space != b.Space {
		return Diff{}, ErrSpaceMismatch
	}
	return Diff{From: from, To: to, Changes: diffEntries(a.Entries, b.Entries)}, nil
}
func entriesMap(in []Entry) map[string]model.ObjectID {
	out := make(map[string]model.ObjectID, len(in))
	for _, e := range in {
		out[string(e.Key)] = e.Value
	}
	return out
}
func mapEntries(in map[string]model.ObjectID) []Entry {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Entry, len(keys))
	for i, k := range keys {
		out[i] = Entry{Key: []byte(k), Value: in[k]}
	}
	return out
}
func same(a model.ObjectID, ap bool, b model.ObjectID, bp bool) bool {
	return ap == bp && (!ap || a == b)
}
func (s *Store) planJoinLocked(left, right model.RevisionID) (JoinPlan, error) {
	base, err := s.mergeBaseLocked(left, right)
	if err != nil {
		return JoinPlan{}, err
	}
	b := s.revisions[base]
	l := s.revisions[left]
	r := s.revisions[right]
	bm, lm, rm := entriesMap(b.Entries), entriesMap(l.Entries), entriesMap(r.Entries)
	keys := make(map[string]struct{}, len(bm)+len(lm)+len(rm))
	for k := range bm {
		keys[k] = struct{}{}
	}
	for k := range lm {
		keys[k] = struct{}{}
	}
	for k := range rm {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	plan := JoinPlan{Base: base, Left: left, Right: right}
	for _, k := range sorted {
		bv, bp := bm[k]
		lv, lp := lm[k]
		rv, rp := rm[k]
		var value model.ObjectID
		present := false
		switch {
		case same(lv, lp, rv, rp):
			value, present = lv, lp
		case same(lv, lp, bv, bp):
			value, present = rv, rp
		case same(rv, rp, bv, bp):
			value, present = lv, lp
		default:
			plan.Conflicts = append(plan.Conflicts, Conflict{Key: []byte(k), Base: bv, Left: lv, Right: rv})
			continue
		}
		if present {
			if len(plan.State) >= MaxRevisionEntries {
				return JoinPlan{}, ErrLimit
			}
			plan.State = append(plan.State, Entry{Key: []byte(k), Value: value})
		}
		if c, changed := change([]byte(k), lv, value); changed {
			plan.Changes = append(plan.Changes, c)
		}
	}
	return plan, nil
}
func (s *Store) PlanJoin(left, right model.RevisionID) (JoinPlan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.planJoinLocked(left, right)
}

// Resolve records a decision for an existing conflict only. Empty value means
// deletion. New values, like all state references, require semantic validation
// by the publishing adapter; resolving a key grants no execution permissions.
func (p *JoinPlan) Resolve(key []byte, value model.ObjectID) error {
	if p == nil {
		return ErrInvalidJoinPlan
	}
	if len(key) > MaxEntryKeyBytes {
		return ErrLimit
	}
	if value != "" && !validDigest(string(value)) {
		return ErrInvalidRevision
	}
	conflict := -1
	for i, c := range p.Conflicts {
		if bytes.Equal(c.Key, key) {
			conflict = i
			break
		}
	}
	if conflict < 0 {
		return ErrJoinConflict
	}
	at := sort.Search(len(p.State), func(i int) bool { return bytes.Compare(p.State[i].Key, key) >= 0 })
	exists := at < len(p.State) && bytes.Equal(p.State[at].Key, key)
	if value != "" && !exists && len(p.State) >= MaxRevisionEntries {
		return ErrLimit
	}
	old := p.Conflicts[conflict]
	if value == "" {
		if exists {
			p.State = append(p.State[:at], p.State[at+1:]...)
		}
	} else if exists {
		p.State[at] = Entry{Key: bytes.Clone(key), Value: value}
	} else {
		p.State = append(p.State, Entry{})
		copy(p.State[at+1:], p.State[at:])
		p.State[at] = Entry{Key: bytes.Clone(key), Value: value}
	}
	if c, changed := change(key, old.Left, value); changed {
		p.Changes = append(p.Changes, c)
		sort.Slice(p.Changes, func(i, j int) bool { return bytes.Compare(p.Changes[i].Key, p.Changes[j].Key) < 0 })
	}
	p.Resolutions = append(p.Resolutions, Resolution{Key: bytes.Clone(key), Value: value})
	sort.Slice(p.Resolutions, func(i, j int) bool { return bytes.Compare(p.Resolutions[i].Key, p.Resolutions[j].Key) < 0 })
	p.Conflicts = append(p.Conflicts[:conflict], p.Conflicts[conflict+1:]...)
	return nil
}
func (p *JoinPlan) ResolveDelete(key []byte) error { return p.Resolve(key, "") }
func equalChanges(a, b []Change) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || a[i].Kind != b[i].Kind || a[i].Before != b[i].Before || a[i].After != b[i].After {
			return false
		}
	}
	return true
}

// PreviewJoin validates the complete plan and builds its immutable Revision
// without installing it. Durable callers publish this candidate before PutRevision.
func (s *Store) PreviewJoin(proposed JoinPlan, root model.ObjectID, events []model.EventID) (Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.previewJoinLocked(proposed, root, events)
}

func (s *Store) previewJoinLocked(proposed JoinPlan, root model.ObjectID, events []model.EventID) (Revision, error) {
	expected, err := s.planJoinLocked(proposed.Left, proposed.Right)
	if err != nil {
		return Revision{}, err
	}
	if expected.Base != proposed.Base {
		return Revision{}, ErrInvalidJoinPlan
	}
	if len(proposed.Resolutions) > len(expected.Conflicts) {
		return Revision{}, ErrInvalidJoinPlan
	}
	for i, resolution := range proposed.Resolutions {
		if i > 0 && bytes.Compare(proposed.Resolutions[i-1].Key, resolution.Key) >= 0 {
			return Revision{}, ErrInvalidJoinPlan
		}
		if err := expected.Resolve(resolution.Key, resolution.Value); err != nil {
			return Revision{}, fmt.Errorf("%w: %v", ErrInvalidJoinPlan, err)
		}
	}
	if len(expected.Conflicts) != 0 || len(proposed.Conflicts) != 0 {
		return Revision{}, ErrJoinConflict
	}
	if !equalEntries(expected.State, proposed.State) || !equalChanges(expected.Changes, proposed.Changes) {
		return Revision{}, ErrInvalidJoinPlan
	}
	joins := []model.RevisionID{proposed.Right}
	if proposed.Right == proposed.Left {
		joins = nil
	}
	r, err := BuildRevision(string(s.revisions[proposed.Left].Space), proposed.Left, joins, root, events, expected.State)
	if err != nil {
		return Revision{}, err
	}
	return cloneRevision(r), nil
}

func (s *Store) CommitJoin(proposed JoinPlan, root model.ObjectID, events []model.EventID) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.previewJoinLocked(proposed, root, events)
	if err != nil {
		return Revision{}, err
	}
	if err = s.putLocked(r); err != nil {
		return Revision{}, err
	}
	return r, nil
}
