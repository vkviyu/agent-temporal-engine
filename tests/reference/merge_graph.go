package reference

import (
	"errors"
	"maps"
)

var ErrNoBase = errors.New("independent graph has no common base")
var ErrManyBases = errors.New("independent graph has incomparable common bases")

type RevisionDAG struct {
	Parents map[string][]string
	Spaces  map[string]string
}

func NewRevisionDAG() *RevisionDAG {
	return &RevisionDAG{Parents: map[string][]string{}, Spaces: map[string]string{}}
}
func (g *RevisionDAG) Add(id, space string, parents ...string) {
	g.Parents[id] = append([]string(nil), parents...)
	g.Spaces[id] = space
}

// Relation derives ancestry by monotone closure of a boolean relation. It does
// not share the production DFS, direct-parent optimization, or hash identities.
func (g *RevisionDAG) Relation() map[[2]string]bool {
	r := map[[2]string]bool{}
	for id, parents := range g.Parents {
		r[[2]string{id, id}] = true
		for _, p := range parents {
			r[[2]string{id, p}] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for a := range g.Parents {
			for b := range g.Parents {
				for c := range g.Parents {
					if r[[2]string{a, b}] && r[[2]string{b, c}] && !r[[2]string{a, c}] {
						r[[2]string{a, c}], changed = true, true
					}
				}
			}
		}
	}
	return r
}
func (g *RevisionDAG) MergeBase(left, right string) (string, error) {
	if _, ok := g.Parents[left]; !ok {
		return "", ErrRejected
	}
	if _, ok := g.Parents[right]; !ok || g.Spaces[left] != g.Spaces[right] {
		return "", ErrRejected
	}
	r := g.Relation()
	common := map[string]bool{}
	for id := range g.Parents {
		if r[[2]string{left, id}] && r[[2]string{right, id}] {
			common[id] = true
		}
	}
	if len(common) == 0 {
		return "", ErrNoBase
	}
	best := []string{}
	for id := range common {
		hasNewer := false
		for other := range common {
			hasNewer = hasNewer || other != id && r[[2]string{other, id}]
		}
		if !hasNewer {
			best = append(best, id)
		}
	}
	if len(best) != 1 {
		return "", ErrManyBases
	}
	return best[0], nil
}

type BranchProjection struct {
	Head    string
	Version uint64
	Pruned  bool
}
type BranchBook struct {
	Branches map[string]BranchProjection
	Spaces   map[string]string
}

func NewBranchBook(spaces map[string]string) *BranchBook {
	return &BranchBook{Branches: map[string]BranchProjection{}, Spaces: maps.Clone(spaces)}
}
func (b *BranchBook) Fork(name, source string) error {
	if _, ok := b.Branches[name]; ok || b.Spaces[source] == "" {
		return ErrRejected
	}
	b.Branches[name] = BranchProjection{Head: source}
	return nil
}
func (b *BranchBook) Advance(name string, expected uint64, head string) error {
	old, ok := b.Branches[name]
	if !ok || old.Pruned || old.Version != expected || b.Spaces[head] == "" || b.Spaces[head] != b.Spaces[old.Head] {
		return ErrRejected
	}
	old.Head, old.Version = head, old.Version+1
	b.Branches[name] = old
	return nil
}
func (b *BranchBook) Prune(name string, expected uint64, head string) error {
	old, ok := b.Branches[name]
	if !ok || old.Pruned || old.Version != expected || old.Head != head {
		return ErrRejected
	}
	old.Pruned = true
	old.Version++
	b.Branches[name] = old
	return nil
}
