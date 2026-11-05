package reference

import "maps"

type MergeConflict struct{ Base, Left, Right string }
type MergePlan struct {
	State     map[string]string
	Conflicts map[string]MergeConflict
	Decisions map[string]string
}

// Merge treats each key as a three-valued observation: absent, unchanged, or
// changed from the base. Concurrent unequal changes require an explicit choice.
func Merge(base, left, right map[string]string) MergePlan {
	keys := map[string]bool{}
	for _, state := range []map[string]string{base, left, right} {
		for k := range state {
			keys[k] = true
		}
	}
	p := MergePlan{State: map[string]string{}, Conflicts: map[string]MergeConflict{}, Decisions: map[string]string{}}
	for key := range keys {
		b, l, r := base[key], left[key], right[key]
		candidates := map[string]bool{}
		if l != b {
			candidates[l] = true
		}
		if r != b {
			candidates[r] = true
		}
		value := b
		switch len(candidates) {
		case 0:
		case 1:
			for v := range candidates {
				value = v
			}
		default:
			p.Conflicts[key] = MergeConflict{b, l, r}
			continue
		}
		if value != "" {
			p.State[key] = value
		}
	}
	return p
}
func (p *MergePlan) Resolve(key, value string) error {
	if _, exists := p.Conflicts[key]; !exists {
		return ErrRejected
	}
	if value == "" {
		delete(p.State, key)
	} else {
		p.State[key] = value
	}
	delete(p.Conflicts, key)
	p.Decisions[key] = value
	return nil
}
func (p MergePlan) Clone() MergePlan {
	return MergePlan{maps.Clone(p.State), maps.Clone(p.Conflicts), maps.Clone(p.Decisions)}
}

// ChangesFrom is a set difference between the chosen state and the left input.
// Unresolved keys have no decision yet and cannot be presented as deletions.
func (p MergePlan) ChangesFrom(left map[string]string) map[string]MergeConflict {
	out := map[string]MergeConflict{}
	keys := map[string]bool{}
	for k := range left {
		keys[k] = true
	}
	for k := range p.State {
		keys[k] = true
	}
	for k := range keys {
		if _, pending := p.Conflicts[k]; pending {
			continue
		}
		if left[k] != p.State[k] {
			out[k] = MergeConflict{Left: left[k], Right: p.State[k]}
		}
	}
	return out
}
