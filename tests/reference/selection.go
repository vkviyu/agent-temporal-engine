package reference

import (
	"math/big"
	"slices"
	"sort"
)

// Selection is a small finite-set specification of the recorded V1 policy.
// Closure uses a transitive relation matrix, independently of the production
// work queue. Ratios use arbitrary-precision rationals rather than products.
type SelectionBundle struct {
	ID, Conclusion     string
	Evidence, Counters []string
	Benefit            uint64
}
type SelectionProposal struct {
	Costs     map[string]uint64
	Mandatory []string
	Bundles   []SelectionBundle
	Limit     uint64
}
type SelectionPlan struct {
	Mandatory, Selected, Bundles []string
	Omitted                      map[string]string
	Used                         uint64
}

func Select(p SelectionProposal) (SelectionPlan, error) {
	ids := make([]string, 0, len(p.Costs))
	for id := range p.Costs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if len(ids) > 63 {
		return SelectionPlan{}, ErrRejected
	}
	indexes := map[string]int{}
	for i, id := range ids {
		indexes[id] = i
	}
	reach := make([]uint64, len(ids))
	for i := range reach {
		reach[i] = 1 << i
	}
	for _, b := range p.Bundles {
		i, ok := indexes[b.Conclusion]
		if !ok {
			return SelectionPlan{}, ErrRejected
		}
		for _, id := range append(slices.Clone(b.Evidence), b.Counters...) {
			j, ok := indexes[id]
			if !ok {
				return SelectionPlan{}, ErrRejected
			}
			reach[i] |= 1 << j
		}
	}
	for k := range ids {
		for i := range ids {
			if reach[i]&(1<<k) != 0 {
				reach[i] |= reach[k]
			}
		}
	}
	members := func(bits uint64) []string {
		out := []string{}
		for i, id := range ids {
			if bits&(1<<i) != 0 {
				out = append(out, id)
			}
		}
		return out
	}
	cost := func(bits uint64) uint64 {
		var n uint64
		for i, id := range ids {
			if bits&(1<<i) != 0 {
				n += p.Costs[id]
			}
		}
		return n
	}
	var chosen uint64
	for _, id := range p.Mandatory {
		i, ok := indexes[id]
		if !ok {
			return SelectionPlan{}, ErrRejected
		}
		chosen |= reach[i]
	}
	out := SelectionPlan{Mandatory: members(chosen), Selected: members(chosen), Bundles: []string{}, Omitted: map[string]string{}, Used: cost(chosen)}
	if out.Used > p.Limit {
		return SelectionPlan{}, ErrRejected
	}
	done := map[string]bool{}
	for {
		type choice struct {
			bundle SelectionBundle
			added  uint64
			tokens uint64
			ratio  *big.Rat
		}
		options := []choice{}
		for _, b := range p.Bundles {
			if done[b.ID] || b.Benefit == 0 {
				continue
			}
			added := reach[indexes[b.Conclusion]] &^ chosen
			tokens := cost(added)
			if tokens > p.Limit-out.Used {
				continue
			}
			ratio := new(big.Rat)
			if tokens > 0 {
				ratio.SetFrac(new(big.Int).SetUint64(b.Benefit), new(big.Int).SetUint64(tokens))
			}
			options = append(options, choice{b, added, tokens, ratio})
		}
		if len(options) == 0 {
			break
		}
		sort.Slice(options, func(i, j int) bool {
			a, b := options[i], options[j]
			if (a.tokens == 0) != (b.tokens == 0) {
				return a.tokens == 0
			}
			if a.tokens != 0 && a.ratio.Cmp(b.ratio) != 0 {
				return a.ratio.Cmp(b.ratio) > 0
			}
			return a.bundle.ID < b.bundle.ID
		})
		next := options[0]
		done[next.bundle.ID] = true
		chosen |= next.added
		out.Used += next.tokens
		out.Selected = append(out.Selected, members(next.added)...)
		out.Bundles = append(out.Bundles, next.bundle.ID)
	}
	proposed, positive := uint64(0), uint64(0)
	for _, b := range p.Bundles {
		bits := reach[indexes[b.Conclusion]]
		proposed |= bits
		if b.Benefit > 0 {
			positive |= bits
		}
	}
	for i, id := range ids {
		bit := uint64(1) << i
		if chosen&bit != 0 {
			continue
		}
		reason := "capacity"
		if proposed&bit == 0 {
			reason = "not_proposed"
		} else if positive&bit == 0 {
			reason = "zero_benefit"
		}
		out.Omitted[id] = reason
	}
	return out, nil
}
