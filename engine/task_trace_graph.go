package engine

import (
	"context"
	"math/bits"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (r taskTraceRead) overview(ctx context.Context, ids []model.EventID, milestones map[model.EventID]api.TraceMilestone) ([]api.TaskTraceEdge, []model.EventID, error) {
	// Condense only recorded dependencies, stopping at the next selected
	// predecessor on each traversal path. Discover shared ancestry once, then
	// propagate page membership from children to parents in topological order.
	if len(ids) > maxTaskTracePage {
		return nil, nil, storage.ErrLimit
	}
	var edges []api.TaskTraceEdge
	relevant := make(map[model.EventID]bool, len(ids))
	type ancestor struct {
		id       model.EventID
		parents  []int
		incoming int
		pages    [2]uint64 // maxTaskTracePage is 128.
		terminal bool
		boundary bool
	}
	var nodes []ancestor
	positions := map[model.EventID]int{}
	links := 0
	add := func(id model.EventID) (int, error) {
		links++
		if links > maxDerivationEdges {
			return 0, storage.ErrLimit
		}
		if index, ok := positions[id]; ok {
			return index, nil
		}
		index := len(nodes)
		positions[id] = index
		nodes = append(nodes, ancestor{id: id})
		return index, nil
	}
	for page, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		relevant[id] = true
		event, err := r.loadEvent(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		for _, parent := range event.CausalParents {
			index, err := add(parent)
			if err != nil {
				return nil, nil, err
			}
			nodes[index].pages[page/64] |= uint64(1) << (page % 64)
		}
	}
	for i := 0; i < len(nodes); i++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		id := nodes[i].id
		relevant[id] = true
		_, selected := milestones[id]
		if !selected {
			meta, found, err := r.metadata(ctx, id)
			if err != nil {
				return nil, nil, err
			}
			selected = found && isTraceMilestoneOperation(meta.operation)
		}
		if selected {
			if _, err := r.loadEvent(ctx, id); err != nil {
				return nil, nil, err
			}
			nodes[i].terminal = true
			continue
		}
		inside, err := r.inScope(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		if !inside {
			nodes[i].terminal, nodes[i].boundary = true, true
			continue
		}
		event, err := r.loadEvent(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		for _, parent := range event.CausalParents {
			index, err := add(parent)
			if err != nil {
				return nil, nil, err
			}
			nodes[i].parents = append(nodes[i].parents, index)
			nodes[index].incoming++
		}
	}
	var queue []int
	for i := range nodes {
		if nodes[i].incoming == 0 {
			queue = append(queue, i)
		}
	}
	for cursor := 0; cursor < len(queue); cursor++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		node := nodes[queue[cursor]]
		if node.terminal {
			for word, mask := range node.pages {
				for mask != 0 {
					page := word*64 + bits.TrailingZeros64(mask)
					edges = append(edges, api.TaskTraceEdge{From: node.id, To: ids[page], Relation: "causal", Boundary: node.boundary})
					mask &= mask - 1
					if len(edges) > maxTaskTraceEdges {
						return nil, nil, storage.ErrLimit
					}
				}
			}
		}
		for _, parent := range node.parents {
			for word, mask := range node.pages {
				nodes[parent].pages[word] |= mask
			}
			nodes[parent].incoming--
			if nodes[parent].incoming == 0 {
				queue = append(queue, parent)
			}
		}
	}
	if len(queue) != len(nodes) {
		return nil, nil, ErrStoredObject
	}
	page := make(map[model.EventID]bool, len(ids))
	for _, id := range ids {
		page[id] = true
	}
	for _, assertion := range r.edit.Assertions {
		if page[assertion.To] {
			edges = append(edges, api.TaskTraceEdge{From: assertion.From, To: assertion.To, Relation: assertion.Relation, Reason: assertion.Reason, Evidence: assertion.Evidence})
			relevant[assertion.From] = true
		}
	}
	if len(edges) > maxTaskTraceEdges {
		return nil, nil, storage.ErrLimit
	}
	sort.Slice(edges, func(i, j int) bool {
		a, b := edges[i], edges[j]
		if a.To != b.To {
			return a.To < b.To
		}
		if a.From != b.From {
			return a.From < b.From
		}
		return a.Relation < b.Relation
	})
	deps := make([]model.EventID, 0, len(relevant))
	for id := range relevant {
		deps = append(deps, id)
	}
	return edges, deps, nil
}

// between returns the intersection of From's descendants and To's ancestors
// in publication order. An intervening unrelated event never enters the page.
func (r taskTraceRead) between(ctx context.Context, from, to model.EventID) ([]model.EventID, error) {
	insideTo, err := r.inScope(ctx, to)
	if err != nil {
		return nil, err
	}
	if !insideTo {
		return nil, ErrNodeNotFound
	}
	descendants := map[model.EventID]bool{from: true}
	queue := []model.EventID{from}
	visits := 0
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		children, err := r.children(ctx, queue[i])
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			inside, err := r.inScope(ctx, child.id)
			if err != nil {
				return nil, err
			}
			if !inside || descendants[child.id] {
				continue
			}
			visits++
			if visits > maxDerivationEdges {
				return nil, storage.ErrLimit
			}
			// Descendant reachability uses the installed child index only.
			// Side branches need no event body unless ancestor traversal
			// later reaches them from the requested destination.
			descendants[child.id] = true
			queue = append(queue, child.id)
		}
	}
	if !descendants[to] {
		return nil, ErrNodeNotFound
	}
	ancestors := map[model.EventID]bool{to: true}
	queue = []model.EventID{to}
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := queue[i]
		event, err := r.loadEvent(ctx, id)
		if err != nil {
			return nil, err
		}
		for _, parent := range event.CausalParents {
			if parent == from {
				ancestors[parent] = true
				continue
			}
			inside, err := r.inScope(ctx, parent)
			if err != nil {
				return nil, err
			}
			if inside && !ancestors[parent] {
				visits++
				if visits > maxDerivationEdges {
					return nil, storage.ErrLimit
				}
				ancestors[parent] = true
				queue = append(queue, parent)
			}
		}
	}
	var ids []model.EventID
	insideFrom, err := r.inScope(ctx, from)
	if err != nil {
		return nil, err
	}
	if !insideFrom && ancestors[from] {
		ids = append(ids, from)
	}
	for id := range descendants {
		if ancestors[id] && (id != from || insideFrom) {
			ids = append(ids, id)
		}
	}
	positions := make(map[model.EventID]tracePosition, len(ids))
	foundPositions := make(map[model.EventID]bool, len(ids))
	for _, id := range ids {
		// Materialize every in-scope result for node projection and disclosure,
		// including From, where the backward walk deliberately stops.
		// An external From is loaded by the caller as a boundary event.
		if id != from || insideFrom {
			if _, err := r.loadEvent(ctx, id); err != nil {
				return nil, err
			}
		}
		meta, found, err := r.metadata(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			positions[id] = traceItemPosition(meta)
			foundPositions[id] = true
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		aFound := foundPositions[ids[i]]
		bFound := foundPositions[ids[j]]
		if !aFound || !bFound {
			return ids[i] < ids[j]
		}
		return compareTracePosition(positions[ids[i]], positions[ids[j]]) < 0
	})
	return ids, nil
}
