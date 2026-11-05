package engine

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func graphPrunedKey(id model.BranchID) []byte {
	return append([]byte("ate/graph/pruned/"), []byte(id)...)
}

func graphValueAt(ctx context.Context, reader state.ObjectReader, roots model.RootSet, space model.SpaceID, key []byte) ([]byte, bool, error) {
	root, err := slotSpaceRoot(ctx, reader, roots.BranchRefs, space)
	if err != nil {
		return nil, false, err
	}
	id, found, err := state.Get(ctx, reader, space, root, key)
	if err != nil || !found {
		return nil, found, err
	}
	obj, err := readObject(ctx, reader, id)
	if err != nil {
		return nil, false, err
	}
	if obj.Space != space {
		return nil, false, ErrInvalidStateData
	}
	v, err := readValue(obj)
	if err != nil {
		return nil, false, err
	}
	if v.Kind != api.ValueBytes {
		return nil, false, ErrInvalidStateData
	}
	return v.Data, true, nil
}

func graphRevisionAt(ctx context.Context, reader state.ObjectReader, roots model.RootSet, space model.SpaceID, id model.RevisionID) (graph.Revision, error) {
	raw, found, err := graphValueAt(ctx, reader, roots, space, graphRevisionKey(id))
	if err != nil {
		return graph.Revision{}, err
	}
	if !found {
		return graph.Revision{}, graph.ErrUnknownRevision
	}
	r, err := graph.DecodeRevision(raw)
	if err != nil {
		return graph.Revision{}, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if r.ID != id || r.Space != space {
		return graph.Revision{}, ErrInvalidStateData
	}
	return r, nil
}

func graphBranchAt(ctx context.Context, reader state.ObjectReader, roots model.RootSet, space model.SpaceID, id model.BranchID) (graph.Branch, *graph.PruneRecord, error) {
	raw, found, err := graphValueAt(ctx, reader, roots, space, graphBranchKey(id))
	if err != nil {
		return graph.Branch{}, nil, err
	}
	if !found {
		return graph.Branch{}, nil, graph.ErrUnknownBranch
	}
	b, err := graph.DecodeBranch(raw)
	if err != nil {
		return graph.Branch{}, nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if b.ID != id {
		return graph.Branch{}, nil, ErrInvalidStateData
	}
	raw, found, err = graphValueAt(ctx, reader, roots, space, graphPrunedKey(id))
	if err != nil {
		return graph.Branch{}, nil, err
	}
	if !found {
		return b, nil, nil
	}
	p, err := graph.DecodePrune(raw)
	if err != nil {
		return graph.Branch{}, nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if p.Branch != b.ID || p.Head != b.Head || p.Version != b.Version {
		return graph.Branch{}, nil, ErrInvalidStateData
	}
	return b, &p, nil
}

// loadGraphParents builds a private, authenticated pure graph in parent-first
// order. It never reads mutable latest state and detects active-path cycles.
func loadGraphParents(ctx context.Context, reader state.ObjectReader, roots model.RootSet, space model.SpaceID, heads []model.RevisionID) (*graph.Store, map[model.RevisionID]graph.Revision, error) {
	type frame struct {
		id   model.RevisionID
		exit bool
	}
	stack := make([]frame, 0, len(heads))
	for _, id := range heads {
		stack = append(stack, frame{id: id})
	}
	colors := map[model.RevisionID]uint8{}
	records := map[model.RevisionID]graph.Revision{}
	out := graph.New()
	edges := 0
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if f.exit {
			if err := out.PutRevision(records[f.id]); err != nil {
				return nil, nil, fmt.Errorf("%w: %w", ErrInvalidStateData, err)
			}
			colors[f.id] = 2
			continue
		}
		if colors[f.id] == 2 {
			continue
		}
		if colors[f.id] == 1 {
			return nil, nil, fmt.Errorf("%w: graph parent cycle", ErrInvalidStateData)
		}
		if len(records) >= graph.MaxWalkNodes {
			return nil, nil, graph.ErrLimit
		}
		r, err := graphRevisionAt(ctx, reader, roots, space, f.id)
		if err != nil {
			return nil, nil, err
		}
		records[f.id] = r
		colors[f.id] = 1
		stack = append(stack, frame{id: f.id, exit: true})
		count := len(r.JoinParents)
		if r.StateParent != "" {
			count++
		}
		if count > graph.MaxWalkEdges-edges {
			return nil, nil, graph.ErrLimit
		}
		edges += count
		for i := len(r.JoinParents) - 1; i >= 0; i-- {
			stack = append(stack, frame{id: r.JoinParents[i]})
		}
		if r.StateParent != "" {
			stack = append(stack, frame{id: r.StateParent})
		}
	}
	return out, records, nil
}

func verifyGraphEntries(ctx context.Context, reader state.ObjectReader, r graph.Revision) error {
	entries, err := state.Entries(ctx, reader, r.Space, r.StateRoot, state.ScanLimits{})
	if err != nil {
		return err
	}
	if len(entries) != len(r.Entries) {
		return ErrInvalidStateData
	}
	for i, item := range entries {
		if !bytes.Equal(item.Key, r.Entries[i].Key) || item.Value != r.Entries[i].Value {
			return ErrInvalidStateData
		}
		obj, err := readObject(ctx, reader, item.Value)
		if err != nil {
			return err
		}
		if obj.Space != r.Space {
			return ErrInvalidStateData
		}
		if _, err = readValue(obj); err != nil {
			return err
		}
	}
	return nil
}

type graphValueWrite struct{ key, raw []byte }

// The branch StateOps and immutable new Revision bind the join output; the
// canonical command binds ExpectedDomainRoot and the complete resolution set.
// Replay recomputes both root deltas and compares every transaction record.
func (e *Engine) prepareGraphValues(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, b *objectBuilder, writes []graphValueWrite, domain *model.ObjectID) (*commitPlan, error) {
	if b == nil {
		b = newBuilder(ctx, e.index, e.options.MaxObjects)
	}
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := b.add(command, storage.KindObject)
	if err != nil {
		return nil, err
	}
	beforeRoot, err := slotSpaceRoot(ctx, e.index, e.roots.BranchRefs, req.Space)
	if err != nil {
		return nil, err
	}
	sort.Slice(writes, func(i, j int) bool { return bytes.Compare(writes[i].key, writes[j].key) < 0 })
	changes := make([]stateChange, len(writes))
	mutations := make([]state.Mutation, len(writes))
	for i, w := range writes {
		if len(w.raw) > api.MaxStateValueBytes {
			return nil, storage.ErrLimit
		}
		before, found, err := state.Get(ctx, e.index, req.Space, beforeRoot, w.key)
		if err != nil {
			return nil, err
		}
		if found {
			obj, err := readObject(ctx, e.index, before)
			if err != nil {
				return nil, err
			}
			if obj.Space != req.Space {
				return nil, ErrInvalidStateData
			}
			if _, err = readValue(obj); err != nil {
				return nil, err
			}
		}
		obj, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: w.raw})
		if err != nil {
			return nil, err
		}
		after, err := b.add(obj, storage.KindObject)
		if err != nil {
			return nil, err
		}
		changes[i] = stateChange{Key: bytes.Clone(w.key), Before: before, After: after}
		mutations[i] = state.Mutation{Key: bytes.Clone(w.key), Value: after}
	}
	delta, err := state.Apply(ctx, b, req.Space, beforeRoot, mutations)
	if err != nil {
		return nil, err
	}
	for _, obj := range delta.Objects {
		if _, err = b.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	ops, err := makeOperations(operationsRecord{Space: req.Space, BeforeRoot: beforeRoot, AfterRoot: delta.Root, Changes: changes})
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		expected, err := makeOperations(*recorded)
		if err != nil || expected.Space != ops.Space || !bytes.Equal(expected.Payload, ops.Payload) {
			return nil, ErrInvalidStateData
		}
	}
	opsID, err := b.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	roots := e.roots
	catalog, err := state.Apply(ctx, b, systemSpace, e.roots.BranchRefs, []state.Mutation{{Key: []byte(req.Space), Value: delta.Root}})
	if err != nil {
		return nil, err
	}
	for _, obj := range catalog.Objects {
		if _, err = b.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	roots.BranchRefs = catalog.Root
	if domain != nil {
		dc, err := state.Apply(ctx, b, systemSpace, e.roots.DomainStates, []state.Mutation{{Key: []byte(req.Space), Value: *domain, Delete: *domain == state.EmptyRoot}})
		if err != nil {
			return nil, err
		}
		for _, obj := range dc.Objects {
			if _, err = b.add(obj, storage.KindObject); err != nil {
				return nil, err
			}
		}
		roots.DomainStates = dc.Root
	}
	return e.finishPlan(ctx, req, digest, txID, lsn, b, roots, delta.Root, commandID, opsID)
}

func (e *Engine) prepareGraphJoin(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, c graph.JoinCommand) (*commitPlan, error) {
	reader := &cursorReader{source: e.index}
	branch, pruned, err := graphBranchAt(ctx, reader, e.roots, req.Space, c.Branch)
	if err != nil {
		return nil, err
	}
	if pruned != nil {
		return nil, graph.ErrBranchPruned
	}
	if branch.Version != c.Expected || branch.Head != c.Left {
		return nil, errCAS
	}
	if branch.Version == math.MaxUint64 {
		return nil, graph.ErrLimit
	}
	current, err := domainRoot(ctx, reader, e.roots, req.Space)
	if err != nil {
		return nil, err
	}
	if current != c.ExpectedDomainRoot {
		return nil, errCAS
	}
	g, records, err := loadGraphParents(ctx, reader, e.roots, req.Space, []model.RevisionID{c.Left, c.Right})
	if err != nil {
		return nil, err
	}
	plan, err := g.PlanJoin(c.Left, c.Right)
	if err != nil {
		return nil, err
	}
	for _, id := range []model.RevisionID{plan.Base, c.Left, c.Right} {
		if err = verifyGraphEntries(ctx, reader, records[id]); err != nil {
			return nil, err
		}
	}
	entries, err := state.Entries(ctx, reader, req.Space, current, state.ScanLimits{})
	if err != nil {
		return nil, err
	}
	allowed := map[model.ObjectID]bool{}
	for _, item := range entries {
		obj, err := readObject(ctx, reader, item.Value)
		if err != nil {
			return nil, err
		}
		if obj.Space != req.Space {
			return nil, ErrInvalidStateData
		}
		if _, err = readValue(obj); err != nil {
			return nil, err
		}
		allowed[item.Value] = true
	}
	for _, id := range []model.RevisionID{plan.Base, c.Left, c.Right} {
		for _, item := range records[id].Entries {
			allowed[item.Value] = true
		}
	}
	for _, resolution := range c.Resolutions {
		if resolution.Value != "" && !allowed[resolution.Value] {
			return nil, graph.ErrStateUnverified
		}
		if err = plan.Resolve(resolution.Key, resolution.Value); err != nil {
			return nil, err
		}
	}
	if len(plan.Conflicts) > 0 {
		return nil, graph.ErrJoinConflict
	}
	for _, id := range c.Events {
		if !e.eventKnown(id, req.Space, req.Principal) {
			return nil, graph.ErrStateUnverified
		}
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	want := map[string]model.ObjectID{}
	for _, item := range plan.State {
		want[string(item.Key)] = item.Value
	}
	var mutations []state.Mutation
	for _, item := range entries {
		value, ok := want[string(item.Key)]
		if !ok {
			mutations = append(mutations, state.Mutation{Key: bytes.Clone(item.Key), Delete: true})
		} else if value != item.Value {
			mutations = append(mutations, state.Mutation{Key: bytes.Clone(item.Key), Value: value})
		}
		delete(want, string(item.Key))
	}
	for key, value := range want {
		mutations = append(mutations, state.Mutation{Key: []byte(key), Value: value})
	}
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
	joinedRoot := current
	for start := 0; start < len(mutations); start += state.MaxMutations {
		end := start + state.MaxMutations
		if end > len(mutations) {
			end = len(mutations)
		}
		delta, err := state.Apply(ctx, b, req.Space, joinedRoot, mutations[start:end])
		if err != nil {
			return nil, err
		}
		for _, obj := range delta.Objects {
			if _, err = b.add(obj, storage.KindObject); err != nil {
				return nil, err
			}
		}
		joinedRoot = delta.Root
	}
	r, err := g.PreviewJoin(plan, joinedRoot, c.Events)
	if err != nil {
		return nil, err
	}
	if err = verifyContentClosure(ctx, b, graph.ContentReferences(r)); err != nil {
		return nil, err
	}
	branch.Head = r.ID
	branch.Version++
	rawRevision, err := graph.EncodeRevision(r)
	if err != nil {
		return nil, err
	}
	rawBranch, err := graph.EncodeBranch(branch)
	if err != nil {
		return nil, err
	}
	return e.prepareGraphValues(ctx, req, digest, txID, lsn, recorded, b, []graphValueWrite{{key: graphRevisionKey(r.ID), raw: rawRevision}, {key: graphBranchKey(branch.ID), raw: rawBranch}}, &joinedRoot)
}

func (e *Engine) prepareGraphPrune(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, c graph.PruneBranchCommand) (*commitPlan, error) {
	b, pruned, err := graphBranchAt(ctx, e.index, e.roots, req.Space, c.Branch)
	if err != nil {
		return nil, err
	}
	if pruned != nil {
		return nil, graph.ErrBranchPruned
	}
	if b.Version != c.Expected || b.Head != c.Head {
		return nil, errCAS
	}
	if b.Version == math.MaxUint64 {
		return nil, graph.ErrLimit
	}
	if _, err = e.graphRevision(ctx, req.Space, b.Head); err != nil {
		return nil, err
	}
	b.Version++
	p := graph.PruneRecord{Branch: b.ID, Head: b.Head, Version: b.Version, Reason: c.Reason}
	rawBranch, err := graph.EncodeBranch(b)
	if err != nil {
		return nil, err
	}
	rawPrune, err := graph.EncodePrune(p)
	if err != nil {
		return nil, err
	}
	return e.prepareGraphValues(ctx, req, digest, txID, lsn, recorded, nil, []graphValueWrite{{key: graphBranchKey(b.ID), raw: rawBranch}, {key: graphPrunedKey(b.ID), raw: rawPrune}}, nil)
}
