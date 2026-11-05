package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func graphRevisionKey(id model.RevisionID) []byte {
	return append([]byte("ate/graph/revision/"), []byte(id)...)
}
func graphBranchKey(id model.BranchID) []byte {
	return append([]byte("ate/graph/branch/"), []byte(id)...)
}

// ApplyGraph submits the canonical intent, not a snapshot compiled against
// latest state. The common coordinator looks up receipts before checking CAS.
func (e *Engine) ApplyGraph(ctx context.Context, req api.ApplyGraphRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := graph.EncodeCommand(req.Key.Space, req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyGraph, Body: body, ExpectedFence: req.ExpectedFence})
}

func (e *Engine) graphRecord(ctx context.Context, space model.SpaceID, key []byte) ([]byte, bool, error) {
	root, err := slotSpaceRoot(ctx, e.index, e.roots.BranchRefs, space)
	if err != nil {
		return nil, false, err
	}
	id, found, err := state.Get(ctx, e.index, space, root, key)
	if err != nil || !found {
		return nil, found, err
	}
	obj, err := readObject(ctx, e.index, id)
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

func (e *Engine) graphRevision(ctx context.Context, space model.SpaceID, id model.RevisionID) (graph.Revision, error) {
	raw, found, err := e.graphRecord(ctx, space, graphRevisionKey(id))
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

// A graph revision may point only to a domain state root that occurred in this
// authenticated prefix. Arbitrary radix-looking content, knowledge roots and
// roots from another Space are not domain snapshot authority.
func (e *Engine) graphDomainRootKnown(ctx context.Context, space model.SpaceID, want model.ObjectID) (bool, error) {
	if want == state.EmptyRoot {
		return true, nil
	}
	current, err := domainRoot(ctx, e.index, e.roots, space)
	if err != nil {
		return false, err
	}
	if current == want {
		return true, nil
	}
	proofs := make(map[uint64]commitEnvelope, len(e.history)+len(e.baseHistory))
	for lsn, p := range e.baseHistory {
		proofs[lsn] = p
	}
	for lsn, p := range e.history {
		proofs[lsn] = p
	}
	lsns := make([]uint64, 0, len(proofs))
	for lsn := range proofs {
		lsns = append(lsns, lsn)
	}
	sort.Slice(lsns, func(i, j int) bool { return lsns[i] < lsns[j] })
	for _, lsn := range lsns {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		p := proofs[lsn]
		if p.rootSet == "" {
			continue
		}
		obj, err := readObject(ctx, e.index, p.rootSet)
		if err != nil {
			return false, err
		}
		roots, err := readRootSet(obj)
		if err != nil {
			return false, err
		}
		r, err := domainRoot(ctx, e.index, roots, space)
		if err != nil {
			return false, err
		}
		if r == want {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) verifyGraphState(ctx context.Context, r graph.Revision) error {
	known, err := e.graphDomainRootKnown(ctx, r.Space, r.StateRoot)
	if err != nil {
		return err
	}
	if !known {
		return graph.ErrStateUnverified
	}
	entries, err := state.Entries(ctx, e.index, r.Space, r.StateRoot, state.ScanLimits{})
	if err != nil {
		return err
	}
	if len(entries) != len(r.Entries) {
		return graph.ErrStateMismatch
	}
	for i, item := range entries {
		if !bytes.Equal(item.Key, r.Entries[i].Key) || item.Value != r.Entries[i].Value {
			return graph.ErrStateMismatch
		}
		obj, err := readObject(ctx, e.index, item.Value)
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

func (e *Engine) prepareGraph(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	space, c, err := graph.DecodeCommand(req.Body)
	if err != nil {
		return nil, err
	}
	if space != req.Space {
		return nil, graph.ErrSpaceMismatch
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	var key, raw []byte
	switch {
	case c.AddRevision != nil:
		x := c.AddRevision
		r, err := graph.BuildRevision(string(space), x.Parent, x.Joins, x.Root, x.Events, x.Entries)
		if err != nil {
			return nil, err
		}
		if r.StateParent != "" {
			if _, err = e.graphRevision(ctx, space, r.StateParent); err != nil {
				return nil, err
			}
		}
		for _, parent := range r.JoinParents {
			if _, err = e.graphRevision(ctx, space, parent); err != nil {
				return nil, err
			}
		}
		// Event IDs are actual authenticated occurrence-object addresses;
		// a digest-shaped claim alone cannot become revision evidence.
		for _, id := range r.Events {
			if !e.options.EventProfile || !e.eventKnown(id, req.Space, req.Principal) {
				return nil, graph.ErrStateUnverified
			}
		}
		if err = e.verifyGraphState(ctx, r); err != nil {
			return nil, err
		}
		if err = verifyContentClosure(ctx, e.index, graph.ContentReferences(r)); err != nil {
			return nil, err
		}
		key = graphRevisionKey(r.ID)
		raw, err = graph.EncodeRevision(r)
		if err != nil {
			return nil, err
		}
	case c.Fork != nil:
		x := c.Fork
		if _, err = e.graphRevision(ctx, space, x.Source); err != nil {
			return nil, err
		}
		_, found, err := e.graphRecord(ctx, space, graphBranchKey(x.Branch))
		if err != nil {
			return nil, err
		}
		if found {
			return nil, errCAS
		}
		key = graphBranchKey(x.Branch)
		raw, err = graph.EncodeBranch(graph.Branch{ID: x.Branch, Head: x.Source})
		if err != nil {
			return nil, err
		}
	case c.Advance != nil:
		x := c.Advance
		_, pruned, err := graphBranchAt(ctx, e.index, e.roots, space, x.Branch)
		if err != nil {
			return nil, err
		}
		if pruned != nil {
			return nil, graph.ErrBranchPruned
		}
		before, found, err := e.graphRecord(ctx, space, graphBranchKey(x.Branch))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, graph.ErrUnknownBranch
		}
		b, err := graph.DecodeBranch(before)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
		}
		if b.ID != x.Branch {
			return nil, ErrInvalidStateData
		}
		if b.Version != x.Expected {
			return nil, errCAS
		}
		if b.Version == math.MaxUint64 {
			return nil, graph.ErrLimit
		}
		if _, err = e.graphRevision(ctx, space, b.Head); err != nil {
			if errors.Is(err, graph.ErrUnknownRevision) {
				return nil, ErrInvalidStateData
			}
			return nil, err
		}
		if _, err = e.graphRevision(ctx, space, x.Head); err != nil {
			return nil, err
		}
		b.Head = x.Head
		b.Version++
		key = graphBranchKey(b.ID)
		raw, err = graph.EncodeBranch(b)
		if err != nil {
			return nil, err
		}
	case c.Join != nil:
		return e.prepareGraphJoin(ctx, req, digest, txID, lsn, recorded, *c.Join)
	case c.Prune != nil:
		return e.prepareGraphPrune(ctx, req, digest, txID, lsn, recorded, *c.Prune)
	default:
		return nil, graph.ErrCommandShape
	}
	return e.prepareTypedValue(ctx, req, digest, txID, lsn, recorded, key, raw)
}

func graphCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrInvalidStateData) || errors.Is(err, ErrObjectIO) {
		return false
	}
	return errors.Is(err, graph.ErrCommandShape) || errors.Is(err, graph.ErrUnknownBranch) || errors.Is(err, graph.ErrUnknownRevision) || errors.Is(err, graph.ErrInvalidRevision) || errors.Is(err, graph.ErrSpaceMismatch) || errors.Is(err, graph.ErrRevisionCycle) || errors.Is(err, graph.ErrStateMismatch) || errors.Is(err, graph.ErrStateUnverified) || errors.Is(err, graph.ErrUnsupportedSchema) || errors.Is(err, graph.ErrBranchPruned) || errors.Is(err, graph.ErrJoinConflict) || errors.Is(err, graph.ErrInvalidJoinPlan) || errors.Is(err, graph.ErrNoMergeBase) || errors.Is(err, graph.ErrAmbiguousMergeBase)
}
