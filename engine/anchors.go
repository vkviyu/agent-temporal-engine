package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// rootsAtFence authenticates the whole cut, including genesis. The caller
// holds e.mu or owns an isolated candidate; a caller's invalid anchor never
// turns into a storage corruption or a lookup of ObjectID("").
func (e *Engine) rootsAtFence(ctx context.Context, fence model.CommitFence, publishedOnly bool) (model.RootSet, model.ObjectID, error) {
	if cut := e.readCut(ctx); cut != nil {
		if fence.Validate() != nil || fence.Store != cut.publication.Store || fence.LSN > cut.publication.Position.LSN {
			return model.RootSet{}, "", model.ErrAnchorMismatch
		}
		if err := e.mu.LockContext(ctx); err != nil {
			return model.RootSet{}, "", err
		}
		proof, found := e.history[fence.LSN]
		e.mu.Unlock()
		if !found || proof.fence != fence {
			return model.RootSet{}, "", model.ErrAnchorMismatch
		}
		if proof.rootSet == "" {
			return model.RootSet{}, "", nil
		}
		object, err := readObject(ctx, e.reader(ctx), proof.rootSet)
		if err != nil {
			return model.RootSet{}, "", err
		}
		roots, err := readRootSet(object)
		return roots, proof.rootSet, err
	}
	if fence.Validate() != nil || fence.Store != e.publication.Store {
		return model.RootSet{}, "", model.ErrAnchorMismatch
	}
	history := e.history
	if publishedOnly && e.baseHistory != nil {
		history = e.baseHistory
	}
	proof, ok := history[fence.LSN]
	if !ok && !publishedOnly {
		proof, ok = e.baseHistory[fence.LSN]
	}
	if !ok || proof.fence != fence {
		return model.RootSet{}, "", model.ErrAnchorMismatch
	}
	if proof.rootSet == "" {
		return model.RootSet{}, "", nil
	}
	obj, err := readObject(ctx, e.index, proof.rootSet)
	if err != nil {
		return model.RootSet{}, "", err
	}
	roots, err := readRootSet(obj)
	return roots, proof.rootSet, err
}
