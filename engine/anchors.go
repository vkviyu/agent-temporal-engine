package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// rootsAtFence authenticates the whole cut, including genesis. The caller
// holds e.mu or owns an isolated candidate; a caller's invalid anchor never
// turns into a storage corruption or a lookup of ObjectID("").
func (e *Engine) rootsAtFence(ctx context.Context, fence model.CommitFence, publishedOnly bool) (model.RootSet, model.ObjectID, error) {
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
