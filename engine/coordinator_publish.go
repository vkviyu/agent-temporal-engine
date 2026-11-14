package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) publishBatch(plan batchPlan) (storage.PublishedBatch, bool) {
	prepared := make([]*storage.PreparedTransaction, len(plan.planned))
	for i, work := range plan.planned {
		prepared[i] = work.prepared
	}

	e.mu.Lock()
	err := e.workerReady()
	if err == nil {
		// A durable CURRENT may change before the matching in-memory roots can
		// be installed. Every output gate must fail closed through this window.
		e.publicationPending.Store(true)
	}
	e.mu.Unlock()
	if err != nil {
		e.stopAdmission(err)
		for _, work := range plan.planned {
			e.complete(work.flight, api.CommitResult{Code: api.CodeStoreFenced, RequestID: work.flight.request.RequestID}, err)
		}
		e.rejectDeferred(plan.deferred, err)
		return storage.PublishedBatch{}, false
	}
	// Caller wait contexts are deliberately absent from planning and P1.
	published, err := e.journal.PublishPreparedBatch(context.Background(), prepared)
	if err != nil {
		if errors.Is(err, storage.ErrCommitUnknown) {
			e.mu.Lock()
			e.fenced = err
			e.publicationPending.Store(false)
			e.mu.Unlock()
			e.stopAdmission(fmt.Errorf("%w: %w", storage.ErrStoreFenced, err))
			for _, work := range plan.planned {
				e.complete(work.flight, api.CommitResult{Code: api.CodeCommitUnknown, RequestID: work.flight.request.RequestID, TxID: work.plan.transaction.TxID, RequestDigest: work.flight.digest}, err)
			}
			e.rejectDeferred(plan.deferred, fmt.Errorf("%w: %w", storage.ErrStoreFenced, err))
			return storage.PublishedBatch{}, false
		}
		if errors.Is(err, storage.ErrStoreFenced) {
			e.mu.Lock()
			e.fenced = err
			e.publicationPending.Store(false)
			e.mu.Unlock()
			e.stopAdmission(err)
		} else if !isLimit(err) {
			// A prepared chain that the sole publisher cannot accept indicates
			// an internal state mismatch, not a reason to spin and retry it.
			err = e.fenceWriter(err)
		} else {
			e.publicationPending.Store(false)
		}
		for _, work := range plan.planned {
			code := api.CodeNotSubmitted
			if isLimit(err) {
				code = api.CodeResourceLimit
			}
			if errors.Is(err, storage.ErrStoreFenced) {
				code = api.CodeStoreFenced
			}
			e.complete(work.flight, api.CommitResult{Code: code, RequestID: work.flight.request.RequestID}, err)
		}
		if errors.Is(err, storage.ErrStoreFenced) {
			e.rejectDeferred(plan.deferred, err)
		} else {
			var retry []*flight
			for _, decision := range plan.deferred {
				retry = append(retry, decision.flight)
			}
			e.requeueFront(retry)
		}
		return storage.PublishedBatch{}, false
	}
	return published, true
}
