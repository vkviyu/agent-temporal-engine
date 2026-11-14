package engine

import (
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) installBatch(plan batchPlan, published storage.PublishedBatch) bool {
	if len(published.Transactions) != len(plan.planned) {
		e.abortPublished(plan.planned, plan.deferred, ErrInvalidStateData)
		return false
	}
	for i, work := range plan.planned {
		if len(published.Transactions[i].RecordLocations) != len(work.plan.builder.objects) {
			e.abortPublished(plan.planned, plan.deferred, ErrInvalidStateData)
			return false
		}
	}
	e.mu.Lock()
	if e.fenced != nil {
		cause := e.fenced
		e.mu.Unlock()
		e.abortPublished(plan.planned, plan.deferred, cause)
		return false
	}
	// No public core reader can observe a partially merged batch. Hints are
	// updated before flights disappear, closing the admission/publication race.
	for i, work := range plan.planned {
		for n, obj := range work.plan.builder.objects {
			e.index.put(obj.id, published.Transactions[i].RecordLocations[n])
		}
		member := published.Transactions[i].Publication
		e.envelopes[work.plan.transaction.TxID] = commitEnvelope{fence: member.Fence(), rootSet: work.plan.rootSet, space: work.plan.transaction.Request.Space, principal: work.plan.transaction.Request.Principal}
		e.history[member.Position.LSN] = e.envelopes[work.plan.transaction.TxID]
		e.installEvent(work.plan)
	}
	e.roots = plan.view.roots
	e.rootSet = plan.view.rootSet
	e.publication = published.Publication
	e.publicationPending.Store(false)
	e.mu.Unlock()
	return true
}

func (e *Engine) completeBatch(plan batchPlan, published storage.PublishedBatch) {
	a := e.admission
	a.mu.Lock()
	for _, work := range plan.planned {
		a.hints[keyHint(work.flight.request.Key())] = struct{}{}
	}
	a.fence = published.Publication.Fence()
	for i, work := range plan.planned {
		completeLocked(a, work.flight, api.CommitResult{Code: api.CodeDurableAck, RequestID: work.flight.request.RequestID, TxID: work.plan.transaction.TxID,
			Fence: published.Transactions[i].Publication.Fence(), RequestDigest: work.flight.digest, ResultDigest: work.plan.receipt, StateRoot: work.plan.stateRoot, RootSet: work.plan.rootSet}, nil)
	}
	for _, decision := range plan.deferred {
		completeLocked(a, decision.flight, decision.result, decision.err)
	}
	a.mu.Unlock()
}
