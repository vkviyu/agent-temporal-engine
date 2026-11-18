package engine

import (
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) abortPlanning(remaining []*flight, planned []plannedFlight, deferred []deferredDecision, cause error) {
	err := e.fenceWriter(cause)
	for _, work := range planned {
		e.complete(work.flight, api.CommitResult{Code: api.CodeStoreFenced, RequestID: work.flight.request.RequestID}, err)
	}
	e.rejectRemaining(remaining, err)
	e.rejectDeferred(deferred, err)
}

func (e *Engine) abortPublished(planned []plannedFlight, deferred []deferredDecision, cause error) {
	err := fmt.Errorf("%w: %w", storage.ErrCommitUnknown, e.fenceWriter(cause))
	for _, work := range planned {
		e.complete(work.flight, api.CommitResult{Code: api.CodeCommitUnknown, RequestID: work.flight.request.RequestID, TxID: work.plan.transaction.TxID}, err)
	}
	e.rejectDeferred(deferred, err)
}

// fenceWriter is called with writerMu held and mu released. Output remains
// closed until both the in-memory state and physical publisher are fenced.
func (e *Engine) fenceWriter(cause error) error {
	e.traceEvidence.stop()
	e.traceProfiles.stop()
	e.mu.Lock()
	if e.fenced == nil {
		e.fenced = semanticError(cause)
	}
	cause = e.fenced
	e.publicationPending.Store(true)
	e.mu.Unlock()
	err := e.journal.Fence(cause)
	e.publicationPending.Store(false)
	e.stopAdmission(err)
	return err
}

func (e *Engine) rejectDeferred(deferred []deferredDecision, err error) {
	for _, decision := range deferred {
		e.complete(decision.flight, api.CommitResult{Code: api.CodeStoreFenced, RequestID: decision.flight.request.RequestID}, err)
	}
}
