package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type deferredDecision struct {
	flight *flight
	result api.CommitResult
	err    error
}

type plannedFlight struct {
	flight   *flight
	plan     *commitPlan
	prepared *storage.PreparedTransaction
}

type batchPlan struct {
	view     *Engine
	planned  []plannedFlight
	deferred []deferredDecision
	bytes    uint64
}

func (e *Engine) coordinate() {
	defer close(e.admission.done)
	for {
		batch := e.collect()
		if len(batch) == 0 {
			return
		}
		e.processBatch(batch)
	}
}

// workerReady permits draining previously admitted work, but never a fenced
// publisher. Public ready() additionally refuses new reads while draining.
func (e *Engine) workerReady() error {
	if e.closed {
		return storage.ErrClosed
	}
	if e.fenced != nil {
		return fmt.Errorf("%w: %w", storage.ErrStoreFenced, e.fenced)
	}
	return nil
}

func (e *Engine) rejectRemaining(batch []*flight, err error) {
	for _, f := range batch {
		e.complete(f, api.CommitResult{Code: api.CodeStoreFenced, RequestID: f.request.RequestID}, err)
	}
}

func (e *Engine) processBatch(batch []*flight) {
	e.writerMu.Lock()
	defer e.writerMu.Unlock()

	view, ok := e.beginBatch(batch)
	if !ok {
		return
	}
	plan, ok := e.prepareBatch(view, batch)
	if !ok || len(plan.planned) == 0 {
		return
	}
	published, ok := e.publishBatch(plan)
	if !ok {
		return
	}
	if !e.installBatch(plan, published) {
		return
	}
	e.completeBatch(plan, published)
}

func (e *Engine) beginBatch(batch []*flight) (*Engine, bool) {
	e.mu.Lock()
	err := e.workerReady()
	var view *Engine
	if err == nil {
		view = e.stagingView()
	}
	e.mu.Unlock()
	if err == nil {
		_, err = e.journal.Published()
	}
	if err != nil {
		e.stopAdmission(err)
		e.rejectRemaining(batch, err)
		return nil, false
	}
	return view, true
}

func (e *Engine) prepareBatch(view *Engine, batch []*flight) (batchPlan, bool) {
	plan := batchPlan{view: view}
	// A decision evaluated against a candidate predecessor is private too. It
	// cannot be delivered until that predecessor is durably published.
	finishValidation := func(f *flight, result api.CommitResult, err error) {
		if len(plan.planned) > 0 {
			plan.deferred = append(plan.deferred, deferredDecision{f, result, err})
		} else {
			e.complete(f, result, err)
		}
	}
	for i, f := range batch {
		old, found, err := view.lookup(context.Background(), f.request.Key())
		if err != nil {
			e.abortPlanning(batch[i:], plan.planned, plan.deferred, err)
			return plan, false
		}
		if found {
			if old.RequestDigest != f.digest {
				e.complete(f, api.CommitResult{Code: api.CodeConflict, RequestID: f.request.RequestID, Conflict: api.ConflictIdempotency}, nil)
			} else {
				e.complete(f, old, nil)
			}
			continue
		}
		err = validateQueued(f.request)
		if err == nil && (f.request.Method == api.MethodApplyAgent || f.request.Method == api.MethodApplyWork) && !e.options.AgentProfile {
			err = ErrUnsupportedMethod
		}
		if err != nil {
			e.complete(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
			continue
		}
		var random [16]byte
		if _, err = rand.Read(random[:]); err != nil {
			e.complete(f, api.CommitResult{Code: api.CodeNotSubmitted, RequestID: f.request.RequestID}, err)
			continue
		}
		txID := model.TxID(hex.EncodeToString(random[:]))
		compiled, err := view.prepare(context.Background(), f.request, f.digest, txID, view.publication.Position.LSN+1, nil)
		if err != nil {
			if errors.Is(err, errCAS) {
				finishValidation(f, api.CommitResult{Code: api.CodeConflict, RequestID: f.request.RequestID, Conflict: api.ConflictPrecondition}, nil)
				continue
			}
			if isLimit(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeResourceLimit, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodApplyAgent && agentCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if errors.Is(err, ErrActionReservation) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodApplyWork && workCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodApplyAnnotation && annotationCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodUpdateInvestigation && investigationCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodApplyTaskTrace && taskTraceRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodApplySummary && summaryCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodImportTransfer && transferCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if f.request.Method == api.MethodApplyEmission && emissionCommandRejection(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			if (f.request.Method == api.MethodApplyKnowledge && knowledgeCommandRejection(err)) || (f.request.Method == api.MethodApplyGraph && graphCommandRejection(err)) || (f.request.Method == api.MethodApplyMemory && memoryCommandRejection(err)) {
				finishValidation(f, api.CommitResult{Code: api.CodeRejected, RequestID: f.request.RequestID}, err)
				continue
			}
			e.abortPlanning(batch[i:], plan.planned, plan.deferred, err)
			return plan, false
		}
		prepared, err := storage.PrepareTransaction(compiled.transaction, e.options.Journal.Limits)
		if err == nil {
			err = view.checkPrepared(prepared)
		}
		if err != nil {
			if isLimit(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeResourceLimit, RequestID: f.request.RequestID}, err)
				continue
			}
			e.abortPlanning(batch[i:], plan.planned, plan.deferred, err)
			return plan, false
		}
		if prepared.Size() > e.options.Batch.MaxBytes-plan.bytes {
			if len(plan.planned) == 0 {
				e.complete(f, api.CommitResult{Code: api.CodeResourceLimit, RequestID: f.request.RequestID}, storage.ErrLimit)
				continue
			}
			e.requeueFront(batch[i:])
			break
		}
		if err = view.acceptStaged(compiled, prepared); err != nil {
			e.abortPlanning(batch[i:], plan.planned, plan.deferred, err)
			return plan, false
		}
		plan.bytes += prepared.Size()
		plan.planned = append(plan.planned, plannedFlight{flight: f, plan: compiled, prepared: prepared})
	}
	return plan, true
}
