package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
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

// collect never holds the core while waiting for more proposals. Admissions,
// lookups of existing flights and caller cancellation remain independent of IO.
func (e *Engine) collect() []*flight {
	a := e.admission
	var batch []*flight
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		a.mu.Lock()
		for len(a.queue) > 0 && len(batch) < e.options.Batch.MaxCount {
			f := a.queue[0]
			a.queue[0] = nil
			a.queue = a.queue[1:]
			f.phase = "selected"
			batch = append(batch, f)
		}
		stopping := !a.accepting
		a.mu.Unlock()
		if len(batch) == e.options.Batch.MaxCount || (stopping && len(batch) > 0) {
			return batch
		}
		if len(batch) == 0 && stopping {
			return nil
		}
		if len(batch) > 0 && timer == nil {
			timer = time.NewTimer(e.options.Batch.MaxDelay)
		}
		var deadline <-chan time.Time
		if timer != nil {
			deadline = timer.C
		}
		select {
		case <-a.wake:
		case <-deadline:
			return batch
		}
	}
}

func (e *Engine) requeueFront(batch []*flight) {
	if len(batch) == 0 {
		return
	}
	a := e.admission
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, f := range batch {
		f.phase = "queued"
	}
	queue := make([]*flight, 0, len(batch)+len(a.queue))
	queue = append(queue, batch...)
	queue = append(queue, a.queue...)
	a.queue = queue
	signal(a)
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
	_, err := e.journal.Published()
	return err
}

func (e *Engine) rejectRemaining(batch []*flight, err error) {
	for _, f := range batch {
		e.complete(f, api.CommitResult{Code: api.CodeStoreFenced, RequestID: f.request.RequestID}, err)
	}
}

func (e *Engine) processBatch(batch []*flight) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.workerReady(); err != nil {
		e.stopAdmission(err)
		e.rejectRemaining(batch, err)
		return
	}
	view := e.stagingView()
	var planned []plannedFlight
	var deferred []deferredDecision
	// A decision evaluated against a candidate predecessor is private too. It
	// cannot be delivered until that predecessor is durably published.
	finishValidation := func(f *flight, result api.CommitResult, err error) {
		if len(planned) > 0 {
			deferred = append(deferred, deferredDecision{f, result, err})
		} else {
			e.complete(f, result, err)
		}
	}
	var bytes uint64
	for i, f := range batch {
		old, found, err := view.lookup(context.Background(), f.request.Key())
		if err != nil {
			e.abortPlanning(batch[i:], planned, deferred, err)
			return
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
		plan, err := view.prepare(context.Background(), f.request, f.digest, txID, view.publication.Position.LSN+1, nil)
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
			e.abortPlanning(batch[i:], planned, deferred, err)
			return
		}
		prepared, err := storage.PrepareTransaction(plan.transaction, e.options.Journal.Limits)
		if err == nil {
			err = view.checkPrepared(prepared)
		}
		if err != nil {
			if isLimit(err) {
				finishValidation(f, api.CommitResult{Code: api.CodeResourceLimit, RequestID: f.request.RequestID}, err)
				continue
			}
			e.abortPlanning(batch[i:], planned, deferred, err)
			return
		}
		if prepared.Size() > e.options.Batch.MaxBytes-bytes {
			if len(planned) == 0 {
				e.complete(f, api.CommitResult{Code: api.CodeResourceLimit, RequestID: f.request.RequestID}, storage.ErrLimit)
				continue
			}
			e.requeueFront(batch[i:])
			break
		}
		if err = view.acceptStaged(plan, prepared); err != nil {
			e.abortPlanning(batch[i:], planned, deferred, err)
			return
		}
		bytes += prepared.Size()
		planned = append(planned, plannedFlight{flight: f, plan: plan, prepared: prepared})
	}
	if len(planned) == 0 {
		return
	}
	prepared := make([]*storage.PreparedTransaction, len(planned))
	for i, work := range planned {
		prepared[i] = work.prepared
	}
	// Caller wait contexts are deliberately absent from planning and P1.
	published, err := e.journal.PublishPreparedBatch(context.Background(), prepared)
	if err != nil {
		if errors.Is(err, storage.ErrCommitUnknown) {
			e.fenced = err
			e.stopAdmission(fmt.Errorf("%w: %w", storage.ErrStoreFenced, err))
			for _, work := range planned {
				e.complete(work.flight, api.CommitResult{Code: api.CodeCommitUnknown, RequestID: work.flight.request.RequestID, TxID: work.plan.transaction.TxID, RequestDigest: work.flight.digest}, err)
			}
			e.rejectDeferred(deferred, fmt.Errorf("%w: %w", storage.ErrStoreFenced, err))
			return
		}
		if errors.Is(err, storage.ErrStoreFenced) {
			e.fenced = err
			e.stopAdmission(err)
		} else if !isLimit(err) {
			// A prepared chain that the sole publisher cannot accept indicates
			// an internal state mismatch, not a reason to spin and retry it.
			err = e.readFailure(err)
		}
		for _, work := range planned {
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
			e.rejectDeferred(deferred, err)
		} else {
			var retry []*flight
			for _, decision := range deferred {
				retry = append(retry, decision.flight)
			}
			e.requeueFront(retry)
		}
		return
	}
	if len(published.Transactions) != len(planned) {
		e.abortPublished(planned, deferred, ErrInvalidStateData)
		return
	}
	for i, work := range planned {
		if len(published.Transactions[i].RecordLocations) != len(work.plan.builder.objects) {
			e.abortPublished(planned, deferred, ErrInvalidStateData)
			return
		}
	}
	// No public core reader can observe a partially merged batch. Hints are
	// updated before flights disappear, closing the admission/publication race.
	for i, work := range planned {
		for n, obj := range work.plan.builder.objects {
			e.index.put(obj.id, published.Transactions[i].RecordLocations[n])
		}
		member := published.Transactions[i].Publication
		e.envelopes[work.plan.transaction.TxID] = commitEnvelope{fence: member.Fence(), rootSet: work.plan.rootSet, space: work.plan.transaction.Request.Space, principal: work.plan.transaction.Request.Principal}
		e.history[member.Position.LSN] = e.envelopes[work.plan.transaction.TxID]
		e.installEvent(work.plan)
	}
	e.roots = view.roots
	e.rootSet = view.rootSet
	e.publication = published.Publication
	a := e.admission
	a.mu.Lock()
	for _, work := range planned {
		a.hints[keyHint(work.flight.request.Key())] = struct{}{}
	}
	a.fence = e.publication.Fence()
	for i, work := range planned {
		completeLocked(a, work.flight, api.CommitResult{Code: api.CodeDurableAck, RequestID: work.flight.request.RequestID, TxID: work.plan.transaction.TxID,
			Fence: published.Transactions[i].Publication.Fence(), RequestDigest: work.flight.digest, ResultDigest: work.plan.receipt, StateRoot: work.plan.stateRoot, RootSet: work.plan.rootSet}, nil)
	}
	for _, decision := range deferred {
		completeLocked(a, decision.flight, decision.result, decision.err)
	}
	a.mu.Unlock()
}

func validateQueued(req api.CommitRequest) error {
	if err := validSpace(req.Space); err != nil {
		return err
	}
	switch req.Method {
	case api.MethodImportTransfer:
		_, err := api.DecodeImportTransfer(req.Body)
		return err
	case api.MethodApplyAnnotation:
		_, err := annotation.DecodeRecord(req.Body)
		return err
	case api.MethodApplySummary:
		_, err := knowledge.DecodeSummary(req.Body)
		return err
	case api.MethodApplyEmission:
		_, err := api.DecodeEmissionCommand(req.Body)
		return err
	case api.MethodApplyWork:
		_, err := api.DecodeWorkUnit(req.Body)
		return err
	case api.MethodMutateState:
		_, err := DecodeStateBatch(req.Body)
		return err
	case api.MethodApplyAgent:
		_, err := agentprofile.DecodeCommand(req.Body)
		return err
	case api.MethodApplyGraph:
		space, _, err := graph.DecodeCommand(req.Body)
		if err == nil && space != req.Space {
			return graph.ErrSpaceMismatch
		}
		return err
	case api.MethodApplyKnowledge:
		_, err := knowledge.DecodeCommand(req.Body)
		if err != nil {
			return fmt.Errorf("knowledge admission decode: %w", err)
		}
		return nil
	case api.MethodApplyMemory:
		_, err := memory.DecodeCommand(req.Body)
		return err
	default:
		return ErrUnsupportedMethod
	}
}

func (e *Engine) abortPlanning(remaining []*flight, planned []plannedFlight, deferred []deferredDecision, cause error) {
	err := e.readFailure(cause)
	for _, work := range planned {
		e.complete(work.flight, api.CommitResult{Code: api.CodeStoreFenced, RequestID: work.flight.request.RequestID}, err)
	}
	e.rejectRemaining(remaining, err)
	e.rejectDeferred(deferred, err)
}

func (e *Engine) abortPublished(planned []plannedFlight, deferred []deferredDecision, cause error) {
	err := fmt.Errorf("%w: %w", storage.ErrCommitUnknown, e.readFailure(cause))
	for _, work := range planned {
		e.complete(work.flight, api.CommitResult{Code: api.CodeCommitUnknown, RequestID: work.flight.request.RequestID, TxID: work.plan.transaction.TxID}, err)
	}
	e.rejectDeferred(deferred, err)
}

func (e *Engine) rejectDeferred(deferred []deferredDecision, err error) {
	for _, decision := range deferred {
		e.complete(decision.flight, api.CommitResult{Code: api.CodeStoreFenced, RequestID: decision.flight.request.RequestID}, err)
	}
}
