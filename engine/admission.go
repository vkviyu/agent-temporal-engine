package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var (
	ErrAdmissionLimit  = errors.New("admission resource limit exceeded")
	ErrWaitInterrupted = errors.New("command wait interrupted after admission")
)

type AdmissionOptions struct {
	MaxPending           int
	MaxPendingBytes      uint64
	MaxWaiters           int
	MaxWaitersPerRequest int
}
type BatchOptions struct {
	MaxCount int
	MaxBytes uint64
	MaxDelay time.Duration
}

func (a AdmissionOptions) checked() (AdmissionOptions, error) {
	if a.MaxPending == 0 {
		a.MaxPending = 64
	}
	if a.MaxPendingBytes == 0 {
		a.MaxPendingBytes = 64 << 20
	}
	if a.MaxWaiters == 0 {
		a.MaxWaiters = 4096
	}
	if a.MaxWaitersPerRequest == 0 {
		a.MaxWaitersPerRequest = 64
	}
	if a.MaxPending < 1 || a.MaxPending > 4096 || a.MaxPendingBytes > 256<<20 || a.MaxWaiters < 1 || a.MaxWaiters > 65536 || a.MaxWaitersPerRequest < 1 || a.MaxWaitersPerRequest > 1024 {
		return a, storage.ErrLimit
	}
	return a, nil
}

func (b BatchOptions) checked() (BatchOptions, error) {
	if b.MaxCount == 0 {
		b.MaxCount = 16
	}
	if b.MaxBytes == 0 {
		b.MaxBytes = storage.MaxPreparedBatchBytes
	}
	if b.MaxDelay == 0 {
		b.MaxDelay = time.Millisecond
	}
	if b.MaxCount < 1 || b.MaxCount > storage.MaxPreparedBatchTransactions || b.MaxBytes > storage.MaxPreparedBatchBytes || b.MaxDelay < 0 || b.MaxDelay > time.Second {
		return b, storage.ErrLimit
	}
	return b, nil
}

type flight struct {
	request api.CommitRequest
	digest  model.ObjectID
	result  api.CommitResult
	err     error
	done    chan struct{}
	waiters int
	phase   string
	bytes   uint64
}

// The membership hint is a complete, rebuildable index of published request
// keys. A hit only triggers verified tree lookup; it never supplies an Ack or
// proves absence to the caller. This lets genuinely new keys enter the bounded
// queue while the core publisher is synchronizing an earlier batch.
type admissionState struct {
	mu           sync.Mutex
	flights      map[model.RequestKey]*flight
	queue        []*flight
	pendingBytes uint64
	waiters      int
	accepting    bool
	terminal     error
	hints        map[[32]byte]struct{}
	fence        model.CommitFence
	wake         chan struct{}
	done         chan struct{}
}

func keyHint(key model.RequestKey) [32]byte {
	b := append([]byte("ATE-REQUEST-KEY\x00"), receiptKey(key)...)
	return sha256.Sum256(b)
}

func requestBytes(req api.CommitRequest) uint64 {
	n := uint64(len(req.Body) + len(req.Space) + len(req.Principal) + len(req.RequestID) + len(req.Method) + 128)
	if req.ExpectedFence != nil {
		n += uint64(len(req.ExpectedFence.Store) + len(req.ExpectedFence.CommitDigest) + 16)
	}
	return n
}

func ownRequest(req api.CommitRequest) api.CommitRequest {
	req.Body = bytes.Clone(req.Body)
	if req.ExpectedFence != nil {
		f := *req.ExpectedFence
		req.ExpectedFence = &f
	}
	return req
}

func (e *Engine) startCoordinator() {
	e.admission = &admissionState{flights: make(map[model.RequestKey]*flight), hints: e.receiptHints, accepting: true,
		fence: e.publication.Fence(), wake: make(chan struct{}, 1), done: make(chan struct{})}
	go e.coordinate()
}

func signal(a *admissionState) {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) publicGate() error {
	if e.retired.Load() {
		return storage.ErrClosed
	}
	if e.draining.Load() {
		return storage.ErrClosing
	}
	return nil
}

func (e *Engine) stopAdmission(reason error) {
	a := e.admission
	if a == nil {
		return
	}
	a.mu.Lock()
	a.accepting = false
	if a.terminal == nil || errors.Is(reason, storage.ErrStoreFenced) {
		a.terminal = reason
	}
	signal(a)
	a.mu.Unlock()
}

func admissionFailure(id model.RequestID, err error) (api.CommitResult, error) {
	code := api.CodeNotSubmitted
	if errors.Is(err, storage.ErrStoreFenced) {
		code = api.CodeStoreFenced
	}
	return api.CommitResult{Code: code, RequestID: id}, err
}

// attachLocked adds a waiter without holding the core or borrowing any input.
func (e *Engine) attachLocked(f *flight, digest model.ObjectID) error {
	if f.digest != digest {
		return apiConflict{}
	}
	a := e.admission
	if a.waiters >= e.options.Admission.MaxWaiters || f.waiters >= e.options.Admission.MaxWaitersPerRequest {
		return fmt.Errorf("%w: %w: waiters", ErrAdmissionLimit, storage.ErrLimit)
	}
	f.waiters++
	a.waiters++
	return nil
}

// apiConflict distinguishes an immediate in-flight body conflict from a failed
// admission. It carries no claim that the original attempt is already durable.
type apiConflict struct{}

func (apiConflict) Error() string { return "in-flight idempotency conflict" }

func (e *Engine) commitQueued(ctx context.Context, req api.CommitRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.RequestID, err)
	}
	if err := e.publicGate(); err != nil {
		return admissionFailure(req.RequestID, err)
	}
	digest, err := CommandDigest(req)
	if err != nil {
		return admissionFailure(req.RequestID, err)
	}
	a := e.admission
	if a == nil {
		return admissionFailure(req.RequestID, storage.ErrClosed)
	}
	// A present durable hint takes the slow verified lookup path. Otherwise the
	// worker still performs receipt-first lookup before compilation.
	var absentAt model.CommitFence
	for {
		a.mu.Lock()
		if !a.accepting {
			err := a.terminal
			a.mu.Unlock()
			return admissionFailure(req.RequestID, err)
		}
		if err := ctx.Err(); err != nil {
			a.mu.Unlock()
			return admissionFailure(req.RequestID, err)
		}
		if f, ok := a.flights[req.Key()]; ok {
			err := e.attachLocked(f, digest)
			a.mu.Unlock()
			if _, conflict := err.(apiConflict); conflict {
				return api.CommitResult{Code: api.CodeConflict, RequestID: req.RequestID, Conflict: api.ConflictIdempotency}, nil
			}
			if err != nil {
				return admissionFailure(req.RequestID, err)
			}
			return e.wait(ctx, f)
		}
		_, possibleReceipt := a.hints[keyHint(req.Key())]
		if possibleReceipt && absentAt != a.fence {
			a.mu.Unlock()
			if err := e.mu.LockContext(ctx); err != nil {
				return admissionFailure(req.RequestID, err)
			}
			// Receipt-first admission remains valid while another writer is in
			// P1. The durable receipt belongs to the already-published prefix,
			// and does not disclose a new state or issue a read grant.
			if err := e.workerReady(); err != nil {
				e.mu.Unlock()
				return admissionFailure(req.RequestID, err)
			}
			old, found, err := e.lookup(ctx, req.Key())
			if err != nil {
				err = e.readFailure(err)
				e.mu.Unlock()
				return admissionFailure(req.RequestID, err)
			}
			observed := e.publication.Fence()
			e.mu.Unlock()
			if found {
				if old.RequestDigest != digest {
					return api.CommitResult{Code: api.CodeConflict, RequestID: req.RequestID, Conflict: api.ConflictIdempotency}, nil
				}
				return old, nil
			}
			absentAt = observed // possible hash collision; never disclose cache absence
			continue
		}
		limits := e.options.Admission
		cost := requestBytes(req)
		if len(a.flights) >= limits.MaxPending || a.waiters >= limits.MaxWaiters || cost > limits.MaxPendingBytes-a.pendingBytes {
			a.mu.Unlock()
			return admissionFailure(req.RequestID, fmt.Errorf("%w: %w: pending commands/bytes", ErrAdmissionLimit, storage.ErrLimit))
		}
		if err := ctx.Err(); err != nil {
			a.mu.Unlock()
			return admissionFailure(req.RequestID, err)
		}
		f := &flight{request: ownRequest(req), digest: digest, done: make(chan struct{}), waiters: 1, phase: "queued", bytes: cost}
		a.flights[req.Key()] = f
		a.queue = append(a.queue, f)
		a.pendingBytes += cost
		a.waiters++
		signal(a)
		a.mu.Unlock()
		return e.wait(ctx, f)
	}
}

func (e *Engine) wait(ctx context.Context, f *flight) (api.CommitResult, error) {
	defer func() { a := e.admission; a.mu.Lock(); f.waiters--; a.waiters--; a.mu.Unlock() }()
	select {
	case <-f.done:
		return f.result, f.err
	case <-ctx.Done():
		select {
		case <-f.done:
			return f.result, f.err
		default:
		}
		return api.CommitResult{Code: api.CodeWaitInterrupted, RequestID: f.request.RequestID, RequestDigest: f.digest}, fmt.Errorf("%w: %w", ErrWaitInterrupted, ctx.Err())
	}
}

func (e *Engine) complete(f *flight, result api.CommitResult, err error) {
	a := e.admission
	a.mu.Lock()
	defer a.mu.Unlock()
	completeLocked(a, f, result, err)
}

func completeLocked(a *admissionState, f *flight, result api.CommitResult, err error) {
	f.result = result
	f.err = err
	f.phase = "done"
	delete(a.flights, f.request.Key())
	a.pendingBytes -= f.bytes
	// Completed waiters need only their immutable result/key. Do not retain a
	// large owned payload until every awakened caller has been scheduled again.
	f.request.Body = nil
	f.request.ExpectedFence = nil
	close(f.done)
}

func (e *Engine) inFlight(req api.LookupRequestRequest) (api.LookupRequestResult, bool, error) {
	a := e.admission
	if a == nil {
		return api.LookupRequestResult{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.accepting {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, true, a.terminal
	}
	if _, ok := a.flights[req.Key()]; ok {
		return api.LookupRequestResult{Code: api.CodeInFlight, ObservedFence: a.fence}, true, nil
	}
	return api.LookupRequestResult{}, false, nil
}
