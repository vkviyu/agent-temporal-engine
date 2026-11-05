package engine

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestAdmissionPreCanceledRequestDoesNotConsumeKey(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	req := coordinatorRequest(t, "pre-canceled", "key", "first", api.ValueCondition{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := e.Commit(ctx, req)
	if r.Code != api.CodeNotSubmitted || !errors.Is(err, context.Canceled) || errors.Is(err, ErrWaitInterrupted) {
		t.Fatalf("pre-admission cancellation: %+v, %v", r, err)
	}
	// A different digest with this identity proves no reservation was retained.
	req = coordinatorRequest(t, "pre-canceled", "key", "replacement", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), req)))
}

func TestAdmissionOptionsDefaultsAndHardLimits(t *testing.T) {
	o, err := (Options{}).checked()
	if err != nil {
		t.Fatal(err)
	}
	if o.Admission.MaxPending != 64 || o.Admission.MaxPendingBytes != 64<<20 || o.Admission.MaxWaiters != 4096 || o.Admission.MaxWaitersPerRequest != 64 {
		t.Fatalf("unexpected bounded admission defaults: %+v", o.Admission)
	}
	if o.Batch.MaxCount != 16 || o.Batch.MaxBytes != 64<<20 || o.Batch.MaxDelay != time.Millisecond {
		t.Fatalf("unexpected batching defaults: %+v", o.Batch)
	}
	for name, bad := range map[string]Options{
		"pending-negative":       {Admission: AdmissionOptions{MaxPending: -1}},
		"pending-hard-limit":     {Admission: AdmissionOptions{MaxPending: math.MaxInt}},
		"bytes-hard-limit":       {Admission: AdmissionOptions{MaxPendingBytes: math.MaxUint64}},
		"waiters-negative":       {Admission: AdmissionOptions{MaxWaiters: -1}},
		"waiters-hard-limit":     {Admission: AdmissionOptions{MaxWaiters: math.MaxInt}},
		"aliases-negative":       {Admission: AdmissionOptions{MaxWaitersPerRequest: -1}},
		"aliases-hard-limit":     {Admission: AdmissionOptions{MaxWaitersPerRequest: math.MaxInt}},
		"batch-count-negative":   {Batch: BatchOptions{MaxCount: -1}},
		"batch-count-hard-limit": {Batch: BatchOptions{MaxCount: math.MaxInt}},
		"batch-bytes-hard-limit": {Batch: BatchOptions{MaxBytes: math.MaxUint64}},
		"batch-delay-negative":   {Batch: BatchOptions{MaxDelay: -time.Nanosecond}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := bad.checked(); !errors.Is(err, storage.ErrLimit) {
				t.Fatalf("unbounded/invalid options accepted: %+v, %v", bad, err)
			}
		})
	}
}

func TestAdmissionCountOneDisablesBatchDelay(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1, MaxDelay: time.Second}})
	g := newCoordinatorGate()
	io.arm("write", g)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timeout := time.AfterFunc(750*time.Millisecond, cancel)
	defer timeout.Stop()
	result := coordinatorCommit(e, ctx, coordinatorRequest(t, "no-delay", "key", "value", api.ValueCondition{}))
	select {
	case <-g.entered:
		timeout.Stop()
	case got := <-result:
		t.Fatalf("MaxCount=1 waited for its configured one-second delay: %+v, %v", got.result, got.err)
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, result))
}

func TestAdmissionDurableReceiptPrecedesSaturatedBudgets(t *testing.T) {
	e, io := coordinatorStore(t, Options{Admission: AdmissionOptions{MaxPending: 1, MaxWaiters: 1, MaxWaitersPerRequest: 1}, Batch: BatchOptions{MaxCount: 1}})
	req := coordinatorRequest(t, "receipt-first", "key", "first", api.ValueCondition{})
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), req)))
	g := newCoordinatorGate()
	io.arm("write", g)
	busy := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "busy", "key", "second", api.ValueCondition{}))
	coordinatorWait(t, g.entered)
	retryCtx := &coordinatorWaitingContext{Context: context.Background(), waiting: make(chan struct{})}
	retryCh := coordinatorCommit(e, retryCtx, req)
	select {
	case <-retryCtx.waiting:
	case retry := <-retryCh:
		if retry.err != nil || retry.result != ack {
			t.Fatalf("durable receipt lost to occupied admission budget: %+v, %v; want %+v", retry.result, retry.err, ack)
		}
		// Keep the result available for the common verification below.
		done := make(chan coordinatorOutcome, 1)
		done <- retry
		retryCh = done
	case <-time.After(10 * time.Second):
		t.Fatal("receipt retry reached neither core wait nor a result")
	}
	conflicting := coordinatorRequest(t, string(req.RequestID), "key", "different", api.ValueCondition{})
	conflictCtx := &coordinatorWaitingContext{Context: context.Background(), waiting: make(chan struct{})}
	conflictCh := coordinatorCommit(e, conflictCtx, conflicting)
	select {
	case <-conflictCtx.waiting:
	case conflict := <-conflictCh:
		if conflict.err != nil || conflict.result.Code != api.CodeConflict || conflict.result.Conflict != api.ConflictIdempotency {
			t.Fatalf("durable idempotency conflict lost to admission budget: %+v, %v", conflict.result, conflict.err)
		}
		done := make(chan coordinatorOutcome, 1)
		done <- conflict
		conflictCh = done
	case <-time.After(10 * time.Second):
		t.Fatal("receipt conflict reached neither core wait nor a result")
	}
	g.unblock()
	retry := coordinatorWait(t, retryCh)
	if retry.err != nil || retry.result != ack {
		t.Fatalf("durable receipt lost to occupied admission budget: %+v, %v; want %+v", retry.result, retry.err, ack)
	}
	conflict := coordinatorWait(t, conflictCh)
	if conflict.err != nil || conflict.result.Code != api.CodeConflict || conflict.result.Conflict != api.ConflictIdempotency {
		t.Fatalf("durable idempotency conflict lost to admission budget: %+v, %v", conflict.result, conflict.err)
	}
	coordinatorAck(t, coordinatorWait(t, busy))
}

func TestAdmissionOversizedRequestDoesNotConsumeKey(t *testing.T) {
	small := coordinatorRequest(t, "bytes", "key", "small", api.ValueCondition{})
	req := coordinatorRequest(t, "bytes", "key", "a value larger than the admission byte cap", api.ValueCondition{})
	e, _ := coordinatorStore(t, Options{Admission: AdmissionOptions{MaxPendingBytes: requestBytes(small)}, Batch: BatchOptions{MaxCount: 1}})
	r, err := e.Commit(context.Background(), req)
	if r.Code != api.CodeNotSubmitted || !errors.Is(err, storage.ErrLimit) {
		t.Fatalf("byte overflow must be pre-admission: %+v, %v", r, err)
	}
	lookup := coordinatorLookup(t, e, req)
	if lookup.Code != api.CodeNotFoundAtFence {
		t.Fatalf("rejected request retained an in-flight identity: %+v", lookup)
	}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), small)))
}

func TestAdmissionPendingByteOverflowDoesNotReserveIdentityOrBytes(t *testing.T) {
	firstReq := coordinatorRequest(t, "bytes-active", "one", "first", api.ValueCondition{})
	rejectedReq := coordinatorRequest(t, "bytes-rejected", "two", "second-value", api.ValueCondition{})
	limit := requestBytes(firstReq) + requestBytes(rejectedReq) - 1
	e, io := coordinatorStore(t, Options{Admission: AdmissionOptions{MaxPending: 8, MaxPendingBytes: limit}, Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("write", g)
	first := coordinatorCommit(e, context.Background(), firstReq)
	coordinatorWait(t, g.entered)
	rejected := coordinatorWait(t, coordinatorCommit(e, context.Background(), rejectedReq))
	if rejected.result.Code != api.CodeNotSubmitted || !errors.Is(rejected.err, storage.ErrLimit) {
		t.Fatalf("pending byte overflow was admitted: %+v, %v", rejected.result, rejected.err)
	}
	e.admission.mu.Lock()
	_, reserved := e.admission.flights[rejectedReq.Key()]
	charged := e.admission.pendingBytes
	e.admission.mu.Unlock()
	if reserved || charged != requestBytes(firstReq) {
		t.Fatalf("overflow changed byte/identity accounting: reserved=%v bytes=%d; want %d", reserved, charged, requestBytes(firstReq))
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, first))
	replacement := coordinatorRequest(t, string(rejectedReq.RequestID), "two", "replacement", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), replacement)))
}

func TestAdmissionPendingSlotOverflowDoesNotReserveIdentity(t *testing.T) {
	e, io := coordinatorStore(t, Options{Admission: AdmissionOptions{MaxPending: 2}, Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("write", g)
	first := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "slot-active", "one", "one", api.ValueCondition{}))
	coordinatorWait(t, g.entered)
	queuedReq := coordinatorRequest(t, "slot-queued", "two", "two", api.ValueCondition{})
	queued := coordinatorCommit(e, context.Background(), queuedReq)
	coordinatorQueued(t, e, queuedReq)
	rejectedReq := coordinatorRequest(t, "slot-rejected", "three", "old-digest", api.ValueCondition{})
	rejected := coordinatorWait(t, coordinatorCommit(e, context.Background(), rejectedReq))
	if rejected.result.Code != api.CodeNotSubmitted || !errors.Is(rejected.err, storage.ErrLimit) {
		t.Fatalf("pending overflow was accepted/reserved: %+v, %v", rejected.result, rejected.err)
	}
	e.admission.mu.Lock()
	_, reserved := e.admission.flights[rejectedReq.Key()]
	e.admission.mu.Unlock()
	if reserved {
		t.Fatal("overflow reserved a request key")
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, first))
	coordinatorAck(t, coordinatorWait(t, queued))
	replacement := coordinatorRequest(t, string(rejectedReq.RequestID), "three", "new-digest", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), replacement)))
}

func TestAdmissionAliasWaitersAreBoundedAndCancellationIsLocal(t *testing.T) {
	e, io := coordinatorStore(t, Options{Admission: AdmissionOptions{MaxWaiters: 16, MaxWaitersPerRequest: 2}, Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("write", g)
	req := coordinatorRequest(t, "alias", "key", "value", api.ValueCondition{})
	first := coordinatorCommit(e, context.Background(), req)
	coordinatorWait(t, g.entered)
	aliasCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	alias := coordinatorCommit(e, aliasCtx, req)
	coordinatorFlightWaiters(t, e, req, 2)
	overflow := coordinatorWait(t, coordinatorCommit(e, context.Background(), req))
	if overflow.result.Code != api.CodeNotSubmitted || !errors.Is(overflow.err, storage.ErrLimit) {
		t.Fatalf("per-request waiter overflow: %+v, %v", overflow.result, overflow.err)
	}
	conflicting := coordinatorRequest(t, string(req.RequestID), "key", "different", api.ValueCondition{})
	conflict := coordinatorWait(t, coordinatorCommit(e, context.Background(), conflicting))
	if conflict.err != nil || conflict.result.Code != api.CodeConflict || conflict.result.Conflict != api.ConflictIdempotency {
		t.Fatalf("waiter saturation hid an idempotency conflict: %+v, %v", conflict.result, conflict.err)
	}
	cancel()
	coordinatorInterrupted(t, coordinatorWait(t, alias), context.Canceled)
	coordinatorFlightWaiters(t, e, req, 1)
	// A released alias slot is reusable without changing the original flight.
	newAlias := coordinatorCommit(e, context.Background(), req)
	coordinatorFlightWaiters(t, e, req, 2)
	g.unblock()
	ack := coordinatorAck(t, coordinatorWait(t, first))
	if got := coordinatorAck(t, coordinatorWait(t, newAlias)); got != ack {
		t.Fatalf("same-digest aliases did not share one acknowledgment: %+v != %+v", got, ack)
	}
}

func TestAdmissionCancellationAfterInitialCheckDoesNotAttachAlias(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("write", g)
	req := coordinatorRequest(t, "cancel-before-alias", "key", "value", api.ValueCondition{})
	first := coordinatorCommit(e, context.Background(), req)
	coordinatorWait(t, g.entered)
	for _, alias := range []api.CommitRequest{req, coordinatorRequest(t, string(req.RequestID), "key", "different", api.ValueCondition{})} {
		base, cancel := context.WithCancel(context.Background())
		ctx := &coordinatorCancelAfterCheck{Context: base, cancel: cancel}
		got := coordinatorWait(t, coordinatorCommit(e, ctx, alias))
		cancel()
		if got.result.Code != api.CodeNotSubmitted || !errors.Is(got.err, context.Canceled) || errors.Is(got.err, ErrWaitInterrupted) {
			t.Fatalf("cancellation before alias admission was reclassified: %+v, %v", got.result, got.err)
		}
		coordinatorFlightWaiters(t, e, req, 1)
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, first))
}

func TestAdmissionReplayedReceiptPrecedesExhaustedByteBudget(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	req := coordinatorRequest(t, "replayed-quota", "key", "value", api.ValueCondition{})
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), req)))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// A new command cannot fit at all. Historical receipts are still usable.
	reopened, report, err := OpenExisting(io, Options{Admission: AdmissionOptions{MaxPendingBytes: 1}, Batch: BatchOptions{MaxCount: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if !report.Verified || report.ScannedTransactions != 1 {
		t.Fatalf("receipt prefix did not replay: %+v", report)
	}
	if got := coordinatorWait(t, coordinatorCommit(reopened, context.Background(), req)); got.err != nil || got.result != ack {
		t.Fatalf("replayed receipt lost to write admission quota: %+v, %v", got.result, got.err)
	}
	conflicting := coordinatorRequest(t, string(req.RequestID), "key", "different", api.ValueCondition{})
	if got := coordinatorWait(t, coordinatorCommit(reopened, context.Background(), conflicting)); got.err != nil || got.result.Code != api.CodeConflict || got.result.Conflict != api.ConflictIdempotency {
		t.Fatalf("replayed receipt conflict lost to write admission quota: %+v, %v", got.result, got.err)
	}
	newReq := coordinatorRequest(t, "new-quota", "key", "value", api.ValueCondition{})
	if got := coordinatorWait(t, coordinatorCommit(reopened, context.Background(), newReq)); got.result.Code != api.CodeNotSubmitted || !errors.Is(got.err, storage.ErrLimit) {
		t.Fatalf("control request unexpectedly fit the exhausted byte budget: %+v, %v", got.result, got.err)
	}
}

func TestAdmissionGlobalWaiterOverflowDoesNotReserveIdentity(t *testing.T) {
	e, io := coordinatorStore(t, Options{Admission: AdmissionOptions{MaxWaiters: 2, MaxWaitersPerRequest: 16}, Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("write", g)
	firstReq := coordinatorRequest(t, "waiter-active", "one", "one", api.ValueCondition{})
	first := coordinatorCommit(e, context.Background(), firstReq)
	coordinatorWait(t, g.entered)
	secondReq := coordinatorRequest(t, "waiter-queued", "two", "two", api.ValueCondition{})
	second := coordinatorCommit(e, context.Background(), secondReq)
	coordinatorQueued(t, e, secondReq)
	rejectedReq := coordinatorRequest(t, "waiter-rejected", "three", "old", api.ValueCondition{})
	rejected := coordinatorWait(t, coordinatorCommit(e, context.Background(), rejectedReq))
	if rejected.result.Code != api.CodeNotSubmitted || !errors.Is(rejected.err, storage.ErrLimit) {
		t.Fatalf("global waiter overflow: %+v, %v", rejected.result, rejected.err)
	}
	e.admission.mu.Lock()
	_, reserved := e.admission.flights[rejectedReq.Key()]
	count := e.admission.waiters
	e.admission.mu.Unlock()
	if reserved || count != 2 {
		t.Fatalf("overflow changed admission accounting: reserved=%v waiters=%d", reserved, count)
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, first))
	coordinatorAck(t, coordinatorWait(t, second))
	rejectedReq = coordinatorRequest(t, string(rejectedReq.RequestID), "three", "replacement", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), rejectedReq)))
}

func TestCoordinatorReadsCancelWhileCoreMutexIsBusy(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	for _, name := range []string{"snapshot", "lookup", "get"} {
		t.Run(name, func(t *testing.T) {
			e.mu.Lock()
			locked := true
			defer func() {
				if locked {
					e.mu.Unlock()
				}
			}()
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &coordinatorObservedContext{Context: base, checked: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				var err error
				switch name {
				case "snapshot":
					_, err = e.Snapshot(ctx)
				case "lookup":
					_, err = e.LookupRequest(ctx, api.LookupRequestRequest{Space: "coordinator-space", Principal: "host", RequestID: "absent"})
				case "get":
					_, err = e.GetState(ctx, api.ReadStateRequest{Space: "coordinator-space", Key: []byte("key")})
				}
				done <- err
			}()
			coordinatorWait(t, ctx.checked)
			cancel()
			if err := coordinatorWait(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("%s could not cancel its core-mutex wait: %v", name, err)
			}
			e.mu.Unlock()
			locked = false
		})
	}
}
