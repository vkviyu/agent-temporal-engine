package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestCoordinatorBatchLimitsSplitWithoutReorderingQueuedWork(t *testing.T) {
	for _, test := range []struct {
		name       string
		batch      BatchOptions
		queued     int
		payload    int
		wantCycles int
	}{
		{name: "count", batch: BatchOptions{MaxCount: 2}, queued: 4, payload: 1, wantCycles: 4},
		{name: "bytes", batch: BatchOptions{MaxCount: 16, MaxBytes: 384 << 10}, queued: 3, payload: 128 << 10, wantCycles: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			e, io := coordinatorStore(t, Options{Batch: test.batch})
			_, syncBefore, currentBefore, _ := io.counts()
			baseGate := newCoordinatorGate()
			io.arm("current", baseGate)
			base := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "split-base", "order", "0", api.ValueCondition{}))
			coordinatorWait(t, baseGate.entered)
			request := func(i int) api.CommitRequest {
				body, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{
					{Key: []byte("payload"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: bytes.Repeat([]byte{byte(i)}, test.payload)}},
					{Key: []byte("order"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte(fmt.Sprint(i))},
						Condition: api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte(fmt.Sprint(i - 1))}}},
				}})
				if err != nil {
					t.Fatal(err)
				}
				return api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: model.RequestID(fmt.Sprintf("split-%02d", i)), Method: api.MethodMutateState, Body: body}
			}
			var outcomes []<-chan coordinatorOutcome
			for i := 1; i <= test.queued; i++ {
				req := request(i)
				outcomes = append(outcomes, coordinatorCommit(e, context.Background(), req))
				coordinatorQueued(t, e, req)
			}
			// The next physical group is already fixed when this gate fires.
			// Later arrivals must remain behind members deferred by its limit.
			groupGate := newCoordinatorGate()
			io.arm("current", groupGate)
			baseGate.unblock()
			baseAck := coordinatorAck(t, coordinatorWait(t, base))
			coordinatorWait(t, groupGate.entered)
			late := request(test.queued + 1)
			outcomes = append(outcomes, coordinatorCommit(e, context.Background(), late))
			coordinatorQueued(t, e, late)
			groupGate.unblock()
			var final api.CommitResult
			for i, outcome := range outcomes {
				final = coordinatorAck(t, coordinatorWait(t, outcome))
				if final.Fence.LSN != baseAck.Fence.LSN+uint64(i+1) {
					t.Fatalf("batch splitting reordered member %d: base=%+v member=%+v", i+1, baseAck.Fence, final.Fence)
				}
			}
			_, syncAfter, currentAfter, _ := io.counts()
			if currentAfter-currentBefore != test.wantCycles || syncAfter-syncBefore != test.wantCycles {
				t.Fatalf("batch limit used wrong publication count: CURRENT=%d sync=%d; want %d", currentAfter-currentBefore, syncAfter-syncBefore, test.wantCycles)
			}
			if test.batch.MaxBytes != 0 {
				for i, size := range io.appendSizes() {
					if size > test.batch.MaxBytes {
						t.Fatalf("physical group %d exceeded MaxBytes: %d > %d", i+1, size, test.batch.MaxBytes)
					}
				}
			}
			read, err := e.GetState(context.Background(), api.ReadStateRequest{Space: late.Space, Key: []byte("order")})
			if err != nil || !read.Found || string(read.Value.Data) != fmt.Sprint(test.queued+1) || read.Fence != final.Fence {
				t.Fatalf("FIFO condition chain did not reach its final value: %+v, %v", read, err)
			}
		})
	}
}

func TestCoordinatorLastWaiterCancellationPreservesQueuedCommandAndOwnsInput(t *testing.T) {
	e, _ := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	snapshot, err := e.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	req := coordinatorRequest(t, "owned-input", "key", "original-value", api.ValueCondition{})
	req.ExpectedFence = &snapshot.Fence
	owned := req
	owned.Body = bytes.Clone(req.Body)
	fenceCopy := *req.ExpectedFence
	owned.ExpectedFence = &fenceCopy
	digest, err := CommandDigest(owned)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	locked := true
	defer func() {
		if locked {
			e.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := coordinatorCommit(e, ctx, req)
	coordinatorFlightWaiters(t, e, req, 1)
	lookup := coordinatorLookup(t, e, req)
	if lookup.Code != api.CodeInFlight || lookup.Result != (api.CommitResult{}) {
		t.Fatalf("queued request was reported absent or published: %+v", lookup)
	}
	cancel()
	coordinatorInterrupted(t, coordinatorWait(t, interrupted), context.Canceled)
	coordinatorFlightWaiters(t, e, req, 0)
	// Ownership has transferred at admission, even when nobody is waiting.
	req.Body[len(req.Body)-1] ^= 1
	req.ExpectedFence.LSN++
	e.mu.Unlock()
	locked = false
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), owned)))
	if ack.RequestDigest != digest {
		t.Fatalf("caller mutation changed the admitted command digest: %+v", ack)
	}
	read, err := e.GetState(context.Background(), api.ReadStateRequest{Space: owned.Space, Key: []byte("key")})
	if err != nil || !read.Found || !bytes.Equal(read.Value.Data, []byte("original-value")) {
		t.Fatalf("caller mutation changed the admitted command body: %+v, %v", read, err)
	}
	if got := coordinatorLookup(t, e, owned); got.Code != api.CodeDurableAck || got.Result != ack {
		t.Fatalf("zero-waiter command did not retain a durable receipt: %+v", got)
	}
}

func TestCoordinatorBatchUsesPredecessorStagingAndOneCurrent(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 16}})
	_, syncBefore, currentBefore, _ := io.counts()
	g := newCoordinatorGate()
	io.arm("current", g)
	baseReq := coordinatorRequest(t, "batch-base", "key", "zero", api.ValueCondition{})
	base := coordinatorCommit(e, context.Background(), baseReq)
	coordinatorWait(t, g.entered)
	oneReq := coordinatorRequest(t, "batch-one", "key", "one", api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte("zero")}})
	twoReq := coordinatorRequest(t, "batch-two", "key", "two", api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte("one")}})
	one := coordinatorCommit(e, context.Background(), oneReq)
	coordinatorQueued(t, e, oneReq)
	two := coordinatorCommit(e, context.Background(), twoReq)
	coordinatorQueued(t, e, oneReq, twoReq)
	g.unblock()
	baseAck := coordinatorAck(t, coordinatorWait(t, base))
	oneAck := coordinatorAck(t, coordinatorWait(t, one))
	twoAck := coordinatorAck(t, coordinatorWait(t, two))
	if oneAck.Fence.LSN != baseAck.Fence.LSN+1 || twoAck.Fence.LSN != oneAck.Fence.LSN+1 || oneAck.Fence.Store != twoAck.Fence.Store || oneAck.RootSet == twoAck.RootSet {
		t.Fatalf("batch did not preserve per-member historical publications: base=%+v one=%+v two=%+v", baseAck, oneAck, twoAck)
	}
	_, syncAfter, currentAfter, _ := io.counts()
	if syncAfter-syncBefore != 2 || currentAfter-currentBefore != 2 {
		t.Fatalf("base plus queued two-member batch must use two sync/CURRENT cycles, got sync=%d CURRENT=%d", syncAfter-syncBefore, currentAfter-currentBefore)
	}
	for _, check := range []struct {
		req api.CommitRequest
		ack api.CommitResult
	}{{oneReq, oneAck}, {twoReq, twoAck}} {
		got := coordinatorLookup(t, e, check.req)
		if got.Code != api.CodeDurableAck || got.Result != check.ack || got.ObservedFence != twoAck.Fence {
			t.Fatalf("historical receipt was replaced by batch-final roots: %+v; want %+v", got, check.ack)
		}
	}
	read, err := e.GetState(context.Background(), api.ReadStateRequest{Space: twoReq.Space, Key: []byte("key")})
	if err != nil || !read.Found || !bytes.Equal(read.Value.Data, []byte("two")) || read.Fence != twoAck.Fence {
		t.Fatalf("successor did not read and replace staged predecessor state: %+v, %v", read, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenExisting(io, Options{Batch: BatchOptions{MaxCount: 16}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if !report.Verified || !report.PublicationSynced || report.ScannedTransactions != 3 {
		t.Fatalf("grouped state did not replay from CURRENT: %+v", report)
	}
	for _, check := range []struct {
		req api.CommitRequest
		ack api.CommitResult
	}{{baseReq, baseAck}, {oneReq, oneAck}, {twoReq, twoAck}} {
		if got := coordinatorLookup(t, reopened, check.req); got.Code != api.CodeDurableAck || got.Result != check.ack || got.ObservedFence != twoAck.Fence {
			t.Fatalf("replay changed a batch member's historical acknowledgment: %+v; want %+v", got, check.ack)
		}
	}
}

func TestCoordinatorRejectedPredecessorDoesNotLeakIntoStaging(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 16}})
	g := newCoordinatorGate()
	io.arm("current", g)
	base := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "reject-base", "key", "zero", api.ValueCondition{}))
	coordinatorWait(t, g.entered)
	badReq := coordinatorRequest(t, "reject-predecessor", "key", "ghost", api.ValueCondition{Kind: api.ConditionAbsent})
	dependentReq := coordinatorRequest(t, "reject-dependent", "key", "impossible", api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte("ghost")}})
	validReq := coordinatorRequest(t, "reject-valid", "key", "valid", api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte("zero")}})
	bad := coordinatorCommit(e, context.Background(), badReq)
	coordinatorQueued(t, e, badReq)
	dependent := coordinatorCommit(e, context.Background(), dependentReq)
	coordinatorQueued(t, e, badReq, dependentReq)
	valid := coordinatorCommit(e, context.Background(), validReq)
	coordinatorQueued(t, e, badReq, dependentReq, validReq)
	g.unblock()
	baseAck := coordinatorAck(t, coordinatorWait(t, base))
	for _, ch := range []<-chan coordinatorOutcome{bad, dependent} {
		got := coordinatorWait(t, ch)
		if got.err != nil || got.result.Code != api.CodeConflict || got.result.Conflict != api.ConflictPrecondition || got.result.Fence != (model.CommitFence{}) || got.result.RootSet != "" {
			t.Fatalf("rejected staged predecessor/dependent leaked a publication: %+v, %v", got.result, got.err)
		}
	}
	validAck := coordinatorAck(t, coordinatorWait(t, valid))
	if validAck.Fence.LSN != baseAck.Fence.LSN+1 {
		t.Fatalf("rejected CAS consumed log positions: base=%+v valid=%+v", baseAck, validAck)
	}
	for _, req := range []api.CommitRequest{badReq, dependentReq} {
		if got := coordinatorLookup(t, e, req); got.Code != api.CodeNotFoundAtFence {
			t.Fatalf("rejected CAS retained a receipt or flight: %+v", got)
		}
	}
	// The rejected identity remains reusable with a different digest.
	retry := coordinatorRequest(t, string(badReq.RequestID), "key", "retry", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), retry)))
}

func TestCoordinatorStagedPreconditionDecisionWaitsForPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "durable"
		if fail {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 16}})
			baseGate := newCoordinatorGate()
			io.arm("current", baseGate)
			base := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "decision-base", "unrelated", "base", api.ValueCondition{}))
			coordinatorWait(t, baseGate.entered)
			predecessorReq := coordinatorRequest(t, "decision-predecessor", "candidate", "one", api.ValueCondition{})
			dependentReq := coordinatorRequest(t, "decision-dependent", "candidate", "two", api.ValueCondition{Kind: api.ConditionAbsent})
			predecessor := coordinatorCommit(e, context.Background(), predecessorReq)
			coordinatorQueued(t, e, predecessorReq)
			dependent := coordinatorCommit(e, context.Background(), dependentReq)
			coordinatorQueued(t, e, predecessorReq, dependentReq)
			groupGate := newCoordinatorGate()
			if fail {
				groupGate.err = errors.New("test: candidate publication failed")
			}
			io.arm("current", groupGate)
			baseGate.unblock()
			coordinatorAck(t, coordinatorWait(t, base))
			coordinatorWait(t, groupGate.entered)
			select {
			case got := <-dependent:
				t.Fatalf("decision based on unpublished predecessor escaped the batch: %+v, %v", got.result, got.err)
			default:
			}
			if got := coordinatorLookup(t, e, dependentReq); got.Code != api.CodeInFlight {
				t.Fatalf("dependent decision ceased being in flight before publication: %+v", got)
			}
			groupGate.unblock()
			first, second := coordinatorWait(t, predecessor), coordinatorWait(t, dependent)
			if fail {
				if first.result.Code != api.CodeCommitUnknown || !errors.Is(first.err, storage.ErrCommitUnknown) {
					t.Fatalf("predecessor did not become unknown: %+v, %v", first.result, first.err)
				}
				if second.result.Code != api.CodeStoreFenced || !errors.Is(second.err, storage.ErrStoreFenced) || second.result.Conflict != "" {
					t.Fatalf("uncertain predecessor leaked a staged precondition decision: %+v, %v", second.result, second.err)
				}
			} else {
				coordinatorAck(t, first)
				if second.err != nil || second.result.Code != api.CodeConflict || second.result.Conflict != api.ConflictPrecondition {
					t.Fatalf("durable predecessor lost its dependent precondition decision: %+v, %v", second.result, second.err)
				}
			}
		})
	}
}

func TestCoordinatorUnknownBatchFencesAllMembersAndQueuedSuccessors(t *testing.T) {
	for _, boundary := range []struct {
		name  string
		kind  string
		after bool
	}{{"segment-write", "write", false}, {"segment-sync", "sync", false}, {"current-before", "current", false}, {"current-after", "current", true}} {
		t.Run(boundary.name, func(t *testing.T) {
			e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 16}})
			baseGate := newCoordinatorGate()
			io.arm("current", baseGate)
			base := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "fault-base", "base", "base", api.ValueCondition{}))
			coordinatorWait(t, baseGate.entered)
			oneReq := coordinatorRequest(t, "fault-one", "one", "one", api.ValueCondition{})
			twoReq := coordinatorRequest(t, "fault-two", "two", "two", api.ValueCondition{})
			one := coordinatorCommit(e, context.Background(), oneReq)
			coordinatorQueued(t, e, oneReq)
			two := coordinatorCommit(e, context.Background(), twoReq)
			coordinatorQueued(t, e, oneReq, twoReq)
			fault := newCoordinatorGate()
			fault.err = errors.New("test: grouped publication IO failed")
			fault.after = boundary.after
			io.arm(boundary.kind, fault)
			baseGate.unblock()
			coordinatorAck(t, coordinatorWait(t, base))
			coordinatorWait(t, fault.entered)
			for _, req := range []api.CommitRequest{oneReq, twoReq} {
				if got := coordinatorLookup(t, e, req); got.Code != api.CodeInFlight || got.Result != (api.CommitResult{}) {
					t.Fatalf("unpublished batch member exposed absence/old publication: %+v", got)
				}
			}
			successorReq := coordinatorRequest(t, "fault-successor", "later", "later", api.ValueCondition{})
			successor := coordinatorCommit(e, context.Background(), successorReq)
			coordinatorQueued(t, e, successorReq)
			fault.unblock()
			for _, ch := range []<-chan coordinatorOutcome{one, two} {
				got := coordinatorWait(t, ch)
				if got.result.Code != api.CodeCommitUnknown || !errors.Is(got.err, storage.ErrCommitUnknown) || !errors.Is(got.err, fault.err) {
					t.Fatalf("batch uncertainty was split into inconsistent outcomes: %+v, %v", got.result, got.err)
				}
				if got.result.Fence != (model.CommitFence{}) || got.result.RootSet != "" || got.result.StateRoot != "" || got.result.ResultDigest != "" {
					t.Fatalf("unknown result leaked unpublished or stale roots: %+v", got.result)
				}
			}
			if got := coordinatorWait(t, successor); got.result.Code != api.CodeStoreFenced || !errors.Is(got.err, storage.ErrStoreFenced) {
				t.Fatalf("queued successor survived an unknown group: %+v, %v", got.result, got.err)
			}
			for _, req := range []api.CommitRequest{oneReq, twoReq, successorReq} {
				got, err := e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID})
				if !errors.Is(err, storage.ErrStoreFenced) || got.Code == api.CodeNotFoundAtFence || got.Code == api.CodeDurableAck || got.Result != (api.CommitResult{}) || got.ObservedFence != (model.CommitFence{}) {
					t.Fatalf("fenced lookup exposed absence or an old publication: %+v, %v", got, err)
				}
			}
			if got, err := e.Snapshot(context.Background()); !errors.Is(err, storage.ErrStoreFenced) || got != (api.StateSnapshot{}) {
				t.Fatalf("fenced snapshot returned old roots: %+v, %v", got, err)
			}
			if got, err := e.GetState(context.Background(), api.ReadStateRequest{Space: oneReq.Space, Key: []byte("one")}); !errors.Is(err, storage.ErrStoreFenced) || got.Found || got.RootSet != "" || got.Fence != (model.CommitFence{}) {
				t.Fatalf("fenced state read returned old/unpublished state: %+v, %v", got, err)
			}
		})
	}
}

func TestCoordinatorCloseContextStopsAdmissionAndDrainsAfterCancellation(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("sync", g)
	req := coordinatorRequest(t, "accepted-before-close", "key", "value", api.ValueCondition{})
	accepted := coordinatorCommit(e, context.Background(), req)
	coordinatorWait(t, g.entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- e.CloseContext(ctx) }()
	if err := coordinatorWait(t, closeDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseContext did not cancel only its own wait: %v", err)
	}
	_, _, _, closes := io.counts()
	if closes != 0 {
		t.Fatalf("owner lock released before accepted work drained: %d", closes)
	}
	rejected := coordinatorWait(t, coordinatorCommit(e, context.Background(), coordinatorRequest(t, "after-close", "other", "value", api.ValueCondition{})))
	if rejected.result.Code != api.CodeNotSubmitted || !errors.Is(rejected.err, storage.ErrClosing) {
		t.Fatalf("new request was admitted after close began: %+v, %v", rejected.result, rejected.err)
	}
	for _, name := range []string{"snapshot", "lookup", "get"} {
		done := make(chan error, 1)
		go func() {
			var err error
			switch name {
			case "snapshot":
				_, err = e.Snapshot(context.Background())
			case "lookup":
				_, err = e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID})
			case "get":
				_, err = e.GetState(context.Background(), api.ReadStateRequest{Space: req.Space, Key: []byte("key")})
			}
			done <- err
		}()
		if err := coordinatorWait(t, done); !errors.Is(err, storage.ErrClosing) {
			t.Fatalf("new %s did not observe closing: %v", name, err)
		}
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, accepted))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, _, closes = io.counts()
	if closes != 1 {
		t.Fatalf("owner must retire exactly once after draining: %d", closes)
	}
}

func TestCoordinatorCancelingOneCloseWaitDoesNotCancelAnother(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	g := newCoordinatorGate()
	io.arm("write", g)
	accepted := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "close-waiters", "key", "value", api.ValueCondition{}))
	coordinatorWait(t, g.entered)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := make(chan error, 1)
	go func() { interrupted <- e.CloseContext(ctx) }()
	coordinatorUntil(t, e.draining.Load)
	other := make(chan error, 1)
	go func() { other <- e.Close() }()
	cancel()
	if err := coordinatorWait(t, interrupted); !errors.Is(err, context.Canceled) {
		t.Fatalf("one close waiter could not leave the drain: %v", err)
	}
	select {
	case err := <-other:
		t.Fatalf("another close waiter returned before the accepted command drained: %v", err)
	default:
	}
	_, _, _, closes := io.counts()
	if closes != 0 {
		t.Fatalf("canceled close waiter released owner lock: %d", closes)
	}
	g.unblock()
	coordinatorAck(t, coordinatorWait(t, accepted))
	if err := coordinatorWait(t, other); err != nil {
		t.Fatal(err)
	}
	_, _, _, closes = io.counts()
	if closes != 1 {
		t.Fatalf("concurrent close waiters retired owner %d times", closes)
	}
}
