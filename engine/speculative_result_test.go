package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Both calls are admitted before releasing planning. Waiting for selection
// establishes that the worker collected both into the same two-member batch.
func speculativePair(t *testing.T, e *Engine, first, second api.CommitRequest) (<-chan coordinatorOutcome, <-chan coordinatorOutcome, *flight, *flight) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	one := coordinatorCommit(e, context.Background(), first)
	coordinatorFlightWaiters(t, e, first, 1)
	two := coordinatorCommit(e, context.Background(), second)
	coordinatorFlightWaiters(t, e, second, 1)
	var firstFlight, secondFlight *flight
	coordinatorUntil(t, func() bool {
		e.admission.mu.Lock()
		defer e.admission.mu.Unlock()
		firstFlight = e.admission.flights[first.Key()]
		secondFlight = e.admission.flights[second.Key()]
		return firstFlight != nil && secondFlight != nil && firstFlight.phase == "selected" && secondFlight.phase == "selected"
	})
	return one, two, firstFlight, secondFlight
}

func speculativePending(t *testing.T, f *flight) {
	t.Helper()
	select {
	case <-f.done:
		t.Fatalf("unpublished predecessor resolved its dependent early: %+v, %v", f.result, f.err)
	default:
	}
}

func speculativeNoProof(t *testing.T, result api.CommitResult) {
	t.Helper()
	if result.TxID != "" || result.Fence != (model.CommitFence{}) || result.RootSet != "" || result.StateRoot != "" || result.ResultDigest != "" {
		t.Fatalf("unsubmitted dependent exposed a candidate publication: %+v", result)
	}
}

func TestCoordinatorSpeculativeDecisionsWaitForPublication(t *testing.T) {
	for _, decision := range []string{"CAS", "objects", "journal-bytes", "journal-transactions"} {
		for _, failAppend := range []bool{false, true} {
			name := decision + "/success"
			if failAppend {
				name = decision + "/append-failure"
			}
			t.Run(name, func(t *testing.T) {
				firstReq := coordinatorRequest(t, "speculative-a", "key", "one", api.ValueCondition{})
				secondReq := coordinatorRequest(t, "speculative-b", "alt", "two", api.ValueCondition{})
				options := Options{Batch: BatchOptions{MaxCount: 2, MaxDelay: time.Second}}
				switch decision {
				case "CAS":
					secondReq = coordinatorRequest(t, "speculative-b", "key", "two", api.ValueCondition{Kind: api.ConditionAbsent})
				case "objects", "journal-bytes":
					// Each request fits by itself. Only the predecessor's staged
					// objects/encoded bytes make the dependent exceed the budget.
					firstPlan, firstPrepared := stagingPrepare(t, stagingEngine(t), firstReq, "a")
					secondPlan, secondPrepared := stagingPrepare(t, stagingEngine(t), secondReq, "b")
					if decision == "objects" {
						options.MaxObjects = uint64(len(firstPlan.builder.objects))
						if uint64(len(secondPlan.builder.objects)) > options.MaxObjects {
							t.Fatal("second request must fit the published-only object budget")
						}
					} else {
						options.Journal.MaxPublishedBytes = storage.SegmentHeaderSize + firstPrepared.Size()
						if secondPrepared.Size() > firstPrepared.Size() {
							t.Fatal("second request must fit the published-only journal byte budget")
						}
					}
				case "journal-transactions":
					options.Journal.MaxPublishedTransactions = 1
				}
				e, io := coordinatorStore(t, options)
				appendGate := newCoordinatorGate()
				if failAppend {
					appendGate.err = errors.New("test: first append failed without writing bytes")
				}
				io.arm("write", appendGate)
				var currentGate *coordinatorGate
				if !failAppend {
					currentGate = newCoordinatorGate()
					io.arm("current", currentGate)
				}
				first, second, firstFlight, secondFlight := speculativePair(t, e, firstReq, secondReq)
				coordinatorWait(t, appendGate.entered)
				speculativePending(t, firstFlight)
				speculativePending(t, secondFlight)
				if got := coordinatorLookup(t, e, secondReq); got.Code != api.CodeInFlight || got.Result != (api.CommitResult{}) {
					t.Fatalf("dependent lost its flight before publication: %+v", got)
				}
				appendGate.unblock()
				if currentGate != nil {
					coordinatorWait(t, currentGate.entered)
					speculativePending(t, firstFlight)
					speculativePending(t, secondFlight)
					currentGate.unblock()
				}
				firstResult := coordinatorWait(t, first)
				secondResult := coordinatorWait(t, second)
				speculativeNoProof(t, secondResult.result)
				if !failAppend {
					firstAck := coordinatorAck(t, firstResult)
					if firstAck.Fence.LSN != 1 {
						t.Fatalf("rejected dependent consumed a position: %+v", firstAck)
					}
					if decision == "CAS" {
						if secondResult.err != nil || secondResult.result.Code != api.CodeConflict || secondResult.result.Conflict != api.ConflictPrecondition {
							t.Fatalf("published predecessor did not resolve deferred CAS: %+v, %v", secondResult.result, secondResult.err)
						}
					} else if secondResult.result.Code != api.CodeResourceLimit || !errors.Is(secondResult.err, storage.ErrLimit) {
						t.Fatalf("published predecessor did not resolve deferred quota: %+v, %v", secondResult.result, secondResult.err)
					}
					if got := coordinatorLookup(t, e, secondReq); got.Code != api.CodeNotFoundAtFence || got.ObservedFence != firstAck.Fence {
						t.Fatalf("rejected dependent retained a receipt: %+v", got)
					}
					return
				}
				if firstResult.result.Code != api.CodeCommitUnknown || !errors.Is(firstResult.err, storage.ErrCommitUnknown) || !errors.Is(firstResult.err, appendGate.err) {
					t.Fatalf("predecessor failure lost its unknown outcome: %+v, %v", firstResult.result, firstResult.err)
				}
				if secondResult.result.Code != api.CodeStoreFenced || !errors.Is(secondResult.err, storage.ErrStoreFenced) || !errors.Is(secondResult.err, appendGate.err) || secondResult.result.Conflict != "" {
					t.Fatalf("unpublished predecessor resolved a speculative rejection: %+v, %v", secondResult.result, secondResult.err)
				}
				if err := e.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, report, err := OpenExisting(io, options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := reopened.Close(); err != nil {
						t.Error(err)
					}
				})
				if report.Publication.Position.LSN != 0 || report.ScannedTransactions != 0 || reopened.index.count() != 0 {
					t.Fatalf("failed first append exposed a speculative prefix: %+v", report)
				}
				for _, req := range []api.CommitRequest{firstReq, secondReq} {
					if got := coordinatorLookup(t, reopened, req); got.Code != api.CodeNotFoundAtFence || got.ObservedFence.LSN != 0 {
						t.Fatalf("failed speculative batch retained a receipt: %+v", got)
					}
				}
			})
		}
	}
}
