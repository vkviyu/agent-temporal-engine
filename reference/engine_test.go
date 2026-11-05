package reference_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/reference"
)

func command(id string) api.CommitRequest {
	return api.CommitRequest{Space: "s", Principal: "p", RequestID: model.RequestID(id), Method: "record", Body: []byte("body")}
}

func commit(t *testing.T, e *reference.Engine, req api.CommitRequest) api.CommitResult {
	t.Helper()
	result, err := e.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRequestNamespaceIsolation(t *testing.T) {
	e := reference.New()
	original := command("same")
	first := commit(t, e, original)
	if first.Code != api.CodeVolatileAck {
		t.Fatalf("commit code: %s", first.Code)
	}
	for _, namespace := range []string{"space", "principal"} {
		t.Run(namespace, func(t *testing.T) {
			req := original
			if namespace == "space" {
				req.Space = "other-space"
			} else {
				req.Principal = "other-principal"
			}
			lookup, err := e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID})
			if err != nil {
				t.Fatal(err)
			}
			if lookup.Code != api.CodeVolatileNotFound || lookup.Result != (api.CommitResult{}) {
				t.Fatalf("receipt leaked: %+v", lookup)
			}
			if lookup.ObservedFence != e.Fence() {
				t.Fatal("absence was not bound to current volatile prefix")
			}
			got := commit(t, e, req)
			if got.Code != api.CodeVolatileAck || got.TxID == first.TxID {
				t.Fatalf("independent command not committed: %+v", got)
			}
		})
	}
	lookup, err := e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: original.Space, Principal: original.Principal, RequestID: original.RequestID})
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Result != first || lookup.Code != api.CodeVolatileAck {
		t.Fatalf("original receipt changed: %+v", lookup)
	}
}

func TestReceiptPrecedesCASAndChangedExpectedFenceConflicts(t *testing.T) {
	e := reference.New()
	initial := e.Fence()
	req := command("first")
	req.ExpectedFence = &initial
	first := commit(t, e, req)
	if first.Code != api.CodeVolatileAck {
		t.Fatalf("initial commit: %+v", first)
	}
	commit(t, e, command("second"))
	before := e.Fence()
	if retry := commit(t, e, req); retry != first {
		t.Fatalf("retry changed receipt: %+v", retry)
	}
	current := e.Fence()
	changed := req
	changed.ExpectedFence = &current
	conflict := commit(t, e, changed)
	if conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
		t.Fatalf("changed condition accepted: %+v", conflict)
	}
	changed = req
	changed.Body = []byte("different")
	conflict = commit(t, e, changed)
	if conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
		t.Fatalf("changed body accepted: %+v", conflict)
	}
	if e.Fence() != before {
		t.Fatal("retry/conflict advanced prefix")
	}
}

func TestStaleCASDoesNotReserveRequestKey(t *testing.T) {
	e := reference.New()
	initial := e.Fence()
	commit(t, e, command("advance"))
	before := e.Fence()
	req := command("retry-after-rejection")
	req.ExpectedFence = &initial
	got := commit(t, e, req)
	if got.Code != api.CodeConflict || got.Conflict != api.ConflictPrecondition {
		t.Fatalf("stale CAS result: %+v", got)
	}
	if e.Fence() != before {
		t.Fatal("rejected CAS advanced prefix")
	}
	lookup, err := e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID})
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Code != api.CodeVolatileNotFound {
		t.Fatalf("rejected command reserved key: %+v", lookup)
	}
	// This probes absence of a receipt; callers correcting rejected requests still
	// follow the protocol rule requiring a fresh RequestID.
	req.ExpectedFence = &before
	if got := commit(t, e, req); got.Code != api.CodeVolatileAck {
		t.Fatalf("key was consumed: %+v", got)
	}
}

func TestCancellationBeforeAdmissionDoesNotAdvance(t *testing.T) {
	e := reference.New()
	before := e.Fence()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := command("canceled")
	got, err := e.Commit(ctx, req)
	if !errors.Is(err, context.Canceled) || got.Code != api.CodeNotSubmitted {
		t.Fatalf("cancellation: %+v, %v", got, err)
	}
	if e.Fence() != before {
		t.Fatal("canceled call advanced prefix")
	}
	if got := commit(t, e, req); got.Code != api.CodeVolatileAck {
		t.Fatalf("canceled call reserved key: %+v", got)
	}
}

func TestInstancesHaveIndependentFences(t *testing.T) {
	a, b := reference.New(), reference.New()
	if a.Fence().Store == b.Fence().Store {
		t.Fatal("engines share store identity")
	}
	foreign := a.Fence()
	req := command("foreign")
	req.ExpectedFence = &foreign
	before := b.Fence()
	got := commit(t, b, req)
	if got.Code != api.CodeConflict || got.Conflict != api.ConflictPrecondition || b.Fence() != before {
		t.Fatalf("foreign fence accepted: %+v", got)
	}
}

func TestConcurrentRetriesCommitOnce(t *testing.T) {
	e := reference.New()
	before := e.Fence()
	req := command("concurrent")
	req.ExpectedFence = &before
	const count = 32
	results := make(chan api.CommitResult, count)
	errs := make(chan error, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := e.Commit(context.Background(), req)
			results <- got
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first api.CommitResult
	for got := range results {
		if got.Code != api.CodeVolatileAck {
			t.Fatalf("retry rejected: %+v", got)
		}
		if first == (api.CommitResult{}) {
			first = got
		} else if got != first {
			t.Fatalf("multiple receipts: %+v vs %+v", first, got)
		}
	}
	if e.Fence() != first.Fence || first.Fence.LSN != before.LSN+1 {
		t.Fatal("concurrent retries advanced more than once")
	}
}
