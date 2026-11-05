package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type integrationStore struct {
	t       *testing.T
	d       *storage.DirectoryIO
	e       *engine.Engine
	options engine.Options
}

func newIntegrationStore(t *testing.T, options engine.Options) *integrationStore {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native durability profile is supported on Darwin and Linux")
	}
	d, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &integrationStore{t: t, d: d, options: options}
	t.Cleanup(func() {
		if s.e != nil {
			if err := s.e.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := s.d.Close(); err != nil {
			t.Error(err)
		}
	})
	s.e, err = engine.Create(d, options)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *integrationStore) reopen() storage.RecoveryReport {
	s.t.Helper()
	if err := s.e.Close(); err != nil {
		s.t.Fatal(err)
	}
	s.e = nil
	e, report, err := engine.OpenExisting(s.d, s.options)
	if err != nil {
		s.t.Fatal(err)
	}
	s.e = e
	if !report.Verified || !report.PublicationSynced {
		s.t.Fatalf("reopen did not verify and sync its publication: %+v", report)
	}
	return report
}

func integrationRequest(id string, operations ...api.StateMutation) api.MutateRequest {
	return api.MutateRequest{
		Key:   model.RequestKey{Space: "host-space", Principal: "host-principal", RequestID: model.RequestID(id)},
		Batch: api.StateBatch{Operations: operations},
	}
}

func integrationPut(key []byte, value api.Value) api.StateMutation {
	return api.StateMutation{Key: key, Kind: api.MutationPut, Value: value}
}

func integrationBytes(data ...byte) api.Value { return api.Value{Kind: api.ValueBytes, Data: data} }

func integrationMutate(t *testing.T, e *engine.Engine, request api.MutateRequest) api.CommitResult {
	t.Helper()
	got, err := e.Mutate(context.Background(), request)
	if err != nil || got.Code != api.CodeDurableAck {
		t.Fatalf("mutation did not commit: %+v, %v", got, err)
	}
	if err := got.Fence.Validate(); err != nil {
		t.Fatal(err)
	}
	if !model.ValidDigest(string(got.RootSet)) || !model.ValidDigest(string(got.RequestDigest)) || !model.ValidDigest(string(got.ResultDigest)) {
		t.Fatalf("durable ack omitted content identities: %+v", got)
	}
	return got
}

func integrationSnapshot(t *testing.T, e *engine.Engine) api.StateSnapshot {
	t.Helper()
	got, err := e.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func integrationLookup(t *testing.T, e *engine.Engine, key model.RequestKey) api.LookupRequestResult {
	t.Helper()
	got, err := e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: key.Space, Principal: key.Principal, RequestID: key.RequestID})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func integrationRead(t *testing.T, e *engine.Engine, space model.SpaceID, key []byte, found bool, want api.Value) api.StateValueResult {
	t.Helper()
	got, err := e.GetState(context.Background(), api.ReadStateRequest{Space: space, Key: key})
	if err != nil || got.Found != found || got.Value.Kind != want.Kind || !bytes.Equal(got.Value.Data, want.Data) {
		t.Fatalf("GetState(%q,%x) = %+v, %v; want found=%v value=%+v", space, key, got, err, found, want)
	}
	return got
}

func TestDurableStateLifecycleAndHistoricalReceipts(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	initial := integrationSnapshot(t, s.e)
	emptyRoot := model.ObjectID("")
	request := integrationRequest("first",
		integrationPut(nil, api.Value{Kind: api.ValueNull}),
		integrationPut([]byte("a"), integrationBytes()),
		integrationPut([]byte("ab"), integrationBytes('c', 'h', 'i', 'l', 'd')),
		integrationPut([]byte{0, 255, 0}, integrationBytes(0, 255)),
		api.StateMutation{Key: []byte("missing"), Kind: api.MutationDelete},
	)
	request.ExpectedFence = &initial.Fence
	request.Batch.ExpectedRoot = &emptyRoot
	first := integrationMutate(t, s.e, request)
	if first.Fence.LSN != initial.Fence.LSN+1 || first.StateRoot == "" {
		t.Fatalf("bad first publication: %+v", first)
	}
	for _, check := range []struct {
		key   []byte
		value api.Value
	}{
		{nil, api.Value{Kind: api.ValueNull}},
		{[]byte("a"), integrationBytes()},
		{[]byte("ab"), integrationBytes('c', 'h', 'i', 'l', 'd')},
		{[]byte{0, 255, 0}, integrationBytes(0, 255)},
	} {
		got := integrationRead(t, s.e, request.Key.Space, check.key, true, check.value)
		if got.StateRoot != first.StateRoot || got.RootSet != first.RootSet || got.Fence != first.Fence {
			t.Fatal("state read was not bound to its published roots and fence")
		}
	}
	integrationRead(t, s.e, request.Key.Space, []byte("missing"), false, api.Value{})
	oldSnapshot := integrationSnapshot(t, s.e)
	savedSnapshot := oldSnapshot
	if oldSnapshot.RootSet != first.RootSet || oldSnapshot.Roots.DomainStates == "" || oldSnapshot.Roots.CommandReceipts == "" {
		t.Fatalf("snapshot omitted state/receipt roots: %+v", oldSnapshot)
	}
	secondRequest := integrationRequest("second",
		integrationPut(nil, integrationBytes()),
		api.StateMutation{Key: []byte("a"), Kind: api.MutationDelete},
	)
	second := integrationMutate(t, s.e, secondRequest)
	if second.RootSet == first.RootSet || second.StateRoot == first.StateRoot || oldSnapshot != savedSnapshot {
		t.Fatal("new mutation failed to replace current roots or changed an old snapshot value")
	}
	// A stale fence/root and now-failing conditions do not supersede a receipt.
	if retry := integrationMutate(t, s.e, request); retry != first {
		t.Fatalf("retry changed historical ack: %+v != %+v", retry, first)
	}
	report := s.reopen()
	if report.ScannedTransactions != 2 {
		t.Fatalf("retry wrote another transaction: %+v", report)
	}
	for _, check := range []struct {
		request api.MutateRequest
		ack     api.CommitResult
	}{{request, first}, {secondRequest, second}} {
		lookup := integrationLookup(t, s.e, check.request.Key)
		if lookup.Code != api.CodeDurableAck || lookup.Result != check.ack || lookup.ObservedFence != second.Fence {
			t.Fatalf("reopen changed receipt: %+v != %+v", lookup, check.ack)
		}
	}
	if retry := integrationMutate(t, s.e, request); retry != first {
		t.Fatal("replayed receipt did not return its original roots and fence")
	}
	integrationRead(t, s.e, request.Key.Space, nil, true, integrationBytes())
	integrationRead(t, s.e, request.Key.Space, []byte("a"), false, api.Value{})
	integrationRead(t, s.e, request.Key.Space, []byte("ab"), true, integrationBytes('c', 'h', 'i', 'l', 'd'))
	if got := integrationSnapshot(t, s.e); got.RootSet != second.RootSet || got.Fence != second.Fence || got == oldSnapshot {
		t.Fatalf("reopen did not reconstruct current snapshot: %+v", got)
	}
}

func TestHistoricalViewEntriesReturnsAuthenticatedCompleteState(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	request := integrationRequest("entries", integrationPut([]byte("b"), integrationBytes('2')), integrationPut([]byte("a"), integrationBytes('1')))
	integrationMutate(t, s.e, request)
	view, err := s.e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "state"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref})
	got, err := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if got.Anchor != view.Anchor || got.Fence != view.Anchor.Fence || got.StateRoot == "" || len(got.Items) != 2 {
		t.Fatalf("entries metadata=%+v items=%+v", got, got.Items)
	}
	if string(got.Items[0].Key) != "a" || string(got.Items[1].Key) != "b" {
		t.Fatalf("entries not sorted: %+v", got.Items)
	}
	if got.Items[0].Value == "" || got.Items[1].Value == "" {
		t.Fatal("entry omitted authenticated value identity")
	}
	if _, err := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: view.Ref, Limits: api.StateEntriesLimits{MaxEntries: 1}}); err == nil {
		t.Fatal("limit returned partial state")
	}
}

func TestDurablePreconditionsAreAtomicAndDoNotConsumeRequestKey(t *testing.T) {
	for _, condition := range []string{"fence", "root", "absent", "value-null-versus-empty"} {
		t.Run(condition, func(t *testing.T) {
			s := newIntegrationStore(t, engine.Options{})
			initial := integrationSnapshot(t, s.e)
			seed := integrationMutate(t, s.e, integrationRequest("seed", integrationPut([]byte("target"), api.Value{Kind: api.ValueNull})))
			before := integrationSnapshot(t, s.e)
			request := integrationRequest("retry-after-conflict",
				integrationPut([]byte("earlier"), integrationBytes('a')),
				integrationPut([]byte("target"), integrationBytes('b')),
			)
			empty := model.ObjectID("")
			switch condition {
			case "fence":
				request.ExpectedFence = &initial.Fence
			case "root":
				request.Batch.ExpectedRoot = &empty
			case "absent":
				request.Batch.Operations[1].Condition.Kind = api.ConditionAbsent
			case "value-null-versus-empty":
				request.Batch.Operations[1].Condition = api.ValueCondition{Kind: api.ConditionValue, Value: integrationBytes()}
			}
			got, err := s.e.Mutate(context.Background(), request)
			if err != nil || got.Code != api.CodeConflict || got.Conflict != api.ConflictPrecondition {
				t.Fatalf("precondition accepted: %+v, %v", got, err)
			}
			if after := integrationSnapshot(t, s.e); after != before {
				t.Fatal("failed precondition changed publication")
			}
			integrationRead(t, s.e, request.Key.Space, []byte("earlier"), false, api.Value{})
			integrationRead(t, s.e, request.Key.Space, []byte("target"), true, api.Value{Kind: api.ValueNull})
			lookup := integrationLookup(t, s.e, request.Key)
			if lookup.Code != api.CodeNotFoundAtFence || lookup.Result != (api.CommitResult{}) || lookup.ObservedFence != before.Fence {
				t.Fatalf("failed precondition reserved key: %+v", lookup)
			}
			request.ExpectedFence = &before.Fence
			request.Batch.ExpectedRoot = &seed.StateRoot
			request.Batch.Operations[1].Condition = api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueNull}}
			ack := integrationMutate(t, s.e, request)
			if ack.Fence.LSN != before.Fence.LSN+1 {
				t.Fatal("failed precondition wrote a transaction")
			}
		})
	}
}

func TestDurableReceiptPrecedesCASAndFullQuota(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{Journal: storage.JournalOptions{MaxPublishedTransactions: 1}})
	initial := integrationSnapshot(t, s.e)
	empty := model.ObjectID("")
	request := integrationRequest("only-transaction", integrationPut([]byte("key"), api.Value{Kind: api.ValueNull}))
	request.ExpectedFence = &initial.Fence
	request.Batch.ExpectedRoot = &empty
	request.Batch.Operations[0].Condition.Kind = api.ConditionAbsent
	first := integrationMutate(t, s.e, request)
	for pass := 0; pass < 2; pass++ {
		if pass == 1 {
			s.reopen()
		}
		if retry := integrationMutate(t, s.e, request); retry != first {
			t.Fatalf("full quota changed receipt: %+v", retry)
		}
		if lookup := integrationLookup(t, s.e, request.Key); lookup.Code != api.CodeDurableAck || lookup.Result != first {
			t.Fatalf("full quota hid receipt: %+v", lookup)
		}
		changed := request
		changed.Batch.Operations = []api.StateMutation{integrationPut([]byte("key"), integrationBytes())}
		conflict, err := s.e.Mutate(context.Background(), changed)
		if err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
			t.Fatalf("quota superseded idempotency conflict: %+v %v", conflict, err)
		}
		fresh := integrationRequest("quota-rejected", integrationPut([]byte("other"), integrationBytes('x')))
		limited, _ := s.e.Mutate(context.Background(), fresh)
		if limited.Code != api.CodeResourceLimit {
			t.Fatalf("quota not enforced: %+v", limited)
		}
		if lookup := integrationLookup(t, s.e, fresh.Key); lookup.Code != api.CodeNotFoundAtFence || lookup.Result != (api.CommitResult{}) {
			t.Fatalf("quota rejection reserved key: %+v", lookup)
		}
		if current := integrationSnapshot(t, s.e); current.Fence != first.Fence || current.RootSet != first.RootSet {
			t.Fatal("retries or quota rejection changed publication")
		}
	}
}

func TestDurableRequestNamespacesAndStateSpaces(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	original := integrationRequest("same", integrationPut([]byte("key"), integrationBytes('a')))
	original.Key.Space, original.Key.Principal = "host\x00space", "actor"
	first := integrationMutate(t, s.e, original)
	for _, key := range []model.RequestKey{
		{Space: "other", Principal: original.Key.Principal, RequestID: original.Key.RequestID},
		{Space: original.Key.Space, Principal: "other", RequestID: original.Key.RequestID},
		{Space: original.Key.Space, Principal: original.Key.Principal, RequestID: "other"},
		// These two namespace tuples collide under unsafe NUL joining.
		{Space: "host", Principal: "space\x00actor", RequestID: original.Key.RequestID},
	} {
		lookup := integrationLookup(t, s.e, key)
		if lookup.Code != api.CodeNotFoundAtFence || lookup.Result != (api.CommitResult{}) {
			t.Fatalf("receipt leaked across namespace: %+v", lookup)
		}
		if key.Space != original.Key.Space {
			integrationRead(t, s.e, key.Space, []byte("key"), false, api.Value{})
		}
		request := original
		request.Key = key
		ack := integrationMutate(t, s.e, request)
		if ack.TxID == first.TxID || ack.RequestDigest == first.RequestDigest {
			t.Fatalf("independent namespace reused receipt: %+v", ack)
		}
	}
	s.reopen()
	if lookup := integrationLookup(t, s.e, original.Key); lookup.Code != api.CodeDurableAck || lookup.Result != first {
		t.Fatalf("original namespace receipt changed: %+v", lookup)
	}
}

func TestDurableConcurrentIdenticalRequestsCommitOnce(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	initial := integrationSnapshot(t, s.e)
	request := integrationRequest("concurrent", integrationPut([]byte{0, 'k'}, integrationBytes('v')))
	request.ExpectedFence = &initial.Fence
	const count = 32
	type outcome struct {
		result api.CommitResult
		err    error
	}
	results := make(chan outcome, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := s.e.Mutate(context.Background(), request)
			results <- outcome{result, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var first api.CommitResult
	for got := range results {
		if got.err != nil || got.result.Code != api.CodeDurableAck {
			t.Fatalf("concurrent retry rejected: %+v %v", got.result, got.err)
		}
		if first == (api.CommitResult{}) {
			first = got.result
		} else if got.result != first {
			t.Fatalf("concurrent calls got different receipts: %+v != %+v", got.result, first)
		}
	}
	if snapshot := integrationSnapshot(t, s.e); snapshot.Fence != first.Fence || snapshot.Fence.LSN != initial.Fence.LSN+1 {
		t.Fatalf("identical concurrent requests committed more than once: %+v", snapshot)
	}
	if report := s.reopen(); report.ScannedTransactions != 1 {
		t.Fatalf("concurrent retries wrote extra transactions: %+v", report)
	}
	if lookup := integrationLookup(t, s.e, request.Key); lookup.Code != api.CodeDurableAck || lookup.Result != first {
		t.Fatalf("concurrent receipt was not durable: %+v", lookup)
	}
}

func TestDurableLowLevelCommitAndMutationNormalization(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	request := integrationRequest("canonical",
		integrationPut([]byte("z"), integrationBytes('z')),
		integrationPut(nil, api.Value{Kind: api.ValueNull}),
		integrationPut([]byte("a"), integrationBytes('a')),
	)
	first := integrationMutate(t, s.e, request)
	request.Batch.Operations[0], request.Batch.Operations[2] = request.Batch.Operations[2], request.Batch.Operations[0]
	if retry := integrationMutate(t, s.e, request); retry != first {
		t.Fatal("equivalent mutation order changed canonical request identity")
	}
	body, err := engine.EncodeStateBatch(request.Batch)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.e.Commit(context.Background(), api.CommitRequest{
		Space: request.Key.Space, Principal: request.Key.Principal, RequestID: request.Key.RequestID,
		Method: api.MethodMutateState, Body: body,
	})
	if err != nil || got != first {
		t.Fatalf("low-level Commit disagreed with Mutate: %+v, %v", got, err)
	}
}

func TestDurableDeletingLastKeyUsesEmptyRootAndReceiptsNoOp(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	first := integrationMutate(t, s.e, integrationRequest("put-only-key", integrationPut(nil, api.Value{Kind: api.ValueNull})))
	request := integrationRequest("delete-only-key", api.StateMutation{Kind: api.MutationDelete})
	deleted := integrationMutate(t, s.e, request)
	if deleted.StateRoot != "" || deleted.Fence.LSN != first.Fence.LSN+1 {
		t.Fatalf("last deletion did not publish empty state root: %+v", deleted)
	}
	integrationRead(t, s.e, request.Key.Space, nil, false, api.Value{})
	request.Key.RequestID = "delete-absent-key"
	noOp := integrationMutate(t, s.e, request)
	if noOp.StateRoot != "" || noOp.RootSet == deleted.RootSet || noOp.Fence.LSN != deleted.Fence.LSN+1 {
		t.Fatalf("delete absent did not commit its own receipt: %+v", noOp)
	}
	s.reopen()
	integrationRead(t, s.e, request.Key.Space, nil, false, api.Value{})
	if lookup := integrationLookup(t, s.e, request.Key); lookup.Code != api.CodeDurableAck || lookup.Result != noOp {
		t.Fatalf("no-op receipt was not recovered: %+v", lookup)
	}
}

func TestDurableRejectsInvalidMutationShapesWithoutPublishing(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*api.MutateRequest)
	}{
		{"duplicate keys", func(r *api.MutateRequest) { r.Batch.Operations = append(r.Batch.Operations, r.Batch.Operations[0]) }},
		{"any carries value", func(r *api.MutateRequest) { r.Batch.Operations[0].Condition.Value = api.Value{Kind: api.ValueNull} }},
		{"absent carries value", func(r *api.MutateRequest) {
			r.Batch.Operations[0].Condition = api.ValueCondition{Kind: api.ConditionAbsent, Value: integrationBytes()}
		}},
		{"value missing kind", func(r *api.MutateRequest) { r.Batch.Operations[0].Condition.Kind = api.ConditionValue }},
		{"null carries bytes", func(r *api.MutateRequest) {
			r.Batch.Operations[0].Value = api.Value{Kind: api.ValueNull, Data: []byte{1}}
		}},
		{"reserved space", func(r *api.MutateRequest) { r.Key.Space = "ate:system" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIntegrationStore(t, engine.Options{})
			before := integrationSnapshot(t, s.e)
			request := integrationRequest(fmt.Sprintf("invalid-%s", tc.name), integrationPut([]byte("key"), integrationBytes('v')))
			tc.edit(&request)
			got, err := s.e.Mutate(context.Background(), request)
			if err == nil || (got.Code != api.CodeRejected && got.Code != api.CodeNotSubmitted) {
				t.Fatalf("invalid request accepted: %+v, %v", got, err)
			}
			if after := integrationSnapshot(t, s.e); after != before {
				t.Fatal("invalid mutation changed roots/fence")
			}
		})
	}
}
