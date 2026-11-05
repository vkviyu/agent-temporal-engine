package ate_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func nativeFacadeDirectory(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native durability profile is supported on Darwin and Linux")
	}
	return t.TempDir()
}

func TestPublicCloseContextStopsAndCanFinishAfterCanceledWait(t *testing.T) {
	path := nativeFacadeDirectory(t)
	s, err := ate.Create(path, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = s.CloseContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got, err := s.Mutate(context.Background(), facadeRequest()); got.Code != api.CodeNotSubmitted || !errors.Is(err, storage.ErrClosing) {
		t.Fatalf("draining store admitted: %+v %v", got, err)
	}
	if _, err := s.Snapshot(context.Background()); !errors.Is(err, storage.ErrClosing) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.CloseContext(ctx); err != nil {
		t.Fatalf("completed close is not idempotent: %v", err)
	}
	reopened, _, err := ate.OpenExisting(path, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func facadeRequest() api.MutateRequest {
	return api.MutateRequest{
		Key: model.RequestKey{Space: "host-space", Principal: "host-principal", RequestID: "facade-request"},
		Batch: api.StateBatch{Operations: []api.StateMutation{
			{Key: nil, Kind: api.MutationPut, Value: api.Value{Kind: api.ValueNull}},
			{Key: []byte{0, 'k', 255}, Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte{0, 1, 255}}},
		}},
	}
}

func TestPublicFacadeDurableLifecycleAndOwnership(t *testing.T) {
	ctx := context.Background()
	path := nativeFacadeDirectory(t)
	s, err := ate.Create(path, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	initial, err := s.Snapshot(ctx)
	if err != nil || initial.Fence.Validate() != nil {
		t.Fatalf("initial snapshot: %+v, %v", initial, err)
	}
	if competing, err := ate.Create(path, ate.Options{}); competing != nil || !errors.Is(err, storage.ErrOwnerBusy) {
		if competing != nil {
			_ = competing.Close()
		}
		t.Fatalf("Create accepted a competing owner: %v", err)
	}
	if competing, _, err := ate.OpenExisting(path, ate.Options{}); competing != nil || !errors.Is(err, storage.ErrOwnerBusy) {
		if competing != nil {
			_ = competing.Close()
		}
		t.Fatalf("OpenExisting accepted a competing owner: %v", err)
	}
	request := facadeRequest()
	request.ExpectedFence = &initial.Fence
	ack, err := s.Mutate(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck || ack.Fence.LSN != initial.Fence.LSN+1 || !model.ValidDigest(string(ack.StateRoot)) || !model.ValidDigest(string(ack.RootSet)) {
		t.Fatalf("mutation: %+v, %v", ack, err)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil || snapshot.Fence != ack.Fence || snapshot.RootSet != ack.RootSet || snapshot.Roots.DomainStates == "" || snapshot.Roots.CommandReceipts == "" {
		t.Fatalf("published snapshot: %+v, %v", snapshot, err)
	}
	for _, operation := range request.Batch.Operations {
		got, err := s.GetState(ctx, api.ReadStateRequest{Space: request.Key.Space, Key: operation.Key})
		if err != nil || !got.Found || got.Value.Kind != operation.Value.Kind || !bytes.Equal(got.Value.Data, operation.Value.Data) || got.Fence != ack.Fence || got.RootSet != ack.RootSet || got.StateRoot != ack.StateRoot {
			t.Fatalf("read after mutation: %+v, %v", got, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	opened, report, err := ate.OpenExisting(path, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Close(); err != nil {
			t.Error(err)
		}
	})
	if !report.Verified || !report.PublicationSynced || report.ScannedTransactions != 1 {
		t.Fatalf("recovery report: %+v", report)
	}
	lookup, err := opened.LookupRequest(ctx, api.LookupRequestRequest{Space: request.Key.Space, Principal: request.Key.Principal, RequestID: request.Key.RequestID})
	if err != nil || lookup.Code != api.CodeDurableAck || lookup.Result != ack || lookup.ObservedFence != ack.Fence {
		t.Fatalf("recovered receipt differs from durable ack: %+v, %v", lookup, err)
	}
	if retry, err := opened.Mutate(ctx, request); err != nil || retry != ack {
		t.Fatalf("retry did not return original ack: %+v, %v", retry, err)
	}
	if got, err := opened.Snapshot(ctx); err != nil || got != snapshot {
		t.Fatalf("reopen changed snapshot: %+v, %v", got, err)
	}
	for _, operation := range request.Batch.Operations {
		got, err := opened.GetState(ctx, api.ReadStateRequest{Space: request.Key.Space, Key: operation.Key})
		if err != nil || !got.Found || got.Value.Kind != operation.Value.Kind || !bytes.Equal(got.Value.Data, operation.Value.Data) || got.StateRoot != ack.StateRoot {
			t.Fatalf("recovered value: %+v, %v", got, err)
		}
	}
}

func TestPublicFacadeClosedStoreRejectsAPIsAndCloseIsIdempotent(t *testing.T) {
	s, err := ate.Create(nativeFacadeDirectory(t), ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	request := facadeRequest()
	if _, err := s.Mutate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	ctx := context.Background()
	if got, err := s.Mutate(ctx, request); !errors.Is(err, storage.ErrClosed) || got.Code == api.CodeDurableAck || got.RootSet != "" || got.StateRoot != "" || got.Fence != (model.CommitFence{}) {
		t.Fatalf("closed Mutate returned evidence: %+v, %v", got, err)
	}
	if got, err := s.Commit(ctx, api.CommitRequest{Space: request.Key.Space, Principal: request.Key.Principal, RequestID: request.Key.RequestID, Method: api.MethodMutateState}); !errors.Is(err, storage.ErrClosed) || got.Code == api.CodeDurableAck || got.RootSet != "" || got.Fence != (model.CommitFence{}) {
		t.Fatalf("closed Commit returned evidence: %+v, %v", got, err)
	}
	if got, err := s.LookupRequest(ctx, api.LookupRequestRequest{Space: request.Key.Space, Principal: request.Key.Principal, RequestID: request.Key.RequestID}); !errors.Is(err, storage.ErrClosed) || got.Code != api.CodeIndeterminate || got.Result != (api.CommitResult{}) || got.ObservedFence != (model.CommitFence{}) {
		t.Fatalf("closed LookupRequest returned evidence: %+v, %v", got, err)
	}
	if got, err := s.GetState(ctx, api.ReadStateRequest{Space: request.Key.Space}); !errors.Is(err, storage.ErrClosed) || got.Found || got.RootSet != "" || got.StateRoot != "" || got.Fence != (model.CommitFence{}) {
		t.Fatalf("closed GetState returned evidence: %+v, %v", got, err)
	}
	if got, err := s.Snapshot(ctx); !errors.Is(err, storage.ErrClosed) || got != (api.StateSnapshot{}) {
		t.Fatalf("closed Snapshot returned evidence: %+v, %v", got, err)
	}
}

func TestPublicViewsPinHistoricalStateAndCloseLifecycle(t *testing.T) {
	ctx := context.Background()
	path := nativeFacadeDirectory(t)
	s, err := ate.Create(path, ate.Options{Views: ate.ViewOptions{MaxViews: 4, TTL: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	req := api.MutateRequest{Key: model.RequestKey{Space: "view-space", Principal: "viewer", RequestID: "view-write"}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("k"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("v")}}}}}
	if _, err = s.Mutate(ctx, req); err != nil {
		t.Fatal(err)
	}
	latest, err := s.OpenView(ctx, api.OpenViewRequest{Space: req.Key.Space, Principal: req.Key.Principal, Mode: "state"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetViewState(ctx, api.ReadViewStateRequest{View: latest.Ref, Key: []byte("k")})
	if err != nil || !got.Found || string(got.Value.Data) != "v" {
		t.Fatalf("latest view: %+v %v", got, err)
	}
	historical, err := s.OpenView(ctx, api.OpenViewRequest{Space: req.Key.Space, Principal: req.Key.Principal, Anchor: &model.SnapshotAnchor{Fence: before.Fence, Space: req.Key.Space}, Mode: "audit_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.GetViewState(ctx, api.ReadViewStateRequest{View: historical.Ref, Key: []byte("k")})
	if err != nil || old.Found || old.Fence != before.Fence {
		t.Fatalf("historical view: %+v %v", old, err)
	}
	if err = s.CloseView(ctx, api.CloseViewRequest{Ref: historical.Ref}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetViewState(ctx, api.ReadViewStateRequest{View: historical.Ref, Key: []byte("k")}); !errors.Is(err, engine.ErrInvalidView) {
		t.Fatalf("closed view error: %v", err)
	}
	if err = s.CloseView(ctx, api.CloseViewRequest{Ref: latest.Ref}); err != nil {
		t.Fatal(err)
	}
}
