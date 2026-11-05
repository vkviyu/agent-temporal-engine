package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestDomainCursorViewReadsRevisionStateAndPreservesFence(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	old := integrationMutate(t, s.e, integrationRequest("cursor-api-old", integrationPut([]byte("key"), api.Value{Kind: api.ValueBytes, Data: []byte("old")})))
	plain, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: plain.Ref})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: plain.Ref})
	es := make([]graph.Entry, len(entries.Items))
	for i, v := range entries.Items {
		es[i] = graph.Entry{Key: v.Key, Value: v.Value}
	}
	revision, err := graph.BuildRevision("host-space", "", nil, old.StateRoot, nil, es)
	if err != nil {
		t.Fatal(err)
	}
	graphCoreApply(t, s.e, "cursor-api-revision", graph.Command{AddRevision: &graph.AddRevisionCommand{Root: old.StateRoot, Entries: es}})
	integrationMutate(t, s.e, integrationRequest("cursor-api-new", integrationPut([]byte("key"), api.Value{Kind: api.ValueBytes, Data: []byte("new")})))
	tip := integrationSnapshot(t, s.e)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: tip.Fence, Cursor: &model.Cursor{Store: tip.Fence.Store, Space: "host-space", Revision: revision.ID}}
	var digest model.ObjectID
	for pass := 0; pass < 2; pass++ {
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
		if err != nil {
			t.Fatalf("cursor view: %v", err)
		}
		if !reflect.DeepEqual(view.Anchor, anchor) {
			t.Fatalf("selector was discarded: %+v", view.Anchor)
		}
		value, err := s.e.GetViewState(ctx, api.ReadViewStateRequest{View: view.Ref, Key: []byte("key")})
		if err != nil || !value.Found || string(value.Value.Data) != "old" || value.StateRoot != old.StateRoot || value.Fence != tip.Fence || value.RootSet != tip.RootSet {
			t.Fatalf("mixed historical selection: %+v %v", value, err)
		}
		got, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
		if err != nil || !reflect.DeepEqual(got.Items, entries.Items) || got.StateRoot != old.StateRoot || !reflect.DeepEqual(got.Anchor, anchor) {
			t.Fatalf("entries ignored cursor: %+v %v", got, err)
		}
		if pass == 0 {
			digest = view.ViewDigest
		} else if view.ViewDigest != digest {
			t.Fatal("reopen changed semantic cursor view digest")
		}
		_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		if pass == 0 {
			s.reopen()
		}
	}
	plain, err = s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: tip.Fence}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: plain.Ref})
	if plain.ViewDigest == digest {
		t.Fatal("Cursor omitted from semantic ViewDigest")
	}
	anchor.Fence = old.Fence
	if _, err = s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor}); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("future revision visible at old fence: %v", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatalf("unknown cursor fenced store: %v", err)
	}
}
