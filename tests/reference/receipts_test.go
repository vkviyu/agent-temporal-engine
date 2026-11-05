package reference_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentDurableReceiptsAndVisibleEventCuts(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	opts := ate.Options{EventProfile: true}
	store, err := ate.Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	book := oracle.NewReceiptBook()
	initial, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fences := map[uint64]model.CommitFence{0: initial.Fence}
	acks := map[oracle.RequestIdentity]api.CommitResult{}
	history := map[string]api.ViewDescriptor{}
	visible := func(space, principal string, at uint64) {
		t.Helper()
		f := fences[at]
		anchor := model.SnapshotAnchor{Space: model.SpaceID(space), Fence: f}
		v, e := store.OpenView(ctx, api.OpenViewRequest{Space: model.SpaceID(space), Principal: model.PrincipalID(principal), Anchor: &anchor, Mode: "audit_snapshot"})
		if e != nil {
			t.Fatal(e)
		}
		defer store.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
		page, e := store.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Layer: "events", EntityKind: "Event"})
		if e != nil {
			t.Fatal(e)
		}
		facts := []oracle.VisibleOccurrence{}
		previous := map[uint64]model.EventID{}
		events := []model.Event{}
		for _, n := range page.Items {
			detail, e := store.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: n.Ref})
			if e != nil {
				t.Fatal(e)
			}
			var event model.Event
			if e := json.Unmarshal(detail.Data, &event); e != nil {
				t.Fatal(e)
			}
			if event.Attribution != nil {
				t.Fatal("core-only oracle unexpectedly saw typed profile event")
			}
			var lsn uint64
			for key, ack := range acks {
				if key.Space == space && key.Principal == principal && ack.TxID == event.Transaction {
					lsn = ack.Fence.LSN
				}
			}
			if lsn == 0 {
				t.Fatal("occurrence has no accepted command receipt")
			}
			facts = append(facts, oracle.VisibleOccurrence{Space: string(event.Space), Principal: string(event.RecordedBy), Method: event.Method, Publication: lsn, Sequence: event.Sequence})
			events = append(events, event)
			previous[event.Sequence] = event.ID
		}
		sort.Slice(facts, func(i, j int) bool { return facts[i].Sequence < facts[j].Sequence })
		if !reflect.DeepEqual(facts, book.Visible(space, principal, at)) {
			t.Fatalf("visible event set differs: got=%+v want=%+v", facts, book.Visible(space, principal, at))
		}
		for _, event := range events {
			if event.Sequence == 1 {
				if len(event.CausalParents) != 0 {
					t.Fatal("first stream occurrence has predecessor")
				}
			} else if len(event.CausalParents) != 1 || event.CausalParents[0] != previous[event.Sequence-1] {
				t.Fatal("stream predecessor differs from accepted publication order")
			}
		}
	}
	submit := func(req api.MutateRequest) {
		t.Helper()
		command := fmt.Sprintf("%q/%v/%v", req.Batch.Operations[0].Value.Data, req.ExpectedFence, req.Batch.Operations[0].Kind)
		var expected *uint64
		if req.ExpectedFence != nil {
			x := req.ExpectedFence.LSN
			expected = &x
		}
		k := oracle.RequestIdentity{string(req.Key.Space), string(req.Key.Principal), string(req.Key.RequestID)}
		status, lsn := book.Submit(k, command, api.MethodMutateState, expected)
		before, _ := store.Snapshot(ctx)
		got, e := store.Mutate(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		switch status {
		case "ack":
			if got.Code != api.CodeDurableAck || got.Fence.LSN != lsn {
				t.Fatalf("receipt mismatch: %+v oracle=%s/%d", got, status, lsn)
			}
			if old, exists := acks[k]; exists && old != got {
				t.Fatal("original receipt changed")
			}
			acks[k] = got
			fences[lsn] = got.Fence
		case "idempotency_conflict":
			if got.Code != api.CodeConflict || got.Conflict != api.ConflictIdempotency {
				t.Fatal(got)
			}
		case "precondition_failed":
			if got.Code != api.CodeConflict || got.Conflict != api.ConflictPrecondition {
				t.Fatal(got)
			}
		}
		after, _ := store.Snapshot(ctx)
		if after.Fence.LSN != book.Publication {
			t.Fatal("rejected/retried command changed publication")
		}
		if status != "ack" && after != before {
			t.Fatal("rejection changed roots")
		}
		lookup, e := store.LookupRequest(ctx, api.LookupRequestRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID})
		if e != nil {
			t.Fatal(e)
		}
		if original, ok := acks[k]; ok {
			if lookup.Result != original || lookup.Code != api.CodeDurableAck {
				t.Fatal("lookup differs from original receipt")
			}
		} else if lookup.Code != api.CodeNotFoundAtFence {
			t.Fatalf("rejection consumed namespace: %+v", lookup)
		}
	}
	request := func(space, principal, id, value string) api.MutateRequest {
		return api.MutateRequest{Key: model.RequestKey{Space: model.SpaceID(space), Principal: model.PrincipalID(principal), RequestID: model.RequestID(id)}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("value"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)}}}}}
	}
	first := request("space", "alice", "same", "one")
	first.ExpectedFence = &initial.Fence
	submit(first)
	anchor := model.SnapshotAnchor{Space: "space", Fence: fences[1]}
	history["first"], err = store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "alice", Anchor: &anchor, Mode: "audit_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	submit(request("space", "bob", "same", "other owner"))
	submit(request("other-space", "alice", "same", "other space"))
	submit(request("space", "alice", "advance", "two"))
	submit(first)
	bad := first
	bad.Batch.Operations = append([]api.StateMutation(nil), first.Batch.Operations...)
	bad.Batch.Operations[0].Value.Data = []byte("changed body")
	submit(bad)
	current := fences[4]
	bad = first
	bad.ExpectedFence = &current
	submit(bad)
	stale := request("space", "alice", "rejected", "stale")
	stale.ExpectedFence = &initial.Fence
	submit(stale)
	for _, scope := range [][2]string{{"space", "alice"}, {"space", "bob"}, {"other-space", "alice"}, {"space", "charlie"}} {
		visible(scope[0], scope[1], book.Publication)
		visible(scope[0], scope[1], 1)
	}
	frozen, e := store.ListNodes(ctx, api.ListNodesRequest{View: history["first"].Ref, Layer: "events", EntityKind: "Event"})
	if e != nil || len(frozen.Items) != 1 {
		t.Fatal("later publications polluted frozen view", e)
	}
	if err := store.CloseView(ctx, api.CloseViewRequest{Ref: history["first"].Ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, _, err = ate.OpenExisting(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	submit(first)
	visible("space", "alice", 1)
	visible("space", "alice", book.Publication)
}
