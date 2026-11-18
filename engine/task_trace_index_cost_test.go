package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestTaskTraceScopePreservesOwnerSpaceAndFence(t *testing.T) {
	key := traceEventKey{"space", "owner", "execution", "one"}
	otherOwner := key
	otherOwner.principal = "other"
	otherSpace := key
	otherSpace.space = "other"
	e := &Engine{
		traceEventMeta: map[model.EventID]eventMetadataItem{
			"at":      {entry: eventEntry{LSN: 2}},
			"future":  {entry: eventEntry{LSN: 3}},
			"owner":   {entry: eventEntry{LSN: 1}},
			"space":   {entry: eventEntry{LSN: 1}},
			"outside": {entry: eventEntry{LSN: 1}},
		},
		traceEventKeys: map[model.EventID][]traceEventKey{
			"at":     {otherOwner, key},
			"future": {key},
			"owner":  {otherOwner},
			"space":  {otherSpace},
		},
	}
	keys := []traceEventKey{key, key}
	scope := newTraceScope(keys)
	keys[0], keys[1] = otherOwner, otherSpace
	for _, id := range []model.EventID{"at", "future", "owner", "space", "outside", "missing"} {
		found, err := e.traceEventInScope(context.Background(), id, scope, model.CommitFence{LSN: 2})
		if err != nil || found != (id == "at") {
			t.Fatalf("%s: found=%v err=%v", id, found, err)
		}
	}
	e.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.traceEventInScope(ctx, "at", scope, model.CommitFence{LSN: 2})
	e.mu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scope lookup: %v", err)
	}
}

func TestTaskTraceHistoricalChildrenOwnedDuringAppend(t *testing.T) {
	children := make([]eventMetadataItem, 32, 2048)
	for i := range children {
		children[i] = eventMetadataItem{entry: eventEntry{LSN: uint64(i + 1)}}
	}
	e := &Engine{traceCausalChildren: map[model.EventID][]eventMetadataItem{"parent": children}}
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for lsn := uint64(33); lsn <= 2049; lsn++ {
			e.mu.Lock()
			e.traceCausalChildren["parent"] = append(e.traceCausalChildren["parent"], eventMetadataItem{entry: eventEntry{LSN: lsn}})
			e.mu.Unlock()
		}
	}()
	defer writer.Wait()
	for i := 0; i < 100; i++ {
		got, err := e.traceEventChildren(context.Background(), "parent", model.CommitFence{LSN: 16})
		if err != nil || len(got) != 16 {
			t.Fatalf("historical children: len=%d err=%v", len(got), err)
		}
		for j, item := range got {
			if item.entry.LSN != uint64(j+1) {
				t.Fatalf("historical child %d: LSN=%d", j, item.entry.LSN)
			}
		}
		got[0].entry.LSN = 999 // Returned metadata must not alias the index.
	}
	for _, parent := range []model.EventID{"parent", "missing"} {
		got, err := e.traceEventChildren(context.Background(), parent, model.CommitFence{})
		if err != nil || len(got) != 0 {
			t.Fatalf("empty prefix: len=%d err=%v", len(got), err)
		}
	}
}
