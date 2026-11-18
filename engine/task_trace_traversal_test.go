package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// The requested path is from -> middle -> to. Side branches are descendants
// of from, but cannot contribute evidence for to.
func traceTraversalFixture(branches int) taskTraceRead {
	events := map[model.EventID]model.Event{}
	metadata := map[model.EventID]eventMetadataItem{}
	children := map[model.EventID][]eventMetadataItem{}
	add := func(id model.EventID, parents ...model.EventID) {
		item := eventMetadataItem{id: id, entry: eventEntry{LSN: uint64(len(events) + 1)}}
		events[id] = model.Event{ID: id, Method: "method/" + string(id), Payload: model.ObjectID("payload/" + string(id)), CausalParents: parents}
		metadata[id] = item
		for _, parent := range parents {
			children[parent] = append(children[parent], item)
		}
	}
	add("from")
	add("middle", "from")
	for i := 0; i < branches; i++ {
		add(model.EventID(fmt.Sprintf("side-%d", i)), "from")
	}
	add("to", "middle")
	r := taskTraceRead{events: make(map[model.EventID]taskTraceOccurrence)}
	r.inScope = func(ctx context.Context, id model.EventID) (bool, error) {
		_, found := events[id]
		return found, ctx.Err()
	}
	r.children = func(ctx context.Context, id model.EventID) ([]eventMetadataItem, error) {
		return children[id], ctx.Err()
	}
	r.metadata = func(ctx context.Context, id model.EventID) (eventMetadataItem, bool, error) {
		item, found := metadata[id]
		return item, found, ctx.Err()
	}
	r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
		if err := ctx.Err(); err != nil {
			return model.Event{}, err
		}
		if got, found := r.events[id]; found {
			return got.event, nil
		}
		event, found := events[id]
		if !found {
			return model.Event{}, ErrNodeNotFound
		}
		r.events[id] = taskTraceOccurrence{event: event, lsn: metadata[id].entry.LSN}
		return event, nil
	}
	return r
}

func TestTaskTraceEvidenceSkipsSideBodiesAndLoadsSource(t *testing.T) {
	r := traceTraversalFixture(2048)
	load := r.loadEvent
	r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
		switch id {
		case "from", "middle", "to":
			return load(ctx, id)
		default:
			t.Fatalf("loaded unrelated descendant %s", id)
			return model.Event{}, ErrStoredObject
		}
	}
	ids, err := r.between(context.Background(), "from", "to")
	if err != nil || !reflect.DeepEqual(ids, []model.EventID{"from", "middle", "to"}) {
		t.Fatal(ids, err)
	}
	if len(r.events) != 3 {
		t.Fatalf("loaded %d events, want 3", len(r.events))
	}
	node := r.node("from", api.TraceMilestone{})
	if node.Operation != "method/from" {
		t.Fatalf("source event was not materialized: %+v", node)
	}
	// Disclosure must include the source event's payload even on a later page.
	e := &Engine{}
	deps, err := e.taskTraceDependencies(context.Background(), api.TaskTraceRequest{}, r, ids)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range deps {
		found = found || id == "payload/from"
	}
	if !found {
		t.Fatalf("source payload absent from disclosure dependencies: %v", deps)
	}
}

func TestTaskTraceEvidencePreservesMultiplePaths(t *testing.T) {
	r := traceTraversalFixture(1)
	load, children := r.loadEvent, r.children
	r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
		event, err := load(ctx, id)
		if id == "to" {
			event.CausalParents = []model.EventID{"middle", "side-0"}
			r.events[id] = taskTraceOccurrence{event: event}
		}
		return event, err
	}
	r.children = func(ctx context.Context, id model.EventID) ([]eventMetadataItem, error) {
		if id == "side-0" {
			return []eventMetadataItem{{id: "to"}}, nil
		}
		return children(ctx, id)
	}
	ids, err := r.between(context.Background(), "from", "to")
	if err != nil || !reflect.DeepEqual(ids, []model.EventID{"from", "middle", "side-0", "to"}) {
		t.Fatal(ids, err)
	}
}

func TestTaskTraceEvidenceKeepsExternalSourceAsBoundary(t *testing.T) {
	r := traceTraversalFixture(0)
	inScope, load := r.inScope, r.loadEvent
	r.inScope = func(ctx context.Context, id model.EventID) (bool, error) {
		if id == "from" {
			return false, nil
		}
		return inScope(ctx, id)
	}
	r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
		if id == "from" {
			t.Fatal("external source must be loaded by the boundary reader")
		}
		return load(ctx, id)
	}
	ids, err := r.between(context.Background(), "from", "to")
	if err != nil || !reflect.DeepEqual(ids, []model.EventID{"from", "middle", "to"}) || len(r.events) != 2 {
		t.Fatal(ids, err, len(r.events))
	}
}

func TestTaskTraceEvidenceTraversalErrors(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		r := traceTraversalFixture(0)
		r.children = func(context.Context, model.EventID) ([]eventMetadataItem, error) { return nil, nil }
		_, err := r.between(context.Background(), "from", "to")
		if !errors.Is(err, ErrNodeNotFound) || len(r.events) != 0 {
			t.Fatal(err, len(r.events))
		}
	})
	for _, failID := range []model.EventID{"from", "middle", "to"} {
		t.Run("body/"+string(failID), func(t *testing.T) {
			r := traceTraversalFixture(0)
			load := r.loadEvent
			r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
				if id == failID {
					return model.Event{}, ErrStoredObject
				}
				return load(ctx, id)
			}
			ids, err := r.between(context.Background(), "from", "to")
			if !errors.Is(err, ErrStoredObject) || ids != nil {
				t.Fatal(ids, err)
			}
		})
	}
	t.Run("metadata", func(t *testing.T) {
		r := traceTraversalFixture(0)
		r.metadata = func(context.Context, model.EventID) (eventMetadataItem, bool, error) {
			return eventMetadataItem{}, false, ErrStoredObject
		}
		ids, err := r.between(context.Background(), "from", "to")
		if !errors.Is(err, ErrStoredObject) || ids != nil {
			t.Fatal(ids, err)
		}
	})
	t.Run("limit", func(t *testing.T) {
		r := traceTraversalFixture(maxDerivationEdges)
		_, err := r.between(context.Background(), "from", "to")
		if !errors.Is(err, storage.ErrLimit) {
			t.Fatal(err)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		r := traceTraversalFixture(0)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := r.between(ctx, "from", "to")
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func BenchmarkTaskTraceEvidenceBranches(b *testing.B) {
	for _, branches := range []int{0, 2048} {
		b.Run(fmt.Sprintf("branches=%d", branches), func(b *testing.B) {
			r := traceTraversalFixture(branches)
			ctx := context.Background()
			loads := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clear(r.events)
				ids, err := r.between(ctx, "from", "to")
				if err != nil || len(ids) != 3 {
					b.Fatal(ids, err)
				}
				loads += len(r.events)
			}
			b.ReportMetric(float64(loads)/float64(b.N), "events/op")
		})
	}
}
