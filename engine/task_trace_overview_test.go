package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func overviewReadFixture(events map[model.EventID]model.Event) taskTraceRead {
	r := taskTraceRead{events: map[model.EventID]taskTraceOccurrence{}}
	r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
		event, ok := events[id]
		if !ok {
			return model.Event{}, ErrNodeNotFound
		}
		r.events[id] = taskTraceOccurrence{event: event}
		return event, ctx.Err()
	}
	r.metadata = func(ctx context.Context, id model.EventID) (eventMetadataItem, bool, error) {
		event, ok := events[id]
		return eventMetadataItem{id: id, operation: event.Method}, ok, ctx.Err()
	}
	r.inScope = func(ctx context.Context, id model.EventID) (bool, error) {
		_, ok := events[id]
		return ok, ctx.Err()
	}
	return r
}

func sharedOverviewFixture(depth, pages int) (taskTraceRead, []model.EventID, map[model.EventID]api.TraceMilestone) {
	events := map[model.EventID]model.Event{"source": {ID: "source", Method: "CreateTask"}}
	parent := model.EventID("source")
	for i := 0; i < depth; i++ {
		id := model.EventID(fmt.Sprintf("ancestor-%d", i))
		events[id] = model.Event{ID: id, CausalParents: []model.EventID{parent}}
		parent = id
	}
	var ids []model.EventID
	milestones := map[model.EventID]api.TraceMilestone{}
	for i := 0; i < pages; i++ {
		id := model.EventID(fmt.Sprintf("agent-%d", i))
		events[id] = model.Event{ID: id, CausalParents: []model.EventID{parent}}
		ids = append(ids, id)
		milestones[id] = api.TraceMilestone{Event: id}
	}
	return overviewReadFixture(events), ids, milestones
}

func TestTaskTraceOverviewSharesDeepAncestry(t *testing.T) {
	r, ids, milestones := sharedOverviewFixture(1024, 128)
	metadataCalls := 0
	metadata := r.metadata
	r.metadata = func(ctx context.Context, id model.EventID) (eventMetadataItem, bool, error) {
		metadataCalls++
		return metadata(ctx, id)
	}
	edges, deps, err := r.overview(context.Background(), ids, milestones)
	if err != nil || len(edges) != len(ids) || len(deps) != 1024+1+len(ids) || metadataCalls != 1025 {
		t.Fatalf("shared ancestry: edges=%d deps=%d metadata=%d err=%v", len(edges), len(deps), metadataCalls, err)
	}
	for _, edge := range edges {
		if edge.From != "source" || edge.Boundary || edge.Relation != "causal" {
			t.Fatalf("bad condensed edge: %+v", edge)
		}
	}
}

// This deliberately simple per-target walk is an independent oracle for the
// shared traversal. The graph generator includes joins, milestones and foreign
// boundary nodes, so caching cannot accidentally cross a stopping point.
type overviewEdgeKey struct {
	from, to model.EventID
	boundary bool
}

func referenceOverview(events map[model.EventID]model.Event, ids []model.EventID, milestones map[model.EventID]api.TraceMilestone) (map[overviewEdgeKey]bool, map[model.EventID]bool) {
	edges, deps := map[overviewEdgeKey]bool{}, map[model.EventID]bool{}
	for _, id := range ids {
		deps[id] = true
		seen := map[model.EventID]bool{}
		queue := slices.Clone(events[id].CausalParents)
		for i := 0; i < len(queue); i++ {
			parent := queue[i]
			if seen[parent] {
				continue
			}
			seen[parent], deps[parent] = true, true
			event, inside := events[parent]
			_, selected := milestones[parent]
			if selected || isTraceMilestoneOperation(event.Method) || !inside {
				edges[overviewEdgeKey{parent, id, !inside}] = true
			} else {
				queue = append(queue, event.CausalParents...)
			}
		}
	}
	return edges, deps
}

func TestTaskTraceOverviewMatchesIndependentPaths(t *testing.T) {
	for seed := int64(0); seed < 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		events := map[model.EventID]model.Event{}
		milestones := map[model.EventID]api.TraceMilestone{}
		var all, ids []model.EventID
		all = append(all, "boundary")
		for i := 0; i < 80; i++ {
			id := model.EventID(fmt.Sprintf("event-%03d", i))
			event := model.Event{ID: id}
			for j := 0; j < 3; j++ {
				parent := all[rng.Intn(len(all))]
				if !slices.Contains(event.CausalParents, parent) {
					event.CausalParents = append(event.CausalParents, parent)
				}
			}
			if i%13 == 0 {
				event.Method = "CreateTask"
			}
			if i%7 == 0 {
				milestones[id] = api.TraceMilestone{Event: id}
			}
			if i >= 72 {
				milestones[id] = api.TraceMilestone{Event: id}
				ids = append(ids, id)
			}
			events[id], all = event, append(all, id)
		}
		r := overviewReadFixture(events)
		edges, deps, err := r.overview(context.Background(), ids, milestones)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		gotEdges, gotDeps := map[overviewEdgeKey]bool{}, map[model.EventID]bool{}
		for _, edge := range edges {
			gotEdges[overviewEdgeKey{edge.From, edge.To, edge.Boundary}] = true
		}
		for _, id := range deps {
			gotDeps[id] = true
		}
		wantEdges, wantDeps := referenceOverview(events, ids, milestones)
		if len(edges) != len(gotEdges) || !reflect.DeepEqual(gotEdges, wantEdges) || !reflect.DeepEqual(gotDeps, wantDeps) {
			t.Fatalf("seed %d: condensed graph or disclosure dependencies changed", seed)
		}
	}
}

func TestTaskTraceOverviewTraversalBounds(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		r := overviewReadFixture(map[model.EventID]model.Event{
			"end": {ID: "end", CausalParents: []model.EventID{"a"}},
			"a":   {ID: "a", CausalParents: []model.EventID{"b"}},
			"b":   {ID: "b", CausalParents: []model.EventID{"a"}},
		})
		if _, _, err := r.overview(context.Background(), []model.EventID{"end"}, nil); !errors.Is(err, ErrStoredObject) {
			t.Fatal(err)
		}
	})
	t.Run("unique-work-limit", func(t *testing.T) {
		r, ids, milestones := sharedOverviewFixture(maxDerivationEdges, 1)
		if _, _, err := r.overview(context.Background(), ids, milestones); !errors.Is(err, storage.ErrLimit) {
			t.Fatal(err)
		}
	})
	t.Run("output-limit", func(t *testing.T) {
		r, ids, milestones := sharedOverviewFixture(0, 128)
		load := r.loadEvent
		r.loadEvent = func(ctx context.Context, id model.EventID) (model.Event, error) {
			event, err := load(ctx, id)
			if id != "source" {
				event.CausalParents = []model.EventID{"foreign-1", "foreign-2", "foreign-3", "foreign-4", "foreign-5"}
			}
			return event, err
		}
		if _, _, err := r.overview(context.Background(), ids, milestones); !errors.Is(err, storage.ErrLimit) {
			t.Fatal(err)
		}
	})
	for _, failure := range []string{"cancel", "metadata", "scope", "body"} {
		t.Run(failure, func(t *testing.T) {
			r, ids, milestones := sharedOverviewFixture(3, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := error(ErrStoredObject)
			switch failure {
			case "cancel":
				cancel()
				want = context.Canceled
			case "metadata":
				r.metadata = func(context.Context, model.EventID) (eventMetadataItem, bool, error) {
					return eventMetadataItem{}, false, want
				}
			case "scope":
				r.inScope = func(context.Context, model.EventID) (bool, error) { return false, want }
			case "body":
				r.loadEvent = func(context.Context, model.EventID) (model.Event, error) { return model.Event{}, want }
			}
			if _, _, err := r.overview(ctx, ids, milestones); !errors.Is(err, want) {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkTaskTraceOverviewSharedAncestors(b *testing.B) {
	for _, pages := range []int{32, 128} {
		b.Run(fmt.Sprintf("pages=%d/depth=1024", pages), func(b *testing.B) {
			r, ids, milestones := sharedOverviewFixture(1024, pages)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clear(r.events)
				if _, _, err := r.overview(context.Background(), ids, milestones); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
