package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// A fan-out/join corpus: all agent chains contribute to the terminal event.
// The fixture is in memory; benchmarks exclude journal IO and disclosure gates.
func longEvidenceFixture(agents, depth int) (taskTraceRead, api.TaskTraceEvidenceRequest, []model.EventID) {
	events := map[model.EventID]model.Event{}
	metadata := map[model.EventID]eventMetadataItem{}
	children := map[model.EventID][]eventMetadataItem{}
	var ids []model.EventID
	add := func(parents ...model.EventID) model.EventID {
		id := model.EventID(fmt.Sprintf("%064x", len(ids)+1))
		item := eventMetadataItem{id: id, entry: eventEntry{LSN: uint64(len(ids) + 1)}}
		events[id] = model.Event{ID: id, Method: "recorded", Payload: model.ObjectID("payload/" + string(id)), CausalParents: parents}
		metadata[id] = item
		for _, parent := range parents {
			children[parent] = append(children[parent], item)
		}
		ids = append(ids, id)
		return id
	}
	from := add()
	var ends []model.EventID
	for agent := 0; agent < agents; agent++ {
		parent := from
		for step := 0; step < depth; step++ {
			parent = add(parent)
		}
		ends = append(ends, parent)
	}
	to := add(ends...)
	r := overviewReadFixture(events)
	r.entry = &viewEntry{rootSet: "historical-root"}
	r.record = "trace-edit"
	r.metadata = func(ctx context.Context, id model.EventID) (eventMetadataItem, bool, error) {
		item, found := metadata[id]
		return item, found, ctx.Err()
	}
	r.children = func(ctx context.Context, id model.EventID) ([]eventMetadataItem, error) {
		return children[id], ctx.Err()
	}
	req := api.TaskTraceEvidenceRequest{
		TaskTraceRequest: api.TaskTraceRequest{Space: "space", Principal: "owner", Subject: api.TraceSubject{Kind: "query", ID: "query"}, Limit: 32},
		From:             from, To: to, Relation: "causal",
	}
	return r, req, ids
}

func TestTaskTraceEvidenceCacheReusesWholePathAcrossPages(t *testing.T) {
	for _, agents := range []int{1, 16} {
		t.Run(fmt.Sprintf("agents=%d", agents), func(t *testing.T) {
			r, req, want := longEvidenceFixture(agents, 2048/agents)
			e := &Engine{}
			ctx := context.Background()
			first, err := e.causalTraceEvidence(ctx, req, r)
			if err != nil || !slices.Equal(first.ids, want) {
				t.Fatalf("initial path: %v", err)
			}
			if !slices.Contains(first.dependencies, model.ObjectID("payload/"+string(req.To))) {
				t.Fatal("cached dependencies omit off-page terminal payload")
			}
			// A hit must not traverse, sort or expand any historical provenance.
			r.children = func(context.Context, model.EventID) ([]eventMetadataItem, error) {
				t.Fatal("cached page traversed the DAG")
				return nil, nil
			}
			r.metadata = func(context.Context, model.EventID) (eventMetadataItem, bool, error) {
				t.Fatal("cached page sorted the DAG")
				return eventMetadataItem{}, false, nil
			}
			var got []model.EventID
			for {
				clear(r.events)
				value, err := e.causalTraceEvidence(ctx, req, r)
				if err != nil || !reflect.DeepEqual(value, first) {
					t.Fatalf("cached path changed: %v", err)
				}
				start, end, next, err := tracePage(req.TaskTraceRequest, r.anchor, [3]string{string(req.From), string(req.To), req.Relation}, len(value.ids))
				if err != nil {
					t.Fatal(err)
				}
				for _, id := range value.ids[start:end] {
					if _, err := r.loadEvent(ctx, id); err != nil {
						t.Fatal(err)
					}
					if r.node(id, api.TraceMilestone{}).Operation != "recorded" {
						t.Fatal("cached page lost node projection")
					}
					got = append(got, id)
				}
				if len(r.events) != end-start {
					t.Fatal("cached page materialized off-page events")
				}
				if next == "" {
					break
				}
				req.Continuation = next
			}
			if !slices.Equal(got, want) {
				t.Fatal("paging lost or duplicated events")
			}
			// Eviction is transparent to the stateless continuation protocol.
			e.traceEvidence = traceEvidenceCache{}
			fresh, _, _ := longEvidenceFixture(agents, 2048/agents)
			rebuilt, err := e.causalTraceEvidence(ctx, req, fresh)
			if err != nil || !reflect.DeepEqual(rebuilt, first) {
				t.Fatalf("cold continuation changed evidence: %v", err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := e.causalTraceEvidence(canceled, req, fresh); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestTraceEvidenceCacheScope(t *testing.T) {
	r, req, _ := longEvidenceFixture(1, 2)
	key, err := traceEvidenceKey(req, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*api.TaskTraceEvidenceRequest, *taskTraceRead){
		func(q *api.TaskTraceEvidenceRequest, _ *taskTraceRead) { q.Principal = "other" },
		func(q *api.TaskTraceEvidenceRequest, _ *taskTraceRead) { q.Session = "other" },
		func(q *api.TaskTraceEvidenceRequest, _ *taskTraceRead) { q.Subject.ID = "other" },
		func(q *api.TaskTraceEvidenceRequest, _ *taskTraceRead) { q.Subject.Kind = "task" },
		func(q *api.TaskTraceEvidenceRequest, _ *taskTraceRead) { q.From = "other" },
		func(q *api.TaskTraceEvidenceRequest, _ *taskTraceRead) { q.To = "other" },
		func(_ *api.TaskTraceEvidenceRequest, r *taskTraceRead) { r.anchor.Space = "other" },
		func(_ *api.TaskTraceEvidenceRequest, r *taskTraceRead) { r.anchor.Fence.Store = "other" },
		func(_ *api.TaskTraceEvidenceRequest, r *taskTraceRead) { r.anchor.Fence.LSN++ },
		func(_ *api.TaskTraceEvidenceRequest, r *taskTraceRead) { r.anchor.Fence.CommitDigest = "other" },
		func(_ *api.TaskTraceEvidenceRequest, r *taskTraceRead) { r.record = "other" },
		func(_ *api.TaskTraceEvidenceRequest, r *taskTraceRead) { r.entry = &viewEntry{rootSet: "other"} },
	} {
		q, read := req, r
		change(&q, &read)
		changed, err := traceEvidenceKey(q, read)
		if err != nil || changed == key {
			t.Fatal("cache scope collision", err)
		}
	}
	req.Limit, req.Continuation = 1, "different-page"
	if changed, err := traceEvidenceKey(req, r); err != nil || changed != key {
		t.Fatal("page-independent path did not share cache", err)
	}
}

func TestTraceEvidenceCacheBoundsOwnershipAndStop(t *testing.T) {
	c := &traceEvidenceCache{}
	source := &traceEvidence{ids: []model.EventID{"event"}, dependencies: []model.ObjectID{"payload"}, externalFrom: true}
	c.put("owned", source)
	source.ids[0], source.dependencies[0] = "changed", "changed"
	value, found := c.get("owned")
	if !found || value.ids[0] != "event" || value.dependencies[0] != "payload" || !value.externalFrom {
		t.Fatal("cache borrowed construction buffers")
	}
	for i := 0; i < traceEvidenceCacheEntries; i++ {
		c.put(fmt.Sprint(i), source)
		c.get("owned")
	}
	if _, found := c.get("0"); found || len(c.entries) != traceEvidenceCacheEntries {
		t.Fatal("LRU entry limit failed")
	}
	if _, found := c.get("owned"); !found {
		t.Fatal("recently used path was evicted")
	}
	large := &traceEvidence{ids: []model.EventID{model.EventID(strings.Repeat("x", traceEvidenceCacheBytes/2))}}
	c.put("large-1", large)
	c.put("large-2", large)
	if c.bytes > traceEvidenceCacheBytes || len(c.entries) != 1 {
		t.Fatal("byte limit failed")
	}
	c.put("oversized", &traceEvidence{ids: []model.EventID{model.EventID(strings.Repeat("x", traceEvidenceCacheBytes))}})
	if _, found := c.get("oversized"); found || len(c.entries) != 1 {
		t.Fatal("oversized path was retained")
	}
	c.stop()
	c.put("late-builder", source)
	if len(c.entries) != 0 || c.bytes != 0 {
		t.Fatal("late builder repopulated stopped cache")
	}
}

func TestTraceEvidenceCacheConcurrentReaders(t *testing.T) {
	c := &traceEvidenceCache{}
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprint((worker + i) % 64)
				c.put(key, &traceEvidence{ids: []model.EventID{model.EventID(key)}})
				if value, ok := c.get(key); ok && value.ids[0] != model.EventID(key) {
					t.Error("concurrent cache value changed")
				}
			}
		}(worker)
	}
	workers.Wait()
	if len(c.entries) > traceEvidenceCacheEntries || c.bytes > traceEvidenceCacheBytes {
		t.Fatal("concurrent insertion exceeded bounds")
	}
	c.stop()
}

func BenchmarkTaskTraceEvidenceRepeatedPages(b *testing.B) {
	for _, cached := range []bool{false, true} {
		b.Run(fmt.Sprintf("cached=%t/agents=16/events=2050", cached), func(b *testing.B) {
			r, req, _ := longEvidenceFixture(16, 128)
			e := &Engine{}
			ctx := context.Background()
			if _, err := e.causalTraceEvidence(ctx, req, r); err != nil {
				b.Fatal(err)
			}
			loads := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !cached {
					e.traceEvidence = traceEvidenceCache{}
				}
				clear(r.events)
				value, err := e.causalTraceEvidence(ctx, req, r)
				if err != nil {
					b.Fatal(err)
				}
				start := (i % ((len(value.ids) + 31) / 32)) * 32
				for _, id := range value.ids[start:min(start+32, len(value.ids))] {
					if _, err := r.loadEvent(ctx, id); err != nil {
						b.Fatal(err)
					}
				}
				loads += len(r.events)
			}
			b.ReportMetric(float64(loads)/float64(b.N), "events/op")
		})
	}
}
