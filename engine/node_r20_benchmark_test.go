package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const (
	nodeR20Space     model.SpaceID     = "r20-space"
	nodeR20Principal model.PrincipalID = "r20-principal"
)

type nodeR20Observation struct {
	reads     uint64
	readBytes uint64
	scans     uint64
	scanBytes uint64
}

type nodeR20Fixture struct {
	e              *engine.Engine
	obs            *observation.Collector
	current        api.ViewDescriptor
	historical     api.ViewDescriptor
	historicalCut  model.SnapshotAnchor
	contentRef     model.NodeRef
	contentKey     []byte
	page           api.ListNodesRequest
	search         api.SearchNodesRequest
	trace          api.TraceRequest
	node           api.GetNodeRequest
	content        api.ReadContentRequest
	state          api.ReadViewStateRequest
	entries        api.ReadViewEntriesRequest
	representative map[string]nodeR20Observation
	limits         api.StateEntriesLimits
	records        int
}

func newNodeR20Fixture(b *testing.B, records int) *nodeR20Fixture {
	b.Helper()
	d, err := storage.OpenDirectory(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	obs, err := observation.New(observation.MaxCapacity)
	if err != nil {
		_ = d.Close()
		b.Fatal(err)
	}
	e, err := engine.Create(d, engine.Options{Observation: obs, EventProfile: true})
	if err != nil {
		_ = d.Close()
		b.Fatal(err)
	}
	f := &nodeR20Fixture{e: e, obs: obs, records: records}
	b.Cleanup(func() {
		if err := e.Close(); err != nil {
			b.Error(err)
		}
		if err := d.Close(); err != nil {
			b.Error(err)
		}
	})

	operations := make([]api.StateMutation, records)
	for i := range operations {
		key := []byte(fmt.Sprintf("r20-key-%06d", i))
		value := []byte(fmt.Sprintf("r20-content-%06d-%s", i, "payload-with-stable-width"))
		operations[i] = api.StateMutation{
			Key:   key,
			Kind:  api.MutationPut,
			Value: api.Value{Kind: api.ValueBytes, Data: value},
		}
	}
	first := nodeR20Mutate(b, e, "initial", operations)
	f.historicalCut = model.SnapshotAnchor{Space: nodeR20Space, Fence: first.Fence}
	nodeR20Mutate(b, e, "advance", []api.StateMutation{{
		Key:   []byte("r20-key-000000"),
		Kind:  api.MutationPut,
		Value: api.Value{Kind: api.ValueBytes, Data: []byte("r20-updated")},
	}})

	ctx := context.Background()
	current, err := e.OpenView(ctx, api.OpenViewRequest{Space: nodeR20Space, Principal: nodeR20Principal})
	if err != nil {
		b.Fatal(err)
	}
	historical, err := e.OpenView(ctx, api.OpenViewRequest{
		Space: nodeR20Space, Principal: nodeR20Principal, Anchor: &f.historicalCut,
	})
	if err != nil {
		b.Fatal(err)
	}
	f.current, f.historical = current, historical
	b.Cleanup(func() {
		if err := e.CloseView(ctx, api.CloseViewRequest{Ref: current.Ref}); err != nil {
			b.Error(err)
		}
		if err := e.CloseView(ctx, api.CloseViewRequest{Ref: historical.Ref}); err != nil {
			b.Error(err)
		}
	})

	f.limits = api.StateEntriesLimits{
		MaxNodes: 4 * records, MaxEntries: records, MaxBytes: 16 << 20, MaxReadBytes: 64 << 20,
	}
	f.entries = api.ReadViewEntriesRequest{View: current.Ref, Limits: f.limits}
	f.contentKey = []byte("r20-key-000000")
	entries, err := e.GetViewEntries(ctx, f.entries)
	if err != nil || len(entries.Items) != records {
		b.Fatalf("entries preflight: len=%d err=%v", len(entries.Items), err)
	}
	f.contentRef = model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}
	f.page = api.ListNodesRequest{
		View: current.Ref, Layer: "content", Limit: 1,
		Limits: api.QueryLimits{MaxNodes: 4 * records, MaxEdges: 4 * records, MaxBytes: 8 << 20, MaxDepth: 4},
	}
	f.search = api.SearchNodesRequest{
		View: current.Ref, Layer: "content", Text: "r20-key", Match: "prefix", Limit: 1,
		Limits: api.QueryLimits{MaxNodes: 4 * records, MaxEdges: 4 * records, MaxBytes: 8 << 20, MaxDepth: 4},
	}
	f.trace = api.TraceRequest{
		View: current.Ref, Start: f.contentRef, Direction: "parents",
		Limits: api.QueryLimits{MaxNodes: 4 * records, MaxEdges: 4 * records, MaxBytes: 8 << 20, MaxDepth: 4},
	}
	f.node = api.GetNodeRequest{
		View: current.Ref, Ref: f.contentRef,
		Limit: api.QueryLimits{MaxNodes: 4 * records, MaxEdges: 4 * records, MaxBytes: 8 << 20, MaxDepth: 4},
	}
	f.content = api.ReadContentRequest{
		View: current.Ref, Ref: f.contentRef, Projection: "payload",
		Length: 64, MaxBytes: 1 << 20,
	}
	f.state = api.ReadViewStateRequest{View: current.Ref, Key: f.contentKey}

	if page, err := e.ListNodes(ctx, f.page); err != nil || len(page.Items) != 1 || !page.Truncated || page.Continuation == "" {
		b.Fatalf("list preflight: %+v err=%v", page, err)
	} else {
		next := f.page
		next.Continuation = page.Continuation
		if second, err := e.ListNodes(ctx, next); err != nil || len(second.Items) != 1 {
			b.Fatalf("list continuation preflight: %+v err=%v", second, err)
		}
	}
	if search, err := e.SearchNodes(ctx, f.search); err != nil || len(search.Items) != 1 || search.Coverage != "complete_layer" {
		b.Fatalf("search preflight: %+v err=%v", search, err)
	}
	if traced, err := e.Trace(ctx, f.trace); err != nil || traced.Start != f.contentRef {
		b.Fatalf("trace preflight: %+v err=%v", traced, err)
	}
	if detail, err := e.GetNode(ctx, f.node); err != nil || detail.Ref != f.contentRef || len(detail.Data) == 0 {
		b.Fatalf("node preflight: %+v err=%v", detail, err)
	}
	if content, err := e.ReadContent(ctx, f.content); err != nil || content.Ref != f.contentRef || len(content.Data) == 0 || content.Digest == "" {
		b.Fatalf("content preflight: %+v err=%v", content, err)
	}
	if state, err := e.GetViewState(ctx, f.state); err != nil || !state.Found {
		b.Fatalf("state preflight: %+v err=%v", state, err)
	}
	if historicalEntries, err := e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: historical.Ref, Limits: f.limits}); err != nil || len(historicalEntries.Items) != records {
		b.Fatalf("historical entries preflight: len=%d err=%v", len(historicalEntries.Items), err)
	}
	f.drainObservation()
	f.representative = make(map[string]nodeR20Observation)
	f.recordRepresentative("list", func() {
		page, err := e.ListNodes(ctx, f.page)
		if err != nil || len(page.Items) != 1 {
			b.Fatalf("list representative: %+v err=%v", page, err)
		}
	})
	f.recordRepresentative("search", func() {
		result, err := e.SearchNodes(ctx, f.search)
		if err != nil || len(result.Items) != 1 {
			b.Fatalf("search representative: %+v err=%v", result, err)
		}
	})
	f.recordRepresentative("trace", func() {
		result, err := e.Trace(ctx, f.trace)
		if err != nil || result.Start != f.contentRef {
			b.Fatalf("trace representative: %+v err=%v", result, err)
		}
	})
	f.recordRepresentative("get-node", func() {
		detail, err := e.GetNode(ctx, f.node)
		if err != nil || detail.Ref != f.contentRef {
			b.Fatalf("node representative: %+v err=%v", detail, err)
		}
	})
	f.recordRepresentative("read-content", func() {
		content, err := e.ReadContent(ctx, f.content)
		if err != nil || content.Ref != f.contentRef {
			b.Fatalf("content representative: %+v err=%v", content, err)
		}
	})
	f.recordRepresentative("view-state", func() {
		state, err := e.GetViewState(ctx, f.state)
		if err != nil || !state.Found {
			b.Fatalf("state representative: %+v err=%v", state, err)
		}
	})
	f.recordRepresentative("view-entries", func() {
		entries, err := e.GetViewEntries(ctx, f.entries)
		if err != nil || len(entries.Items) != records {
			b.Fatalf("entries representative: len=%d err=%v", len(entries.Items), err)
		}
	})
	f.recordRepresentative("open-historical", func() {
		view, err := e.OpenView(ctx, api.OpenViewRequest{
			Space: nodeR20Space, Principal: nodeR20Principal, Anchor: &f.historicalCut,
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
			b.Fatal(err)
		}
	})
	return f
}

func nodeR20Mutate(b *testing.B, e *engine.Engine, id string, operations []api.StateMutation) api.CommitResult {
	b.Helper()
	result, err := e.Mutate(context.Background(), api.MutateRequest{
		Key:   model.RequestKey{Space: nodeR20Space, Principal: nodeR20Principal, RequestID: model.RequestID(id)},
		Batch: api.StateBatch{Operations: operations},
	})
	if err != nil || result.Code != api.CodeDurableAck {
		b.Fatalf("mutation %s failed: %+v err=%v", id, result, err)
	}
	return result
}

func (f *nodeR20Fixture) drainObservation() nodeR20Observation {
	events := make([]observation.Event, observation.MaxCapacity)
	n := f.obs.Drain(events)
	var out nodeR20Observation
	for _, event := range events[:n] {
		switch event.Kind {
		case observation.Read:
			out.reads += event.Count
			out.readBytes += event.Bytes
		case observation.Scan:
			out.scans += event.Count
			out.scanBytes += event.Bytes
		}
	}
	return out
}

func (f *nodeR20Fixture) recordRepresentative(name string, operation func()) {
	f.drainObservation()
	operation()
	f.representative[name] = f.drainObservation()
}

var (
	nodeR20NodeSink    api.NodeDetail
	nodeR20PageSink    api.NodePage
	nodeR20SearchSink  api.NodeSearchResult
	nodeR20TraceSink   api.NodeTrace
	nodeR20ContentSink api.ContentRange
	nodeR20StateSink   api.StateValueResult
	nodeR20EntriesSink api.StateEntriesResult
)

func BenchmarkNodeR20Queries(b *testing.B) {
	for _, records := range []int{32, 256, 1024} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			f := newNodeR20Fixture(b, records)
			ctx := context.Background()
			b.Run("list/page-1", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.ListNodes(ctx, f.page)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20PageSink = got
				}
				b.StopTimer()
				f.report(b, "list")
			})
			b.Run("search/page-1", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.SearchNodes(ctx, f.search)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20SearchSink = got
				}
				b.StopTimer()
				f.report(b, "search")
			})
			b.Run("trace/page-1", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.Trace(ctx, f.trace)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20TraceSink = got
				}
				b.StopTimer()
				f.report(b, "trace")
			})
			b.Run("get-node", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.GetNode(ctx, f.node)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20NodeSink = got
				}
				b.StopTimer()
				f.report(b, "get-node")
			})
			b.Run("read-content", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.ReadContent(ctx, f.content)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20ContentSink = got
				}
				b.StopTimer()
				f.report(b, "read-content")
			})
			b.Run("view-state", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.GetViewState(ctx, f.state)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20StateSink = got
				}
				b.StopTimer()
				f.report(b, "view-state")
			})
			b.Run("view-entries", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.GetViewEntries(ctx, f.entries)
					if err != nil {
						b.Fatal(err)
					}
					nodeR20EntriesSink = got
				}
				b.StopTimer()
				f.report(b, "view-entries")
			})
			b.Run("open-view/historical", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := f.e.OpenView(ctx, api.OpenViewRequest{
						Space: nodeR20Space, Principal: nodeR20Principal, Anchor: &f.historicalCut,
					})
					if err != nil {
						b.Fatal(err)
					}
					if err := f.e.CloseView(ctx, api.CloseViewRequest{Ref: got.Ref}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				f.report(b, "open-historical")
			})
		})
	}
}

func (f *nodeR20Fixture) report(b *testing.B, name string) {
	// Timed calls may fill the lossy mailbox. Drain it outside the timer, but
	// report the separately captured representative preflight for this query.
	f.drainObservation()
	metrics := f.representative[name]
	b.ReportMetric(float64(f.records), "records")
	b.ReportMetric(float64(metrics.reads), "representative-reads")
	b.ReportMetric(float64(metrics.readBytes), "representative-read-B")
	b.ReportMetric(float64(metrics.scans), "representative-scans")
	b.ReportMetric(float64(metrics.scanBytes), "representative-scan-B")
}
