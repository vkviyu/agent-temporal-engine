package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// These benchmarks isolate metadata costs; they exclude event decoding,
// authorization, and journal IO.
func BenchmarkTaskTraceScopeLookup(b *testing.B) {
	for _, count := range []int{1, 128, 1024} {
		b.Run(fmt.Sprintf("keys=%d", count), func(b *testing.B) {
			keys := make([]traceEventKey, count)
			for i := range keys {
				keys[i] = traceEventKey{"space", "owner", "execution", fmt.Sprint(i)}
			}
			e := &Engine{
				traceEventMeta: map[model.EventID]eventMetadataItem{"event": {entry: eventEntry{LSN: 1}}},
				traceEventKeys: map[model.EventID][]traceEventKey{"event": {keys[count-1]}},
			}
			ctx := context.Background()
			fence := model.CommitFence{LSN: 1}
			// Scope construction is paid once per request, outside traversal.
			selected := newTraceScope(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				found, err := e.traceEventInScope(ctx, "event", selected, fence)
				if err != nil || !found {
					b.Fatal(found, err)
				}
			}
		})
	}
}

func BenchmarkTaskTraceHistoricalChildren(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("children=%d", count), func(b *testing.B) {
			children := make([]eventMetadataItem, count)
			for i := range children {
				children[i] = eventMetadataItem{id: model.EventID(fmt.Sprint(i)), entry: eventEntry{LSN: uint64(i + 1)}}
			}
			e := &Engine{traceCausalChildren: map[model.EventID][]eventMetadataItem{"parent": children}}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := e.traceEventChildren(ctx, "parent", model.CommitFence{LSN: 32})
				if err != nil || len(got) != 32 {
					b.Fatal(len(got), err)
				}
			}
		})
	}
}

func BenchmarkTaskTraceSmallPage(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("events=%d", count), func(b *testing.B) {
			key := traceEventKey{"space", "owner", "task", "task"}
			items := make([]eventMetadataItem, count)
			for i := range items {
				items[i] = eventMetadataItem{id: model.EventID(fmt.Sprint(i)), entry: eventEntry{LSN: uint64(i + 1)}}
			}
			e := &Engine{traceEvents: map[traceEventKey][]eventMetadataItem{key: items}}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, _, _, err := e.traceEventPage(ctx, []traceEventKey{key}, model.CommitFence{LSN: uint64(count)}, tracePosition{}, 32, nil)
				if err != nil || len(got) != 32 {
					b.Fatal(len(got), err)
				}
			}
		})
	}
}
