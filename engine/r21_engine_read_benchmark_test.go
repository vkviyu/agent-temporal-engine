package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
)

var (
	r21EngineNodeSink    api.NodeDetail
	r21EngineContextSink api.NodeContext
)

func BenchmarkR21EngineDisclosureReads(b *testing.B) {
	for _, records := range []int{32, 256, 1024} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			fixture := newNodeR20Fixture(b, records)
			ctx := context.Background()
			contextRequest := api.NodeContextRequest{
				View: fixture.current.Ref, Ref: fixture.contentRef, Mode: "parents",
				Limits: api.QueryLimits{
					MaxNodes: 4 * records, MaxEdges: 4 * records,
					MaxBytes: 8 << 20, MaxDepth: 4,
				},
			}
			b.Run("warm/get-node", func(b *testing.B) {
				if _, err := fixture.e.GetNode(ctx, fixture.node); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := fixture.e.GetNode(ctx, fixture.node)
					if err != nil {
						b.Fatal(err)
					}
					r21EngineNodeSink = got
				}
			})
			b.Run("warm/get-node-context", func(b *testing.B) {
				if _, err := fixture.e.GetNodeContext(ctx, contextRequest); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, err := fixture.e.GetNodeContext(ctx, contextRequest)
					if err != nil {
						b.Fatal(err)
					}
					r21EngineContextSink = got
				}
			})
		})
	}
}
