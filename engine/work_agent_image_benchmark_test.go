package engine

import (
	"context"
	"fmt"
	"testing"
)

var workImageBenchmarkPlan *commitPlan

// Full private Work preparation: boundary validation, four sequential agent
// reductions (plus one interleaved knowledge reduction in mixed mode), object
// encoding/absorption, and final transaction/receipt construction. Every call
// starts at the same historical root with a fresh slot; no cross-Work warming.
// Fixture setup, admission, journal publication/fsync and event generation are
// excluded. The history has tasks, executions, steps and payload-bearing queries.
func BenchmarkWorkAgentImagePreparation(b *testing.B) {
	for _, groups := range []int{16, 128} {
		for _, mixed := range []bool{false, true} {
			b.Run(fmt.Sprintf("history=%d/mixed=%t", groups, mixed), func(b *testing.B) {
				e, req, profileBytes := workImageFixture(b, groups, mixed, false)
				ctx := context.Background()
				cached, err := workImagePrepare(b, ctx, e, req, true)
				if err != nil {
					b.Fatal(err)
				}
				uncached, err := workImagePrepare(b, ctx, e, req, false)
				if err != nil {
					b.Fatal(err)
				}
				assertWorkImagePlansEqual(b, cached, uncached)
				for _, useCache := range []bool{false, true} {
					b.Run(fmt.Sprintf("cached=%t", useCache), func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							plan, err := workImagePrepare(b, ctx, e, req, useCache)
							if err != nil {
								b.Fatal(err)
							}
							workImageBenchmarkPlan = plan
						}
						b.StopTimer()
						b.ReportMetric(float64(profileBytes), "root-envelope-B")
						b.ReportMetric(float64(len(cached.builder.objects)), "objects/op")
					})
				}
			})
		}
	}
}
