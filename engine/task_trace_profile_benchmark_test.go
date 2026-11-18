package engine

import (
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

var traceProfileBenchmarkKeys []traceEventKey
var traceProfileBenchmarkProvenance traceProvenance

func BenchmarkTaskTraceProfile(b *testing.B) {
	for _, records := range []int{128, 1024} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			snapshot := agentprofile.Snapshot{}
			for i := 0; i < records; i++ {
				task := profileCoordinatorTask(model.EntityID(fmt.Sprintf("task-%d", i)))
				snapshot.Tasks = append(snapshot.Tasks, task)
				snapshot.Lifecycle.Queries = append(snapshot.Lifecycle.Queries, agentprofile.Query{
					ID: fmt.Sprintf("query-%d", i), Session: "session", Space: task.Space, Owner: task.Owner,
					Version: 1, Status: "open", Input: make([]byte, 512), Answer: make([]byte, 512),
				})
				parent := model.ExecutionID("")
				for j := 0; j < 4; j++ {
					id := model.ExecutionID(fmt.Sprintf("execution-%d-%d", i, j))
					snapshot.Executions = append(snapshot.Executions, agentprofile.Execution{
						ID: id, Task: task.ID, Parent: parent, Actor: "worker", Incarnation: 1,
						AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
					})
					parent = id
				}
			}
			image, err := agentprofile.EncodeRecordImage(snapshot)
			if err != nil {
				b.Fatal(err)
			}
			key := taskTraceProfileKey{"coordinator-space", "host", "profile-object"}
			roots := model.RootSet{LiveControl: "historical-control", DomainStates: "per-request-domain"}
			p := newTaskTraceProfile(snapshot)
			cache := &taskTraceProfileCache{}
			cache.put(key, p)
			if _, ok := cache.get(key); !ok {
				b.Fatal("benchmark profile exceeds cache capacity")
			}
			for _, kind := range []string{"task", "query"} {
				req := api.TaskTraceRequest{Space: key.space, Principal: key.principal, Subject: api.TraceSubject{Kind: kind, ID: kind + "-0"}}
				for _, mode := range []string{"cold_projection", "warm_scope_provenance"} {
					b.Run(kind+"/"+mode, func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							var keys []traceEventKey
							var provenance traceProvenance
							var err error
							switch mode {
							case "cold_projection":
								cold := newTaskTraceProfile(snapshot)
								keys, err = cold.scope(req)
								provenance = cold.provenanceAt(roots)
							case "warm_scope_provenance":
								warm, ok := cache.get(key)
								if !ok {
									b.Fatal("unexpected miss")
								}
								keys, err = warm.scope(req)
								provenance = warm.provenanceAt(roots)
							}
							if err != nil {
								b.Fatal(err)
							}
							traceProfileBenchmarkKeys, traceProfileBenchmarkProvenance = keys, provenance
						}
						b.StopTimer()
						b.ReportMetric(float64(p.bytes), "profile-bytes")
						b.ReportMetric(float64(image.Size()), "record-bytes")
					})
				}
			}
		})
	}
}
