package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// R19 scope fixtures exercise the immutable profile/query projection layer.
// They deliberately exclude journal IO, publication, authorization and record
// image validation; those costs belong to the Engine integration and recovery
// baselines. Cross-task links are represented because profile.scope preserves
// that query topology after the durable reducer has validated it.
type taskTraceR19ScopeFixture struct {
	name         string
	snapshot     agentprofile.Snapshot
	profile      *taskTraceProfile
	request      api.TaskTraceRequest
	scopeKeys    []traceEventKey
	profileBytes int
	records      int
}

func newTaskTraceR19ScopeFixture(name string, width, depth int, sharedTask bool) taskTraceR19ScopeFixture {
	snapshot := agentprofile.Snapshot{}
	rootTask := profileCoordinatorTask("r19-root")
	snapshot.Tasks = append(snapshot.Tasks, rootTask)
	root := model.ExecutionID("r19-root-execution")
	snapshot.Executions = append(snapshot.Executions, agentprofile.Execution{
		ID: root, Task: rootTask.ID, Actor: "worker", Incarnation: 1,
		AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
	})

	addTask := func(id model.EntityID) {
		snapshot.Tasks = append(snapshot.Tasks, profileCoordinatorTask(id))
	}
	addExecution := func(id model.ExecutionID, task model.EntityID, parent model.ExecutionID) {
		snapshot.Executions = append(snapshot.Executions, agentprofile.Execution{
			ID: id, Task: task, Parent: parent, Actor: "worker", Incarnation: 1,
			AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
		})
	}

	switch name {
	case "deep":
		parent := root
		for i := 0; i < depth; i++ {
			task := model.EntityID(fmt.Sprintf("r19-deep-task-%d", i))
			addTask(task)
			execution := model.ExecutionID(fmt.Sprintf("r19-deep-execution-%d", i))
			addExecution(execution, task, parent)
			parent = execution
		}
	case "wide", "shared":
		for i := 0; i < width; i++ {
			task := model.EntityID(fmt.Sprintf("r19-wide-task-%d", i))
			if sharedTask {
				task = "r19-shared-task"
			} else {
				addTask(task)
			}
			execution := model.ExecutionID(fmt.Sprintf("r19-wide-execution-%d", i))
			addExecution(execution, task, root)
		}
		if sharedTask {
			addTask("r19-shared-task")
		}
	default:
		panic("unknown R19 scope fixture: " + name)
	}

	profile := newTaskTraceProfile(snapshot)
	request := api.TaskTraceRequest{
		Space: "coordinator-space", Principal: "host",
		Subject: api.TraceSubject{Kind: "task", ID: string(rootTask.ID)},
	}
	keys, err := profile.scope(request)
	if err != nil {
		panic(err)
	}
	seen := make(map[traceEventKey]struct{}, len(keys))
	for _, key := range keys {
		if key.space != request.Space || key.principal != request.Principal {
			panic("R19 scope crossed identity boundary")
		}
		if _, found := seen[key]; found {
			panic("R19 scope emitted duplicate key")
		}
		seen[key] = struct{}{}
	}
	return taskTraceR19ScopeFixture{
		name: name, snapshot: snapshot, profile: profile, request: request,
		scopeKeys: keys, profileBytes: profile.bytes,
		records: len(snapshot.Tasks) + len(snapshot.Executions),
	}
}

var (
	taskTraceR19ScopeSink    []traceEventKey
	taskTraceR19PageSink     []eventMetadataItem
	taskTraceR19OverviewSink []api.TaskTraceEdge
	taskTraceR19EvidenceSink *traceEvidence
)

func BenchmarkTaskTraceR19Scope(b *testing.B) {
	for _, fixture := range []taskTraceR19ScopeFixture{
		newTaskTraceR19ScopeFixture("deep", 0, 32, false),
		newTaskTraceR19ScopeFixture("deep", 0, 256, false),
		newTaskTraceR19ScopeFixture("deep", 0, 1024, false),
		newTaskTraceR19ScopeFixture("wide", 32, 0, false),
		newTaskTraceR19ScopeFixture("wide", 256, 0, false),
		newTaskTraceR19ScopeFixture("wide", 1024, 0, false),
		newTaskTraceR19ScopeFixture("shared", 32, 0, true),
		newTaskTraceR19ScopeFixture("shared", 256, 0, true),
		newTaskTraceR19ScopeFixture("shared", 1024, 0, true),
	} {
		b.Run(fmt.Sprintf("%s/records=%d", fixture.name, fixture.records), func(b *testing.B) {
			b.Run("warm-scope", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					keys, err := fixture.profile.scope(fixture.request)
					if err != nil {
						b.Fatal(err)
					}
					taskTraceR19ScopeSink = keys
				}
				b.StopTimer()
				b.ReportMetric(float64(len(fixture.scopeKeys)), "scope-keys/op")
				b.ReportMetric(float64(fixture.profileBytes), "profile-bytes")
				b.ReportMetric(float64(fixture.records), "records")
			})
			b.Run("cold-profile-and-scope", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					profile := newTaskTraceProfile(fixture.snapshot)
					keys, err := profile.scope(fixture.request)
					if err != nil {
						b.Fatal(err)
					}
					taskTraceR19ScopeSink = keys
				}
				b.StopTimer()
				b.ReportMetric(float64(len(fixture.scopeKeys)), "scope-keys/op")
				b.ReportMetric(float64(fixture.profileBytes), "profile-bytes")
				b.ReportMetric(float64(fixture.records), "records")
			})
		})
	}
}

func taskTraceR19EventFixture(keys []traceEventKey, eventsPerKey int) *Engine {
	e := &Engine{traceEvents: make(map[traceEventKey][]eventMetadataItem, len(keys))}
	for keyIndex, key := range keys {
		items := make([]eventMetadataItem, eventsPerKey)
		for i := range items {
			operation := "trace.noise"
			if i%8 == 0 {
				operation = "Control.SpawnChild"
			}
			id := model.EventID(fmt.Sprintf("r19-event-%d-%d", keyIndex, i))
			if i%16 == 0 {
				id = model.EventID(fmt.Sprintf("r19-shared-event-%d", i/16))
			}
			items[i] = eventMetadataItem{
				id:        id,
				operation: operation,
				entry:     eventEntry{LSN: uint64(i*len(keys) + keyIndex + 1)},
			}
		}
		e.traceEvents[key] = items
	}
	return e
}

func BenchmarkTaskTraceR19EventFilterAndPage(b *testing.B) {
	for _, fixture := range []taskTraceR19ScopeFixture{
		newTaskTraceR19ScopeFixture("deep", 0, 32, false),
		newTaskTraceR19ScopeFixture("deep", 0, 256, false),
		newTaskTraceR19ScopeFixture("wide", 32, 0, false),
		newTaskTraceR19ScopeFixture("wide", 256, 0, false),
		newTaskTraceR19ScopeFixture("shared", 256, 0, true),
	} {
		for _, eventsPerKey := range []int{32, 256} {
			b.Run(fmt.Sprintf("%s/keys=%d/events-per-key=%d", fixture.name, len(fixture.scopeKeys), eventsPerKey), func(b *testing.B) {
				e := taskTraceR19EventFixture(fixture.scopeKeys, eventsPerKey)
				fence := model.CommitFence{LSN: uint64(len(fixture.scopeKeys) * eventsPerKey)}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					items, _, _, err := e.traceEventPage(ctx, fixture.scopeKeys, fence, tracePosition{}, 32, func(item eventMetadataItem) bool {
						return isTraceMilestoneOperation(item.operation)
					})
					if err != nil || len(items) == 0 {
						b.Fatal(len(items), err)
					}
					taskTraceR19PageSink = items
				}
				b.StopTimer()
				b.ReportMetric(float64(len(fixture.scopeKeys)), "scope-keys")
				b.ReportMetric(float64(eventsPerKey*len(fixture.scopeKeys)), "indexed-events")
				b.ReportMetric(float64((eventsPerKey+15)/16), "shared-event-ids")
				b.ReportMetric(float64(len(taskTraceR19PageSink)), "page-items/op")
			})
		}
	}
}

func BenchmarkTaskTraceR19OverviewAndEvidence(b *testing.B) {
	for _, depth := range []int{32, 256, 1024} {
		for _, pages := range []int{1, 32, 128} {
			b.Run(fmt.Sprintf("overview/depth=%d/pages=%d", depth, pages), func(b *testing.B) {
				r, ids, milestones := sharedOverviewFixture(depth, pages)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					edges, _, err := r.overview(ctx, ids, milestones)
					if err != nil {
						b.Fatal(err)
					}
					taskTraceR19OverviewSink = edges
				}
				b.StopTimer()
				b.ReportMetric(float64(depth+1), "shared-ancestry")
				b.ReportMetric(float64(pages), "selected-events")
				b.ReportMetric(float64(len(taskTraceR19OverviewSink)), "edges/op")
			})
		}
	}

	for _, fixture := range []struct {
		agents int
		depth  int
	}{
		{1, 2048},
		{16, 128},
		{64, 32},
	} {
		b.Run(fmt.Sprintf("evidence/agents=%d/depth=%d", fixture.agents, fixture.depth), func(b *testing.B) {
			r, req, _ := longEvidenceFixture(fixture.agents, fixture.depth)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e := &Engine{}
				clear(r.events)
				value, err := e.causalTraceEvidence(ctx, req, r)
				if err != nil {
					b.Fatal(err)
				}
				taskTraceR19EvidenceSink = value
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.agents*fixture.depth+2), "path-events")
			b.ReportMetric(float64(len(taskTraceR19EvidenceSink.dependencies)), "dependencies/op")
		})
	}
}
