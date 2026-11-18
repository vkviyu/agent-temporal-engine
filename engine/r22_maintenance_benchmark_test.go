package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const (
	r22Space     model.SpaceID     = "host-space"
	r22Principal model.PrincipalID = "host-principal"
)

type r22Fixture struct {
	d         *storage.DirectoryIO
	e         *engine.Engine
	options   engine.Options
	obs       *observation.Collector
	records   int
	profile   bool
	published uint64
}

func newR22Fixture(tb testing.TB, records int, profile bool) *r22Fixture {
	tb.Helper()
	d, err := storage.OpenDirectory(tb.TempDir())
	if err != nil {
		tb.Fatal(err)
	}
	obs, err := observation.New(observation.MaxCapacity)
	if err != nil {
		_ = d.Close()
		tb.Fatal(err)
	}
	options := engine.Options{
		Observation:  obs,
		EventProfile: profile,
		Journal: storage.JournalOptions{
			SegmentTargetBytes: 1024,
		},
	}
	if profile {
		options.AgentProfile = true
	}
	e, err := engine.Create(d, options)
	if err != nil {
		_ = d.Close()
		tb.Fatal(err)
	}
	f := &r22Fixture{d: d, e: e, options: options, obs: obs, records: records, profile: profile}
	tb.Cleanup(func() {
		if f.e != nil {
			if err := f.e.Close(); err != nil {
				tb.Error(err)
			}
			f.e = nil
		}
		if err := f.d.Close(); err != nil {
			tb.Error(err)
		}
	})

	if profile {
		r22ApplyAgent(tb, e, "profile-task", agentprofile.Command{
			CreateTask: &agentprofile.Task{
				ID: "r22-task", Space: r22Space, Owner: r22Principal,
				Goal: "measure maintenance", GoalEpoch: 1, Status: agentprofile.TaskOpen,
			},
		})
		r22ApplyAgent(tb, e, "profile-execution", agentprofile.Command{
			Spawn: &agentprofile.Execution{
				ID: "r22-execution", Task: "r22-task", Actor: "r22-actor",
				Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
			},
		})
	}
	for i := 0; i < records; i++ {
		r22Mutate(tb, e, fmt.Sprintf("state-%06d", i), []byte(fmt.Sprintf(
			"r22-payload-%06d-%s", i,
			"stable payload used to force repeated physical journal frames",
		)))
	}
	f.published = r22Snapshot(tb, e).Fence.LSN
	return f
}

func r22Mutate(tb testing.TB, e *engine.Engine, requestID string, value []byte) {
	tb.Helper()
	result, err := e.Mutate(context.Background(), api.MutateRequest{
		Key: model.RequestKey{Space: r22Space, Principal: r22Principal, RequestID: model.RequestID(requestID)},
		Batch: api.StateBatch{Operations: []api.StateMutation{{
			// Keep logical state bounded while growing transaction history.
			// Unique keys would make each reducer image grow until the journal
			// frame limit dominated this maintenance benchmark.
			Key:   []byte("r22-key"),
			Kind:  api.MutationPut,
			Value: api.Value{Kind: api.ValueBytes, Data: value},
		}}},
	})
	if err != nil || result.Code != api.CodeDurableAck {
		tb.Fatalf("R22 mutation %s: %+v %v", requestID, result, err)
	}
}

func r22ApplyAgent(tb testing.TB, e *engine.Engine, requestID string, command agentprofile.Command) {
	tb.Helper()
	result, err := e.ApplyAgent(context.Background(), api.ApplyAgentRequest{
		Key:     model.RequestKey{Space: r22Space, Principal: r22Principal, RequestID: model.RequestID(requestID)},
		Command: command,
	})
	if err != nil || result.Code != api.CodeDurableAck {
		tb.Fatalf("R22 agent command %s: %+v %v", requestID, result, err)
	}
}

func r22Snapshot(tb testing.TB, e *engine.Engine) api.StateSnapshot {
	tb.Helper()
	snapshot, err := e.Snapshot(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	return snapshot
}

func (f *r22Fixture) drain() []observation.Event {
	events := make([]observation.Event, observation.MaxCapacity)
	return events[:f.obs.Drain(events)]
}

func r22Event(events []observation.Event, kind observation.Kind) (observation.Event, bool) {
	for _, event := range events {
		if event.Kind == kind {
			return event, true
		}
	}
	return observation.Event{}, false
}

func (f *r22Fixture) close(tb testing.TB) {
	tb.Helper()
	if f.e == nil {
		return
	}
	if err := f.e.Close(); err != nil {
		tb.Fatal(err)
	}
	f.e = nil
}

func (f *r22Fixture) open(tb testing.TB, disableCheckpoint bool) storage.RecoveryReport {
	tb.Helper()
	options := f.options
	options.DisableRecoveryCheckpoints = disableCheckpoint
	e, report, err := engine.OpenExisting(f.d, options)
	if err != nil {
		tb.Fatal(err)
	}
	f.e = e
	return report
}

func r22ReportOpen(b *testing.B, f *r22Fixture, report storage.RecoveryReport) {
	b.Helper()
	event, ok := r22Event(f.drain(), observation.Open)
	if !ok {
		b.Fatal("missing Open observation")
	}
	b.ReportMetric(float64(report.ScannedTransactions), "scanned-transactions")
	b.ReportMetric(float64(report.CheckpointTransactions), "checkpoint-transactions")
	b.ReportMetric(float64(event.Replayed), "replayed-transactions")
	b.ReportMetric(float64(event.Certified), "certified-transactions")
	b.ReportMetric(float64(event.Bytes), "published-bytes")
}

func r22ReportMaintenance(b *testing.B, f *r22Fixture, kind observation.Kind) {
	b.Helper()
	event, ok := r22Event(f.drain(), kind)
	if !ok {
		b.Fatalf("missing maintenance observation kind=%d", kind)
	}
	b.ReportMetric(float64(event.Count), "maintenance-count")
	b.ReportMetric(float64(event.Bytes), "maintenance-bytes")
}

func BenchmarkR22OpenFullReplay(b *testing.B) {
	for _, records := range []int{16, 64, 128} {
		for _, profile := range []bool{false, true} {
			name := fmt.Sprintf("records=%d/profile=%t", records, profile)
			b.Run(name, func(b *testing.B) {
				f := newR22Fixture(b, records, profile)
				f.drain()
				f.close(b)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					report := f.open(b, true)
					b.StopTimer()
					r22ReportOpen(b, f, report)
					f.close(b)
					b.StartTimer()
				}
				b.StopTimer()
				b.ReportMetric(float64(records), "fixture-records")
			})
		}
	}
}

func BenchmarkR22OpenCheckpointSuffix(b *testing.B) {
	for _, records := range []int{16, 64, 128} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			f := newR22Fixture(b, records/2, true)
			if _, err := f.e.CreateRecoveryCheckpoint(context.Background()); err != nil {
				b.Fatal(err)
			}
			for i := records / 2; i < records; i++ {
				r22Mutate(b, f.e, fmt.Sprintf("suffix-%06d", i), []byte(fmt.Sprintf("r22-suffix-%06d", i)))
			}
			f.drain()
			f.close(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				report := f.open(b, false)
				b.StopTimer()
				r22ReportOpen(b, f, report)
				f.close(b)
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(records), "fixture-records")
			b.ReportMetric(float64(records/2), "suffix-records")
		})
	}
}

func BenchmarkR22CreateRecoveryCheckpoint(b *testing.B) {
	for _, records := range []int{16, 64, 128} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			f := newR22Fixture(b, records, true)
			f.drain()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := f.e.CreateRecoveryCheckpoint(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			r22ReportMaintenance(b, f, observation.Checkpoint)
			b.ReportMetric(float64(records), "fixture-records")
		})
	}
}

func BenchmarkR22CompactAuditFull(b *testing.B) {
	for _, records := range []int{16, 64, 128} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			f := newR22Fixture(b, records, true)
			if _, err := f.e.CreateRecoveryCheckpoint(context.Background()); err != nil {
				b.Fatal(err)
			}
			f.drain()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := f.e.CompactAuditFull(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			r22ReportMaintenance(b, f, observation.Compaction)
			b.ReportMetric(float64(records), "fixture-records")
		})
	}
}

func BenchmarkR22OpenCheckpointAfterCompaction(b *testing.B) {
	for _, records := range []int{16, 64, 128} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			f := newR22Fixture(b, records, true)
			if _, err := f.e.CreateRecoveryCheckpoint(context.Background()); err != nil {
				b.Fatal(err)
			}
			if _, err := f.e.CompactAuditFull(context.Background()); err != nil {
				b.Fatal(err)
			}
			f.drain()
			f.close(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				report := f.open(b, false)
				b.StopTimer()
				r22ReportOpen(b, f, report)
				f.close(b)
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(records), "fixture-records")
		})
	}
}

func BenchmarkR22OpenFullReplayAfterCompaction(b *testing.B) {
	for _, records := range []int{16, 64, 128} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			f := newR22Fixture(b, records, true)
			if _, err := f.e.CreateRecoveryCheckpoint(context.Background()); err != nil {
				b.Fatal(err)
			}
			if _, err := f.e.CompactAuditFull(context.Background()); err != nil {
				b.Fatal(err)
			}
			f.drain()
			f.close(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				report := f.open(b, true)
				b.StopTimer()
				r22ReportOpen(b, f, report)
				f.close(b)
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(records), "fixture-records")
		})
	}
}
