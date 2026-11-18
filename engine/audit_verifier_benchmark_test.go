package engine

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Prebuilt synthetic plans isolate event installation from command preparation,
// encoding, journal IO and semantic replay. Each transaction installs a core
// occurrence and one attributed fact; facts form 16 independent causal chains.
func auditInstallPlans(count int) []commitPlan {
	plans := make([]commitPlan, count)
	heads := make([]model.EventID, 16)
	for i := range plans {
		stream := i % len(heads)
		execution := model.ExecutionID(fmt.Sprintf("execution-%d", stream))
		tx := model.TxID(fmt.Sprintf("%032x", i+1))
		core := model.Event{ID: model.EventID(fmt.Sprintf("%064x", 2*i+1)),
			Space: "audit-space", RecordedBy: "host", Transaction: tx, Sequence: uint64(i + 1)}
		fact := model.Event{ID: model.EventID(fmt.Sprintf("%064x", 2*i+2)),
			Space: core.Space, RecordedBy: core.RecordedBy, Transaction: tx,
			Stream:   model.EventStreamKey{Kind: "execution", Owner: string(execution)},
			Sequence: uint64(i/len(heads) + 1),
			Attribution: &model.EventAttribution{Operation: "Control.Checkpoint", Ordinal: 1,
				Query: "query", Task: model.EntityID(fmt.Sprintf("task-%d", stream)), Execution: execution}}
		if parent := heads[stream]; parent != "" {
			fact.CausalParents = []model.EventID{parent}
		}
		heads[stream] = fact.ID
		plans[i] = commitPlan{event: &core, profileEvents: []model.Event{fact},
			transaction: storage.Transaction{LSN: uint64(i + 1)}}
	}
	return plans
}

func TestAuditInstallEventCausalIndexes(t *testing.T) {
	indexed, verifier := &Engine{}, newAuditVerifier(Options{})
	plans := auditInstallPlans(32)
	for i := range plans {
		indexed.installEvent(&plans[i])
		verifier.installEvent(&plans[i])
		assertAuditIndexesAbsent(t, verifier)
	}
	assertAuditIndexesPopulated(t, indexed)
	if len(indexed.traceCausalChildren) != 16 {
		t.Fatalf("zero-value Engine lost causal children: %d", len(indexed.traceCausalChildren))
	}
	if !reflect.DeepEqual(indexed.events, verifier.events) ||
		!reflect.DeepEqual(indexed.eventHeads, verifier.eventHeads) ||
		!reflect.DeepEqual(indexed.eventBlocks, verifier.eventBlocks) {
		t.Fatal("skipping causal indexes changed semantic event state")
	}
}

func BenchmarkAuditEventInstallation(b *testing.B) {
	for _, count := range []int{256, 4096} {
		plans := auditInstallPlans(count)
		for _, skip := range []bool{false, true} {
			mode := "indexed"
			if skip {
				mode = "audit"
			}
			b.Run(fmt.Sprintf("transactions=%d/%s", count, mode), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var e *Engine
					if skip {
						e = newAuditVerifier(Options{})
					} else {
						e = newEngine(Options{})
					}
					for j := range plans {
						e.installEvent(&plans[j])
					}
					runtime.KeepAlive(e)
				}
				b.ReportMetric(float64(count*2), "events/op")
			})
		}
	}
}
