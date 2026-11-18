package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type importedKnowledgeFact struct {
	logicalID    string
	importedFrom string
	payload      model.ObjectID
	sourceFence  uint64
	sequence     uint64
}

type knowledgeImportOracle struct {
	source []importedKnowledgeFact
	target []importedKnowledgeFact
}

func (o *knowledgeImportOracle) Observe(logicalID string, payload model.ObjectID, fence uint64) {
	o.source = append(o.source, importedKnowledgeFact{
		logicalID: logicalID, payload: payload, sourceFence: fence,
		sequence: uint64(len(o.source) + 1),
	})
}

func (o *knowledgeImportOracle) Import(logicalID string, selectedFence uint64) error {
	for _, fact := range o.source {
		if fact.logicalID == logicalID {
			if fact.sourceFence > selectedFence {
				return errors.New("source fact is after selected cut")
			}
			o.target = append(o.target, importedKnowledgeFact{
				logicalID:    "target/" + fact.logicalID,
				importedFrom: fact.logicalID,
				payload:      fact.payload,
				sourceFence:  selectedFence,
				sequence:     uint64(len(o.target) + 1),
			})
			return nil
		}
	}
	return errors.New("unknown source fact")
}

func compareImportedFacts(got []knowledge.Observation, expected []importedKnowledgeFact, target model.KnowledgeKey, ids map[string]model.ObjectID) error {
	if len(got) != len(expected) {
		return fmt.Errorf("import count=%d want=%d", len(got), len(expected))
	}
	bySequence := make(map[uint64]knowledge.Observation, len(got))
	for _, observation := range got {
		bySequence[observation.Sequence] = observation
	}
	for _, fact := range expected {
		observation, found := bySequence[fact.sequence]
		if !found || observation.Key != target || observation.Payload != fact.payload ||
			observation.ImportedFrom != ids[fact.importedFrom] || observation.Source.Anchor.Fence.LSN != fact.sourceFence {
			return fmt.Errorf("import sequence=%d differs from logical fact %+v", fact.sequence, fact)
		}
	}
	return nil
}

func TestIndependentKnowledgeImportRetainsSelectedHistoryAcrossRecovery(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	want := &knowledgeImportOracle{}

	one := knowledgeObservation(payload, source, "one", 1)
	_, firstAck := applyObservation(t, s.e, "independent-import-one", one)
	first, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{
		Space: "host-space", Principal: "host-principal", Knowledge: &one.Key,
	})
	if err != nil {
		t.Fatal(err)
	}
	want.Observe("source/one", payload, firstAck.Fence.LSN)

	anchor := first.Anchor.Clone()
	anchor.Cursor = &model.Cursor{
		Store: source.Store, Space: "host-space", Revision: first.Revision.ID,
	}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{
		Space: "host-space", Principal: "host-principal",
		Mode: "execution_knowledge", Anchor: &anchor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}

	target := model.KnowledgeKey{Actor: "fresh-actor", Execution: "fresh"}
	execution := agentprofile.Execution{
		ID: target.Execution, Task: "kt", Actor: target.Actor,
		Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
	}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{
		Key:     agentKey("independent-import-execution"),
		Command: agentprofile.Command{Spawn: &execution},
	})

	two := knowledgeObservation(payload, source, "one", 2)
	applyObservation(t, s.e, "independent-import-two", two)
	want.Observe("source/two", payload, integrationSnapshot(t, s.e).Fence.LSN)

	if err := want.Import("source/one", first.Fence.LSN); err != nil {
		t.Fatal(err)
	}
	request := api.ApplyKnowledgeRequest{
		Key: agentKey("independent-import"),
		Command: knowledge.Command{Import: &knowledge.ImportCommand{
			Target: target,
			Source: knowledge.SourceAnchor{
				Anchor:     anchor,
				Frontier:   first.Frontier,
				ViewDigest: view.ViewDigest,
			},
			Observations: []model.ObjectID{one.ID},
		}},
	}
	ack, err := s.e.ApplyKnowledge(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("import=%+v %v", ack, err)
	}

	check := func() {
		t.Helper()
		targetState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{
			Space: "host-space", Principal: "host-principal", Knowledge: &target,
		})
		if err != nil {
			t.Fatalf("target=%+v %v", targetState, err)
		}
		if err := compareImportedFacts(targetState.Snapshot.Observations, want.target, target,
			map[string]model.ObjectID{"source/one": one.ID, "source/two": two.ID}); err != nil {
			t.Fatal(err)
		}
		got := targetState.Snapshot.Observations[0]
		expected := want.target[0]
		if got.Key != target || got.Payload != expected.payload ||
			got.ImportedFrom != one.ID || got.Sequence != expected.sequence ||
			!reflect.DeepEqual(got.Source.Anchor, anchor) {
			t.Fatalf("import projection=%+v expected=%+v", got, expected)
		}

		sourceState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{
			Space: "host-space", Principal: "host-principal", Knowledge: &one.Key,
		})
		if err != nil || len(sourceState.Snapshot.Observations) != 2 {
			t.Fatalf("source=%+v %v", sourceState, err)
		}
		bySequence := map[uint64]knowledge.Observation{}
		for _, observation := range sourceState.Snapshot.Observations {
			bySequence[observation.Sequence] = observation
		}
		if bySequence[one.Sequence].ID != one.ID ||
			bySequence[two.Sequence].ID != two.ID {
			t.Fatalf("source projection changed=%+v", sourceState.Snapshot.Observations)
		}
		before := integrationSnapshot(t, s.e)
		if retry, err := s.e.ApplyKnowledge(ctx, request); err != nil || retry != ack || integrationSnapshot(t, s.e) != before {
			t.Fatalf("import retry changed receipt: %+v %v", retry, err)
		}
	}
	check()

	before := integrationSnapshot(t, s.e)
	bad := *request.Command.Import
	bad.Observations = []model.ObjectID{two.ID}
	modelErr := want.Import("source/two", first.Fence.LSN)
	rejected, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{
		Key:     agentKey("independent-import-future"),
		Command: knowledge.Command{Import: &bad},
	})
	if modelErr == nil || (err == nil) != (modelErr == nil) || rejected.Code != api.CodeRejected ||
		!errors.Is(err, knowledge.ErrUnknownObservation) ||
		integrationSnapshot(t, s.e) != before {
		t.Fatalf("future source accepted: %+v %v", rejected, err)
	}
	check()

	end := integrationSnapshot(t, s.e)
	if _, err := s.e.CreateRecoveryCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if report := s.reopen(); report.CheckpointTransactions != end.Fence.LSN {
		t.Fatalf("checkpoint not used: %+v", report)
	}
	check()
	s.options.DisableRecoveryCheckpoints = true
	if report := s.reopen(); report.CheckpointTransactions != 0 || report.ScannedTransactions != end.Fence.LSN {
		t.Fatalf("full replay not used: %+v", report)
	}
	s.options.DisableRecoveryCheckpoints = false
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func TestIndependentKnowledgeOracleDetectsFutureImport(t *testing.T) {
	want := &knowledgeImportOracle{}
	want.Observe("early", "payload", 1)
	want.Observe("future", "payload", 3)
	if err := want.Import("early", 2); err != nil {
		t.Fatal(err)
	}
	if err := want.Import("future", 2); err == nil {
		t.Fatal("model accepted future fact")
	}
	target := model.KnowledgeKey{Actor: "target", Execution: "execution"}
	ids := map[string]model.ObjectID{"early": "early-id", "future": "future-id"}
	got := []knowledge.Observation{{
		Key: target, Payload: "payload", ImportedFrom: "early-id", Sequence: 1,
		Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Fence: model.CommitFence{LSN: 2}}},
	}}
	if err := compareImportedFacts(got, want.target, target, ids); err != nil {
		t.Fatal(err)
	}
	got[0].ImportedFrom = "future-id"
	if err := compareImportedFacts(got, want.target, target, ids); err == nil {
		t.Fatal("oracle accepted a fact from after the selected cut")
	}
}
