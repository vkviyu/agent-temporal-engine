package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentKnowledgeVisibilityAcrossExecutionAndHistoricalCut(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	want := oracle.NewKnowledge()
	type publication struct {
		id    string
		fence model.CommitFence
	}
	var publications []publication
	for i := range 6 {
		execution := model.ExecutionID("one")
		if i%2 == 1 {
			execution = "two"
		}
		o := knowledgeObservation(payload, source, execution, uint64(i/2+1))
		_, ack := applyObservation(t, s.e, fmt.Sprintf("visibility-%d", i), o)
		want.Adopt(oracle.Scope{Space: "host-space", Principal: "host-principal", Actor: "same-actor", Execution: string(execution)}, string(o.ID), ack.Fence.LSN)
		publications = append(publications, publication{string(o.ID), ack.Fence})
	}
	check := func() {
		t.Helper()
		for _, cut := range publications {
			for _, execution := range []model.ExecutionID{"one", "two"} {
				key := model.KnowledgeKey{Actor: "same-actor", Execution: execution}
				anchor := model.SnapshotAnchor{Space: "host-space", Fence: cut.fence, Knowledge: &key}
				got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
				if err != nil {
					t.Fatal(err)
				}
				seen := map[string]bool{}
				for _, o := range got.Snapshot.Observations {
					seen[string(o.ID)] = true
				}
				for _, fact := range publications {
					expected := want.Visible(oracle.Scope{Space: "host-space", Principal: "host-principal", Actor: "same-actor", Execution: string(execution)}, fact.id, cut.fence.LSN)
					if seen[fact.id] != expected {
						t.Fatalf("execution=%s cut=%d fact=%s visible=%v expected=%v", execution, cut.fence.LSN, fact.id, seen[fact.id], expected)
					}
				}
				if _, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "other", Anchor: &anchor}); engine.ClassifyError(err) != api.ErrorUnauthorized {
					t.Fatalf("foreign principal not rejected: %v", err)
				}
			}
		}
	}
	check()
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}
