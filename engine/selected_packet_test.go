package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestSelectedPacketCoreAuthenticatesHistoricalPlanAndReplays(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	origins := make([]knowledge.Observation, 5)
	for i := range origins {
		o := knowledgeObservation(payload, source, "one", uint64(i+1))
		o.Label = []string{"goal", "conclusion", "evidence", "counterexample", "unrelated"}[i]
		o.ID = knowledge.ObservationID(o)
		applyObservation(t, s.e, "selection-"+o.Label, o)
		origins[i] = o
	}
	key := origins[0].Key
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	proposal := knowledge.ContextSelectionRequest{Snapshot: known.Snapshot, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: known.Frontier, ViewDigest: view.ViewDigest}, Capacity: knowledge.ContextCapacity{Window: 12, OutputReserve: 2, FramingReserve: 2}, Mandatory: []model.ObjectID{origins[0].ID}, Tokenizer: "framed-test/v1", CountQuality: knowledge.TokenCountEstimated}
	for _, origin := range origins {
		proposal.Costs = append(proposal.Costs, knowledge.ContextCost{Observation: origin.ID, Tokens: 2})
	}
	bundle := knowledge.ContextBundle{Conclusion: origins[1].ID, Evidence: []model.ObjectID{origins[2].ID}, Counterexamples: []model.ObjectID{origins[3].ID}, Benefit: 6}
	bundle.ID = knowledge.ContextBundleID(bundle)
	proposal.Bundles = []knowledge.ContextBundle{bundle}
	record, err := knowledge.RecordContextSelection(proposal)
	if err != nil {
		t.Fatal(err)
	}
	if record.Result.UsedTokens != 8 || len(record.Result.Omitted) != 1 || record.Result.Omitted[0].Observation != origins[4].ID {
		t.Fatalf("unexpected source plan: %+v", record.Result)
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &key, Source: proposal.Source, GoalRef: payload, Renderer: "framed-test/v1", Tokenizer: proposal.Tokenizer, EstimatedTokens: record.Result.UsedTokens + proposal.Capacity.FramingReserve, Input: &knowledge.RecordedInput{Object: payload, Anchor: sourceAnchor(source), Encoding: "application/test"}, Selection: &record}
	byID := map[model.ObjectID]knowledge.Observation{}
	for _, origin := range origins {
		byID[origin.ID] = origin
	}
	for i, id := range record.Result.Selected {
		o := byID[id]
		packet.Items = append(packet.Items, knowledge.ContextItem{Ordinal: uint64(i), Observation: id, Content: o.Payload, Label: o.Label})
	}
	packet.ID = knowledge.PacketID(packet)
	request := api.ApplyKnowledgeRequest{Key: agentKey("selected-packet"), Command: knowledge.Command{Packet: &packet}}
	ack, err := s.e.ApplyKnowledge(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("selected packet: %+v %v", ack, err)
	}
	bad := packet.Clone()
	bad.Selection.Result.Omitted[0].Reason = "capacity"
	bad.ID = knowledge.PacketID(bad)
	before := integrationSnapshot(t, s.e)
	if out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("selection-forged-omission"), Command: knowledge.Command{Packet: &bad}}); !errors.Is(err, knowledge.ErrContextSelectionInvalid) || out.Code == api.CodeDurableAck {
		t.Fatalf("forged omission accepted: %+v %v", out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("invalid selection changed or fenced publication")
	}
	// Later knowledge must not alter the persisted source or the omitted set.
	applyObservation(t, s.e, "selection-later-knowledge", knowledgeObservation(payload, source, "one", 6))
	check := func() {
		t.Helper()
		got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
		if err != nil || len(got.Snapshot.Packets) != 1 {
			t.Fatalf("recorded packet missing: %+v %v", got, err)
		}
		if !reflect.DeepEqual(got.Snapshot.Packets[0], packet) {
			t.Fatal("stored selection changed after subsequent knowledge or reopen")
		}
		if retry, err := s.e.ApplyKnowledge(ctx, request); err != nil || retry != ack {
			t.Fatalf("selected-packet receipt changed: %+v %v", retry, err)
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
