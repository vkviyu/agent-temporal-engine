package engine_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestKnowledgeIndexedWritesExceedLegacyOwnerSnapshotAndPreserveCuts(t *testing.T) {
	ctx := t.Context()
	s, payload, source := knowledgeFixture(t)
	legacyObservation := knowledgeObservation(payload, source, "one", 1)
	body, err := knowledge.EncodeCommand(knowledge.Command{Observation: &legacyObservation})
	if err != nil {
		t.Fatal(err)
	}
	const legacyVersionOffset = len("ATE-KNOWLEDGE-COMMAND\x00")
	body[legacyVersionOffset] = 1
	legacy := api.CommitRequest{Space: "host-space", Principal: "host-principal", RequestID: "legacy-observation", Method: api.MethodApplyKnowledge, Body: body}
	legacyAck, err := s.e.Commit(ctx, legacy)
	if err != nil || legacyAck.Code != api.CodeDurableAck {
		t.Fatalf("legacy write = %+v, %v", legacyAck, err)
	}
	key := legacyObservation.Key
	legacyCut := model.SnapshotAnchor{Space: "host-space", Fence: legacyAck.Fence, Knowledge: &key}
	legacyState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &legacyCut})
	if err != nil || legacyState.Revision == nil || legacyState.Revision.Sequence != 1 {
		t.Fatalf("legacy state = %+v, %v", legacyState, err)
	}
	indexed := knowledgeObservation(payload, source, "one", 2)
	indexedRequest, indexedAck := applyObservation(t, s.e, "indexed-observation", indexed)

	// Every packet refers to the same exact adoption cut but contains its own
	// render metadata. No packet is itself near the per-record bound.
	other := knowledgeObservation(payload, source, "two", 1)
	applyObservation(t, s.e, "other-observation", other)
	const itemCount = 512
	// Duplicate packet items are forbidden. Seed a sufficiently rich execution
	// using modest individual writes before fixing the source of the packets.
	items := make([]knowledge.ContextItem, itemCount)
	items[0] = knowledge.ContextItem{Observation: other.ID, Content: other.Payload, Label: "evidence"}
	for i := 1; i < itemCount; i++ {
		o := knowledgeObservation(payload, source, "two", uint64(i+1))
		applyObservation(t, s.e, fmt.Sprintf("packet-source-%04d", i), o)
		items[i] = knowledge.ContextItem{Ordinal: uint64(i), Observation: o.ID, Content: o.Payload, Label: "independent evidence reference"}
	}
	packetCut, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &other.Key})
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &packetCut.Anchor})
	if err != nil {
		t.Fatal(err)
	}
	packetSource := knowledge.SourceAnchor{Anchor: packetCut.Anchor, Frontier: packetCut.Frontier, ViewDigest: view.ViewDigest}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	var total, count int
	for total <= knowledge.MaxSnapshotBytes {
		id := fmt.Sprintf("render-%04d", count)
		packet := knowledge.ContextPacket{Space: "host-space", Key: &other.Key, GoalRef: payload, Source: packetSource, Renderer: id, Tokenizer: "test/v1", Items: items}
		packet.ID = knowledge.PacketID(packet)
		raw, err := packet.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		ack, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey(id), Command: knowledge.Command{Packet: &packet}})
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatalf("append packet %d = %+v, %v", count, ack, err)
		}
		total += len(raw)
		count++
	}
	check := func() {
		t.Helper()
		first, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &legacyCut})
		if err != nil || !reflect.DeepEqual(first, legacyState) {
			t.Fatalf("old cut changed = %+v, %v", first, err)
		}
		current, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
		if err != nil || current.Revision == nil || current.Revision.Sequence != 2 || len(current.Snapshot.Observations) != 2 || len(current.Snapshot.Packets) != 0 {
			t.Fatalf("indexed scoped state = %+v, %v", current, err)
		}
		want, err := knowledge.Revisions(knowledge.Snapshot{Observations: []knowledge.Observation{legacyObservation, indexed}}, "host-space", key)
		if err != nil || *current.Revision != want[2] {
			t.Fatalf("indexed revision changed logical identity: %+v, %v", current.Revision, err)
		}
		ack, err := s.e.ApplyKnowledge(ctx, indexedRequest)
		if err != nil || ack != indexedAck {
			t.Fatalf("indexed retry = %+v, %v", ack, err)
		}
		ack, err = s.e.Commit(ctx, legacy)
		if err != nil || ack != legacyAck {
			t.Fatalf("legacy retry = %+v, %v", ack, err)
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
