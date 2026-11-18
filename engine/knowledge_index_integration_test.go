package engine_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestKnowledgeIndexedWritesExceedEightMiBAggregateAndPreserveCuts(t *testing.T) {
	ctx := t.Context()
	s, payload, source := knowledgeFixture(t)
	firstObservation := knowledgeObservation(payload, source, "one", 1)
	firstRequest, firstAck := applyObservation(t, s.e, "first-observation", firstObservation)
	key := firstObservation.Key
	firstCut := model.SnapshotAnchor{Space: "host-space", Fence: firstAck.Fence, Knowledge: &key}
	firstState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &firstCut})
	if err != nil || firstState.Revision == nil || firstState.Revision.Sequence != 1 || len(firstState.Snapshot.Observations) != 1 {
		t.Fatalf("first indexed state = %+v, %v", firstState, err)
	}
	indexed := knowledgeObservation(payload, source, "one", 2)
	indexedRequest, indexedAck := applyObservation(t, s.e, "indexed-observation", indexed)
	indexedCut := model.SnapshotAnchor{Space: "host-space", Fence: indexedAck.Fence, Knowledge: &key}
	indexedState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &indexedCut})
	if err != nil {
		t.Fatal(err)
	}

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
	// A fixed aggregate target exceeds the former 8 MiB owner snapshot limit
	// while each independently indexed packet stays within its record limit.
	const aggregateBudget = 9 << 20
	var total, count int
	packetSizes := make(map[model.ObjectID]int)
	for total < aggregateBudget {
		id := fmt.Sprintf("render-%04d", count)
		packet := knowledge.ContextPacket{Space: "host-space", Key: &other.Key, GoalRef: payload, Source: packetSource, Renderer: id, Tokenizer: "test/v1", Items: items}
		packet.ID = knowledge.PacketID(packet)
		raw, err := packet.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 || len(raw) > knowledge.MaxRecordBytes {
			t.Fatalf("packet size %d is outside the per-record bound", len(raw))
		}
		ack, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey(id), Command: knowledge.Command{Packet: &packet}})
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatalf("append packet %d = %+v, %v", count, ack, err)
		}
		total += len(raw)
		packetSizes[packet.ID] = len(raw)
		count++
	}
	check := func() {
		t.Helper()
		for _, historical := range []api.KnowledgeStateResult{firstState, indexedState, packetCut} {
			got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &historical.Anchor})
			if err != nil || !reflect.DeepEqual(got, historical) {
				t.Fatalf("indexed historical cut at LSN %d changed: %v", historical.Fence.LSN, err)
			}
		}
		current, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
		if err != nil || current.Revision == nil || current.Revision.Sequence != 2 || len(current.Snapshot.Observations) != 2 || len(current.Snapshot.Packets) != 0 {
			t.Fatalf("indexed scoped state = %+v, %v", current, err)
		}
		want, err := knowledge.Revisions(knowledge.Snapshot{Observations: []knowledge.Observation{firstObservation, indexed}}, "host-space", key)
		if err != nil || *current.Revision != want[2] {
			t.Fatalf("indexed revision changed logical identity: %+v, %v", current.Revision, err)
		}
		ack, err := s.e.ApplyKnowledge(ctx, indexedRequest)
		if err != nil || ack != indexedAck {
			t.Fatalf("indexed retry = %+v, %v", ack, err)
		}
		ack, err = s.e.ApplyKnowledge(ctx, firstRequest)
		if err != nil || ack != firstAck {
			t.Fatalf("first indexed retry = %+v, %v", ack, err)
		}
		packets, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &other.Key})
		if err != nil {
			t.Fatal(err)
		}
		if len(packets.Snapshot.Observations) != itemCount || len(packets.Snapshot.Packets) != count || packets.Revision == nil || packetCut.Revision == nil || *packets.Revision != *packetCut.Revision || packets.Frontier != packetCut.Frontier {
			t.Fatalf("packet writes changed adoption history or lost records: observations=%d packets=%d", len(packets.Snapshot.Observations), len(packets.Snapshot.Packets))
		}
		seen := make(map[model.ObjectID]bool, count)
		var persistedBytes int
		for _, packet := range packets.Snapshot.Packets {
			raw, err := packet.Canonical()
			if err != nil || seen[packet.ID] || packetSizes[packet.ID] != len(raw) {
				t.Fatalf("persisted packet %s changed or duplicated: %v", packet.ID, err)
			}
			seen[packet.ID] = true
			persistedBytes += len(raw)
		}
		if persistedBytes != total || persistedBytes < aggregateBudget {
			t.Fatalf("aggregate capacity lost: got %d bytes, want %d (at least %d)", persistedBytes, total, aggregateBudget)
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

func TestKnowledgeRejectsV1CommandWithoutPublicationStateOrReceiptChange(t *testing.T) {
	ctx := t.Context()
	s, payload, source := knowledgeFixture(t)
	first := knowledgeObservation(payload, source, "one", 1)
	firstRequest, firstAck := applyObservation(t, s.e, "v2-before-rejection", first)
	next := knowledgeObservation(payload, source, "one", 2)
	body, err := knowledge.EncodeCommand(knowledge.Command{Observation: &next})
	if err != nil {
		t.Fatal(err)
	}
	// The envelope version alone selects the unsupported command schema.
	// Keep the otherwise valid canonical record; do not recreate a v1 codec.
	const versionOffset = len("ATE-KNOWLEDGE-COMMAND\x00")
	if body[versionOffset] != 2 {
		t.Fatal("expected a v2 command fixture")
	}
	body[versionOffset] = 1
	rejectedKey := agentKey("rejected-v1")
	request := api.CommitRequest{Space: rejectedKey.Space, Principal: rejectedKey.Principal, RequestID: rejectedKey.RequestID, Method: api.MethodApplyKnowledge, Body: body}
	before := integrationSnapshot(t, s.e)
	read := api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &first.Key}
	beforeState, err := s.e.GetKnowledgeState(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	beforeReceipt := integrationLookup(t, s.e, firstRequest.Key)
	beforeAbsent := integrationLookup(t, s.e, rejectedKey)
	if beforeAbsent.Code != api.CodeNotFoundAtFence || beforeAbsent.Result != (api.CommitResult{}) {
		t.Fatalf("v1 request key already has a receipt: %+v", beforeAbsent)
	}
	check := func() {
		t.Helper()
		out, err := s.e.Commit(ctx, request)
		if !errors.Is(err, knowledge.ErrUnsupportedSchema) || out.Code != api.CodeRejected {
			t.Fatalf("v1 command = %+v, %v", out, err)
		}
		if after := integrationSnapshot(t, s.e); after != before {
			t.Fatal("v1 rejection changed publication or roots")
		}
		afterState, err := s.e.GetKnowledgeState(ctx, read)
		if err != nil || !reflect.DeepEqual(afterState, beforeState) {
			t.Fatalf("v1 rejection changed knowledge state: %v", err)
		}
		if got := integrationLookup(t, s.e, rejectedKey); !reflect.DeepEqual(got, beforeAbsent) {
			t.Fatalf("v1 rejection changed receipt absence: %+v", got)
		}
		if got := integrationLookup(t, s.e, firstRequest.Key); !reflect.DeepEqual(got, beforeReceipt) {
			t.Fatalf("v1 rejection changed existing receipt: %+v", got)
		}
		if ack, err := s.e.ApplyKnowledge(ctx, firstRequest); err != nil || ack != firstAck {
			t.Fatalf("v2 retry after v1 rejection = %+v, %v", ack, err)
		}
	}
	check()
	s.reopen()
	check()
	// Rejection must leave both the sequence and the request key available.
	_, ack := applyObservation(t, s.e, string(rejectedKey.RequestID), next)
	if ack.Fence.LSN != before.Fence.LSN+1 {
		t.Fatalf("rejected command consumed a publication: %+v", ack)
	}
}
