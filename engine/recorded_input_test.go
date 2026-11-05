package engine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestRecordedModelInputRetainsExactBytesAcrossPacketRecoveryAndCompaction(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	o := knowledgeObservation(payload, source, "one", 1)
	_, adopted := applyObservation(t, s.e, "record-observation", o)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: adopted.Fence, Knowledge: &o.Key}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	k, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	request := []byte("{\"messages\":[{\"role\":\"system\",\"content\":\"exact framing\"}],\"tools\":[],\"seed\":7}\n")
	ack := integrationMutate(t, s.e, integrationRequest("record-exact-request", integrationPut([]byte("request"), api.Value{Kind: api.ValueBytes, Data: request})))
	inputView, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: inputView.Ref})
	if err != nil {
		t.Fatal(err)
	}
	var inputID model.ObjectID
	for _, entry := range entries.Items {
		if bytes.Equal(entry.Key, []byte("request")) {
			inputID = entry.Value
		}
	}
	if inputID == "" {
		t.Fatal("recorded request missing")
	}
	if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: inputView.Ref}); err != nil {
		t.Fatal(err)
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &o.Key, Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: v.ViewDigest, Frontier: k.Frontier}, GoalRef: payload, Items: []knowledge.ContextItem{{Observation: o.ID, Content: payload, Label: "evidence"}}, Renderer: "request/v1", Tokenizer: "test/v1", Input: &knowledge.RecordedInput{Object: inputID, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}, Encoding: "application/json;request-framing=v1"}}
	packet.ID = knowledge.PacketID(packet)
	if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	req := api.ApplyKnowledgeRequest{Key: agentKey("record-packet"), Command: knowledge.Command{Packet: &packet}}
	committed, err := s.e.ApplyKnowledge(ctx, req)
	if err != nil || committed.Code != api.CodeDurableAck {
		t.Fatal(committed, err)
	}
	readExact := func() {
		t.Helper()
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Knowledge: &o.Key})
		if err != nil {
			t.Fatal(err)
		}
		defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		var got []byte
		for {
			chunk, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: inputID}, Projection: "value_bytes", Offset: uint64(len(got)), Length: 7})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, chunk.Data...)
			if chunk.EOF {
				break
			}
			if len(chunk.Data) == 0 {
				t.Fatal("range made no progress")
			}
		}
		if !bytes.Equal(got, request) {
			t.Fatal("reconstructed request differs from actual framed bytes")
		}
	}
	readExact()
	// Later source content changes do not silently re-render the old Packet.
	integrationMutate(t, s.e, integrationRequest("overwrite-current-request", integrationPut([]byte("request"), integrationBytes('z'))))
	readExact()
	s.reopen()
	readExact()
	if again, err := s.e.ApplyKnowledge(ctx, req); err != nil || again != committed {
		t.Fatal("packet receipt changed", again, err)
	}
	if _, err = s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	readExact()
	s.reopen()
	readExact()
	before := integrationSnapshot(t, s.e)
	bad := packet.Clone()
	bad.Input.Object = model.ObjectID(integrationID('e'))
	bad.ID = knowledge.PacketID(bad)
	out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("guessed-recording"), Command: knowledge.Command{Packet: &bad}})
	if !errors.Is(err, knowledge.ErrUnknownObservation) || out.Code != api.CodeRejected {
		t.Fatal("guessed input accepted or store fenced", out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("invalid recording changed store")
	}
}

var _ api.ContentReader = (*engine.Engine)(nil)
