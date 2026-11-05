package engine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestReadContentGrantBindsOneDeliveryAndOrdersWithdrawal(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	ack := integrationMutate(t, s.e, integrationRequest("grant-read-input", integrationPut([]byte("request"), api.Value{Kind: api.ValueBytes, Data: []byte("abcdef")})))
	object, _ := recoveryValueAt(t, s, ack.Fence, "request")
	view := nodeView(t, s.e, "host-principal", nil)
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	req := api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: object}, Projection: "value_bytes", Length: 2}
	destination := api.ReadDestination{Principal: "host-principal", Session: "session-a", Request: "first"}
	before := integrationSnapshot(t, s.e)
	grant, err := s.e.PrepareContentRead(ctx, req, destination)
	if err != nil || integrationSnapshot(t, s.e) != before {
		t.Fatalf("preparing a read changed the store or failed: %v", err)
	}
	assertDenied := func(dest api.ReadDestination, candidate api.ReadContentRequest) {
		t.Helper()
		if out, err := grant.Consume(dest, candidate); !errors.Is(err, api.ErrReadEmissionDenied) || len(out.Data) != 0 {
			t.Fatalf("invalid delivery released content: %+v %v", out, err)
		}
	}
	otherSession := destination
	otherSession.Session = "session-b"
	assertDenied(otherSession, req)
	next := req
	next.Offset = 2
	assertDenied(destination, next)
	otherRequest := destination
	otherRequest.Request = "second"
	assertDenied(otherRequest, req)
	foreign := destination
	foreign.Principal = "another-principal"
	if got, err := s.e.PrepareContentRead(ctx, req, foreign); !errors.Is(err, api.ErrReadEmissionDenied) || got != nil {
		t.Fatalf("preparing for a foreign principal succeeded: %v", err)
	}
	applyMemory(t, s.e, "withdraw-read-content", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: object, Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "withdrawn between chunks"}}})
	if out, err := grant.Consume(destination, req); err != nil || string(out.Data) != "ab" || out.Anchor.Fence != view.Anchor.Fence {
		t.Fatalf("previously granted delivery changed: %+v %v", out, err)
	}
	assertDenied(destination, req)
	if got, err := s.e.PrepareContentRead(ctx, next, otherRequest); !errors.Is(err, memory.ErrHistoryAccessDenied) || got != nil {
		t.Fatalf("fixed View authorized a later withdrawn chunk: %v", err)
	}
	if out, err := s.e.ReadContent(ctx, next); !errors.Is(err, memory.ErrHistoryAccessDenied) || len(out.Data) != 0 {
		t.Fatalf("embedded read bypassed withdrawal: %+v %v", out, err)
	}
}

func TestReadContentBoundedChunksAuthenticateOriginalHistoricalBytes(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	data := bytes.Repeat([]byte{0, 1, 255, 42, 17}, 40000)
	integrationMutate(t, s.e, integrationRequest("range-original", integrationPut([]byte("artifact"), api.Value{Kind: api.ValueBytes, Data: data})))
	view := nodeView(t, s.e, "host-principal", nil)
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("entries=%+v %v", entries, err)
	}
	ref := model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}
	if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref, Limit: api.QueryLimits{MaxBytes: 100000}}); engine.ClassifyError(err) != api.ErrorResourceLimit {
		t.Fatalf("large detail was not bounded: %v", err)
	}
	integrationMutate(t, s.e, integrationRequest("range-replaced", integrationPut([]byte("artifact"), integrationBytes('b'))))
	request := api.ReadContentRequest{View: view.Ref, Ref: ref, Projection: "value_bytes", Length: 37001, MaxBytes: 60000}
	got := []byte{}
	var digest model.ObjectID
	for {
		part, err := s.e.ReadContent(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if part.Offset != uint64(len(got)) || part.TotalBytes != uint64(len(data)) || part.Anchor.Fence != view.Anchor.Fence || len(part.Data) > int(request.Length) {
			t.Fatalf("bad range=%+v", part)
		}
		if digest != "" && digest != part.Digest {
			t.Fatal("range switched content")
		}
		digest = part.Digest
		got = append(got, part.Data...)
		if part.EOF {
			break
		}
		request.Offset = uint64(len(got))
	}
	sum := sha256.Sum256(got)
	if !bytes.Equal(got, data) || digest != model.ObjectID(hex.EncodeToString(sum[:])) {
		t.Fatal("range content did not reproduce original bytes/digest")
	}
	eof, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: ref, Projection: "value_bytes", Offset: uint64(len(data))})
	if err != nil || !eof.EOF || len(eof.Data) != 0 {
		t.Fatalf("EOF=%+v %v", eof, err)
	}
	for _, bad := range []api.ReadContentRequest{{View: view.Ref, Ref: ref, Offset: ^uint64(0)}, {View: view.Ref, Ref: ref, Length: api.MaxContentChunkBytes + 1}, {View: view.Ref, Ref: ref, MaxBytes: 1}, {View: view.Ref, Ref: ref, Projection: "arbitrary"}} {
		if _, err = s.e.ReadContent(ctx, bad); err == nil {
			t.Fatalf("invalid/unbounded range accepted: %+v", bad)
		}
	}
	ctxCanceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.e.ReadContent(ctxCanceled, api.ReadContentRequest{View: view.Ref, Ref: ref}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled range=%v", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatalf("range errors fenced store: %v", err)
	}
}

func TestReadContentCannotResolveFutureOrSyntheticKnowledgeIDs(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	old := nodeView(t, s.e, "host-principal", nil)
	integrationMutate(t, s.e, integrationRequest("range-future", integrationPut([]byte("future"), integrationBytes('b'))))
	latest := nodeView(t, s.e, "host-principal", nil)
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: latest.Ref})
	if err != nil {
		t.Fatal(err)
	}
	var future model.ObjectID
	for _, item := range entries.Items {
		if string(item.Key) == "future" {
			future = item.Value
		}
	}
	if _, err = s.e.ReadContent(ctx, api.ReadContentRequest{View: old.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: future}}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("future content range bypass=%v", err)
	}
	one := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "range-observe", one)
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	if _, err = s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: one.ID}}); !errors.Is(err, engine.ErrUnsupportedMethod) {
		t.Fatalf("synthetic projection treated as codec object: %v", err)
	}
	if _, err = s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: future}}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("unadopted content range bypass=%v", err)
	}
}
