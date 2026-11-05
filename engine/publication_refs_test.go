package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestResolvePublicationBindsProducerTransactionAndRejectsGuesses(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{Batch: engine.BatchOptions{MaxCount: 8}})
	one := integrationMutate(t, s.e, integrationRequest("producer-one", integrationPut([]byte("a"), integrationBytes('1'))))
	two := integrationMutate(t, s.e, integrationRequest("producer-two", integrationPut([]byte("b"), integrationBytes('2'))))
	ref := model.SourcePublicationRef{Store: one.Fence.Store, Space: "host-space", ProducerTx: one.TxID}
	resolved, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: ref})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Source != ref || resolved.Anchor.Fence != one.Fence || resolved.RootSet != one.RootSet {
		t.Fatalf("producer resolved latest/group tip: %+v", resolved)
	}
	if resolved.Anchor.Fence == two.Fence {
		t.Fatal("producer ref substituted later transaction")
	}
	bad := ref
	bad.ProducerTx = model.TxID(strings.Repeat("e", 32))
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: bad}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatal("guessed tx accepted", err)
	}
	foreign := ref
	foreign.Space = "other"
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: foreign}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatal("foreign scope accepted", err)
	}
	bad = ref
	bad.Cursor = &model.Cursor{Store: ref.Store, Space: ref.Space, Revision: model.RevisionID(integrationID('f'))}
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: bad}); err == nil {
		t.Fatal("unpublished cursor accepted")
	}
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "other-owner", Source: ref}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatal("cross-owner producer accepted", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatalf("source denial unexpectedly fenced store: %v", err)
	}
	s.reopen()
	if got, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: ref}); err != nil || got.Anchor.Fence != one.Fence || got.RootSet != one.RootSet {
		t.Fatalf("replay changed producer envelope=%+v %v", got, err)
	}
}

func TestResolvePublicationAuthenticatesSelectorsAtProducerCut(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	early := integrationMutate(t, s.e, integrationRequest("source-before-revision", integrationPut([]byte("k"), integrationBytes('a'))))
	r := graphRevisionFromCurrent(t, s, "source-revision", "")
	ack := graphCoreApply(t, s.e, "source-branch", graph.Command{Fork: &graph.ForkCommand{Branch: "source", Source: r.ID}})
	ref := model.SourcePublicationRef{Store: ack.Fence.Store, Space: "host-space", ProducerTx: ack.TxID, Cursor: &model.Cursor{Store: ack.Fence.Store, Space: "host-space", Revision: r.ID}}
	got, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: ref})
	if err != nil || got.Anchor.Cursor == nil || *got.Anchor.Cursor != *ref.Cursor || got.Anchor.Fence != ack.Fence {
		t.Fatalf("valid domain selector=%+v %v", got, err)
	}
	bad := ref.Clone()
	bad.ProducerTx = early.TxID
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: bad}); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("later Revision entered producer's old cut: %v", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatalf("bad selector fenced store: %v", err)
	}
	// The returned selectors are caller-owned, never aliases of the request.
	got.Anchor.Cursor.Revision = model.RevisionID(integrationID('f'))
	got.Source.Cursor.Revision = model.RevisionID(integrationID('e'))
	if ref.Cursor.Revision != r.ID {
		t.Fatal("resolver aliased caller selectors")
	}
	s.reopen()
	if again, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: ref}); err != nil || again.Anchor.Fence != ack.Fence {
		t.Fatalf("reopened selector=%+v %v", again, err)
	}
}

func TestResolvePublicationKnowledgeCursorKeepsProducerObservation(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	_, ack := applyObservation(t, s.e, "source-knowledge-one", one)
	first, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil || first.Revision == nil {
		t.Fatal(err)
	}
	ref := model.SourcePublicationRef{Store: ack.Fence.Store, Space: "host-space", ProducerTx: ack.TxID, Knowledge: &one.Key, Cursor: &model.Cursor{Store: ack.Fence.Store, Space: "host-space", Revision: first.Revision.ID}}
	applyObservation(t, s.e, "source-knowledge-two", knowledgeObservation(payload, source, "one", 2))
	latest, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil || latest.Revision == nil {
		t.Fatal(err)
	}
	got, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: ref})
	if err != nil || got.Anchor.Fence != ack.Fence || got.Anchor.Cursor.Revision != first.Revision.ID {
		t.Fatalf("knowledge producer=%+v %v", got, err)
	}
	bad := ref.Clone()
	bad.Cursor.Revision = latest.Revision.ID
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: bad}); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("future knowledge selector entered producer cut: %v", err)
	}
	bad = ref.Clone()
	bad.Knowledge.Execution = "two"
	if _, err = s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: bad}); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("selector switched execution: %v", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatalf("knowledge selector denial fenced store: %v", err)
	}
	s.reopen()
	if again, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: ref}); err != nil || again.Anchor.Fence != ack.Fence || again.RootSet != ack.RootSet {
		t.Fatalf("replayed knowledge producer=%+v %v", again, err)
	}
}
