package service

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestPublicationRefWireUsesAuthenticatedOwnerAndExactSource(t *testing.T) {
	ctx := context.Background()
	core, _ := nativeAgentStore(t, true)
	server := newServer(t, core, agentCredentials())
	owner := pipeClient(t, server, "owner-token")
	other := pipeClient(t, server, "other-token")
	ack, err := owner.ApplyAgent(ctx, agentCreateRequest("source-producer"))
	if err != nil {
		t.Fatal(err)
	}
	ref := model.SourcePublicationRef{Store: ack.Fence.Store, Space: "s", ProducerTx: ack.TxID}
	want, err := core.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: owner.Identity().Principal, Source: ref})
	if err != nil {
		t.Fatal(err)
	}
	got, err := owner.ResolvePublication(ctx, api.ResolvePublicationRequest{Source: ref})
	if err != nil || got.Anchor.Fence != want.Anchor.Fence || got.RootSet != want.RootSet {
		t.Fatal("wire changed source", got, err)
	}
	if _, err = other.ResolvePublication(ctx, api.ResolvePublicationRequest{Source: ref}); err == nil {
		t.Fatal("foreign principal enumerated producer")
	} else {
		remoteCode(t, err, api.ErrorNotFound)
	}
	if _, err = owner.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "forged", Source: ref}); err == nil {
		t.Fatal("client accepted forged principal")
	}
	if _, err = owner.ResolvePublication(ctx, api.ResolvePublicationRequest{Source: ref}); err != nil {
		t.Fatal("denial poisoned owner", err)
	}
}
