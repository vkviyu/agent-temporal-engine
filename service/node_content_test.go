package service

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestContentRangesUseOwnedViewAndMatchEmbeddedReads(t *testing.T) {
	ctx := context.Background()
	core, _ := nativeAgentStore(t, true)
	server := newServer(t, core, agentCredentials())
	owner := pipeClient(t, server, "owner-token")
	other := pipeClient(t, server, "other-token")
	samePrincipal := pipeClient(t, server, "owner-token")
	data := bytes.Repeat([]byte("exact request bytes\x00"), 6000)
	if _, err := owner.Mutate(ctx, api.MutateRequest{Key: model.RequestKey{Space: "s", RequestID: "wire-content"}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("input"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: data}}}}}); err != nil {
		t.Fatal(err)
	}
	view, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := owner.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("entries=%+v %v", entries, err)
	}
	req := api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}, Projection: "value_bytes", Offset: 5, Length: 37}
	got, err := owner.ReadContent(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	want, err := core.ReadContent(ctx, req)
	if err != nil || !reflect.DeepEqual(got, want) || !bytes.Equal(got.Data, data[5:42]) {
		t.Fatalf("wire changed range=%+v want=%+v %v", got, want, err)
	}
	for _, nonOwner := range []*client.Client{other, samePrincipal} {
		if _, err := nonOwner.ReadContent(ctx, req); err == nil {
			t.Fatal("borrowed View content readable")
		} else {
			remoteCode(t, err, api.ErrorUnauthorized)
		}
	}
	if err = owner.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.ReadContent(ctx, req); err == nil {
		t.Fatal("closed View range readable")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
}
