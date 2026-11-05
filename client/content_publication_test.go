package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func nodeResponseClient(t *testing.T, body any) *Client {
	t.Helper()
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer b.Close()
		for _, response := range []any{protocol.HelloResult{Principal: "p", Spaces: []model.SpaceID{"s"}}, body} {
			raw, err := protocol.ReadFrame(b)
			if err != nil {
				return
			}
			req, err := protocol.DecodeRequest(raw)
			if err != nil {
				return
			}
			reply(t, b, req, response)
		}
	}()
	c, err := New(context.Background(), a, Options{Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); <-done })
	return c
}

func TestReadContentClientRejectsMalformedRangeAndDigest(t *testing.T) {
	sum := sha256.Sum256([]byte("abc"))
	ref := model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("a", 64))}
	valid := api.ContentRange{Ref: ref, Anchor: model.SnapshotAnchor{Space: "s", Fence: model.CommitFence{Store: "store", CommitDigest: model.CommitDigest(strings.Repeat("b", 64))}}, ObjectType: "ate.value", ObjectVersion: 1, Projection: "value_bytes", TotalBytes: 3, Digest: model.ObjectID(hex.EncodeToString(sum[:])), Data: []byte("abc"), EOF: true}
	for name, mutate := range map[string]func(*api.ContentRange){"wrong-full-digest": func(r *api.ContentRange) { r.Digest = model.ObjectID(strings.Repeat("e", 64)) }, "wrong-offset": func(r *api.ContentRange) { r.Offset = 1 }, "false-eof": func(r *api.ContentRange) { r.EOF = false }, "short-data": func(r *api.ContentRange) { r.Data = []byte("ab") }, "wrong-projection": func(r *api.ContentRange) { r.Projection = "payload" }, "wrong-total": func(r *api.ContentRange) { r.TotalBytes = 2 }} {
		t.Run(name, func(t *testing.T) {
			bad := valid
			mutate(&bad)
			c := nodeResponseClient(t, bad)
			if _, err := c.ReadContent(context.Background(), api.ReadContentRequest{View: "v", Ref: ref, Projection: "value_bytes", Length: 3}); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("invalid range accepted: %v", err)
			}
		})
	}
}

func TestPublicationClientRejectsChangedSourceAndSelectors(t *testing.T) {
	source := model.SourcePublicationRef{Store: "store", Space: "s", ProducerTx: model.TxID(strings.Repeat("a", 32)), Cursor: &model.Cursor{Store: "store", Space: "s", Revision: model.RevisionID(strings.Repeat("b", 64))}, Knowledge: &model.KnowledgeKey{Actor: "actor", Execution: "e"}}
	valid := api.SourcePublication{Source: source.Clone(), Anchor: model.SnapshotAnchor{Space: "s", Fence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("c", 64))}, Cursor: source.Clone().Cursor, Knowledge: source.Clone().Knowledge}, RootSet: model.ObjectID(strings.Repeat("d", 64))}
	for name, mutate := range map[string]func(*api.SourcePublication){"different-producer": func(r *api.SourcePublication) { r.Source.ProducerTx = model.TxID(strings.Repeat("e", 32)) }, "lost-cursor": func(r *api.SourcePublication) { r.Anchor.Cursor = nil }, "lost-knowledge": func(r *api.SourcePublication) { r.Anchor.Knowledge = nil }, "invalid-root": func(r *api.SourcePublication) { r.RootSet = "bad" }, "genesis": func(r *api.SourcePublication) { r.Anchor.Fence.LSN = 0 }} {
		t.Run(name, func(t *testing.T) {
			bad := valid
			mutate(&bad)
			c := nodeResponseClient(t, bad)
			if _, err := c.ResolvePublication(context.Background(), api.ResolvePublicationRequest{Source: source}); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("invalid producer resolution accepted: %v", err)
			}
		})
	}
}
