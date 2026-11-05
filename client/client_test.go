package client

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
	"net"
	"strings"
	"testing"
	"time"
)

func reply(t *testing.T, c net.Conn, req protocol.Request, body any) {
	t.Helper()
	b, _ := json.Marshal(body)
	raw, _ := json.Marshal(protocol.Response{Version: protocol.Version, CorrelationID: req.CorrelationID, Body: b})
	if err := protocol.WriteFrame(c, raw); err != nil {
		t.Error(err)
	}
}
func TestClientSequentialRPCAndUnknownAfterWrite(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	go func() {
		defer b.Close()
		raw, err := protocol.ReadFrame(b)
		if err != nil {
			return
		}
		req, err := protocol.DecodeRequest(raw)
		if err != nil {
			return
		}
		reply(t, b, req, protocol.HelloResult{Principal: "host", Spaces: []model.SpaceID{"s"}})
		raw, err = protocol.ReadFrame(b)
		if err != nil {
			return
		}
		req, _ = protocol.DecodeRequest(raw)
		reply(t, b, req, api.CommitResult{Code: api.CodeDurableAck, RequestID: "r", TxID: model.TxID(strings.Repeat("a", 32)), Fence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("b", 64))}, RequestDigest: model.ObjectID(strings.Repeat("c", 64)), ResultDigest: model.ObjectID(strings.Repeat("d", 64)), RootSet: model.ObjectID(strings.Repeat("e", 64))})
	}()
	c, err := New(context.Background(), a, Options{Token: "secret"})
	if err == nil { // handshake server intentionally does not validate token, client only needs result
		out, err := c.Commit(context.Background(), api.CommitRequest{Space: "s", Principal: "host", RequestID: "r"})
		if err != nil || out.Code != api.CodeDurableAck {
			t.Fatalf("rpc: %+v %v", out, err)
		}
		_ = c.Close()
	} else {
		t.Fatal(err)
	}
	// A second connection proves cancellation after write is unknown, not a retry.
	x, y := net.Pipe()
	go func() {
		defer y.Close()
		raw, e := protocol.ReadFrame(y)
		if e != nil {
			return
		}
		q, _ := protocol.DecodeRequest(raw)
		reply(t, y, q, protocol.HelloResult{Principal: "host", Spaces: []model.SpaceID{"s"}})
		_, _ = protocol.ReadFrame(y)
		var one [1]byte
		_, _ = y.Read(one[:])
	}()
	cc, err := New(context.Background(), x, Options{Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = cc.Commit(ctx, api.CommitRequest{Space: "s", Principal: "host", RequestID: "unknown"})
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected unknown outcome, got %v", err)
	}
	_ = cc.Close()
}

func TestClientRejectsMissingSuccessBodyAsUnknownCommand(t *testing.T) {
	for _, payload := range []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`{}`), json.RawMessage(`{"Code":"invented","RequestID":"r"}`)} {
		t.Run(string(payload), func(t *testing.T) {
			a, b := net.Pipe()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				defer b.Close()
				raw, e := protocol.ReadFrame(b)
				if e != nil {
					return
				}
				q, e := protocol.DecodeRequest(raw)
				if e != nil {
					return
				}
				reply(t, b, q, protocol.HelloResult{Principal: "host", Spaces: []model.SpaceID{"s"}})
				raw, e = protocol.ReadFrame(b)
				if e != nil {
					return
				}
				q, e = protocol.DecodeRequest(raw)
				if e != nil {
					return
				}
				response, _ := json.Marshal(protocol.Response{Version: protocol.Version, CorrelationID: q.CorrelationID, Body: payload})
				_ = protocol.WriteFrame(b, response)
			}()
			c, e := New(context.Background(), a, Options{Token: "secret"})
			if e != nil {
				t.Fatal(e)
			}
			_, e = c.Commit(context.Background(), api.CommitRequest{Space: "s", RequestID: "r"})
			var unknown *OutcomeUnknownError
			if !errors.As(e, &unknown) || unknown.Key.RequestID != "r" {
				t.Fatalf("missing response turned into success: %v", e)
			}
			_ = c.Close()
			<-serverDone
		})
	}
}
func TestCancelWaitingForConnectionDoesNotCancelActiveRPC(t *testing.T) {
	a, b := net.Pipe()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer b.Close()
		raw, e := protocol.ReadFrame(b)
		if e != nil {
			return
		}
		q, e := protocol.DecodeRequest(raw)
		if e != nil {
			return
		}
		reply(t, b, q, protocol.HelloResult{Principal: "host", Spaces: []model.SpaceID{"s"}})
		raw, e = protocol.ReadFrame(b)
		if e != nil {
			return
		}
		q, e = protocol.DecodeRequest(raw)
		if e != nil {
			return
		}
		close(started)
		<-release
		reply(t, b, q, api.StateValueResult{Fence: model.CommitFence{Store: "store", CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}})
	}()
	c, e := New(context.Background(), a, Options{Token: "secret"})
	if e != nil {
		t.Fatal(e)
	}
	first := make(chan error, 1)
	go func() { _, err := c.GetState(context.Background(), api.ReadStateRequest{Space: "s"}); first <- err }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = c.GetState(ctx, api.ReadStateRequest{Space: "s"})
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	close(release)
	if e = <-first; e != nil {
		t.Fatalf("waiting cancellation damaged active RPC: %v", e)
	}
	_ = c.Close()
	<-done
}
