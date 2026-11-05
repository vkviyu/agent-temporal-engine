package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestReadDeliveryGrantsMatchEmbeddedPayloads(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	ack := integrationMutate(t, s.e, integrationRequest("typed-read-content", integrationPut([]byte("document"), integrationBytes('r'))))
	object, _ := recoveryValueAt(t, s, ack.Fence, "document")
	view := nodeView(t, s.e, "host-principal", nil)
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	ref := model.NodeRef{Kind: model.NodeObject, Object: object}
	for _, test := range []struct {
		method string
		read   func(context.Context) (any, error)
	}{
		{"list_nodes", func(ctx context.Context) (any, error) {
			return s.e.ListNodes(ctx, api.ListNodesRequest{View: view.Ref, Layer: "content"})
		}},
		{"get_node", func(ctx context.Context) (any, error) {
			return s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref})
		}},
		{"get_node_context", func(ctx context.Context) (any, error) {
			return s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: ref})
		}},
		{"search_nodes", func(ctx context.Context) (any, error) {
			return s.e.SearchNodes(ctx, api.SearchNodesRequest{View: view.Ref, Layer: "content", Text: "document"})
		}},
		{"trace_nodes", func(ctx context.Context) (any, error) {
			return s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: ref})
		}},
		{"get_state", func(ctx context.Context) (any, error) {
			return s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("document")})
		}},
		{"get_view_state", func(ctx context.Context) (any, error) {
			return s.e.GetViewState(ctx, api.ReadViewStateRequest{View: view.Ref, Key: []byte("document")})
		}},
		{"get_view_entries", func(ctx context.Context) (any, error) {
			return s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
		}},
		{"resolve_publication", func(ctx context.Context) (any, error) {
			source := model.SourcePublicationRef{Store: ack.Fence.Store, Space: "host-space", ProducerTx: ack.TxID}
			return s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: source})
		}},
	} {
		t.Run(test.method, func(t *testing.T) {
			embedded, err := test.read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(embedded)
			if err != nil {
				t.Fatal(err)
			}
			binding := api.ReadDeliveryBinding{
				Destination: api.ReadDestination{Principal: "host-principal", Session: "session-a", Request: "reply"},
				Method:      test.method, RequestDigest: agentprofile.ExactRequestDigest([]byte(`{"request":"one"}`)),
			}
			before := integrationSnapshot(t, s.e)
			grant, err := s.e.PrepareReadDelivery(ctx, binding, func(scoped context.Context) error {
				out, err := test.read(scoped)
				if err == nil && !reflect.ValueOf(out).IsZero() {
					t.Fatalf("scoped read released payload before consumption: %+v", out)
				}
				return err
			})
			if err != nil || integrationSnapshot(t, s.e) != before {
				t.Fatalf("prepare failed or changed durable state: %v", err)
			}
			wrong := binding
			wrong.Destination.Session = "session-b"
			if body, err := grant.Consume(wrong); !errors.Is(err, api.ErrReadEmissionDenied) || body != nil {
				t.Fatalf("grant accepted a different session: %q %v", body, err)
			}
			wrong = binding
			wrong.RequestDigest = agentprofile.ExactRequestDigest([]byte(`{"request":"two"}`))
			if body, err := grant.Consume(wrong); !errors.Is(err, api.ErrReadEmissionDenied) || body != nil {
				t.Fatalf("grant accepted a different request: %q %v", body, err)
			}
			if body, err := grant.Consume(binding); err != nil || !bytes.Equal(body, want) {
				t.Fatalf("granted body differed from embedded result: %q %v", body, err)
			}
			if body, err := grant.Consume(binding); !errors.Is(err, api.ErrReadEmissionDenied) || body != nil {
				t.Fatalf("grant was consumed twice: %q %v", body, err)
			}
		})
	}
}

func TestReadDeliveryCannotIssueAfterAuthorizationCutChanges(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	ack := integrationMutate(t, s.e, integrationRequest("read-cut-content", integrationPut([]byte("document"), integrationBytes('r'))))
	object, _ := recoveryValueAt(t, s, ack.Fence, "document")
	view := nodeView(t, s.e, "host-principal", nil)
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	binding := api.ReadDeliveryBinding{
		Destination: api.ReadDestination{Principal: "host-principal", Session: "session-a", Request: "reply"},
		Method:      "list_nodes", RequestDigest: agentprofile.ExactRequestDigest([]byte(`{"request":"list"}`)),
	}
	read := func(scoped context.Context) error {
		_, err := s.e.ListNodes(scoped, api.ListNodesRequest{View: view.Ref, Layer: "content"})
		return err
	}
	if grant, err := s.e.PrepareReadDelivery(ctx, binding, func(scoped context.Context) error {
		if err := read(scoped); err != nil {
			return err
		}
		// Repeating the callback read invalidates the capture even if a caller
		// ignores that second error.
		_ = read(scoped)
		return nil
	}); !errors.Is(err, api.ErrReadEmissionDenied) || grant != nil {
		t.Fatalf("two scoped reads produced a grant: %v", err)
	}
	if grant, err := s.e.PrepareReadDelivery(ctx, binding, func(scoped context.Context) error {
		if err := read(scoped); err != nil {
			return err
		}
		applyMemory(t, s.e, "withdraw-typed-read", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: object, Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "withdrawn before grant issuance"}}})
		return nil
	}); !errors.Is(err, api.ErrReadAuthorizationChanged) || grant != nil {
		t.Fatalf("earlier authorization cut issued a grant: %v", err)
	}
	if grant, err := s.e.PrepareReadDelivery(ctx, binding, read); !errors.Is(err, memory.ErrHistoryAccessDenied) || grant != nil {
		t.Fatalf("fixed View obtained a new grant after withdrawal: %v", err)
	}
}
