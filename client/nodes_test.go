package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func TestNodeExplorerRejectsInconsistentRemoteSearchAndTrace(t *testing.T) {
	anchor := model.SnapshotAnchor{Space: "s", Fence: model.CommitFence{Store: "store", CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}}
	start := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "agent", Kind: "Task", ID: "t"}}
	missing := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "agent", Kind: "Execution", ID: "x"}}
	tests := []struct {
		name string
		body any
		call func(*Client) error
	}{
		{"false-search-match", api.NodeSearchResult{Anchor: anchor, SearchVersion: "summary-text/v1", Coverage: "complete_layer", Items: []api.NodeSearchMatch{{Node: api.NodeSummary{Ref: start, Label: "different", Kind: "Task"}, Reasons: []string{"label:exact"}}}}, func(c *Client) error {
			_, err := c.SearchNodes(context.Background(), api.SearchNodesRequest{View: "v", Text: "expected", Match: "exact"})
			return err
		}},
		{"trace-missing-endpoint", api.NodeTrace{Anchor: anchor, Start: start, Nodes: []api.NodeSummary{{Ref: start, Kind: "Task"}}, Edges: []api.NodeTraceEdge{{From: missing, To: start, Relation: "parent"}}}, func(c *Client) error {
			_, err := c.Trace(context.Background(), api.TraceRequest{View: "v", Start: start})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, b := net.Pipe()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer b.Close()
				for _, body := range []any{protocol.HelloResult{Principal: "p", Spaces: []model.SpaceID{"s"}}, test.body} {
					raw, err := protocol.ReadFrame(b)
					if err != nil {
						return
					}
					req, err := protocol.DecodeRequest(raw)
					if err != nil {
						return
					}
					reply(t, b, req, body)
				}
			}()
			c, err := New(context.Background(), a, Options{Token: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			if err = test.call(c); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("inconsistent response accepted: %v", err)
			}
			_ = c.Close()
			<-done
		})
	}
}
