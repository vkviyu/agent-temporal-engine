package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func queryResponse() api.QueryProgressResult {
	fence := func(lsn uint64, d string) model.CommitFence {
		return model.CommitFence{Store: "store", LSN: lsn, CommitDigest: model.CommitDigest(strings.Repeat(d, 64))}
	}
	return api.QueryProgressResult{Found: true, Anchor: model.SnapshotAnchor{Space: "s", Fence: fence(3, "a")}, Progress: api.QueryProgress{Query: "q", Version: 3, Sequence: 1, AdapterVersion: "query/v1", Phase: "tool", Source: model.SnapshotAnchor{Space: "s", Fence: fence(2, "b")}, Next: &api.CommitRequest{Space: "s", Principal: "p", RequestID: "next", Method: api.MethodApplyAgent, Body: []byte("exact command")}, Decisions: []api.QueryDecision{{Sequence: 1, Activity: "model", AcceptedResult: "model-result", Invocations: []api.QueryInvocation{{ID: "read", Activity: "read-activity", ProviderCallKey: "call", Tool: "read", Definition: model.ObjectID(strings.Repeat("c", 64)), ToolVersion: "read/v1", Arguments: []byte("args"), DispatchClass: "pure"}}}}}}
}

func TestQueryClientRejectsMalformedProgress(t *testing.T) {
	for name, mutate := range map[string]func(*api.QueryProgressResult){
		"foreign-space":         func(r *api.QueryProgressResult) { r.Anchor.Space = "other" },
		"invalid-anchor":        func(r *api.QueryProgressResult) { r.Anchor.Fence.CommitDigest = "bad" },
		"different-history":     func(r *api.QueryProgressResult) { r.Anchor.Fence.LSN++ },
		"selector":              func(r *api.QueryProgressResult) { r.Anchor.Cursor = &model.Cursor{} },
		"not-found-with-data":   func(r *api.QueryProgressResult) { r.Found = false },
		"wrong-query":           func(r *api.QueryProgressResult) { r.Progress.Query = "other" },
		"zero-version":          func(r *api.QueryProgressResult) { r.Progress.Version = 0 },
		"future-source":         func(r *api.QueryProgressResult) { r.Progress.Source.Fence.LSN = 4 },
		"foreign-source":        func(r *api.QueryProgressResult) { r.Progress.Source.Space = "other" },
		"same-LSN-wrong-digest": func(r *api.QueryProgressResult) { r.Progress.Source.Fence.LSN = 3 },
		"source-selector": func(r *api.QueryProgressResult) {
			r.Progress.Source.Knowledge = &model.KnowledgeKey{Actor: "a", Execution: "e"}
		},
		"bad-adapter": func(r *api.QueryProgressResult) {
			r.Progress.AdapterVersion = strings.Repeat("a", api.MaxIdentityBytes+1)
		},
		"missing-phase":            func(r *api.QueryProgressResult) { r.Progress.Phase = "" },
		"completed-with-next":      func(r *api.QueryProgressResult) { r.Progress.Completed = true },
		"missing-next":             func(r *api.QueryProgressResult) { r.Progress.Next = nil },
		"foreign-next-owner":       func(r *api.QueryProgressResult) { r.Progress.Next.Principal = "other" },
		"foreign-next-space":       func(r *api.QueryProgressResult) { r.Progress.Next.Space = "other" },
		"invalid-next":             func(r *api.QueryProgressResult) { r.Progress.Next.RequestID = "" },
		"unregistered-next-method": func(r *api.QueryProgressResult) { r.Progress.Next.Method = "shell.exec" },
		"wrong-sequence":           func(r *api.QueryProgressResult) { r.Progress.Sequence = 2 },
		"noncontiguous-decision":   func(r *api.QueryProgressResult) { r.Progress.Decisions[0].Sequence = 0 },
		"missing-result":           func(r *api.QueryProgressResult) { r.Progress.Decisions[0].AcceptedResult = "" },
		"invalid-definition":       func(r *api.QueryProgressResult) { r.Progress.Decisions[0].Invocations[0].Definition = "bad" },
		"invalid-dispatch-class":   func(r *api.QueryProgressResult) { r.Progress.Decisions[0].Invocations[0].DispatchClass = "unsafe" },
		"reused-activity":          func(r *api.QueryProgressResult) { r.Progress.Decisions[0].Invocations[0].Activity = "model" },
		"missing-tool-version":     func(r *api.QueryProgressResult) { r.Progress.Decisions[0].Invocations[0].ToolVersion = "" },
		"completed-pending-tool":   func(r *api.QueryProgressResult) { r.Progress.Completed = true; r.Progress.Next = nil },
		"duplicate-invocation": func(r *api.QueryProgressResult) {
			d := &r.Progress.Decisions[0]
			d.Invocations = append(d.Invocations, d.Invocations[0])
		},
		"duplicate-decision": func(r *api.QueryProgressResult) {
			d := r.Progress.Decisions[0]
			d.Sequence = 2
			r.Progress.Decisions = append(r.Progress.Decisions, d)
			r.Progress.Sequence = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			response := queryResponse()
			anchor := response.Anchor.Clone()
			mutate(&response)
			c := nodeResponseClient(t, response)
			if _, err := c.GetQueryProgress(context.Background(), api.ReadQueryProgressRequest{Space: "s", Query: "q", Anchor: &anchor}); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("accepted invalid query progress: %v", err)
			}
			if !c.closed.Load() {
				t.Fatal("malformed success left connection usable")
			}
		})
	}
}

func TestQueryClientAcceptsPendingCompletedAndHistoricalAbsence(t *testing.T) {
	for _, state := range []string{"open", "pending-tool", "completed", "absent", "long-profile-identities"} {
		t.Run(state, func(t *testing.T) {
			r := queryResponse()
			switch state {
			case "open":
				r.Progress.Sequence = 0
				r.Progress.Decisions = nil
				r.Progress.Version = 1
			case "completed":
				r.Progress.Completed = true
				r.Progress.Next = nil
				r.Progress.Decisions[0].Invocations[0].AcceptedResult = "tool-result"
			case "absent":
				r.Found = false
				r.Progress = api.QueryProgress{}
			case "long-profile-identities":
				r.Progress.Decisions[0].Activity = strings.Repeat("a", 512)
				r.Progress.Decisions[0].AcceptedResult = strings.Repeat("r", 512)
			}
			c := nodeResponseClient(t, r)
			if _, err := c.GetQueryProgress(context.Background(), api.ReadQueryProgressRequest{Space: "s", Query: "q", Anchor: &r.Anchor}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
