package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func emissionResponse() api.EmissionResult {
	fence := func(lsn uint64, digest string) model.CommitFence {
		return model.CommitFence{Store: "store", LSN: lsn, CommitDigest: model.CommitDigest(strings.Repeat(digest, 64))}
	}
	content := model.ObjectID(strings.Repeat("e", 64))
	dependencies := []model.ObjectID{content}
	digest, _ := api.EmissionDependencyDigest(dependencies)
	return api.EmissionResult{
		Found: true, Anchor: model.SnapshotAnchor{Space: "s", Fence: fence(3, "c")}, Fence: fence(3, "c"), RootSet: model.ObjectID(strings.Repeat("d", 64)),
		Record: api.EmissionRecord{
			Spec:   api.EmissionSpec{ID: "grant", Execution: "execution", Task: "task", GoalEpoch: 1, Authority: agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "s", Fence: fence(1, "a")}, Length: 5, Destination: "sink"},
			Status: "issued", Version: 1,
			Proof: &api.EmissionProof{Space: "s", Principal: "p", PolicyFence: fence(2, "b"), Dependencies: dependencies, DependencyDigest: digest, Projection: "payload", ContentDigest: model.ObjectID(strings.Repeat("f", 64)), RangeDigest: model.ObjectID(strings.Repeat("f", 64)), TotalBytes: 5},
		},
	}
}

func TestEmissionClientRejectsMalformedSuccessAndClosesConnection(t *testing.T) {
	for name, mutate := range map[string]func(*api.EmissionResult){
		"missing-proof":           func(r *api.EmissionResult) { r.Record.Proof = nil },
		"different-fence":         func(r *api.EmissionResult) { r.Fence.LSN++ },
		"foreign-space":           func(r *api.EmissionResult) { r.Anchor.Space = "other" },
		"invalid-anchor":          func(r *api.EmissionResult) { r.Anchor.Fence.CommitDigest = "bad" },
		"unrequested-publication": func(r *api.EmissionResult) { r.Anchor.Fence.LSN++; r.Fence = r.Anchor.Fence },
		"unexpected-selector": func(r *api.EmissionResult) {
			r.Anchor.Knowledge = &model.KnowledgeKey{Actor: "actor", Execution: "execution"}
		},
		"invalid-root":              func(r *api.EmissionResult) { r.RootSet = "bad" },
		"missing-root":              func(r *api.EmissionResult) { r.RootSet = "" },
		"false-with-record":         func(r *api.EmissionResult) { r.Found = false },
		"different-grant":           func(r *api.EmissionResult) { r.Record.Spec.ID = "different" },
		"foreign-source-space":      func(r *api.EmissionResult) { r.Record.Spec.Anchor.Space = "other" },
		"foreign-source-store":      func(r *api.EmissionResult) { r.Record.Spec.Anchor.Fence.Store = "other" },
		"future-source":             func(r *api.EmissionResult) { r.Record.Spec.Anchor.Fence.LSN = 4 },
		"same-LSN-wrong-digest":     func(r *api.EmissionResult) { r.Record.Spec.Anchor.Fence.LSN = 3 },
		"malformed-content":         func(r *api.EmissionResult) { r.Record.Spec.Content = "bad" },
		"invalid-spec":              func(r *api.EmissionResult) { r.Record.Spec.Length = 0 },
		"invalid-status":            func(r *api.EmissionResult) { r.Record.Status = "sent" },
		"zero-version":              func(r *api.EmissionResult) { r.Record.Version = 0 },
		"unknown-projection":        func(r *api.EmissionResult) { r.Record.Spec.Projection = "json" },
		"substituted-projection":    func(r *api.EmissionResult) { r.Record.Proof.Projection = "value_bytes" },
		"missing-proof-projection":  func(r *api.EmissionResult) { r.Record.Proof.Projection = "" },
		"projection-without-proof":  func(r *api.EmissionResult) { r.Record.Spec.Projection = "payload"; r.Record.Proof = nil },
		"missing-content-digest":    func(r *api.EmissionResult) { r.Record.Proof.ContentDigest = "" },
		"invalid-range-digest":      func(r *api.EmissionResult) { r.Record.Proof.RangeDigest = "bad" },
		"entire-range-wrong-digest": func(r *api.EmissionResult) { r.Record.Proof.RangeDigest = model.ObjectID(strings.Repeat("d", 64)) },
		"range-truncated":           func(r *api.EmissionResult) { r.Record.Proof.TotalBytes = 4 },
		"offset-after-content":      func(r *api.EmissionResult) { r.Record.Spec.Offset = 6 },
		"length-exceeds-remainder":  func(r *api.EmissionResult) { r.Record.Spec.Offset = 1 },
		"knowledge-source-selector": func(r *api.EmissionResult) {
			r.Record.Spec.Anchor.Knowledge = &model.KnowledgeKey{Actor: "actor", Execution: "execution"}
		},
		"foreign-proof-owner":       func(r *api.EmissionResult) { r.Record.Proof.Principal = "other" },
		"foreign-proof-space":       func(r *api.EmissionResult) { r.Record.Proof.Space = "other" },
		"future-policy":             func(r *api.EmissionResult) { r.Record.Proof.PolicyFence.LSN = 4 },
		"policy-precedes-source":    func(r *api.EmissionResult) { r.Record.Proof.PolicyFence.LSN = 0 },
		"invalid-policy-root":       func(r *api.EmissionResult) { r.Record.Proof.PolicyRoot = "bad" },
		"invalid-dependency-digest": func(r *api.EmissionResult) { r.Record.Proof.DependencyDigest = model.ObjectID(strings.Repeat("f", 64)) },
		"content-not-in-closure": func(r *api.EmissionResult) {
			r.Record.Proof.Dependencies = []model.ObjectID{model.ObjectID(strings.Repeat("f", 64))}
			r.Record.Proof.DependencyDigest, _ = api.EmissionDependencyDigest(r.Record.Proof.Dependencies)
		},
	} {
		t.Run(name, func(t *testing.T) {
			response := emissionResponse()
			anchor := response.Anchor.Clone()
			mutate(&response)
			c := nodeResponseClient(t, response)
			if _, err := c.GetEmission(context.Background(), api.ReadEmissionRequest{Space: "s", ID: "grant", Anchor: &anchor}); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("malformed Emission success accepted: %v", err)
			}
			if !c.closed.Load() {
				t.Fatal("protocol-invalid connection remains reusable")
			}
		})
	}
}

func TestEmissionClientAcceptsV1PolicyBoundRecords(t *testing.T) {
	for _, schema := range []string{"policy-bound", "explicit-value-bytes", "partial-range", "not-found"} {
		t.Run(schema, func(t *testing.T) {
			response := emissionResponse()
			if schema == "explicit-value-bytes" {
				response.Record.Spec.Projection = "value_bytes"
				response.Record.Proof.Projection = "value_bytes"
			}
			if schema == "partial-range" {
				response.Record.Spec.Offset = 1
				response.Record.Spec.Length = 4
				response.Record.Proof.RangeDigest = model.ObjectID(strings.Repeat("d", 64))
			}
			if schema == "not-found" {
				response.Found = false
				response.Record = api.EmissionRecord{}
			}
			c := nodeResponseClient(t, response)
			got, err := c.GetEmission(context.Background(), api.ReadEmissionRequest{Space: "s", ID: "grant", Anchor: &response.Anchor})
			if err != nil || got.Found != response.Found {
				t.Fatalf("valid %s response=%+v err=%v", schema, got, err)
			}
		})
	}
}
