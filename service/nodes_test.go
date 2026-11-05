package service

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestNodeWireEnforcesViewOwnerAndHistoricalEntityCut(t *testing.T) {
	core, _ := nativeAgentStore(t, true)
	s := newServer(t, core, agentCredentials())
	owner := pipeClient(t, s, "owner-token")
	other := pipeClient(t, s, "other-token")
	ctx := context.Background()
	capable := false
	for _, cap := range owner.Identity().Capabilities {
		capable = capable || cap == "core/node-query-v1"
	}
	if !capable {
		t.Fatal("node capability missing")
	}
	ack, err := owner.ApplyAgent(ctx, agentCreateRequest("wire-node-create"))
	if err != nil {
		t.Fatal(err)
	}
	v, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s", Anchor: &model.SnapshotAnchor{Space: "s", Fence: ack.Fence}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := owner.ListNodes(ctx, api.ListNodesRequest{View: v.Ref})
	if err != nil || len(p.Items) != 1 || p.Items[0].Ref.Kind != model.NodeEntity {
		t.Fatalf("page=%+v err=%v", p, err)
	}
	d, err := owner.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: p.Items[0].Ref})
	if err != nil || len(d.Data) == 0 || d.Anchor.Fence != ack.Fence {
		t.Fatalf("detail=%+v err=%v", d, err)
	}
	context, err := owner.GetNodeContext(ctx, api.NodeContextRequest{View: v.Ref, Ref: d.Ref})
	if err != nil || len(context.Sections["node"]) != 1 || context.Anchor.Fence != ack.Fence {
		t.Fatalf("context=%+v err=%v", context, err)
	}
	if _, err := other.ListNodes(ctx, api.ListNodesRequest{View: v.Ref}); err == nil {
		t.Fatal("foreign View accepted")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
	otherView, err := other.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.GetNode(ctx, api.GetNodeRequest{View: otherView.Ref, Ref: d.Ref}); err == nil {
		t.Fatal("foreign entity accepted")
	} else {
		remoteCode(t, err, api.ErrorNotFound)
	}
	if _, err := other.GetNode(ctx, api.GetNodeRequest{View: otherView.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: d.Payload}}); err == nil {
		t.Fatal("foreign snapshot accepted")
	} else {
		remoteCode(t, err, api.ErrorNotFound)
	}
	if _, err := owner.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: d.Ref}); err != nil {
		t.Fatalf("node denial poisoned store/connection: %v", err)
	}
}

func TestNodeExploreWireMatchesLibraryAndRejectsForeignViews(t *testing.T) {
	ctx := context.Background()
	core, _ := nativeAgentStore(t, true)
	s := newServer(t, core, agentCredentials())
	owner := pipeClient(t, s, "owner-token")
	other := pipeClient(t, s, "other-token")
	capable := false
	for _, cap := range owner.Identity().Capabilities {
		capable = capable || cap == "core/node-explore-v1"
	}
	if !capable {
		t.Fatal("explore capability missing")
	}
	if _, err := owner.ApplyAgent(ctx, agentCreateRequest("wire-search-task")); err != nil {
		t.Fatal(err)
	}
	x := agentprofile.Execution{ID: "exec", Task: "t", Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	if _, err := owner.ApplyAgent(ctx, api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", RequestID: "wire-search-exec"}, Command: agentprofile.Command{Spawn: &x}}); err != nil {
		t.Fatal(err)
	}
	v, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	search := api.SearchNodesRequest{View: v.Ref, EntityKind: "Task", Text: "finish", Match: "exact"}
	result, err := owner.SearchNodes(ctx, search)
	if err != nil || len(result.Items) != 1 || result.Coverage != "complete_layer" || result.Items[0].Node.Ref.Entity.ID != "t" {
		t.Fatalf("search=%+v %v", result, err)
	}
	ref := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "agent", Kind: "Execution", ID: model.EntityID(x.ID)}}
	trace, err := owner.Trace(ctx, api.TraceRequest{View: v.Ref, Start: ref})
	if err != nil || len(trace.Edges) != 1 || trace.Edges[0].To != result.Items[0].Node.Ref || trace.Edges[0].Relation != "parent" {
		t.Fatalf("trace=%+v %v", trace, err)
	}
	for _, call := range []func() error{func() error { _, err := other.SearchNodes(ctx, search); return err }, func() error { _, err := other.Trace(ctx, api.TraceRequest{View: v.Ref, Start: ref}); return err }} {
		if err := call(); err == nil {
			t.Fatal("foreign View accepted")
		} else {
			remoteCode(t, err, api.ErrorUnauthorized)
		}
	}
	otherView, err := other.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	hidden, err := other.SearchNodes(ctx, api.SearchNodesRequest{View: otherView.Ref, Text: "finish"})
	if err != nil || len(hidden.Items) != 0 {
		t.Fatalf("search disclosed private task=%+v %v", hidden, err)
	}
	if _, err = other.Trace(ctx, api.TraceRequest{View: otherView.Ref, Start: ref}); err == nil {
		t.Fatal("trace disclosed private path")
	} else {
		remoteCode(t, err, api.ErrorNotFound)
	}
}

// A direct reader remains useful to a trusted embedding host. The wire service
// requires a grant-capable backend before offering the same payload endpoints.
type directNodeReaders struct {
	Backend
	api.BasicNodeReader
	api.ContentReader
}

func TestNodeWireDoesNotFallbackToAnUngrantableReader(t *testing.T) {
	ctx := context.Background()
	core, _ := nativeAgentStore(t, true)
	backend := directNodeReaders{Backend: core, BasicNodeReader: core, ContentReader: core}
	s := newServer(t, backend, agentCredentials())
	owner := pipeClient(t, s, "owner-token")
	for _, capability := range owner.Identity().Capabilities {
		if capability == "core/node-query-v1" || capability == "core/content-range-v1" {
			t.Fatalf("advertised an ungrantable read capability: %s", capability)
		}
	}
	if _, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s"}); err == nil {
		t.Fatal("wire opened a view without a delivery grant")
	} else {
		remoteCode(t, err, api.ErrorUnsupported)
	}
	if _, err := owner.ListNodes(ctx, api.ListNodesRequest{}); err == nil {
		t.Fatal("wire used ungrantable node reader")
	} else {
		remoteCode(t, err, api.ErrorUnsupported)
	}
}
