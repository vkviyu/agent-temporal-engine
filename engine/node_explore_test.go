package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestNodeSearchExactSummaryCoverageHistoricalPagesAndAuthorization(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	for i, goal := range []string{"Alpha evidence", "Beta evidence", "Alpha audit"} {
		task := agentTask(model.EntityID(fmt.Sprintf("search-%d", i)))
		task.Goal = goal
		coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey(fmt.Sprintf("search-create-%d", i)), Command: agentprofile.Command{CreateTask: &task}})
	}
	old := nodeView(t, s.e, "host-principal", nil)
	task := agentTask("future-alpha")
	task.Goal = "Alpha future"
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("search-future"), Command: agentprofile.Command{CreateTask: &task}})
	r := api.SearchNodesRequest{View: old.Ref, EntityKind: "Task", Text: "Alpha", Match: "prefix", Limit: 1}
	first, err := s.e.SearchNodes(ctx, r)
	if err != nil || len(first.Items) != 1 || !first.Truncated || first.Coverage != "complete_layer" || first.SearchVersion != "summary-text/v1" {
		t.Fatalf("first=%+v %v", first, err)
	}
	if !reflect.DeepEqual(first.Items[0].Reasons, []string{"kind:exact", "label:prefix"}) {
		t.Fatalf("reasons=%+v", first.Items[0].Reasons)
	}
	r.Continuation = first.Continuation
	second, err := s.e.SearchNodes(ctx, r)
	if err != nil || len(second.Items) != 1 || second.Truncated || second.Items[0].Node.Ref == first.Items[0].Node.Ref {
		t.Fatalf("second=%+v %v", second, err)
	}
	if !reflect.DeepEqual(first.Anchor, old.Anchor) || !reflect.DeepEqual(second.Anchor, old.Anchor) {
		t.Fatal("search followed latest cut")
	}
	for _, mutate := range []func(*api.SearchNodesRequest){func(x *api.SearchNodesRequest) { x.Text = "Beta" }, func(x *api.SearchNodesRequest) { x.Match = "contains" }, func(x *api.SearchNodesRequest) { x.EntityKind = "Execution" }, func(x *api.SearchNodesRequest) { x.View = nodeView(t, s.e, "host-principal", nil).Ref }} {
		bad := r
		mutate(&bad)
		if _, err = s.e.SearchNodes(ctx, bad); !errors.Is(err, api.ErrInvalidRequest) {
			t.Fatalf("cross-query token accepted: %v", err)
		}
	}
	literal, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: old.Ref, Text: "Alpha.*"})
	if err != nil || len(literal.Items) != 0 {
		t.Fatalf("query treated as pattern: %+v %v", literal, err)
	}
	other := nodeView(t, s.e, "other-principal", nil)
	hidden, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: other.Ref, Text: "Alpha"})
	if err != nil || len(hidden.Items) != 0 {
		t.Fatalf("foreign search=%+v %v", hidden, err)
	}
	if _, err = s.e.SearchNodes(ctx, api.SearchNodesRequest{View: old.Ref, Limits: api.QueryLimits{MaxBytes: 1}}); engine.ClassifyError(err) != api.ErrorResourceLimit {
		t.Fatalf("byte bound=%v", err)
	}
	if _, err = s.e.SearchNodes(ctx, api.SearchNodesRequest{View: old.Ref, TextField: "payload"}); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatalf("implicit payload search: %v", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatalf("search failure fenced store: %v", err)
	}
}

func TestNodeTraceDirectedScopePaginationDepthAndHistory(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	task := agentTask("trace-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("trace-task"), Command: agentprofile.Command{CreateTask: &task}})
	for _, x := range []agentprofile.Execution{{ID: "root", Task: task.ID, Actor: "r", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}, {ID: "child", Task: task.ID, Actor: "c", Parent: "root", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}, {ID: "sibling", Task: task.ID, Actor: "s", Parent: "root", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}} {
		v := x
		coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("trace-" + string(x.ID)), Command: agentprofile.Command{Spawn: &v}})
	}
	view := nodeView(t, s.e, "host-principal", nil)
	start := nodeRef("Execution", "child")
	q := api.TraceRequest{View: view.Ref, Start: start, Direction: "both", Limits: api.QueryLimits{MaxDepth: 2, MaxNodes: 3, MaxEdges: 1}}
	page, err := s.e.Trace(ctx, q)
	if err != nil || !page.Truncated || len(page.Edges) != 1 || len(page.Nodes) != 2 {
		t.Fatalf("first=%+v %v", page, err)
	}
	firstToken := page.Continuation
	seen := map[api.NodeTraceEdge]bool{}
	nodes := map[model.NodeRef]bool{}
	for {
		for _, n := range page.Nodes {
			nodes[n.Ref] = true
		}
		for _, edge := range page.Edges {
			if seen[edge] {
				t.Fatal("repeated edge")
			}
			seen[edge] = true
			if edge.Relation != "parent" {
				t.Fatalf("unregistered relation=%s", edge.Relation)
			}
		}
		if !page.Truncated {
			break
		}
		q.Continuation = page.Continuation
		page, err = s.e.Trace(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 3 || !nodes[nodeRef("Task", "trace-task")] || !nodes[nodeRef("Execution", "root")] || nodes[nodeRef("Execution", "sibling")] {
		t.Fatalf("trace imported sibling or lost path: %+v %+v", nodes, seen)
	}
	bad := q
	bad.Direction = "parents"
	bad.Continuation = firstToken
	if _, err = s.e.Trace(ctx, bad); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatalf("trace token crossed direction: %v", err)
	}
	shallow, err := s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: nodeRef("Execution", "root"), Direction: "children", Limits: api.QueryLimits{MaxDepth: 1}})
	if err != nil || shallow.DepthLimited {
		t.Fatalf("leaf depth=%+v %v", shallow, err)
	}
	parents, err := s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: start, Limits: api.QueryLimits{MaxDepth: 1}})
	if err != nil || !parents.DepthLimited {
		t.Fatalf("missing depth boundary=%+v %v", parents, err)
	}
	other := nodeView(t, s.e, "other-principal", nil)
	if _, err = s.e.Trace(ctx, api.TraceRequest{View: other.Ref, Start: start}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("private path leaked: %v", err)
	}
	if _, err = s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: start, Relations: []string{"arbitrary"}}); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatalf("unknown relation: %v", err)
	}
	if _, err = s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: start, Limits: api.QueryLimits{MaxNodes: 1}}); engine.ClassifyError(err) != api.ErrorResourceLimit {
		t.Fatalf("undersized page=%v", err)
	}
}

func TestEventTraceFollowsOnlyPublishedCausalParents(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	one := integrationMutate(t, s.e, integrationRequest("trace-event-one", integrationPut([]byte("k"), integrationBytes('a'))))
	old := nodeView(t, s.e, "host-principal", nil)
	integrationMutate(t, s.e, integrationRequest("trace-event-two", integrationPut([]byte("k"), integrationBytes('b'))))
	latest := nodeView(t, s.e, "host-principal", nil)
	list, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: latest.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil || len(list.Items) != 2 {
		t.Fatalf("events=%+v %v", list, err)
	}
	var newest model.NodeRef
	for _, n := range list.Items {
		trace, err := s.e.Trace(ctx, api.TraceRequest{View: latest.Ref, Start: n.Ref, Relations: []string{"causal_parent"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(trace.Edges) == 1 {
			newest = n.Ref
			if len(trace.Nodes) != 2 || trace.Edges[0].From != n.Ref {
				t.Fatalf("wrong causal orientation=%+v", trace)
			}
		}
	}
	if newest.Kind == "" {
		t.Fatal("no causal event chain")
	}
	if _, err = s.e.Trace(ctx, api.TraceRequest{View: old.Ref, Start: newest}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("future Event path leaked: %v", err)
	}
	if old.Anchor.Fence != one.Fence {
		t.Fatal("bad event fixture")
	}
}

func TestKnowledgeRevisionNavigationAndTraceKeepSelectedPrefix(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "node-revision-one", one)
	first, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil || first.Revision == nil {
		t.Fatalf("first=%+v %v", first, err)
	}
	two := knowledgeObservation(payload, source, "one", 2)
	applyObservation(t, s.e, "node-revision-two", two)
	latest, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &one.Key})
	if err != nil || latest.Revision == nil {
		t.Fatalf("latest=%+v %v", latest, err)
	}
	anchor := latest.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: source.Store, Space: "host-space", Revision: first.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	search, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: view.Ref, Layer: "content", EntityKind: "KnowledgeRevision"})
	if err != nil || len(search.Items) != 2 {
		t.Fatalf("knowledge revision navigation=%+v %v", search, err)
	}
	start := model.NodeRef{Kind: model.NodeRevision, Revision: first.Revision.ID}
	trace, err := s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: start, Limits: api.QueryLimits{MaxDepth: 3}})
	if err != nil {
		t.Fatal(err)
	}
	seenObservation := false
	seenGenesis := false
	for _, n := range trace.Nodes {
		if n.Ref.Object == two.ID || n.Ref.Revision == latest.Revision.ID {
			t.Fatalf("future revision/evidence in old cursor: %+v", n)
		}
		seenObservation = seenObservation || n.Ref.Object == one.ID
		seenGenesis = seenGenesis || n.Ref.Revision == first.Revision.Parent
	}
	if !seenObservation || !seenGenesis {
		t.Fatalf("knowledge provenance omitted=%+v", trace)
	}
	if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeRevision, Revision: latest.Revision.ID}}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("future knowledge revision detail leaked: %v", err)
	}
}

func TestAdoptedEventNavigationDoesNotGrantUnadoptedEvidence(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	task := agentTask("opaque-event-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("opaque-event-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "opaque-event-exec", Task: task.ID, Actor: "opaque-event-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	ack := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("opaque-event-spawn"), Command: agentprofile.Command{Spawn: &x}})
	owner := nodeView(t, s.e, "host-principal", nil)
	page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: owner.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil || len(page.Items) != 4 {
		t.Fatalf("events=%+v %v", page, err)
	}
	ref := page.Items[0].Ref
	detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: owner.Ref, Ref: ref})
	if err != nil {
		t.Fatal(err)
	}
	observation := knowledge.Observation{Key: model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}}, Payload: model.ObjectID(ref.Event), Sequence: 1, Label: "known occurrence"}
	observation.ID = knowledge.ObservationID(observation)
	if out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("opaque-event-adopt"), Command: knowledge.Command{Observation: &observation}}); err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("adopt=%+v %v", out, err)
	}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Knowledge: &observation.Key, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref}); err != nil {
		t.Fatalf("adopted occurrence hidden: %v", err)
	}
	for _, source := range detail.Sources {
		if source.Kind != model.NodeObject {
			continue
		}
		if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: source}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("unadopted audit evidence became knowledge: %+v %v", source, err)
		}
	}
	trace, err := s.e.Trace(ctx, api.TraceRequest{View: view.Ref, Start: ref, Relations: []string{"source"}})
	if err != nil || len(trace.Edges) != 0 || len(trace.Nodes) != 1 {
		t.Fatalf("trace expanded opaque evidence: %+v %v", trace, err)
	}
}
