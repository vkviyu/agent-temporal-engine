package host_test

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func branchAuthority(id model.ExecutionID, b agentprofile.AuthorityBinding) graph.ExecutionAuthority {
	return graph.ExecutionAuthority{Execution: id, Deployment: b.Deployment, Holder: b.Holder, OwnerEpoch: b.OwnerEpoch, Incarnation: b.Incarnation}
}

func branchValue(key string, value []byte) api.StateMutation {
	return api.StateMutation{Key: []byte(key), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: value}}
}

func (h *host) branch(id model.BranchID) graph.Branch {
	h.t.Helper()
	b, err := h.peer.GetGraphBranch(context.Background(), api.ReadGraphBranchRequest{Space: "project", ID: id})
	if err != nil || !b.Found {
		h.t.Fatalf("branch %q: %+v %v", id, b, err)
	}
	return b.Value
}

func (h *host) forkLocal(id model.BranchID, base model.RevisionID, execution model.ExecutionID, binding agentprofile.AuthorityBinding) {
	h.t.Helper()
	command := graph.Command{ForkLocal: &graph.ForkLocalCommand{Branch: id, Source: base, Authority: branchAuthority(execution, binding)}}
	h.ack(h.peer.ApplyGraph(context.Background(), api.ApplyGraphRequest{Key: key("fork-" + string(id)), Command: command}))
}

func (h *host) writeBranch(id model.BranchID, base model.RevisionID, execution model.ExecutionID, binding agentprofile.AuthorityBinding, operations []api.StateMutation) (graph.Revision, api.CommitResult) {
	h.t.Helper()
	ctx := context.Background()
	batch, err := engine.EncodeStateBatch(api.StateBatch{Operations: operations})
	if err != nil {
		h.t.Fatal(err)
	}
	command := graph.Command{WriteBranch: &graph.WriteBranchCommand{
		Target: graph.BranchPosition{Branch: id, Head: base}, Authority: branchAuthority(execution, binding), Batch: batch,
	}}
	ack := h.ack(h.peer.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key("write-" + string(id)), Command: command}))
	b := h.branch(id)
	rev, err := h.peer.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "project", ID: b.Head})
	if err != nil || !rev.Found || rev.Value.Producer == nil || rev.Value.Producer.Execution != execution {
		h.t.Fatalf("branch revision lacks verified execution: %+v %v", rev, err)
	}
	return rev.Value, ack
}

func (h *host) recordLongTaskTrace(query string) (api.TaskTraceRequest, api.TaskTraceResult) {
	h.t.Helper()
	ctx := context.Background()
	req := api.TaskTraceRequest{Space: "project", Principal: "owner", Subject: api.TraceSubject{Kind: "query", ID: query}, Session: "session", Limit: 128}
	raw, err := h.peer.GetTaskTrace(ctx, req)
	if err != nil || raw.Version != 0 || raw.Continuation != "" {
		h.t.Fatalf("initial trace: %+v %v", raw, err)
	}
	wrongSession := req
	wrongSession.Session = "different-session"
	if _, err := h.peer.GetTaskTrace(ctx, wrongSession); err == nil {
		h.t.Fatal("trace accepted a mismatched session")
	}
	var answer, rootOutcome, rightOutcome api.TaskTraceNode
	var rootStart, rightSpawn, rootFork, rightFork, rightPrune model.EventID
	for _, node := range raw.Nodes {
		switch {
		case node.Operation == "Control.AnswerQuery":
			answer = node
		case node.Operation == "Control.CompleteTask" && node.Task == "root-task":
			rootOutcome = node
		case node.Operation == "Control.CompleteTask" && node.Task == "right-task":
			rightOutcome = node
		case node.Operation == "Control.StartWork" && node.Task == "root-task":
			rootStart = node.At.Ref.Event
		case node.Operation == "Control.SpawnChild" && node.Subject.ID == "right":
			rightSpawn = node.At.Ref.Event
		case node.Operation == "Graph.ForkLocal" && node.Subject.ID == "chosen" && node.Execution == "root":
			rootFork = node.At.Ref.Event
		case node.Operation == "Graph.ForkLocal" && node.Subject.ID == "right" && node.Execution == "right":
			rightFork = node.At.Ref.Event
		case node.Operation == "Graph.Prune" && node.Subject.ID == "right" && node.Execution == "right" && node.Task == "right-task":
			rightPrune = node.At.Ref.Event
		}
	}
	if answer.At.Ref.Event == "" || rootOutcome.At.Ref.Event == "" || rightOutcome.At.Ref.Event == "" {
		h.t.Fatalf("trace missed answer or task outcome: %+v", raw.Nodes)
	}
	paged := req
	paged.Limit = 1
	seen := map[model.EventID]bool{}
	for pageNumber := 0; ; pageNumber++ {
		page, err := h.peer.GetTaskTrace(ctx, paged)
		if err != nil {
			h.t.Fatalf("paged trace %d: %v", pageNumber, err)
		}
		if len(page.Nodes) > paged.Limit {
			h.t.Fatalf("paged trace returned %d nodes for limit %d", len(page.Nodes), paged.Limit)
		}
		for _, node := range page.Nodes {
			if seen[node.At.Ref.Event] {
				h.t.Fatalf("paged trace repeated event %q", node.At.Ref.Event)
			}
			seen[node.At.Ref.Event] = true
		}
		if page.Continuation == "" {
			break
		}
		if pageNumber >= len(raw.Nodes)+2 {
			h.t.Fatal("paged trace did not terminate")
		}
		paged.Anchor = &page.Anchor
		paged.Continuation = page.Continuation
	}
	if len(seen) != len(raw.Nodes) {
		h.t.Fatalf("paged trace lost nodes: got %d want %d", len(seen), len(raw.Nodes))
	}
	expected := map[[2]model.EventID]bool{
		{rootOutcome.At.Ref.Event, answer.At.Ref.Event}: false,
		{rootStart, rootFork}:                           false,
		{rightSpawn, rightFork}:                         false,
		{rightFork, rightPrune}:                         false,
	}
	for _, edge := range raw.Edges {
		if endpoints := [2]model.EventID{edge.From, edge.To}; edge.Relation == "causal" {
			if _, wanted := expected[endpoints]; wanted {
				expected[endpoints] = true
			}
		}
	}
	for endpoints, found := range expected {
		if !found {
			h.t.Fatalf("trace lacks a causal edge from %q to %q", endpoints[0], endpoints[1])
		}
	}
	// Looking back after a different session has started still addresses the
	// first Query. The unrelated Query must not enter its event graph.
	otherQuery := agentprofile.Query{ID: "other-query", Session: "other-session", Space: "project", Owner: "owner", Input: []byte("a new task"), InputFence: raw.Anchor.Fence, Status: "open", Version: 1}
	h.ack(h.peer.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key("other-session-query"), Command: agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &otherQuery}}}))
	otherTrace, err := h.peer.GetTaskTrace(ctx, api.TaskTraceRequest{Space: "project", Principal: "owner", Subject: api.TraceSubject{Kind: "query", ID: otherQuery.ID}, Session: otherQuery.Session})
	if err != nil || len(otherTrace.Nodes) != 1 {
		h.t.Fatalf("unrelated session trace: %+v %v", otherTrace, err)
	}
	edit := api.TaskTraceEdit{Subject: req.Subject, Source: raw.Anchor,
		Milestones: []api.TraceMilestone{
			{Event: rightOutcome.At.Ref.Event, Label: "Alternative left a counterexample", Evidence: []api.TraceCitation{rightOutcome.At}},
			{Event: answer.At.Ref.Event, Label: "Final answer", Evidence: []api.TraceCitation{rootOutcome.At}},
		},
		Assertions: []api.TraceAssertion{{From: rightOutcome.At.Ref.Event, To: answer.At.Ref.Event, Relation: "explains", Reason: "The counterexample is useful in the explanation", Evidence: []api.TraceCitation{rightOutcome.At}}},
	}
	bad := edit
	bad.Source = otherTrace.Anchor
	bad.Assertions = nil
	bad.Milestones = []api.TraceMilestone{{Event: otherTrace.Nodes[0].At.Ref.Event, Label: "other query", Evidence: []api.TraceCitation{otherTrace.Nodes[0].At}}}
	if out, err := h.peer.ApplyTaskTrace(ctx, api.ApplyTaskTraceRequest{Key: key("cross-query-milestone"), Edit: bad}); err == nil || out.Code == api.CodeDurableAck {
		h.t.Fatalf("cross-query milestone was accepted: %+v %v", out, err)
	}
	h.ack(h.peer.ApplyTaskTrace(ctx, api.ApplyTaskTraceRequest{Key: key("curate-long-task"), Edit: edit}))
	req.Anchor = &model.SnapshotAnchor{Space: "project", Fence: h.snapshot().Fence}
	curated, err := h.peer.GetTaskTrace(ctx, req)
	if err != nil || curated.Version != 1 {
		h.t.Fatalf("curated trace: %+v %v", curated, err)
	}
	evidence := api.TaskTraceEvidenceRequest{TaskTraceRequest: req, From: rightOutcome.At.Ref.Event, To: answer.At.Ref.Event, Relation: "explains"}
	detail, err := h.peer.GetTaskTraceEvidence(ctx, evidence)
	if err != nil || !reflect.DeepEqual(detail.Citations, []api.TraceCitation{rightOutcome.At}) {
		h.t.Fatalf("assertion evidence: %+v %v", detail, err)
	}
	evidence.From = rootOutcome.At.Ref.Event
	evidence.Relation, evidence.Limit = "causal", 1
	page, err := h.peer.GetTaskTraceEvidence(ctx, evidence)
	if err != nil || len(page.Events) != 1 || page.Events[0].At.Ref.Event != evidence.From || page.Continuation == "" {
		h.t.Fatalf("first causal page: %+v %v", page, err)
	}
	evidence.Continuation = page.Continuation
	page, err = h.peer.GetTaskTraceEvidence(ctx, evidence)
	if err != nil || len(page.Events) != 1 || page.Events[0].At.Ref.Event != evidence.To || page.Continuation != "" {
		h.t.Fatalf("second causal page: %+v %v", page, err)
	}
	return req, curated
}
