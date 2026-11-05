package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func graphRevisionFromCurrent(t *testing.T, s *integrationStore, id string, parent model.RevisionID) graph.Revision {
	t.Helper()
	ctx := context.Background()
	snapshot := integrationSnapshot(t, s.e)
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	gentries := make([]graph.Entry, len(entries.Items))
	for i, x := range entries.Items {
		gentries[i] = graph.Entry{Key: x.Key, Value: x.Value}
	}
	r, err := graph.BuildRevision("host-space", parent, nil, entries.StateRoot, nil, gentries)
	if err != nil {
		t.Fatal(err)
	}
	graphCoreApply(t, s.e, id, graph.Command{AddRevision: &graph.AddRevisionCommand{Parent: parent, Root: entries.StateRoot, Entries: gentries}})
	if snapshot.Roots.DomainStates != integrationSnapshot(t, s.e).Roots.DomainStates {
		t.Fatal("adding metadata altered domain")
	}
	return r
}

func graphDivergence(t *testing.T, conflict bool) (*integrationStore, graph.Revision, graph.Revision, graph.Revision, model.ObjectID) {
	t.Helper()
	s := newIntegrationStore(t, engine.Options{})
	base := graphRevisionFromCurrent(t, s, "join-base", "")
	graphCoreApply(t, s.e, "join-main", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: base.ID}})
	integrationMutate(t, s.e, integrationRequest("join-left-state", integrationPut([]byte("a"), api.Value{Kind: api.ValueBytes, Data: []byte("left")})))
	left := graphRevisionFromCurrent(t, s, "join-left", base.ID)
	graphCoreApply(t, s.e, "join-left-head", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: left.ID}})
	if conflict {
		integrationMutate(t, s.e, integrationRequest("join-right-state", integrationPut([]byte("a"), api.Value{Kind: api.ValueBytes, Data: []byte("right")})))
	} else {
		integrationMutate(t, s.e, integrationRequest("join-right-state", api.StateMutation{Key: []byte("a"), Kind: api.MutationDelete}, integrationPut([]byte("b"), api.Value{Kind: api.ValueBytes, Data: []byte("right")})))
	}
	right := graphRevisionFromCurrent(t, s, "join-right", base.ID)
	return s, base, left, right, right.StateRoot
}

func TestGraphJoinPublishesBothRootsPruneRetainsHistoryAndRetries(t *testing.T) {
	ctx := context.Background()
	s, base, left, right, current := graphDivergence(t, false)
	plan, err := s.e.PlanGraphJoin(ctx, api.PlanGraphJoinRequest{Space: "host-space", Left: left.ID, Right: right.ID})
	if err != nil || plan.Plan.Base != base.ID || len(plan.Plan.Conflicts) != 0 {
		t.Fatalf("plan=%+v %v", plan, err)
	}
	before := integrationSnapshot(t, s.e)
	join := api.ApplyGraphRequest{Key: graphCoreKey("join"), Command: graph.Command{Join: &graph.JoinCommand{Branch: "main", Expected: 1, Left: left.ID, Right: right.ID, ExpectedDomainRoot: current}}}
	ack, err := s.e.ApplyGraph(ctx, join)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("join=%+v %v", ack, err)
	}
	after := integrationSnapshot(t, s.e)
	if after.Roots.BranchRefs == before.Roots.BranchRefs || after.Roots.DomainStates == before.Roots.DomainStates || after.Roots.ExecutionKnowledge != before.Roots.ExecutionKnowledge || after.Fence.LSN != before.Fence.LSN+1 {
		t.Fatalf("not one atomic two-root transition: before=%+v after=%+v", before, after)
	}
	for _, key := range []string{"a", "b"} {
		v, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte(key)})
		if err != nil || !v.Found {
			t.Fatalf("merged %s=%+v %v", key, v, err)
		}
	}
	b, err := s.e.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "host-space", ID: "main"})
	if err != nil || b.Value.Version != 2 {
		t.Fatalf("head=%+v %v", b, err)
	}
	r, err := s.e.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "host-space", ID: b.Value.Head})
	if err != nil || r.Value.StateParent != left.ID || len(r.Value.JoinParents) != 1 || r.Value.JoinParents[0] != right.ID || len(r.Value.Entries) != 2 {
		t.Fatalf("joined revision=%+v %v", r, err)
	}
	prune := api.ApplyGraphRequest{Key: graphCoreKey("prune"), Command: graph.Command{Prune: &graph.PruneBranchCommand{Branch: "main", Expected: 2, Head: r.Value.ID, Reason: "exploration complete"}}}
	pruned, err := s.e.ApplyGraph(ctx, prune)
	if err != nil || pruned.Code != api.CodeDurableAck {
		t.Fatalf("prune=%+v %v", pruned, err)
	}
	check := func() {
		t.Helper()
		got, err := s.e.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "host-space", ID: "main"})
		if err != nil || !got.Pruned || got.Prune == nil || got.Value.Head != r.Value.ID || got.Value.Version != 3 {
			t.Fatalf("pruned branch=%+v %v", got, err)
		}
		historical, err := s.e.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "host-space", ID: "main", Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}})
		if err != nil || historical.Pruned || historical.Value.Version != 2 {
			t.Fatalf("old branch=%+v %v", historical, err)
		}
		if retry, err := s.e.ApplyGraph(ctx, join); err != nil || retry != ack {
			t.Fatalf("join retry=%+v %v", retry, err)
		}
		if retry, err := s.e.ApplyGraph(ctx, prune); err != nil || retry != pruned {
			t.Fatalf("prune retry=%+v %v", retry, err)
		}
		if _, err := s.e.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "host-space", ID: left.ID}); err != nil {
			t.Fatal("prune removed ancestor", err)
		}
	}
	check()
	beforeDenied := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("advance-pruned"), Command: graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 3, Head: right.ID}}})
	if !errors.Is(err, graph.ErrBranchPruned) || out.Code != api.CodeRejected {
		t.Fatalf("pruned work accepted=%+v %v", out, err)
	}
	if integrationSnapshot(t, s.e) != beforeDenied {
		t.Fatal("rejected pruned work changed roots")
	}
	s.reopen()
	check()
	if snapshot := integrationSnapshot(t, s.e); snapshot != beforeDenied {
		t.Fatal("recovery changed joined roots or receipts")
	}
}

func TestGraphJoinConflictsRequireExactResolutionsAndBothCASGuards(t *testing.T) {
	ctx := context.Background()
	s, _, left, right, current := graphDivergence(t, true)
	before := integrationSnapshot(t, s.e)
	command := graph.JoinCommand{Branch: "main", Expected: 1, Left: left.ID, Right: right.ID, ExpectedDomainRoot: current}
	out, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("unresolved"), Command: graph.Command{Join: &command}})
	if !errors.Is(err, graph.ErrJoinConflict) || out.Code != api.CodeRejected {
		t.Fatalf("unresolved=%+v %v", out, err)
	}
	command.Resolutions = []graph.Resolution{{Key: []byte("not-a-conflict"), Value: left.Entries[0].Value}}
	out, err = s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("non-conflict"), Command: graph.Command{Join: &command}})
	if !errors.Is(err, graph.ErrJoinConflict) || out.Code != api.CodeRejected {
		t.Fatalf("arbitrary patch=%+v %v", out, err)
	}
	command.Resolutions = []graph.Resolution{{Key: []byte("a"), Value: model.ObjectID(integrationID('e'))}}
	out, err = s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("unproven-value"), Command: graph.Command{Join: &command}})
	if !errors.Is(err, graph.ErrStateUnverified) || out.Code != api.CodeRejected {
		t.Fatalf("unproven=%+v %v", out, err)
	}
	command.Resolutions = []graph.Resolution{{Key: []byte("a"), Value: left.Entries[0].Value}}
	command.ExpectedDomainRoot = ""
	out, err = s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("stale-domain"), Command: graph.Command{Join: &command}})
	if err != nil || out.Code != api.CodeConflict {
		t.Fatalf("domain CAS=%+v %v", out, err)
	}
	command.ExpectedDomainRoot = current
	command.Expected = 0
	out, err = s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("stale-branch"), Command: graph.Command{Join: &command}})
	if err != nil || out.Code != api.CodeConflict {
		t.Fatalf("branch CAS=%+v %v", out, err)
	}
	if integrationSnapshot(t, s.e) != before {
		t.Fatal("rejections changed publication")
	}
	command.Expected = 1
	out, err = s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("resolved"), Command: graph.Command{Join: &command}})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("resolved=%+v %v", out, err)
	}
	got, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("a")})
	if err != nil || string(got.Value.Data) != "left" {
		t.Fatalf("wrong resolution=%+v %v", got, err)
	}
}

func TestGraphAnchoredDiffTraceAndPlanDoNotReadLatest(t *testing.T) {
	ctx := context.Background()
	s, base, left, right, current := graphDivergence(t, false)
	first, err := s.e.DiffGraph(ctx, api.DiffGraphRequest{Space: "host-space", From: left.ID, To: right.ID, Limit: 1})
	if err != nil || len(first.Diff.Changes) != 1 || !first.Truncated || first.Continuation == "" {
		t.Fatalf("first diff=%+v %v", first, err)
	}
	graphCoreApply(t, s.e, "advance-live-metadata", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 1, Head: left.ID}})
	second, err := s.e.DiffGraph(ctx, api.DiffGraphRequest{Space: "host-space", Anchor: &first.Anchor, From: left.ID, To: right.ID, Limit: 1, Continuation: first.Continuation})
	if err != nil || len(second.Diff.Changes) != 1 || second.Truncated || first.Diff.Changes[0].Kind != graph.ChangeRemove || second.Diff.Changes[0].Kind != graph.ChangeAdd {
		t.Fatalf("second diff=%+v %v", second, err)
	}
	if _, err = s.e.DiffGraph(ctx, api.DiffGraphRequest{Space: "host-space", Anchor: &first.Anchor, From: right.ID, To: left.ID, Limit: 1, Continuation: first.Continuation}); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatalf("rebound page accepted=%v", err)
	}
	plan, err := s.e.PlanGraphJoin(ctx, api.PlanGraphJoinRequest{Space: "host-space", Anchor: &first.Anchor, Left: left.ID, Right: right.ID})
	if err != nil || plan.Anchor.Fence != first.Anchor.Fence || plan.Plan.Base != base.ID {
		t.Fatalf("historical plan=%+v %v", plan, err)
	}
	trace, err := s.e.TraceGraph(ctx, api.TraceGraphRequest{Space: "host-space", Anchor: &first.Anchor, Start: left.ID, Limit: 1})
	if err != nil || len(trace.Nodes) != 1 || trace.Nodes[0].ID != left.ID || trace.Continuation == "" {
		t.Fatalf("trace=%+v %v", trace, err)
	}
	next, err := s.e.TraceGraph(ctx, api.TraceGraphRequest{Space: "host-space", Anchor: &first.Anchor, Start: left.ID, Limit: 1, Continuation: trace.Continuation})
	if err != nil || len(next.Nodes) != 1 || next.Nodes[0].ID != base.ID || next.Truncated {
		t.Fatalf("trace page=%+v %v", next, err)
	}
	back, err := s.e.TraceGraph(ctx, api.TraceGraphRequest{Space: "host-space", Anchor: &first.Anchor, Start: base.ID, Direction: "descendants"})
	if err != nil || len(back.Nodes) != 3 {
		t.Fatalf("descendants=%+v %v", back, err)
	}
	oldBranch, err := s.e.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "host-space", ID: "main", Anchor: &first.Anchor})
	if err != nil || oldBranch.Value.Version != 1 {
		t.Fatalf("old branch=%+v %v", oldBranch, err)
	}
	join := graph.JoinCommand{Branch: "main", Expected: 2, Left: left.ID, Right: right.ID, ExpectedDomainRoot: current}
	graphCoreApply(t, s.e, "joined-after-query", graph.Command{Join: &join})
	s.reopen()
	again, err := s.e.DiffGraph(ctx, api.DiffGraphRequest{Space: "host-space", Anchor: &first.Anchor, From: left.ID, To: right.ID, Limit: 1})
	if err != nil || !reflect.DeepEqual(again, first) {
		t.Fatalf("reopen changed query=%+v %v", again, err)
	}
}
