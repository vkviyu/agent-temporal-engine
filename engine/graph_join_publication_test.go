package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func graphCoordinatorCurrentRevision(t *testing.T, e *Engine, id string, parent model.RevisionID) graph.Revision {
	t.Helper()
	ctx := context.Background()
	v, err := e.OpenView(ctx, api.OpenViewRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	xs := make([]graph.Entry, len(entries.Items))
	for i, x := range entries.Items {
		xs[i] = graph.Entry{Key: x.Key, Value: x.Value}
	}
	r, err := graph.BuildRevision("coordinator-space", parent, nil, entries.StateRoot, nil, xs)
	if err != nil {
		t.Fatal(err)
	}
	req := graphCoordinatorRequest(t, id, graph.Command{AddRevision: &graph.AddRevisionCommand{Parent: parent, Root: entries.StateRoot, Entries: xs}})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, req)))
	return r
}

func graphCoordinatorJoinFixture(t *testing.T, e *Engine) (api.CommitRequest, model.RevisionID) {
	t.Helper()
	ctx := context.Background()
	base := graphCoordinatorCurrentRevision(t, e, "join-fault-base", "")
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, graphCoordinatorRequest(t, "join-fault-main", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: base.ID}}))))
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, "join-fault-left-state", "a", "left", api.ValueCondition{}))))
	left := graphCoordinatorCurrentRevision(t, e, "join-fault-left", base.ID)
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, graphCoordinatorRequest(t, "join-fault-left-head", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: left.ID}}))))
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, "join-fault-right-state", "a", "right", api.ValueCondition{}))))
	right := graphCoordinatorCurrentRevision(t, e, "join-fault-right", base.ID)
	c := graph.JoinCommand{Branch: "main", Expected: 1, Left: left.ID, Right: right.ID, ExpectedDomainRoot: right.StateRoot, Resolutions: []graph.Resolution{{Key: []byte("a"), Value: left.Entries[0].Value}}}
	return graphCoordinatorRequest(t, "join-fault-commit", graph.Command{Join: &c}), left.ID
}

func TestGraphJoinUnknownPublicationKeepsDomainAndBranchAtomic(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			o := Options{Batch: BatchOptions{MaxCount: 1}}
			e, disk := coordinatorStore(t, o)
			req, left := graphCoordinatorJoinFixture(t, e)
			before, err := e.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			objects := e.index.count()
			gate := newCoordinatorGate()
			gate.err = errors.New("join CURRENT response unavailable")
			gate.after = after
			disk.arm("current", gate)
			pending := coordinatorCommit(e, context.Background(), req)
			coordinatorWait(t, gate.entered)
			gate.unblock()
			unknown := coordinatorWait(t, pending)
			if unknown.result.Code != api.CodeCommitUnknown || !errors.Is(unknown.err, storage.ErrCommitUnknown) {
				t.Fatalf("not unknown: %+v %v", unknown.result, unknown.err)
			}
			e.mu.Lock()
			unchanged := e.roots == before.Roots && e.rootSet == before.RootSet && e.index.count() == objects
			e.mu.Unlock()
			if !unchanged {
				t.Fatal("uncertain Join installed partial roots")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			o.Journal.TailPolicy = storage.TailQuarantine
			opened, _, err := OpenExisting(disk, o)
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			branch, err := opened.GetGraphBranch(context.Background(), api.ReadGraphBranchRequest{Space: "coordinator-space", ID: "main"})
			if err != nil {
				t.Fatal(err)
			}
			value, err := opened.GetState(context.Background(), api.ReadStateRequest{Space: "coordinator-space", Key: []byte("a")})
			if err != nil {
				t.Fatal(err)
			}
			lookup := coordinatorLookup(t, opened, req)
			if after {
				if branch.Value.Head == left || branch.Value.Version != 2 || string(value.Value.Data) != "left" || lookup.Code != api.CodeDurableAck {
					t.Fatalf("torn committed join: branch=%+v value=%+v lookup=%+v", branch, value, lookup)
				}
				again := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(opened, context.Background(), req)))
				if again != lookup.Result {
					t.Fatal("recovered Join receipt changed")
				}
			} else {
				if branch.Value.Head != left || branch.Value.Version != 1 || string(value.Value.Data) != "right" || lookup.Code != api.CodeNotFoundAtFence {
					t.Fatalf("promoted uncommitted join: branch=%+v value=%+v lookup=%+v", branch, value, lookup)
				}
			}
		})
	}
}

func TestGraphJoinThenPruneUsesOneGroupAndKeepsPerTransactionReceipts(t *testing.T) {
	e, disk := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 2, MaxDelay: time.Second}})
	joinReq, _ := graphCoordinatorJoinFixture(t, e)
	// Compute the immutable joined identity from the same pure plan. No output
	// state is trusted by the production command; this is only the next intent.
	_, join, err := graph.DecodeCommand(joinReq.Body)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	g, _, err := loadGraphParents(context.Background(), e.index, e.roots, joinReq.Space, []model.RevisionID{join.Join.Left, join.Join.Right})
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := g.PlanJoin(join.Join.Left, join.Join.Right)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range join.Join.Resolutions {
		if err = plan.Resolve(r.Key, r.Value); err != nil {
			t.Fatal(err)
		}
	}
	left, err := g.Get(join.Join.Left)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := g.PreviewJoin(plan, left.StateRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	pruneReq := graphCoordinatorRequest(t, "prune-staged-join", graph.Command{Prune: &graph.PruneBranchCommand{Branch: "main", Expected: 2, Head: joined.ID, Reason: "prune joined branch"}})
	_, _, before, _ := disk.counts()
	gate := newCoordinatorGate()
	disk.arm("current", gate)
	one, two, oneFlight, twoFlight := speculativePair(t, e, joinReq, pruneReq)
	coordinatorWait(t, gate.entered)
	speculativePending(t, oneFlight)
	speculativePending(t, twoFlight)
	gate.unblock()
	a := coordinatorAck(t, coordinatorWait(t, one))
	b := coordinatorAck(t, coordinatorWait(t, two))
	_, _, after, _ := disk.counts()
	if after-before != 1 || b.Fence.LSN != a.Fence.LSN+1 {
		t.Fatalf("group split: %+v %+v count=%d", a, b, after-before)
	}
	branch, err := e.GetGraphBranch(context.Background(), api.ReadGraphBranchRequest{Space: "coordinator-space", ID: "main"})
	if err != nil || !branch.Pruned || branch.Value.Version != 3 {
		t.Fatalf("prune did not see staged Join: %+v %v", branch, err)
	}
	if lookup := coordinatorLookup(t, e, joinReq); lookup.Code != api.CodeDurableAck || lookup.Result != a {
		t.Fatal("Join lost original group proof")
	}
}
