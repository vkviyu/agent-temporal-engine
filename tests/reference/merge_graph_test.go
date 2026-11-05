package reference_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentMergeBaseAllFiveNodeDAGsIncludeCrissCross(t *testing.T) {
	const n = 5
	for mask := 0; mask < 1<<(n*(n-1)/2); mask++ {
		want := oracle.NewRevisionDAG()
		actual := graph.New()
		ids := map[string]model.RevisionID{}
		names := map[model.RevisionID]string{}
		bit := 0
		for i := 0; i < n; i++ {
			name := fmt.Sprint(i)
			parents := []string{}
			for j := 0; j < i; j++ {
				if mask&(1<<bit) != 0 {
					parents = append(parents, fmt.Sprint(j))
				}
				bit++
			}
			want.Add(name, "space", parents...)
			var parent model.RevisionID
			joins := []model.RevisionID{}
			for j, p := range parents {
				if j == 0 {
					parent = ids[p]
				} else {
					joins = append(joins, ids[p])
				}
			}
			rev, err := actual.AddRevision("space", parent, joins, "", []model.EventID{model.EventID(fmt.Sprintf("%064x", i+1))}, nil)
			if err != nil {
				t.Fatal(err)
			}
			ids[name], names[rev.ID] = rev.ID, name
		}
		relation := want.Relation()
		for l := 0; l < n; l++ {
			for r := 0; r < n; r++ {
				left, right := fmt.Sprint(l), fmt.Sprint(r)
				base, werr := want.MergeBase(left, right)
				got, gerr := actual.MergeBase(ids[left], ids[right])
				switch {
				case werr == nil:
					if gerr != nil || names[got] != base {
						t.Fatalf("DAG %x %s/%s base=%s expected=%s err=%v", mask, left, right, names[got], base, gerr)
					}
				case errors.Is(werr, oracle.ErrNoBase):
					if !errors.Is(gerr, graph.ErrNoMergeBase) {
						t.Fatalf("DAG %x no-base mismatch: %v", mask, gerr)
					}
				case errors.Is(werr, oracle.ErrManyBases):
					if !errors.Is(gerr, graph.ErrAmbiguousMergeBase) {
						t.Fatalf("DAG %x ambiguous-base mismatch: %v", mask, gerr)
					}
				}
				ancestor, err := actual.IsAncestor(ids[left], ids[right])
				if err != nil || ancestor != relation[[2]string{right, left}] {
					t.Fatalf("DAG %x ancestry differs: %v", mask, err)
				}
			}
		}
	}
}

func TestIndependentBranchCASJoinPruneAndHistoricalReceipts(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := ate.Create(path, ate.Options{AgentProfile: true, EventProfile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	ack := func(r api.CommitResult, e error) api.CommitResult {
		t.Helper()
		if e != nil || r.Code != api.CodeDurableAck {
			t.Fatalf("%+v %v", r, e)
		}
		return r
	}
	trace := oracle.NewEventTrace()
	ids := map[string]model.RevisionID{}
	names := map[model.RevisionID]string{}
	roots := map[string]model.ObjectID{}
	values := map[string]model.ObjectID{}
	revision := func(name, parent string) {
		t.Helper()
		stateAck := ack(store.Mutate(ctx, api.MutateRequest{Key: key("state-" + name), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("value"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte(name)}}}}}))
		trace.Publish(string(stateAck.TxID), api.MethodMutateState, "space", "owner", stateAck.Fence.LSN)
		view, e := store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "owner"})
		if e != nil {
			t.Fatal(e)
		}
		entries, e := store.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
		if e != nil || len(entries.Items) != 1 {
			t.Fatal(e)
		}
		_ = store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		roots[name] = entries.StateRoot
		values[name] = entries.Items[0].Value
		gentries := []graph.Entry{{Key: entries.Items[0].Key, Value: entries.Items[0].Value}}
		r, e := graph.BuildRevision("space", ids[parent], nil, entries.StateRoot, nil, gentries)
		if e != nil {
			t.Fatal(e)
		}
		revisionAck := ack(store.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key("revision-" + name), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{Parent: ids[parent], Root: entries.StateRoot, Entries: gentries}}}))
		causes := []string{}
		if parent != "" {
			causes = append(causes, "revision/"+parent)
		}
		trace.Publish(string(revisionAck.TxID), api.MethodApplyGraph, "space", "owner", revisionAck.Fence.LSN, oracle.TypedFact{Key: "revision/" + name, Operation: "Graph.AddRevision", Profile: "graph", Kind: "Revision", Subject: string(r.ID), Causes: causes})
		ids[name], names[r.ID] = r.ID, name
	}
	revision("base", "")
	revision("left", "base")
	revision("right", "base")
	book := oracle.NewBranchBook(map[string]string{"base": "space", "left": "space", "right": "space", "joined": "space"})
	recorded := map[string]api.CommitResult{}
	requests := map[string]api.ApplyGraphRequest{}
	compare := func(label string, anchor *model.SnapshotAnchor, expected oracle.BranchProjection) {
		t.Helper()
		got, e := store.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "space", ID: "main", Anchor: anchor})
		if e != nil || !got.Found {
			t.Fatalf("%s branch missing: %v", label, e)
		}
		actual := oracle.BranchProjection{Head: names[got.Value.Head], Version: got.Value.Version, Pruned: got.Pruned}
		if actual != expected {
			t.Fatalf("%s branch=%+v expected=%+v", label, actual, expected)
		}
		if got.Pruned && (got.Prune == nil || got.Prune.Head != got.Value.Head || got.Prune.Version != got.Value.Version) {
			t.Fatal("prune lost exact retained head/version")
		}
	}
	apply := func(label string, expected error, c graph.Command) api.CommitResult {
		t.Helper()
		r := api.ApplyGraphRequest{Key: key(label), Command: c}
		before, _ := store.Snapshot(ctx)
		out, e := store.ApplyGraph(ctx, r)
		after, _ := store.Snapshot(ctx)
		if (expected == nil) != (e == nil && out.Code == api.CodeDurableAck) {
			t.Fatalf("%s oracle=%v actual=%+v/%v", label, expected, out, e)
		}
		if expected != nil && before != after {
			t.Fatal("failed branch transition changed roots")
		}
		if expected == nil {
			requests[label], recorded[label] = r, out
		}
		return out
	}
	fork := graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: ids["base"]}}
	forkAck := apply("fork", book.Fork("main", "base"), fork)
	trace.Publish(string(forkAck.TxID), api.MethodApplyGraph, "space", "owner", forkAck.Fence.LSN, oracle.TypedFact{Key: "branch/fork", Operation: "Graph.Fork", Profile: "graph", Kind: "Branch", Subject: "main", Causes: []string{"revision/base"}})
	forkState := book.Branches["main"]
	apply("duplicate-fork", book.Fork("main", "base"), fork)
	advance := graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: ids["left"]}}
	advanceAck := apply("advance-left", book.Advance("main", 0, "left"), advance)
	trace.Publish(string(advanceAck.TxID), api.MethodApplyGraph, "space", "owner", advanceAck.Fence.LSN, oracle.TypedFact{Key: "branch/advance", Operation: "Graph.Advance", Profile: "graph", Kind: "Branch", Subject: "main", Causes: []string{"revision/left"}})
	apply("stale-rival", book.Advance("main", 0, "right"), graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: ids["right"]}})
	compare("one CAS winner", nil, book.Branches["main"])
	// The merge has two independent preconditions. A failed branch or domain CAS
	// cannot publish the candidate's data or consume its RequestKey.
	domain := "right"
	joinOracle := func(expected uint64, expectedDomain string) error {
		old := book.Branches["main"]
		if old.Version != expected || old.Head != "left" || old.Pruned || domain != expectedDomain {
			return oracle.ErrRejected
		}
		chosen := oracle.Merge(map[string]string{"value": "base"}, map[string]string{"value": "left"}, map[string]string{"value": "right"})
		if err := chosen.Resolve("value", "left"); err != nil {
			return err
		}
		if !reflect.DeepEqual(chosen.State, map[string]string{"value": "left"}) {
			return oracle.ErrRejected
		}
		if err := book.Advance("main", expected, "joined"); err != nil {
			return err
		}
		domain = "left"
		return nil
	}
	command := graph.JoinCommand{Branch: "main", Expected: 1, Left: ids["left"], Right: ids["right"], ExpectedDomainRoot: roots["right"], Resolutions: []graph.Resolution{{Key: []byte("value"), Value: values["left"]}}}
	bad := command
	bad.Expected = 0
	apply("join-stale-branch", joinOracle(0, "right"), graph.Command{Join: &bad})
	bad = command
	bad.ExpectedDomainRoot = roots["base"]
	apply("join-stale-domain", joinOracle(1, "base"), graph.Command{Join: &bad})
	before, _ := store.Snapshot(ctx)
	joinAck := apply("join", joinOracle(1, "right"), graph.Command{Join: &command})
	after, _ := store.Snapshot(ctx)
	if after.Roots.BranchRefs == before.Roots.BranchRefs || after.Roots.DomainStates == before.Roots.DomainStates || after.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("join did not atomically publish data and branch")
	}
	joined, e := store.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "space", ID: "main"})
	if e != nil {
		t.Fatal(e)
	}
	ids["joined"], names[joined.Value.Head] = joined.Value.Head, "joined"
	trace.Publish(string(joinAck.TxID), api.MethodApplyGraph, "space", "owner", joinAck.Fence.LSN,
		oracle.TypedFact{Key: "revision/joined", Operation: "Graph.JoinRevision", Profile: "graph", Kind: "Revision", Subject: string(joined.Value.Head), Causes: []string{"revision/left", "revision/right"}},
		oracle.TypedFact{Key: "branch/join", Operation: "Graph.Join", Profile: "graph", Kind: "Branch", Subject: "main", Causes: []string{"revision/joined"}})

	compare("joined", nil, book.Branches["main"])
	joinedState := book.Branches["main"]
	state, e := store.GetState(ctx, api.ReadStateRequest{Space: "space", Key: []byte("value")})
	if e != nil || string(state.Value.Data) != domain {
		t.Fatal("chosen merge data differs", e)
	}
	apply("wrong-prune-head", book.Prune("main", 2, "right"), graph.Command{Prune: &graph.PruneBranchCommand{Branch: "main", Expected: 2, Head: ids["right"], Reason: "done"}})
	pruneAck := apply("prune", book.Prune("main", 2, "joined"), graph.Command{Prune: &graph.PruneBranchCommand{Branch: "main", Expected: 2, Head: ids["joined"], Reason: "done"}})
	trace.Publish(string(pruneAck.TxID), api.MethodApplyGraph, "space", "owner", pruneAck.Fence.LSN, oracle.TypedFact{Key: "branch/prune", Operation: "Graph.Prune", Profile: "graph", Kind: "Branch", Subject: "main", Causes: []string{"revision/joined"}})
	apply("advance-pruned", book.Advance("main", 3, "right"), graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 3, Head: ids["right"]}})
	apply("reuse-pruned-name", book.Fork("main", "base"), fork)
	check := func() {
		t.Helper()
		compare("retained tombstone", nil, book.Branches["main"])
		tip, _ := store.Snapshot(ctx)
		compareIndependentEventTrace(t, store, trace, "space", "owner", tip.Fence)
		compareIndependentEventTrace(t, store, trace, "space", "owner", forkAck.Fence)
		a := model.SnapshotAnchor{Space: "space", Fence: forkAck.Fence}
		compare("historical fork", &a, forkState)
		a.Fence = joinAck.Fence
		compare("historical joined", &a, joinedState)
		for _, name := range []string{"base", "left", "right", "joined"} {
			r, e := store.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "space", ID: ids[name]})
			if e != nil || !r.Found {
				t.Fatal("prune removed revision", name, e)
			}
		}
		for label, request := range requests {
			got, e := store.ApplyGraph(ctx, request)
			if e != nil || got != recorded[label] {
				t.Fatalf("%s original receipt changed after later branch transitions: %+v/%v", label, got, e)
			}
		}
	}
	check()
	if _, e := store.CompactAuditFull(ctx); e != nil {
		t.Fatal(e)
	}
	if e := store.Close(); e != nil {
		t.Fatal(e)
	}
	store, _, err = ate.OpenExisting(path, ate.Options{AgentProfile: true, EventProfile: true})
	if err != nil {
		t.Fatal(err)
	}
	check()
}
