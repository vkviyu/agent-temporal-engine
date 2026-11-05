package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func graphCoreKey(id string) model.RequestKey {
	return model.RequestKey{Space: "host-space", Principal: "host-principal", RequestID: model.RequestID(id)}
}
func graphCoreApply(t *testing.T, e *engine.Engine, id string, c graph.Command) api.CommitResult {
	t.Helper()
	r, err := e.ApplyGraph(context.Background(), api.ApplyGraphRequest{Key: graphCoreKey(id), Command: c})
	if err != nil || r.Code != api.CodeDurableAck {
		t.Fatalf("%s: %+v %v", id, r, err)
	}
	return r
}

func TestGraphCoreCanonicalIntentReceiptsAndReopen(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	r, err := graph.BuildRevision("host-space", "", nil, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	add := graph.Command{AddRevision: &graph.AddRevisionCommand{}}
	a := graphCoreApply(t, s.e, "graph-root", add)
	snap := integrationSnapshot(t, s.e)
	if snap.Roots.BranchRefs == "" || snap.Roots.DomainStates != "" {
		t.Fatalf("wrong root ownership: %+v", snap.Roots)
	}
	graphCoreApply(t, s.e, "graph-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r.ID}})
	x := api.ApplyGraphRequest{Key: graphCoreKey("graph-advance"), Command: graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: r.ID}}}
	ack, err := s.e.ApplyGraph(ctx, x)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("advance: %+v %v", ack, err)
	}
	graphCoreApply(t, s.e, "graph-later-advance", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 1, Head: r.ID}})
	if again, err := s.e.ApplyGraph(ctx, x); err != nil || again != ack {
		t.Fatalf("exact retry rechecked latest CAS: %+v %v", again, err)
	}
	s.reopen()
	if again, err := s.e.ApplyGraph(ctx, x); err != nil || again != ack {
		t.Fatalf("reopened retry changed receipt: %+v %v", again, err)
	}
	if again, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("graph-root"), Command: add}); err != nil || again != a {
		t.Fatalf("Add retry changed receipt: %+v %v", again, err)
	}
	x.Command.Advance.Expected = 2
	if conflict, err := s.e.ApplyGraph(ctx, x); err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
		t.Fatalf("changed intent reused key: %+v %v", conflict, err)
	}
}

func TestGraphCoreRejectsRawMutationAndMissingRevisionWithoutFencing(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	r, _ := graph.BuildRevision("host-space", "", nil, "", nil, nil)
	graphCoreApply(t, s.e, "root", graph.Command{AddRevision: &graph.AddRevisionCommand{}})
	graphCoreApply(t, s.e, "fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r.ID}})
	before := integrationSnapshot(t, s.e)
	body, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte("arbitrary/not-graph"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("bad graph")}}}})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := s.e.Commit(ctx, api.CommitRequest{Space: "host-space", Principal: "host-principal", RequestID: "raw-bypass", Method: api.MethodApplyGraph, Body: body})
	if err == nil || bad.Code != api.CodeRejected {
		t.Fatalf("raw graph mutation accepted: %+v %v", bad, err)
	}
	ghost := model.RevisionID(strings.Repeat("a", 64))
	cases := []graph.Command{
		{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: ghost}},
		{Fork: &graph.ForkCommand{Branch: "ghost", Source: ghost}},
		{AddRevision: &graph.AddRevisionCommand{Parent: ghost}},
		{AddRevision: &graph.AddRevisionCommand{Root: model.ObjectID(ghost)}},
	}
	for i, c := range cases {
		key := graphCoreKey(modelID(i))
		out, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key, Command: c})
		if err == nil || out.Code != api.CodeRejected {
			t.Fatalf("case %d: %+v %v", i, out, err)
		}
	}
	after := integrationSnapshot(t, s.e)
	if before != after {
		t.Fatalf("rejection changed publication: before=%+v after=%+v", before, after)
	}
	graphCoreApply(t, s.e, "still-ready", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: r.ID}})
}

func modelID(i int) string { return "invalid-graph-" + string(rune('a'+i)) }

func TestGraphCoreAuthenticatesHistoricalDomainSnapshot(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	one := integrationMutate(t, s.e, integrationRequest("domain-1", integrationPut([]byte("key"), api.Value{Kind: api.ValueBytes, Data: []byte("old")})))
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	integrationMutate(t, s.e, integrationRequest("domain-2", integrationPut([]byte("key"), api.Value{Kind: api.ValueBytes, Data: []byte("new")})))
	es := make([]graph.Entry, len(entries.Items))
	for i, v := range entries.Items {
		es[i] = graph.Entry{Key: v.Key, Value: v.Value}
	}
	cmd := graph.Command{AddRevision: &graph.AddRevisionCommand{Root: one.StateRoot, Entries: es}}
	ack := graphCoreApply(t, s.e, "historical-revision", cmd)
	cmd.AddRevision.Entries[0].Value = model.ObjectID(strings.Repeat("b", 64))
	bad, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("forged-entries"), Command: cmd})
	if err == nil || bad.Code != api.CodeRejected {
		t.Fatalf("forged snapshot accepted: %+v %v", bad, err)
	}
	s.reopen()
	lookup, err := s.e.LookupRequest(ctx, api.LookupRequestRequest{Space: "host-space", Principal: "host-principal", RequestID: "historical-revision"})
	if err != nil || lookup.Result != ack {
		t.Fatalf("replay lost authenticated revision: %+v %v", lookup, err)
	}
}
