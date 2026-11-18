package service

import (
	"cmp"
	"context"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func domainKey(id string) model.RequestKey {
	return model.RequestKey{Space: "s", RequestID: model.RequestID(id)}
}
func domainAck(t *testing.T, ack api.CommitResult, err error) api.CommitResult {
	t.Helper()
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("ack=%+v err=%v", ack, err)
	}
	return ack
}
func domainObservation(id model.ObjectID, source model.CommitFence, key model.KnowledgeKey, sequence uint64) knowledge.Observation {
	o := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "s", Fence: source}}, Payload: id, Sequence: sequence, Label: "read evidence"}
	o.ID = knowledge.ObservationID(o)
	return o
}

func TestDomainServiceSDKCanonicalCommandsRecoveryAndKeyIsolation(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) { runDomainConformance(t, transport) })
	}
}

func runDomainConformance(t *testing.T, transport string) {
	t.Helper()
	ctx := context.Background()
	core, path := nativeAgentStore(t, true)
	server, owner, _ := conformanceClients(t, core, transport)
	caps := map[string]bool{}
	for _, c := range owner.Identity().Capabilities {
		caps[c] = true
	}
	for _, c := range []string{"graph/v1", "memory/v1", "knowledge/v1"} {
		if !caps[c] {
			t.Fatalf("missing capability %s", c)
		}
	}
	ack, err := owner.ApplyAgent(ctx, agentCreateRequest("domain-task"))
	domainAck(t, ack, err)
	for _, id := range []model.ExecutionID{"one", "two"} {
		x := agentprofile.Execution{ID: id, Task: "t", Actor: "same-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
		ack, err = owner.ApplyAgent(ctx, api.ApplyAgentRequest{Key: domainKey("spawn-" + string(id)), Command: agentprofile.Command{Spawn: &x}})
		domainAck(t, ack, err)
	}
	sourceAck, err := owner.Mutate(ctx, api.MutateRequest{Key: domainKey("domain-source"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("source"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("fact evidence")}}}}})
	domainAck(t, sourceAck, err)
	view, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := owner.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("entries=%+v %v", entries, err)
	}
	if err = owner.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	payload := entries.Items[0].Value
	graphEntries := []graph.Entry{{Key: entries.Items[0].Key, Value: payload}}
	r0, err := graph.BuildRevision("s", "", nil, sourceAck.StateRoot, nil, graphEntries)
	if err != nil {
		t.Fatal(err)
	}
	graphRequest := api.ApplyGraphRequest{Key: domainKey("graph-add"), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{Root: sourceAck.StateRoot, Entries: graphEntries}}}
	graphAck, err := owner.ApplyGraph(ctx, graphRequest)
	domainAck(t, graphAck, err)
	forkRequest := api.ApplyGraphRequest{Key: domainKey("graph-fork"), Command: graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r0.ID}}, ExpectedFence: &graphAck.Fence}
	forkAck, err := owner.ApplyGraph(ctx, forkRequest)
	domainAck(t, forkAck, err)
	r1, err := graph.BuildRevision("s", r0.ID, nil, sourceAck.StateRoot, nil, graphEntries)
	if err != nil {
		t.Fatal(err)
	}
	ack, err = owner.ApplyGraph(ctx, api.ApplyGraphRequest{Key: domainKey("graph-child"), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{Parent: r0.ID, Root: sourceAck.StateRoot, Entries: graphEntries}}})
	domainAck(t, ack, err)
	ack, err = owner.ApplyGraph(ctx, api.ApplyGraphRequest{Key: domainKey("graph-advance"), Command: graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: r1.ID}}})
	domainAck(t, ack, err)
	memorySource := memory.Source{SourceID: payload, Anchor: model.SnapshotAnchor{Space: "s", Fence: sourceAck.Fence}}
	m := memory.Memory{Space: "s", Kind: memory.KindFact, Claim: []byte("remember the verified evidence"), Sources: []memory.Source{memorySource}}
	memoryRequest := api.ApplyMemoryRequest{Key: domainKey("memory-candidate"), Command: memory.Command{PutCandidate: &m}}
	memoryAck, err := owner.ApplyMemory(ctx, memoryRequest)
	domainAck(t, memoryAck, err)
	memoryState, err := owner.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "s"})
	if err != nil || len(memoryState.Snapshot.Memories) != 1 {
		t.Fatalf("memory=%+v %v", memoryState, err)
	}
	id := memoryState.Snapshot.Memories[0].ID
	ack, err = owner.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: domainKey("memory-validate"), Command: memory.Command{Validate: &memory.ValidateCommand{ID: id, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{memorySource}}}}})
	domainAck(t, ack, err)
	memoryState, err = owner.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range memoryState.Snapshot.Memories {
		if v.Status == memory.StatusValidated {
			id = v.ID
		}
	}
	ack, err = owner.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: domainKey("memory-promote"), Command: memory.Command{Promote: &id}})
	domainAck(t, ack, err)
	key := model.KnowledgeKey{Actor: "same-actor", Execution: "one"}
	o := domainObservation(payload, sourceAck.Fence, key, 1)
	knowledgeRequest := api.ApplyKnowledgeRequest{Key: domainKey("knowledge-one"), Command: knowledge.Command{Observation: &o}}
	knowledgeAck, err := owner.ApplyKnowledge(ctx, knowledgeRequest)
	domainAck(t, knowledgeAck, err)
	past := model.SnapshotAnchor{Space: "s", Fence: knowledgeAck.Fence, Knowledge: &key}
	otherKey := key
	otherKey.Execution = "two"
	for i, k := range []model.KnowledgeKey{otherKey, key} {
		sequence := uint64(1)
		if k == key {
			sequence = 2
		}
		obs := domainObservation(payload, sourceAck.Fence, k, sequence)
		ack, err = owner.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: domainKey("observe-next-" + string(rune('a'+i))), Command: knowledge.Command{Observation: &obs}})
		domainAck(t, ack, err)
	}
	seedSource := past.Clone()
	chain, err := knowledge.Revisions(knowledge.Snapshot{Observations: []knowledge.Observation{o}}, "s", key)
	if err != nil {
		t.Fatal(err)
	}
	seedSource.Cursor = &model.Cursor{Store: past.Fence.Store, Space: "s", Revision: chain[1].ID}
	seed := knowledge.ImportCommand{Target: otherKey, Source: knowledge.SourceAnchor{Anchor: seedSource}, ExpectedSequence: 1, Observations: []model.ObjectID{o.ID}}
	seedRequest := api.ApplyKnowledgeRequest{Key: domainKey("explicit-wire-knowledge-import"), Command: knowledge.Command{Import: &seed}}
	seedAck, err := owner.ApplyKnowledge(ctx, seedRequest)
	domainAck(t, seedAck, err)
	check := func(store *ate.Store, c *client.Client) {
		t.Helper()
		revisionRead := api.ReadGraphRevisionRequest{Space: "s", ID: r1.ID}
		assertReadEqual(t, func() (api.GraphRevisionResult, error) { return store.GetGraphRevision(ctx, revisionRead) },
			func() (api.GraphRevisionResult, error) { return c.GetGraphRevision(ctx, revisionRead) })
		for _, anchor := range []*model.SnapshotAnchor{nil, {Space: "s", Fence: forkAck.Fence}} {
			branchRead := api.ReadGraphBranchRequest{Space: "s", ID: "main", Anchor: anchor}
			assertReadEqual(t, func() (api.GraphBranchResult, error) { return store.GetGraphBranch(ctx, branchRead) },
				func() (api.GraphBranchResult, error) { return c.GetGraphBranch(ctx, branchRead) })
		}
		memoryRead := api.ReadMemoryStateRequest{Space: "s"}
		assertReadEqual(t, func() (api.MemoryStateResult, error) {
			result, err := store.GetMemoryState(ctx, memoryRead)
			return orderedMemoryResult(result), err
		}, func() (api.MemoryStateResult, error) {
			result, err := c.GetMemoryState(ctx, memoryRead)
			return orderedMemoryResult(result), err
		})
		for _, knowledgeRead := range []api.ReadKnowledgeStateRequest{
			{Space: "s", Principal: "owner", Anchor: &past},
			{Space: "s", Principal: "owner", Anchor: &seedSource},
			{Space: "s", Principal: "owner", Knowledge: &key},
			{Space: "s", Principal: "owner", Knowledge: &otherKey},
		} {
			assertReadEqual(t, func() (api.KnowledgeStateResult, error) { return store.GetKnowledgeState(ctx, knowledgeRead) },
				func() (api.KnowledgeStateResult, error) { return c.GetKnowledgeState(ctx, knowledgeRead) })
		}
		got, err := c.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "s", ID: r1.ID})
		if err != nil || !got.Found || got.Value.ID != r1.ID {
			t.Fatalf("revision=%+v %v", got, err)
		}
		branch, err := c.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "s", ID: "main"})
		if err != nil || !branch.Found || branch.Value.Head != r1.ID || branch.Value.Version != 1 {
			t.Fatalf("branch=%+v %v", branch, err)
		}
		mem, err := c.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "s"})
		if err != nil || len(mem.Snapshot.Memories) != 3 {
			t.Fatalf("memory=%+v %v", mem, err)
		}
		historical, err := c.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "s", Anchor: &past})
		if err != nil || historical.Fence != knowledgeAck.Fence || len(historical.Snapshot.Observations) != 1 || historical.Snapshot.Observations[0].Key != key {
			t.Fatalf("past=%+v %v", historical, err)
		}
		latest, err := c.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "s", Knowledge: &key})
		if err != nil || len(latest.Snapshot.Observations) != 2 {
			t.Fatalf("latest=%+v %v", latest, err)
		}
		if historical.Revision == nil || latest.Revision == nil || historical.Revision.Sequence != 1 || latest.Revision.Sequence != 2 {
			t.Fatal("knowledge revision metadata missing from SDK response")
		}
		selected := latest.Anchor.Clone()
		selected.Cursor = &model.Cursor{Store: selected.Fence.Store, Space: "s", Revision: historical.Revision.ID}
		cut, err := c.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "s", Anchor: &selected})
		if err != nil || cut.Revision == nil || *cut.Revision != *historical.Revision || len(cut.Snapshot.Observations) != 1 || !reflect.DeepEqual(cut.Anchor, selected) {
			t.Fatalf("SDK knowledge cursor leaked latest=%+v %v", cut, err)
		}
		view, err := c.OpenView(ctx, api.OpenViewRequest{Space: "s", Mode: "execution_knowledge", Anchor: &selected})
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := c.ListNodes(ctx, api.ListNodesRequest{View: view.Ref, Layer: "content", EntityKind: "Observation"})
		if err != nil || len(nodes.Items) != 1 {
			t.Fatalf("SDK cursor content=%+v %v", nodes, err)
		}
		if err = c.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
			t.Fatal(err)
		}
		seeded, err := c.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "s", Knowledge: &otherKey})
		if err != nil || len(seeded.Snapshot.Observations) != 2 || seeded.Revision == nil || seeded.Revision.Sequence != 2 {
			t.Fatalf("SDK explicit import=%+v %v", seeded, err)
		}
		foundImport := false
		for _, item := range seeded.Snapshot.Observations {
			if item.ImportedFrom == o.ID && reflect.DeepEqual(item.Source.Anchor, seedSource) {
				foundImport = true
			}
		}
		if !foundImport {
			t.Fatal("wire import omitted source provenance")
		}
		if retry, err := c.ApplyKnowledge(ctx, seedRequest); err != nil || !reflect.DeepEqual(retry, seedAck) {
			t.Fatalf("SDK import retry=%+v %v", retry, err)
		}
		retry, err := c.ApplyGraph(ctx, graphRequest)
		if err != nil || !reflect.DeepEqual(retry, graphAck) {
			t.Fatalf("graph retry=%+v %v", retry, err)
		}
		retry, err = c.ApplyGraph(ctx, forkRequest)
		if err != nil || !reflect.DeepEqual(retry, forkAck) {
			t.Fatalf("fork retry=%+v %v", retry, err)
		}
		retry, err = c.ApplyMemory(ctx, memoryRequest)
		if err != nil || !reflect.DeepEqual(retry, memoryAck) {
			t.Fatalf("memory retry=%+v %v", retry, err)
		}
		retry, err = c.ApplyKnowledge(ctx, knowledgeRequest)
		if err != nil || !reflect.DeepEqual(retry, knowledgeAck) {
			t.Fatalf("knowledge retry=%+v %v", retry, err)
		}
	}
	check(core, owner)
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err = server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err = core.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := ate.OpenExisting(path, ate.Options{AgentProfile: true})
	if err != nil || !report.Verified {
		t.Fatalf("reopen=%+v %v", report, err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	_, reopenedClient, _ := conformanceClients(t, reopened, transport)
	check(reopened, reopenedClient)
}

// Snapshot collections are maps projected into slices, not ordered pages.
// Compare every record and proof field while ignoring only collection order.
func orderedMemoryResult(result api.MemoryStateResult) api.MemoryStateResult {
	s := &result.Snapshot
	s.Memories = slices.Clone(s.Memories)
	s.Paths = slices.Clone(s.Paths)
	s.Bans = slices.Clone(s.Bans)
	s.Successors = slices.Clone(s.Successors)
	s.PathSuccessors = slices.Clone(s.PathSuccessors)
	slices.SortFunc(s.Memories, func(a, b memory.Memory) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(s.Paths, func(a, b memory.ExperiencePath) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(s.Bans, func(a, b memory.SourceBan) int { return cmp.Compare(a.Source, b.Source) })
	for _, links := range [][]memory.VersionLink{s.Successors, s.PathSuccessors} {
		slices.SortFunc(links, func(a, b memory.VersionLink) int {
			if c := cmp.Compare(a.Before, b.Before); c != 0 {
				return c
			}
			return cmp.Compare(a.After, b.After)
		})
	}
	return result
}

func TestDomainServiceRejectsPrincipalSpoofScopeAndRawMutationInjection(t *testing.T) {
	ctx := context.Background()
	core, _ := nativeAgentStore(t, true)
	s := newServer(t, core, agentCredentials())
	owner := pipeClient(t, s, "owner-token")
	graphRequest := api.ApplyGraphRequest{Key: domainKey("empty-revision"), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{}}}
	foreign := graphRequest
	foreign.Key.Space = "foreign"
	if _, err := owner.ApplyGraph(ctx, foreign); err == nil {
		t.Fatal("foreign graph write accepted")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
	foreign = graphRequest
	foreign.Key.Principal = "spoof"
	if _, err := owner.ApplyGraph(ctx, foreign); err != client.ErrPrincipal {
		t.Fatalf("principal spoof=%v", err)
	}
	if _, err := owner.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "foreign"}); err == nil {
		t.Fatal("foreign memory readable")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
	if _, err := owner.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "s", Principal: "spoof"}); err != client.ErrPrincipal {
		t.Fatalf("knowledge principal spoof=%v", err)
	}
	before, err := core.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte("forged"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("snapshot")}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{api.MethodApplyGraph, api.MethodApplyMemory, api.MethodApplyKnowledge} {
		ack, err := owner.Commit(ctx, api.CommitRequest{Space: "s", RequestID: model.RequestID("inject-" + method), Method: method, Body: raw})
		if err == nil && ack.Code == api.CodeDurableAck {
			t.Fatalf("raw domain injection accepted for %s", method)
		}
	}
	after, err := core.Snapshot(ctx)
	if err != nil || after != before {
		t.Fatalf("rejection fenced or changed store: %+v %v", after, err)
	}
	ack, err := owner.ApplyGraph(ctx, graphRequest)
	domainAck(t, ack, err)
	// JSON is only transport. Domain command bytes are canonical binary, and a
	// command's authenticated Space cannot disagree with its outer envelope.
	a, b := net.Pipe()
	defer a.Close()
	if !s.acceptConn(b) {
		t.Fatal("connection refused")
	}
	if r := exchange(t, a, "hello-domain", "hello", protocol.HelloRequest{Token: "owner-token"}); r.Error != nil {
		t.Fatal(r.Error)
	}
	wrongSpace, err := graph.EncodeCommand("foreign", graphRequest.Command)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		method string
		body   any
	}{
		{"apply_graph", protocol.ApplyGraphRequest{Space: "s", RequestID: "wrong-space", Command: wrongSpace}},
		{"apply_graph", protocol.ApplyGraphRequest{Space: "s", RequestID: "json-graph", Command: []byte(`{"AddRevision":{}}`)}},
		{"apply_memory", protocol.ApplyMemoryRequest{Space: "s", RequestID: "json-memory", Command: []byte(`{"PutCandidate":{}}`)}},
		{"apply_knowledge", protocol.ApplyKnowledgeRequest{Space: "s", RequestID: "json-knowledge", Command: []byte(`{"Observation":{}}`)}},
		{"apply_graph", map[string]any{"Space": "s", "RequestID": "spoof-wire", "Command": wrongSpace, "Principal": "other"}},
	}
	for i, tc := range cases {
		reply := exchange(t, a, "bad-domain-"+strings.Repeat("x", i), tc.method, tc.body)
		if reply.Error == nil || reply.Error.Code != api.ErrorInvalidRequest {
			t.Fatalf("%s accepted malformed wire: %+v", tc.method, reply)
		}
	}
}
