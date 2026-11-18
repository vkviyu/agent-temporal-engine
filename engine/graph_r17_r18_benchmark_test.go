package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const graphR17R18Space model.SpaceID = "host-space"
const graphR17R18Principal model.PrincipalID = "host-principal"

type graphR17R18Store struct {
	e   *engine.Engine
	obs *observation.Collector
}

func newGraphR17R18Store(b *testing.B) *graphR17R18Store {
	b.Helper()
	d, err := storage.OpenDirectory(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	obs, err := observation.New(observation.MaxCapacity)
	if err != nil {
		b.Fatal(err)
	}
	e, err := engine.Create(d, engine.Options{Observation: obs})
	if err != nil {
		_ = d.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := e.Close(); err != nil {
			b.Error(err)
		}
		if err := d.Close(); err != nil {
			b.Error(err)
		}
	})
	return &graphR17R18Store{e: e, obs: obs}
}

func graphR17R18Request(space model.SpaceID, id string, operations ...api.StateMutation) api.MutateRequest {
	return api.MutateRequest{
		Key:   model.RequestKey{Space: space, Principal: graphR17R18Principal, RequestID: model.RequestID(id)},
		Batch: api.StateBatch{Operations: operations},
	}
}

func graphR17R18Put(key, value string) api.StateMutation {
	return api.StateMutation{
		Key:   []byte(key),
		Kind:  api.MutationPut,
		Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)},
	}
}

func graphR17R18Mutate(b *testing.B, s *graphR17R18Store, req api.MutateRequest) api.CommitResult {
	b.Helper()
	out, err := s.e.Mutate(context.Background(), req)
	if err != nil || out.Code != api.CodeDurableAck {
		b.Fatalf("mutation %s failed: %+v %v", req.Key.RequestID, out, err)
	}
	return out
}

func graphR17R18GraphKey(space model.SpaceID, id string) model.RequestKey {
	return model.RequestKey{Space: space, Principal: graphR17R18Principal, RequestID: model.RequestID(id)}
}

func graphR17R18Apply(b *testing.B, s *graphR17R18Store, space model.SpaceID, id string, command graph.Command) api.CommitResult {
	b.Helper()
	out, err := s.e.ApplyGraph(context.Background(), api.ApplyGraphRequest{
		Key: graphR17R18GraphKey(space, id), Command: command,
	})
	if err != nil || out.Code != api.CodeDurableAck {
		b.Fatalf("graph command %s failed: %+v %v", id, out, err)
	}
	return out
}

func graphR17R18ApplyRaw(s *graphR17R18Store, space model.SpaceID, id string, command graph.Command) (api.CommitResult, error) {
	return s.e.ApplyGraph(context.Background(), api.ApplyGraphRequest{
		Key: graphR17R18GraphKey(space, id), Command: command,
	})
}

func graphR17R18Entries(b *testing.B, s *graphR17R18Store, space model.SpaceID, anchor *model.SnapshotAnchor) (model.ObjectID, []graph.Entry) {
	b.Helper()
	view, err := s.e.OpenView(context.Background(), api.OpenViewRequest{
		Space: space, Principal: graphR17R18Principal, Anchor: anchor,
	})
	if err != nil {
		b.Fatal(err)
	}
	got, readErr := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: view.Ref})
	closeErr := s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref})
	if readErr != nil {
		b.Fatal(readErr)
	}
	if closeErr != nil {
		b.Fatal(closeErr)
	}
	entries := make([]graph.Entry, len(got.Items))
	for i, item := range got.Items {
		entries[i] = graph.Entry{Key: append([]byte(nil), item.Key...), Value: item.Value}
	}
	return got.StateRoot, entries
}

func graphR17R18AddRevision(b *testing.B, s *graphR17R18Store, space model.SpaceID, requestID string, parent model.RevisionID, joins []model.RevisionID, root model.ObjectID, entries []graph.Entry) graph.Revision {
	b.Helper()
	r, err := graph.BuildRevision(string(space), parent, joins, root, nil, entries)
	if err != nil {
		b.Fatal(err)
	}
	graphR17R18Apply(b, s, space, requestID, graph.Command{AddRevision: &graph.AddRevisionCommand{
		Parent: parent, Joins: joins, Root: root, Entries: entries,
	}})
	return r
}

func graphR17R18EntryBytes(entries []graph.Entry) int {
	n := 0
	for _, entry := range entries {
		n += len(entry.Key) + len(entry.Value)
	}
	return n
}

func graphR17R18CloneEntries(entries []graph.Entry) []graph.Entry {
	out := make([]graph.Entry, len(entries))
	for i, entry := range entries {
		out[i] = graph.Entry{Key: append([]byte(nil), entry.Key...), Value: entry.Value}
	}
	return out
}

func graphR17R18DrainObservation(c *observation.Collector) (objects, bytes, scans, scanBytes uint64) {
	buffer := make([]observation.Event, observation.MaxCapacity)
	for {
		n := c.Drain(buffer)
		if n == 0 {
			return objects, bytes, scans, scanBytes
		}
		for _, event := range buffer[:n] {
			switch event.Kind {
			case observation.Read:
				objects += event.Count
				bytes += event.Bytes
			case observation.Scan:
				scans += event.Count
				scanBytes += event.Bytes
			}
		}
	}
}

type graphR17Fixture struct {
	store         *graphR17R18Store
	currentRoot   model.ObjectID
	current       []graph.Entry
	earlyRoot     model.ObjectID
	early         []graph.Entry
	benchmarkHead model.RevisionID
}

func buildGraphR17Fixture(b *testing.B, history int) *graphR17Fixture {
	b.Helper()
	s := newGraphR17R18Store(b)
	var earlyFence model.CommitFence
	for i := 0; i < history; i++ {
		ack := graphR17R18Mutate(b, s, graphR17R18Request(
			graphR17R18Space,
			fmt.Sprintf("r17-history-%d", i),
			graphR17R18Put(fmt.Sprintf("r17/key/%06d", i), fmt.Sprintf("value-%06d", i)),
		))
		if i == 0 {
			earlyFence = ack.Fence
		}
	}
	earlyRoot, earlyEntries := graphR17R18Entries(b, s, graphR17R18Space, &model.SnapshotAnchor{
		Space: graphR17R18Space, Fence: earlyFence,
	})
	currentRoot, currentEntries := graphR17R18Entries(b, s, graphR17R18Space, nil)
	current := graphR17R18AddRevision(b, s, graphR17R18Space, "r17-current", "", nil, currentRoot, currentEntries)
	early := graphR17R18AddRevision(b, s, graphR17R18Space, "r17-early", current.ID, nil, earlyRoot, earlyEntries)

	if got, err := s.e.GetGraphRevision(context.Background(), api.ReadGraphRevisionRequest{
		Space: graphR17R18Space, ID: current.ID,
	}); err != nil || !got.Found || got.Value.StateRoot != currentRoot {
		b.Fatalf("current membership preflight failed: %+v %v", got, err)
	}
	if got, err := s.e.GetGraphRevision(context.Background(), api.ReadGraphRevisionRequest{
		Space: graphR17R18Space, ID: early.ID,
	}); err != nil || !got.Found || got.Value.StateRoot != earlyRoot {
		b.Fatalf("early membership preflight failed: %+v %v", got, err)
	}

	miss := model.ObjectID(strings.Repeat("f", 64))
	if out, err := graphR17R18ApplyRaw(s, graphR17R18Space, "r17-miss-preflight", graph.Command{
		AddRevision: &graph.AddRevisionCommand{Parent: early.ID, Root: miss},
	}); err == nil || out.Code != api.CodeRejected || !errors.Is(err, graph.ErrStateUnverified) {
		b.Fatalf("miss membership was accepted: %+v %v", out, err)
	}
	forged := graphR17R18CloneEntries(currentEntries)
	forged[0].Value = model.ObjectID(strings.Repeat("b", 64))
	if out, err := graphR17R18ApplyRaw(s, graphR17R18Space, "r17-forged-preflight", graph.Command{
		AddRevision: &graph.AddRevisionCommand{Parent: early.ID, Root: currentRoot, Entries: forged},
	}); err == nil || out.Code != api.CodeRejected {
		b.Fatalf("forged entries were accepted: %+v %v", out, err)
	}

	other := model.SpaceID("other-space")
	otherAck := graphR17R18Mutate(b, s, graphR17R18Request(other, "r17-other-state", graphR17R18Put("other", "space")))
	otherRoot, otherEntries := graphR17R18Entries(b, s, other, &model.SnapshotAnchor{Space: other, Fence: otherAck.Fence})
	otherRevision := graphR17R18AddRevision(b, s, other, "r17-other-revision", "", nil, otherRoot, otherEntries)
	if got, err := s.e.GetGraphRevision(context.Background(), api.ReadGraphRevisionRequest{
		Space: graphR17R18Space, ID: otherRevision.ID,
	}); err != nil || got.Found {
		b.Fatalf("cross-space revision was visible: %+v %v", got, err)
	}
	if out, err := graphR17R18ApplyRaw(s, other, "r17-cross-space-parent", graph.Command{
		AddRevision: &graph.AddRevisionCommand{Parent: current.ID, Root: otherRoot, Entries: otherEntries},
	}); err == nil || out.Code != api.CodeRejected || !errors.Is(err, graph.ErrUnknownRevision) {
		b.Fatalf("cross-space parent was accepted: %+v %v", out, err)
	}

	return &graphR17Fixture{
		store: s, currentRoot: currentRoot, current: currentEntries,
		earlyRoot: earlyRoot, early: earlyEntries, benchmarkHead: early.ID,
	}
}

func benchmarkGraphR17Membership(b *testing.B, history int, kind string) {
	fixture := buildGraphR17Fixture(b, history)
	b.ReportAllocs()
	b.ReportMetric(float64(history), "domain-publications")
	var root model.ObjectID
	var entries []graph.Entry
	switch kind {
	case "current":
		root, entries = fixture.currentRoot, fixture.current
	case "early":
		root, entries = fixture.earlyRoot, fixture.early
	case "miss":
		root, entries = model.ObjectID(strings.Repeat("f", 64)), nil
	default:
		b.Fatalf("unknown membership kind %q", kind)
	}
	revision, err := graph.BuildRevision(string(graphR17R18Space), fixture.benchmarkHead, nil, root, nil, entries)
	if err != nil {
		b.Fatal(err)
	}
	raw, err := graph.EncodeRevision(revision)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	parent := fixture.benchmarkHead
	for i := 0; i < b.N; i++ {
		r, err := graph.BuildRevision(string(graphR17R18Space), parent, nil, root, nil, entries)
		if err != nil {
			b.Fatal(err)
		}
		out, err := graphR17R18ApplyRaw(fixture.store, graphR17R18Space, fmt.Sprintf("r17-bench-%s-%d", kind, i), graph.Command{
			AddRevision: &graph.AddRevisionCommand{Parent: parent, Root: root, Entries: entries},
		})
		if kind == "miss" {
			if err == nil || out.Code != api.CodeRejected || !errors.Is(err, graph.ErrStateUnverified) {
				b.Fatalf("miss membership result: %+v %v", out, err)
			}
			continue
		}
		if err != nil || out.Code != api.CodeDurableAck {
			b.Fatalf("%s membership result: %+v %v", kind, out, err)
		}
		parent = r.ID
	}
	b.StopTimer()
	b.ReportMetric(float64(len(raw)), "revision-bytes")
	b.ReportMetric(float64(graphR17R18EntryBytes(entries)), "entry-bytes")
}

type graphTraceFixture struct {
	store       *graphR17R18Store
	start       model.RevisionID
	anchor      model.SnapshotAnchor
	nodeCount   int
	edgeCount   int
	revisionRaw int
}

func graphR17R18TraceAnchor(b *testing.B, s *graphR17R18Store, revision model.RevisionID) model.SnapshotAnchor {
	b.Helper()
	snapshot, err := s.e.Snapshot(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	return model.SnapshotAnchor{
		Space: graphR17R18Space,
		Fence: snapshot.Fence,
		Cursor: &model.Cursor{
			Store: snapshot.Fence.Store, Space: graphR17R18Space, Revision: revision,
		},
	}
}

func buildGraphTraceFixture(b *testing.B, shape string, size int) *graphTraceFixture {
	b.Helper()
	s := newGraphR17R18Store(b)
	base := graphR17R18AddRevision(b, s, graphR17R18Space, "trace-base", "", nil, "", nil)
	anchor := graphR17R18TraceAnchor(b, s, base.ID)
	ids := []model.RevisionID{base.ID}
	edges := 0
	switch shape {
	case "deep":
		parent := base.ID
		for i := 0; i < size; i++ {
			r := graphR17R18AddRevision(b, s, graphR17R18Space, fmt.Sprintf("trace-deep-%d", i), parent, nil, "", nil)
			parent = r.ID
			ids = append(ids, r.ID)
			edges++
		}
	case "wide":
		for i := 0; i < size; i++ {
			ack := graphR17R18Mutate(b, s, graphR17R18Request(
				graphR17R18Space, fmt.Sprintf("trace-wide-state-%d", i),
				graphR17R18Put(fmt.Sprintf("trace/wide/%06d", i), fmt.Sprintf("value-%06d", i)),
			))
			root, entries := graphR17R18Entries(b, s, graphR17R18Space, &model.SnapshotAnchor{
				Space: graphR17R18Space, Fence: ack.Fence,
			})
			r := graphR17R18AddRevision(b, s, graphR17R18Space, fmt.Sprintf("trace-wide-%d", i), base.ID, nil, root, entries)
			ids = append(ids, r.ID)
			edges++
		}
	case "shared":
		leftAck := graphR17R18Mutate(b, s, graphR17R18Request(
			graphR17R18Space, "trace-shared-left-state", graphR17R18Put("trace/shared/left", "left"),
		))
		leftRoot, leftEntries := graphR17R18Entries(b, s, graphR17R18Space, &model.SnapshotAnchor{
			Space: graphR17R18Space, Fence: leftAck.Fence,
		})
		left := graphR17R18AddRevision(b, s, graphR17R18Space, "trace-shared-left", base.ID, nil, leftRoot, leftEntries)
		rightAck := graphR17R18Mutate(b, s, graphR17R18Request(
			graphR17R18Space, "trace-shared-right-state", graphR17R18Put("trace/shared/right", "right"),
		))
		rightRoot, rightEntries := graphR17R18Entries(b, s, graphR17R18Space, &model.SnapshotAnchor{
			Space: graphR17R18Space, Fence: rightAck.Fence,
		})
		right := graphR17R18AddRevision(b, s, graphR17R18Space, "trace-shared-right", base.ID, nil, rightRoot, rightEntries)
		ids = append(ids, left.ID, right.ID)
		edges += 2
		parent := left.ID
		for i := 0; i < size; i++ {
			r := graphR17R18AddRevision(b, s, graphR17R18Space, fmt.Sprintf("trace-shared-%d", i), parent, []model.RevisionID{right.ID}, "", nil)
			ids = append(ids, r.ID)
			edges += 2
			parent = r.ID
		}
	default:
		b.Fatalf("unknown trace shape %q", shape)
	}
	return &graphTraceFixture{
		store: s, start: base.ID, anchor: anchor,
		nodeCount: len(ids), edgeCount: edges, revisionRaw: graphR17R18RevisionBytes(b, ids, s),
	}
}

func graphR17R18RevisionBytes(b *testing.B, ids []model.RevisionID, s *graphR17R18Store) int {
	b.Helper()
	total := 0
	for _, id := range ids {
		got, err := s.e.GetGraphRevision(context.Background(), api.ReadGraphRevisionRequest{
			Space: graphR17R18Space, ID: id,
		})
		if err != nil || !got.Found {
			b.Fatalf("revision size preflight failed for %s: %+v %v", id, got, err)
		}
		raw, err := graph.EncodeRevision(got.Value)
		if err != nil {
			b.Fatal(err)
		}
		total += len(raw)
	}
	return total
}

func buildGraphDisconnectedFixture(b *testing.B, unrelated int) *graphTraceFixture {
	b.Helper()
	s := newGraphR17R18Store(b)
	target := graphR17R18AddRevision(b, s, graphR17R18Space, "disconnected-target", "", nil, "", nil)
	anchor := graphR17R18TraceAnchor(b, s, target.ID)
	child := graphR17R18AddRevision(b, s, graphR17R18Space, "disconnected-child", target.ID, nil, "", nil)
	ids := []model.RevisionID{target.ID, child.ID}
	for i := 0; i < unrelated; i++ {
		r := graphR17R18AddRevision(b, s, graphR17R18Space, fmt.Sprintf("disconnected-unrelated-%d", i), "", nil, "", nil)
		ids = append(ids, r.ID)
	}
	return &graphTraceFixture{
		store: s, start: target.ID, anchor: anchor, nodeCount: 2,
		edgeCount: 1, revisionRaw: graphR17R18RevisionBytes(b, ids, s),
	}
}

func benchmarkGraphR18Trace(b *testing.B, fixture *graphTraceFixture, pageOne bool) {
	b.ReportAllocs()
	request := api.TraceGraphRequest{
		Space: graphR17R18Space, Start: fixture.start, Direction: "descendants",
	}
	if pageOne {
		request.Limit = 1
	}
	graphR17R18DrainObservation(fixture.store.obs)
	preflight, err := fixture.store.e.TraceGraph(context.Background(), request)
	if err != nil {
		b.Fatal(err)
	}
	if pageOne {
		if len(preflight.Nodes) != 1 || !preflight.Truncated || preflight.Continuation == "" {
			b.Fatalf("page=1 preflight=%+v", preflight)
		}
	} else if len(preflight.Nodes) != fixture.nodeCount || len(preflight.Edges) != fixture.edgeCount {
		b.Fatalf("full descendants preflight=%+v", preflight)
	}
	raw, err := json.Marshal(preflight)
	if err != nil {
		b.Fatal(err)
	}
	objects, bytes, scans, scanBytes := graphR17R18DrainObservation(fixture.store.obs)
	anchored, err := fixture.store.e.TraceGraph(context.Background(), api.TraceGraphRequest{
		Space: graphR17R18Space, Anchor: &fixture.anchor, Start: fixture.start,
		Direction: "descendants",
	})
	if err != nil || len(anchored.Nodes) != 1 || anchored.Nodes[0].ID != fixture.start {
		b.Fatalf("historical anchor saw future descendants: %+v %v", anchored, err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := fixture.store.e.TraceGraph(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		if pageOne {
			if len(got.Nodes) != 1 || !got.Truncated {
				b.Fatalf("page=1 result=%+v", got)
			}
		} else if len(got.Nodes) != fixture.nodeCount || len(got.Edges) != fixture.edgeCount {
			b.Fatalf("full descendants result=%+v", got)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(fixture.nodeCount), "result-nodes")
	b.ReportMetric(float64(fixture.edgeCount), "result-edges")
	b.ReportMetric(float64(fixture.revisionRaw), "fixture-revision-bytes")
	b.ReportMetric(float64(len(raw)), "json-bytes/query")
	b.ReportMetric(float64(objects), "objects/query")
	b.ReportMetric(float64(bytes), "read-bytes/query")
	b.ReportMetric(float64(scans), "scan-objects/query")
	b.ReportMetric(float64(scanBytes), "scan-bytes/query")
}

type graphJoinFixture struct {
	store           *graphR17R18Store
	base, left      graph.Revision
	right           graph.Revision
	entries         int
	conflicts       int
	planJSONBytes   int
	inputEntryBytes int
}

func buildGraphJoinFixture(b *testing.B, entries int, conflict bool) *graphJoinFixture {
	b.Helper()
	s := newGraphR17R18Store(b)
	base := graphR17R18AddRevision(b, s, graphR17R18Space, "join-bench-base", "", nil, "", nil)
	graphR17R18Apply(b, s, graphR17R18Space, "join-bench-fork", graph.Command{
		Fork: &graph.ForkCommand{Branch: "main", Source: base.ID},
	})

	leftOps := make([]api.StateMutation, entries)
	for i := range leftOps {
		key := fmt.Sprintf("join/%06d", i)
		leftOps[i] = graphR17R18Put(key, fmt.Sprintf("left-%06d", i))
	}
	leftAck := graphR17R18Mutate(b, s, graphR17R18Request(graphR17R18Space, "join-bench-left-state", leftOps...))
	leftRoot, leftEntries := graphR17R18Entries(b, s, graphR17R18Space, &model.SnapshotAnchor{
		Space: graphR17R18Space, Fence: leftAck.Fence,
	})
	left := graphR17R18AddRevision(b, s, graphR17R18Space, "join-bench-left", base.ID, nil, leftRoot, leftEntries)
	graphR17R18Apply(b, s, graphR17R18Space, "join-bench-left-head", graph.Command{
		Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: left.ID},
	})

	rightOps := make([]api.StateMutation, entries)
	for i := range rightOps {
		key := fmt.Sprintf("join/%06d", i)
		if !conflict {
			key = fmt.Sprintf("join/right/%06d", i)
		}
		rightOps[i] = graphR17R18Put(key, fmt.Sprintf("right-%06d", i))
	}
	rightAck := graphR17R18Mutate(b, s, graphR17R18Request(graphR17R18Space, "join-bench-right-state", rightOps...))
	rightRoot, rightEntries := graphR17R18Entries(b, s, graphR17R18Space, &model.SnapshotAnchor{
		Space: graphR17R18Space, Fence: rightAck.Fence,
	})
	right := graphR17R18AddRevision(b, s, graphR17R18Space, "join-bench-right", base.ID, nil, rightRoot, rightEntries)

	request := api.PlanGraphJoinRequest{Space: graphR17R18Space, Left: left.ID, Right: right.ID}
	planResult, err := s.e.PlanGraphJoin(context.Background(), request)
	if err != nil {
		b.Fatal(err)
	}
	wantConflicts := 0
	if conflict {
		wantConflicts = entries
	}
	if len(planResult.Plan.Conflicts) != wantConflicts {
		b.Fatalf("join conflict preflight: got=%d want=%d", len(planResult.Plan.Conflicts), wantConflicts)
	}
	planRaw, err := json.Marshal(planResult)
	if err != nil {
		b.Fatal(err)
	}
	if len(planResult.Plan.State) == 0 && !conflict {
		b.Fatal("no-conflict join produced empty state")
	}

	unknownResolution := graph.JoinCommand{
		Branch: "main", Expected: 1, Left: left.ID, Right: right.ID,
		ExpectedDomainRoot: rightRoot,
		Resolutions:        []graph.Resolution{{Key: []byte("join/unknown"), Value: leftEntries[0].Value}},
	}
	if out, err := graphR17R18ApplyRaw(s, graphR17R18Space, "join-unknown-resolution", graph.Command{Join: &unknownResolution}); err == nil || out.Code != api.CodeRejected || !errors.Is(err, graph.ErrJoinConflict) {
		b.Fatalf("unknown resolution was accepted: %+v %v", out, err)
	}
	forged := graphR17R18CloneEntries(rightEntries)
	forged[0].Value = model.ObjectID(strings.Repeat("c", 64))
	if out, err := graphR17R18ApplyRaw(s, graphR17R18Space, "join-forged-entries", graph.Command{
		AddRevision: &graph.AddRevisionCommand{Parent: base.ID, Root: rightRoot, Entries: forged},
	}); err == nil || out.Code != api.CodeRejected {
		b.Fatalf("forged join input was accepted: %+v %v", out, err)
	}
	other := model.SpaceID("other-space")
	if out, err := s.e.PlanGraphJoin(context.Background(), api.PlanGraphJoinRequest{
		Space: other, Left: left.ID, Right: right.ID,
	}); err == nil || !errors.Is(err, graph.ErrUnknownRevision) {
		b.Fatalf("cross-space join was accepted: %+v %v", out, err)
	}

	return &graphJoinFixture{
		store: s, base: base, left: left, right: right,
		entries:   len(rightEntries),
		conflicts: wantConflicts, planJSONBytes: len(planRaw),
		inputEntryBytes: graphR17R18EntryBytes(leftEntries) + graphR17R18EntryBytes(rightEntries),
	}
}

func benchmarkGraphR18JoinPlan(b *testing.B, fixture *graphJoinFixture) {
	b.ReportAllocs()
	graphR17R18DrainObservation(fixture.store.obs)
	preflight, err := fixture.store.e.PlanGraphJoin(context.Background(), api.PlanGraphJoinRequest{
		Space: graphR17R18Space, Left: fixture.left.ID, Right: fixture.right.ID,
	})
	if err != nil || len(preflight.Plan.Conflicts) != fixture.conflicts {
		b.Fatalf("join plan preflight=%+v %v", preflight, err)
	}
	objects, bytes, scans, scanBytes := graphR17R18DrainObservation(fixture.store.obs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := fixture.store.e.PlanGraphJoin(context.Background(), api.PlanGraphJoinRequest{
			Space: graphR17R18Space, Left: fixture.left.ID, Right: fixture.right.ID,
		})
		if err != nil || len(got.Plan.Conflicts) != fixture.conflicts {
			b.Fatalf("join plan=%+v %v", got, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(fixture.entries), "fixture-entries")
	b.ReportMetric(float64(fixture.conflicts), "fixture-conflicts")
	b.ReportMetric(float64(fixture.planJSONBytes), "json-bytes/query")
	b.ReportMetric(float64(fixture.inputEntryBytes), "entry-bytes/query")
	b.ReportMetric(float64(objects), "objects/query")
	b.ReportMetric(float64(bytes), "read-bytes/query")
	b.ReportMetric(float64(scans), "scan-objects/query")
	b.ReportMetric(float64(scanBytes), "scan-bytes/query")
}

func BenchmarkGraphR17R18DomainRootMembership(b *testing.B) {
	for _, tier := range []struct {
		name    string
		history int
	}{
		{name: "history-16", history: 16},
		{name: "history-64-reduced-256", history: 64},
		{name: "history-128-reduced-1024", history: 128},
	} {
		for _, kind := range []string{"current", "early", "miss"} {
			b.Run(fmt.Sprintf("%s/%s", tier.name, kind), func(b *testing.B) {
				benchmarkGraphR17Membership(b, tier.history, kind)
			})
		}
	}
}

func BenchmarkGraphR17R18Descendants(b *testing.B) {
	for _, shape := range []string{"deep", "wide", "shared"} {
		for _, pageOne := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/%s", shape, map[bool]string{false: "full", true: "page-1"}[pageOne]), func(b *testing.B) {
				fixture := buildGraphTraceFixture(b, shape, 32)
				benchmarkGraphR18Trace(b, fixture, pageOne)
			})
		}
	}
	b.Run("disconnected/page-1", func(b *testing.B) {
		fixture := buildGraphDisconnectedFixture(b, 128)
		benchmarkGraphR18Trace(b, fixture, true)
	})
}

func BenchmarkGraphR17R18Join(b *testing.B) {
	for _, entries := range []int{8, 128} {
		for _, conflict := range []bool{false, true} {
			b.Run(fmt.Sprintf("plan/entries-%d/%s", entries, map[bool]string{false: "no-conflict", true: "conflict"}[conflict]), func(b *testing.B) {
				fixture := buildGraphJoinFixture(b, entries, conflict)
				benchmarkGraphR18JoinPlan(b, fixture)
			})
		}
	}
}
