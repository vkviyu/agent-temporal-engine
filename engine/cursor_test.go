package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func cursorDomainRevision(t *testing.T, e *Engine, id string) (model.RevisionID, api.CommitResult, api.StateSnapshot) {
	t.Helper()
	ctx := context.Background()
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, id+"-value", "key", id, api.ValueCondition{}))))
	before, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := state.Entries(ctx, e.index, "coordinator-space", ack.StateRoot, state.ScanLimits{})
	if err != nil {
		t.Fatal(err)
	}
	es := make([]graph.Entry, len(entries))
	for i, x := range entries {
		es[i] = graph.Entry{Key: x.Key, Value: x.Value}
	}
	r, err := graph.BuildRevision("coordinator-space", "", nil, ack.StateRoot, nil, es)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, graphCoordinatorRequest(t, id+"-revision", graph.Command{AddRevision: &graph.AddRevisionCommand{Root: ack.StateRoot, Entries: es}}))))
	return r.ID, ack, before
}

func cursorAnchor(s api.StateSnapshot, id model.RevisionID) model.SnapshotAnchor {
	return model.SnapshotAnchor{Space: "coordinator-space", Fence: s.Fence, Cursor: &model.Cursor{Store: s.Fence.Store, Space: "coordinator-space", Revision: id}}
}

func TestDomainCursorUsesRevisionAtSelectedFenceAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{})
	id, original, _ := cursorDomainRevision(t, e, "cursor-old")
	newACK := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, "cursor-new", "key", "new", api.ValueCondition{}))))
	latest, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := cursorAnchor(latest, id)
	root, err := e.resolveDomainCursor(ctx, latest.Roots, a)
	if err != nil || root != original.StateRoot || root == newACK.StateRoot {
		t.Fatalf("cursor fell back to latest domain root: %s %v", root, err)
	}
	entries, err := state.Entries(ctx, e.index, a.Space, root, state.ScanLimits{})
	if err != nil || len(entries) != 1 {
		t.Fatalf("state: %+v %v", entries, err)
	}
	obj, err := readObject(ctx, e.index, entries[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	v, err := readValue(obj)
	if err != nil || string(v.Data) != "cursor-old" {
		t.Fatalf("historical value changed: %+v %v", v, err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	opened, _, err := OpenExisting(disk, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	root, err = opened.resolveDomainCursor(ctx, latest.Roots, a)
	if err != nil || root != original.StateRoot {
		t.Fatalf("reopen changed cursor: %s %v", root, err)
	}
}

func TestDomainCursorRejectsFutureMissingAndMismatchedSelectors(t *testing.T) {
	ctx := context.Background()
	e, _ := coordinatorStore(t, Options{})
	id, _, beforeRevision := cursorDomainRevision(t, e, "cursor-cut")
	latest, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.resolveDomainCursor(ctx, beforeRevision.Roots, cursorAnchor(beforeRevision, id)); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("future revision leaked into historical cut: %v", err)
	}
	a := cursorAnchor(latest, model.RevisionID(testID('f')))
	if _, err = e.resolveDomainCursor(ctx, latest.Roots, a); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("missing cursor accepted: %v", err)
	}
	a = cursorAnchor(latest, id)
	a.Cursor.Space = "other"
	if _, err = e.resolveDomainCursor(ctx, latest.Roots, a); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("cross-space cursor accepted: %v", err)
	}
	a = cursorAnchor(latest, id)
	a.Cursor.Store = "other-store"
	if _, err = e.resolveDomainCursor(ctx, latest.Roots, a); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("cross-store cursor accepted: %v", err)
	}
	a = cursorAnchor(latest, id)
	a.Knowledge = &model.KnowledgeKey{Actor: "actor", Execution: "exec"}
	if _, err = e.resolveDomainCursor(ctx, latest.Roots, a); !errors.Is(err, ErrUnsupportedMethod) {
		t.Fatalf("invented knowledge revision support: %v", err)
	}
	if _, err = e.Snapshot(ctx); err != nil {
		t.Fatalf("invalid selector fenced store: %v", err)
	}
}

func TestDomainCursorEmptyRevisionAndCanceledRead(t *testing.T) {
	ctx := context.Background()
	e, _ := coordinatorStore(t, Options{})
	r, _ := graph.BuildRevision("coordinator-space", "", nil, "", nil, nil)
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, graphCoordinatorRequest(t, "empty-cursor", graph.Command{AddRevision: &graph.AddRevisionCommand{}}))))
	snap, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if root, err := e.resolveDomainCursor(ctx, snap.Roots, cursorAnchor(snap, r.ID)); err != nil || root != "" {
		t.Fatalf("empty revision cursor: %s %v", root, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = e.resolveDomainCursor(canceled, snap.Roots, cursorAnchor(snap, r.ID)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read ran: %v", err)
	}
}
