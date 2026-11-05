package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func nodeRevisionFixture(t *testing.T) (*integrationStore, graph.Revision, graph.Revision, api.CommitResult, model.ObjectID, model.ObjectID) {
	t.Helper()
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	create := func(name string, parent model.RevisionID, b byte) (graph.Revision, api.CommitResult, model.ObjectID) {
		t.Helper()
		domain := integrationMutate(t, s.e, integrationRequest(name+"-state", integrationPut([]byte("key"), integrationBytes(b))))
		view := nodeView(t, s.e, "host-principal", nil)
		entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
		if err != nil || len(entries.Items) != 1 {
			t.Fatalf("entries=%+v %v", entries, err)
		}
		es := []graph.Entry{{Key: entries.Items[0].Key, Value: entries.Items[0].Value}}
		revision, err := graph.BuildRevision("host-space", parent, nil, domain.StateRoot, nil, es)
		if err != nil {
			t.Fatal(err)
		}
		ack := graphCoreApply(t, s.e, name, graph.Command{AddRevision: &graph.AddRevisionCommand{Parent: parent, Root: domain.StateRoot, Entries: es}})
		return revision, ack, entries.Items[0].Value
	}
	old, cut, oldValue := create("node-rev-old", "", 'a')
	latest, _, latestValue := create("node-rev-new", old.ID, 'b')
	return s, old, latest, cut, oldValue, latestValue
}

func containsNode(items []api.NodeSummary, ref model.NodeRef) bool {
	for _, n := range items {
		if n.Ref == ref {
			return true
		}
	}
	return false
}

func TestRevisionNodeHistoricalMembershipContextAndReopen(t *testing.T) {
	ctx := context.Background()
	s, old, latest, oldCut, oldValue, newValue := nodeRevisionFixture(t)
	oldRef := model.NodeRef{Kind: model.NodeRevision, Revision: old.ID}
	newRef := model.NodeRef{Kind: model.NodeRevision, Revision: latest.ID}
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: oldCut.Fence}
	for pass := 0; pass < 2; pass++ {
		past := nodeView(t, s.e, "host-principal", &anchor)
		page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: past.Ref, Layer: "content", EntityKind: "Revision"})
		if err != nil || len(page.Items) != 1 || page.Items[0].Ref != oldRef {
			t.Fatalf("historical revisions=%+v %v", page, err)
		}
		if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: past.Ref, Ref: newRef}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("future revision visible: %v", err)
		}
		if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: past.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: newValue}}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("future revision content visible: %v", err)
		}
		view := nodeView(t, s.e, "host-principal", nil)
		detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: newRef})
		if err != nil {
			t.Fatal(err)
		}
		var decoded graph.Revision
		if err = json.Unmarshal(detail.Data, &decoded); err != nil || !reflect.DeepEqual(decoded, latest) {
			t.Fatalf("revision projection=%+v %v", decoded, err)
		}
		if !reflect.DeepEqual(detail.Anchor, view.Anchor) || detail.Encoding != "graph/Revision/v1+json" || detail.Payload == "" {
			t.Fatalf("detail lost proof: %+v", detail)
		}
		neighbors, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: newRef, Mode: "parents"})
		if err != nil || !containsNode(neighbors.Sections["parents"], oldRef) || !containsNode(neighbors.Sections["parents"], model.NodeRef{Kind: model.NodeObject, Object: newValue}) {
			t.Fatalf("revision parents=%+v %v", neighbors, err)
		}
		oldContent, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: oldValue}})
		if err != nil || oldContent.Summary.Kind != "revision_content" {
			t.Fatalf("retained revision evidence=%+v %v", oldContent, err)
		}
		if _, err = s.e.Snapshot(ctx); err != nil {
			t.Fatalf("query miss fenced store: %v", err)
		}
		if pass == 0 {
			s.reopen()
		}
	}
}

func TestCursorNodeContentUsesSelectedAncestryUnlessExplicitAudit(t *testing.T) {
	ctx := context.Background()
	s, old, latest, _, oldValue, newValue := nodeRevisionFixture(t)
	tip := integrationSnapshot(t, s.e)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: tip.Fence, Cursor: &model.Cursor{Store: tip.Fence.Store, Space: "host-space", Revision: old.ID}}
	for pass := 0; pass < 2; pass++ {
		view := nodeView(t, s.e, "host-principal", &anchor)
		page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: view.Ref, Layer: "content"})
		if err != nil {
			t.Fatal(err)
		}
		if !containsNode(page.Items, model.NodeRef{Kind: model.NodeObject, Object: oldValue}) || !containsNode(page.Items, model.NodeRef{Kind: model.NodeRevision, Revision: old.ID}) {
			t.Fatalf("selected revision content missing: %+v", page)
		}
		for _, ref := range []model.NodeRef{{Kind: model.NodeObject, Object: newValue}, {Kind: model.NodeRevision, Revision: latest.ID}} {
			if containsNode(page.Items, ref) {
				t.Fatalf("cursor list included descendant content: %+v", page)
			}
			if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref}); !errors.Is(err, engine.ErrNodeNotFound) {
				t.Fatalf("cursor detail included descendant: %v", err)
			}
		}
		state, err := s.e.GetViewState(ctx, api.ReadViewStateRequest{View: view.Ref, Key: []byte("key")})
		if err != nil || len(state.Value.Data) != 1 || state.Value.Data[0] != 'a' {
			t.Fatalf("cursor root ignored: %+v %v", state, err)
		}
		audit, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "audit_snapshot"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.e.GetNode(ctx, api.GetNodeRequest{View: audit.Ref, Ref: model.NodeRef{Kind: model.NodeRevision, Revision: latest.ID}}); err != nil {
			t.Fatalf("explicit audit could not inspect graph: %v", err)
		}
		_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: audit.Ref})
		if pass == 0 {
			s.reopen()
		}
	}
}
