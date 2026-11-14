package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Called by the complete Query-only tool fixture so navigation is checked
// against real immutable decisions, accepted tool results and historical roots.
func assertQueryProgressNodeIsolation(t *testing.T, s *integrationStore, query string, old model.SnapshotAnchor) {
	t.Helper()
	ctx := context.Background()
	latest, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: latest.Ref})
	past, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &old})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: past.Ref})
	other, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "another-owner"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: other.Ref})
	for _, kind := range []string{"QueryProgress", "QueryDecision", "QueryInvocation"} {
		page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: latest.Ref, Layer: "entities", EntityKind: model.EntityKind(kind)})
		wantNodes := 1
		if kind == "QueryDecision" {
			wantNodes = 2
		}
		if err != nil || len(page.Items) != wantNodes {
			t.Fatalf("%s not navigable: %+v %v", kind, page, err)
		}
		ref := page.Items[0].Ref
		detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: latest.Ref, Ref: ref})
		if err != nil || detail.Ref != ref || detail.Anchor.Fence != latest.Anchor.Fence {
			t.Fatalf("%s detail: %+v %v", kind, detail, err)
		}
		switch kind {
		case "QueryProgress":
			var p api.QueryProgress
			if json.Unmarshal(detail.Data, &p) != nil || p.Query != query || !p.Completed || len(p.Decisions) != 2 {
				t.Fatal("wrong typed progress")
			}
		case "QueryDecision":
			var d api.QueryDecision
			if json.Unmarshal(detail.Data, &d) != nil || d.Sequence != 1 || d.AcceptedResult == "" || len(d.Invocations) != 1 {
				t.Fatal("wrong typed decision")
			}
		case "QueryInvocation":
			var i api.QueryInvocation
			if json.Unmarshal(detail.Data, &i) != nil || i.AcceptedResult == "" || len(detail.Sources) != 1 || detail.Sources[0].Object != i.Definition {
				t.Fatal("invocation lost immutable definition/result")
			}
		}
		history, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: past.Ref, Layer: "entities", EntityKind: model.EntityKind(kind)})
		want := 0
		if kind == "QueryProgress" {
			want = 1
		}
		if err != nil || len(history.Items) != want {
			t.Fatalf("%s future entity leaked into old view: %+v %v", kind, history, err)
		}
		page, err = s.e.ListNodes(ctx, api.ListNodesRequest{View: other.Ref, Layer: "entities", EntityKind: model.EntityKind(kind)})
		if err != nil || len(page.Items) != 0 {
			t.Fatalf("%s crossed principal partition: %+v %v", kind, page, err)
		}
		if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: other.Ref, Ref: ref}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("guessed %s ID disclosed: %v", kind, err)
		}
	}
	invocationFacts := 0
	for _, event := range readProfileOccurrences(t, s) {
		a := event.Attribution
		if a == nil || a.Query != query {
			continue
		}
		if a.Execution != "" || a.InitiatingActor != "" || event.Stream.Kind != "query" {
			t.Fatalf("Query fact invented Agent attribution: %+v", event)
		}
		ref, err := eventlog.SubjectRef(*a)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: latest.Ref, Ref: ref}); err != nil {
			t.Fatalf("Query event subject not navigable: %+v %v", a, err)
		}
		if a.Operation == "Query.InvocationRecorded" {
			invocationFacts++
			if a.DecisionSequence != 1 || a.Invocation == "" {
				t.Fatal("query invocation lost typed DecisionRef")
			}
		}
		if a.Operation == "Control.CreateActivity" && a.Subject.ID == "query-tool" && (a.DecisionSequence != 1 || a.Invocation == "") {
			t.Fatal("tool Activity lost Query invocation origin")
		}
	}
	if invocationFacts != 1 {
		t.Fatalf("expected exactly one chosen Query invocation fact, got %d", invocationFacts)
	}
}
