package reference_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func compareIndependentEventTrace(t *testing.T, store *ate.Store, trace *oracle.EventTrace, space, principal string, fence model.CommitFence) {
	t.Helper()
	ctx := context.Background()
	anchor := model.SnapshotAnchor{Space: model.SpaceID(space), Fence: fence}
	view, err := store.OpenView(ctx, api.OpenViewRequest{Space: anchor.Space, Principal: model.PrincipalID(principal), Anchor: &anchor, Mode: "audit_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	page, err := store.ListNodes(ctx, api.ListNodesRequest{View: view.Ref, Layer: "events", EntityKind: "Event", Limits: api.QueryLimits{MaxNodes: 1024, MaxEdges: 4096, MaxBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	expected := trace.Visible(space, principal, fence.LSN)
	byPosition := map[string]oracle.ExpectedEvent{}
	for _, event := range expected {
		ordinal := event.Ordinal
		if event.Fact == nil {
			ordinal = 0
		}
		byPosition[fmt.Sprintf("%s/%d", event.Transaction, ordinal)] = event
	}
	actual := map[string]model.Event{}
	identities := map[model.EventID]string{}
	for _, node := range page.Items {
		detail, e := store.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: node.Ref, Limit: api.QueryLimits{MaxNodes: 1024, MaxEdges: 4096, MaxBytes: 4 << 20}})
		if e != nil {
			t.Fatal(e)
		}
		var event model.Event
		if e := json.Unmarshal(detail.Data, &event); e != nil {
			t.Fatal(e)
		}
		ordinal := uint64(0)
		if event.Attribution != nil {
			ordinal = event.Attribution.Ordinal
		}
		position := fmt.Sprintf("%s/%d", event.Transaction, ordinal)
		want, exists := byPosition[position]
		if !exists {
			t.Fatalf("visible uncommitted/duplicate fact at %s: %+v", position, event)
		}
		if _, duplicate := actual[want.Key]; duplicate {
			t.Fatalf("duplicate fact at %s", position)
		}
		actual[want.Key] = event
		identities[event.ID] = want.Key
	}
	if len(actual) != len(expected) {
		t.Fatalf("visible fact count=%d expected=%d", len(actual), len(expected))
	}
	uniqueSeq := map[oracle.TraceStream]map[uint64]string{}
	for _, want := range expected {
		got := actual[want.Key]
		stream := oracle.TraceStream{Space: string(got.Space), Principal: string(got.RecordedBy), Kind: got.Stream.Kind, Owner: got.Stream.Owner, Incarnation: got.Stream.Incarnation}
		if stream != want.Stream || got.Sequence != want.Sequence || string(got.Transaction) != want.Transaction || got.Method != want.Method {
			t.Fatalf("event %s changed stream/order: actual=%+v expected=%+v", want.Key, got, want)
		}
		if uniqueSeq[stream] == nil {
			uniqueSeq[stream] = map[uint64]string{}
		}
		if old := uniqueSeq[stream][got.Sequence]; old != "" {
			t.Fatalf("stream %+v repeats sequence %d at %s and %s", stream, got.Sequence, old, want.Key)
		}
		uniqueSeq[stream][got.Sequence] = want.Key
		previous := identities[got.PreviousInStream]
		causes := []string{}
		for _, id := range got.CausalParents {
			causes = append(causes, identities[id])
		}
		if want.Fact == nil {
			if got.Attribution != nil {
				t.Fatal("core occurrence gained attribution")
			}
			if len(want.ProfileEvents) == 0 {
				if got.SchemaVersion != 1 {
					t.Fatal("core-only format changed")
				}
				previous = ""
				if len(causes) > 0 {
					if len(causes) != 1 {
						t.Fatal("legacy predecessor spelling gained semantic causes")
					}
					previous = causes[0]
				}
				causes = nil
			} else {
				if got.SchemaVersion != 2 || got.ProfileRegistryVersion != eventlog.ProfileRegistryVersion {
					t.Fatal("typed transaction lost registry")
				}
				manifest := []string{}
				for _, id := range got.ProfileEvents {
					manifest = append(manifest, identities[id])
				}
				if !reflect.DeepEqual(manifest, want.ProfileEvents) {
					t.Fatalf("core manifest=%+v expected=%+v", manifest, want.ProfileEvents)
				}
			}
		} else {
			a, f := got.Attribution, want.Fact
			if a == nil || a.Operation != f.Operation || string(a.Subject.Profile) != f.Profile || string(a.Subject.Kind) != f.Kind || string(a.Subject.ID) != f.Subject || string(a.Execution) != f.Execution || string(a.InitiatingActor) != f.Actor || a.Query != f.Query || a.Session != f.Session || a.Step != f.Step || a.DecisionSequence != f.DecisionSequence || a.Invocation != f.Invocation {
				t.Fatalf("event %s field attribution mismatch\n actual=%+v\n expected=%+v", want.Key, a, f)
			}
			expectedCauses := append([]string{}, f.Causes...)
			sort.Strings(expectedCauses)
			sort.Strings(causes)
			if !reflect.DeepEqual(causes, expectedCauses) {
				t.Fatalf("event %s semantic causes=%+v expected=%+v", want.Key, causes, expectedCauses)
			}
		}
		if previous != want.Previous {
			t.Fatalf("event %s predecessor=%s expected=%s", want.Key, previous, want.Previous)
		}
	}
}
