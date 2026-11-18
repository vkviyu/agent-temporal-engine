package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestNodePolicyRefNamespaces(t *testing.T) {
	id := model.ObjectID(strings.Repeat("a", 64))
	for _, tt := range []struct {
		name string
		ref  model.NodeRef
		kind policyDependencyKind
		id   model.ObjectID
	}{
		{"embedded Knowledge selector", model.NodeRef{Kind: model.NodeObject, Object: id}, policyNode, id},
		{"revision", model.NodeRef{Kind: model.NodeRevision, Revision: model.RevisionID(id)}, policyRevision, id},
		{"event", model.NodeRef{Kind: model.NodeEvent, Event: model.EventID(id)}, policyObject, id},
		{"digest entity", model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "knowledge", Kind: "summary", ID: model.EntityID(id)}}, policyNode, id},
		{"named entity", model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{ID: "task"}}, policyNode, ""},
		{"invalid digest", model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{ID: model.EntityID(strings.Repeat("A", 64))}}, policyNode, ""},
		{"empty object", model.NodeRef{Kind: model.NodeObject}, policyNode, ""},
		{"unknown selector", model.NodeRef{Kind: "unknown", Object: id}, policyNode, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := nodePolicyDependency(tt.ref); got != (policyDependency{kind: tt.kind, id: tt.id}) {
				t.Fatalf("dependency = %+v, want %s/%s", got, tt.kind, tt.id)
			}
			if got := nodeReadRefID(tt.ref); got != tt.id {
				t.Fatalf("policy identity = %q, want %q", got, tt.id)
			}
		})
	}
	// Distinct namespaces deliberately retain the same Space-wide policy ID.
	dependencies := []policyDependency{
		{kind: policyNode, id: id}, {kind: policyRevision, id: id}, {kind: policyObject, id: id},
	}
	if got := policyIdentities(dependencies); !reflect.DeepEqual(got, []model.ObjectID{id, id, id}) {
		t.Fatalf("digest-wide identity boundary changed: %v", got)
	}
}

func nodePolicyJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestNodeDisclosurePolicyAnnotationOrder(t *testing.T) {
	id := model.ObjectID(strings.Repeat("b", 64))
	record := annotation.Record{
		ID: id, Target: model.NodeRef{Kind: model.NodeObject, Object: id},
		Evidence: []model.NodeRef{
			{Kind: model.NodeRevision, Revision: model.RevisionID(id)},
			{Kind: model.NodeEvent, Event: model.EventID(id)},
			{Kind: model.NodeEntity, Entity: model.EntityKey{ID: model.EntityID(id)}},
			{Kind: model.NodeEntity, Entity: model.EntityKey{ID: "named"}},
			{Kind: model.NodeObject},
			{Kind: "unknown", Object: id},
		},
	}
	n := nodeProjection{detail: api.NodeDetail{
		Payload: id, Encoding: "annotation/Record/v1+json", Data: nodePolicyJSON(t, record),
		Sources: []model.NodeRef{
			{Kind: model.NodeObject, Object: id},
			{Kind: model.NodeRevision, Revision: model.RevisionID(id)},
			{Kind: model.NodeObject},
		},
	}}
	want := []policyDependency{
		{kind: policyObject, id: id}, {kind: policyAnnotation, id: id}, {kind: policyAnnotation},
		{kind: policyNode, id: id}, {kind: policyRevision, id: id}, {kind: policyObject, id: id},
		{kind: policyNode, id: id}, {kind: policyNode}, {kind: policyNode, id: id}, {kind: policyNode},
	}
	if got := nodeDisclosureDependencies(n); !reflect.DeepEqual(got, want) {
		t.Fatalf("annotation dependencies = %+v, want %+v", got, want)
	}
	wantIDs := []model.ObjectID{id, id, "", id, id, id, id, "", id, ""}
	if got := nodeDisclosureIDs(n); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("annotation IDs = %v, want %v", got, wantIDs)
	}
}

func TestNodeDisclosurePolicyPacketAndMalformedRecords(t *testing.T) {
	id := model.ObjectID(strings.Repeat("c", 64))
	packet := knowledge.ContextPacket{
		ID: id, Items: []knowledge.ContextItem{{Observation: id, Content: id}, {Observation: id}},
	}
	n := nodeProjection{detail: api.NodeDetail{
		Encoding: "knowledge/ContextPacket/v1+json", Data: nodePolicyJSON(t, packet),
		Sources: []model.NodeRef{{Kind: model.NodeObject, Object: id}, {Kind: model.NodeObject, Object: id}},
	}}
	want := []policyDependency{
		{kind: policyObject}, {kind: policyNode, id: id}, {kind: policyNode, id: id},
		{kind: policyNode}, {kind: policyNode, id: id}, {kind: policyNode},
	}
	if got := nodeDisclosureDependencies(n); !reflect.DeepEqual(got, want) {
		t.Fatalf("packet dependencies = %+v, want %+v", got, want)
	}
	if got := nodeDisclosureIDs(n); !reflect.DeepEqual(got, []model.ObjectID{"", id, id, "", id, ""}) {
		t.Fatalf("packet IDs changed: %v", got)
	}
	for _, encoding := range []string{"annotation/Record/v1+json", "knowledge/ContextPacket/v1+json", "knowledge/Summary/v1+json"} {
		n.detail.Encoding, n.detail.Data = encoding, []byte("{")
		if got := nodeDisclosureIDs(n); !reflect.DeepEqual(got, []model.ObjectID{"", id, id}) {
			t.Fatalf("%s malformed record changed fallback IDs: %v", encoding, got)
		}
	}
}

func TestNodeDisclosurePolicySummaryNamespacesAndSorting(t *testing.T) {
	a, b, c := model.ObjectID(strings.Repeat("a", 64)), model.ObjectID(strings.Repeat("b", 64)), model.ObjectID(strings.Repeat("c", 64))
	summary := knowledge.Summary{
		ID: c,
		Generation: knowledge.SummaryGeneration{
			Input: knowledge.RecordedContent{Object: b}, Output: knowledge.RecordedContent{Object: a},
		},
		Sources: []knowledge.SummarySource{
			{Observation: a, Content: c, PayloadKind: "memory"},
			{Observation: b, Content: b},
		},
		Omissions: []knowledge.SummaryOmission{{Observation: a}},
	}
	n := nodeProjection{detail: api.NodeDetail{
		Payload: c, Encoding: "knowledge/Summary/v1+json", Data: nodePolicyJSON(t, summary),
		Sources: []model.NodeRef{{Kind: model.NodeObject, Object: b}},
	}}
	for _, embedded := range []bool{false, true} {
		wantPayloadKind := policyObject
		if embedded {
			n.detail.Anchor.Knowledge = &model.KnowledgeKey{Actor: "actor", Execution: "execution"}
			wantPayloadKind = policySummary
		}
		want := []policyDependency{
			{kind: wantPayloadKind, id: c}, {kind: policyNode, id: b},
			{kind: policySummary, id: c}, {kind: policySummary},
			{kind: policyObject, id: a}, {kind: policyObject, id: b},
			{kind: policyObservation, id: a}, {kind: policyMemory, id: c},
			{kind: policyObservation, id: b}, {kind: policyObject, id: b},
			{kind: policyObservation, id: a},
		}
		if got := nodeDisclosureDependencies(n); !reflect.DeepEqual(got, want) {
			t.Fatalf("embedded=%v: dependencies = %+v, want %+v", embedded, got, want)
		}
		wantIDs := []model.ObjectID{c, b, c, "", a, b, a, c, b, b, a}
		if got := nodeDisclosureIDs(n); !reflect.DeepEqual(got, wantIDs) {
			t.Fatalf("embedded=%v: IDs = %v, want %v", embedded, got, wantIDs)
		}
	}
}

func TestNodeDisclosurePolicyPayloadNamespaces(t *testing.T) {
	id := model.ObjectID(strings.Repeat("d", 64))
	for _, tt := range []struct {
		encoding string
		kind     policyDependencyKind
	}{
		{"memory/Memory/v1+json", policyMemory},
		{"memory/ExperiencePath/v1+json", policyPath},
		{"memory/SourceBan/v1+json", policyNode},
		{"agent/ActivitySpec/v1+json", policyObject},
	} {
		n := nodeProjection{detail: api.NodeDetail{Payload: id, Encoding: tt.encoding}}
		if got := nodeDisclosureDependencies(n); !reflect.DeepEqual(got, []policyDependency{{kind: tt.kind, id: id}}) {
			t.Fatalf("%s dependency = %+v", tt.encoding, got)
		}
	}
}

func TestTraceRevisionPolicyDependenciesPreserveSortedContent(t *testing.T) {
	a, b, c := model.ObjectID(strings.Repeat("a", 64)), model.ObjectID(strings.Repeat("b", 64)), model.ObjectID(strings.Repeat("c", 64))
	revision := graph.Revision{
		ID: model.RevisionID(c), StateRoot: b,
		Entries: []graph.Entry{{Value: c}, {Value: a}, {Value: b}},
		Events:  []model.EventID{model.EventID(c), model.EventID(a)},
	}
	want := []policyDependency{
		{kind: policyRevision, id: c},
		{kind: policyObject, id: a}, {kind: policyObject, id: b}, {kind: policyObject, id: c},
	}
	if got := traceRevisionPolicyDependencies(revision); !reflect.DeepEqual(got, want) {
		t.Fatalf("revision dependencies = %+v, want %+v", got, want)
	}
	if got := policyIdentities(traceRevisionPolicyDependencies(revision)); !reflect.DeepEqual(got, []model.ObjectID{c, a, b, c}) {
		t.Fatalf("revision IDs changed: %v", got)
	}
}

func TestTraceOccurrencePolicyBoundaryPreservesResolvedIDs(t *testing.T) {
	id := model.ObjectID(strings.Repeat("e", 64))
	outcome := agentprofile.TaskOutcome{Result: id, ValidationRequirement: id}
	wantOutcome := []policyDependency{
		{kind: policyObject, id: id}, {kind: policyObject}, {kind: policyControl, id: id},
	}
	if got := traceOutcomePolicyDependencies(outcome); !reflect.DeepEqual(got, wantOutcome) {
		t.Fatalf("outcome dependencies = %+v, want %+v", got, wantOutcome)
	}
	provenance := traceProvenance{
		answers: map[string]agentprofile.QueryAnswer{"answer": {
			GeneratedFrom: "result", InputEvidence: id, Outcomes: []agentprofile.TaskOutcome{outcome},
		}},
		results: map[string]string{"result": "activity"},
		specs:   map[string]agentprofile.ActivitySpec{"activity": {}},
	}
	// Already resolved policy IDs can include logical records. Preserve this
	// list at the boundary without reclassifying it as physical objects.
	resolved := []model.ObjectID{"", id, id}
	activities := map[string][]model.ObjectID{"activity": resolved}
	event := model.Event{ID: model.EventID(id), Payload: id, Attribution: &model.EventAttribution{
		Subject: model.EntityKey{Profile: "agent", Kind: "QueryAnswer", ID: "answer"},
	}}
	got, err := (*Engine)(nil).traceOccurrenceDependencies(context.Background(), api.TaskTraceRequest{}, provenance, activities, event)
	want := []model.ObjectID{id, "", id, id, id, "", id, "", id, id}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("occurrence IDs = %v, %v; want %v", got, err, want)
	}
	if !reflect.DeepEqual(resolved, []model.ObjectID{"", id, id}) {
		t.Fatalf("resolved source list mutated: %v", resolved)
	}
}
