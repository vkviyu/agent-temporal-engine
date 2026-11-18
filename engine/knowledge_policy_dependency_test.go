package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func knowledgePolicyTestID(digit string) model.ObjectID {
	return model.ObjectID(strings.Repeat(digit, 64))
}

func TestKnowledgePolicyDependenciesPayloadKinds(t *testing.T) {
	id := knowledgePolicyTestID("a")
	for _, test := range []struct {
		name string
		kind string
		want policyDependencyKind
	}{
		{name: "raw", want: policyObject},
		{name: "memory", kind: "memory", want: policyMemory},
		{name: "path", kind: "experience_path", want: policyPath},
		{name: "summary", kind: "summary", want: policySummary},
		{name: "unknown", kind: "future-kind", want: policyNode},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Equal digests retain distinct roles until the identity boundary.
			observation := knowledge.Observation{ID: id, Payload: id, PayloadKind: test.kind, ImportedFrom: id}
			packet := knowledge.ContextPacket{
				ID: id, GoalRef: id, Input: &knowledge.RecordedInput{Object: id},
				Items: []knowledge.ContextItem{{Observation: id, Content: id}},
			}
			snapshot := knowledge.Snapshot{
				Observations: []knowledge.Observation{observation},
				Packets:      []knowledge.ContextPacket{packet},
			}
			wantObservation := []policyDependency{
				{kind: policyObservation, id: id},
				{kind: test.want, id: id},
				{kind: policyObservation, id: id},
			}
			wantPacket := []policyDependency{
				{kind: policyPacket, id: id},
				{kind: test.want, id: id},
				{kind: policyObject, id: id},
				{kind: policyObservation, id: id},
				{kind: test.want, id: id},
				{kind: test.want, id: id},
				{kind: policyObservation, id: id},
			}
			if got := observationPolicyDependencies(observation); !reflect.DeepEqual(got, wantObservation) {
				t.Fatalf("observation dependencies = %#v, want %#v", got, wantObservation)
			}
			if got := packetPolicyDependencies(packet, snapshot); !reflect.DeepEqual(got, wantPacket) {
				t.Fatalf("packet dependencies = %#v, want %#v", got, wantPacket)
			}
			wantKnowledge := append(wantObservation, wantPacket...)
			if got := knowledgePolicyDependencies(snapshot); !reflect.DeepEqual(got, wantKnowledge) {
				t.Fatalf("knowledge dependencies = %#v, want %#v", got, wantKnowledge)
			}
			if got, want := packetPolicyIDs(packet, snapshot), []model.ObjectID{id, id, id, id, id, id, id}; !reflect.DeepEqual(got, want) {
				t.Fatalf("packet IDs = %v, want %v", got, want)
			}
			if got, want := knowledgePolicyIDs(snapshot), []model.ObjectID{id, id, id, id, id, id, id, id, id, id}; !reflect.DeepEqual(got, want) {
				t.Fatalf("knowledge IDs = %v, want %v", got, want)
			}
		})
	}
}

func TestPacketPolicyDependenciesUnresolved(t *testing.T) {
	observationID, payload, other := knowledgePolicyTestID("a"), knowledgePolicyTestID("b"), knowledgePolicyTestID("c")
	snapshot := knowledge.Snapshot{Observations: []knowledge.Observation{
		{ID: observationID, Payload: payload, PayloadKind: "memory"},
	}}
	for _, test := range []struct {
		name string
		item knowledge.ContextItem
		want []policyDependency
	}{
		{
			name: "missing observation even with a known payload",
			item: knowledge.ContextItem{Observation: other, Content: payload},
			want: []policyDependency{{kind: policyObservation, id: other}, {kind: policyNode, id: payload}},
		},
		{
			name: "mismatched payload retains observation provenance",
			item: knowledge.ContextItem{Observation: observationID, Content: other},
			want: []policyDependency{
				{kind: policyObservation, id: observationID}, {kind: policyNode, id: other},
				{kind: policyMemory, id: payload}, {kind: policyObservation},
			},
		},
		{
			name: "empty item",
			want: []policyDependency{{kind: policyObservation}, {kind: policyNode}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			packet := knowledge.ContextPacket{GoalRef: other, Items: []knowledge.ContextItem{test.item}}
			want := append([]policyDependency{{kind: policyPacket}, {kind: policyNode, id: other}}, test.want...)
			if got := packetPolicyDependencies(packet, snapshot); !reflect.DeepEqual(got, want) {
				t.Fatalf("dependencies = %#v, want %#v", got, want)
			}
		})
	}
}

func TestPacketPolicyDependenciesAmbiguousGoal(t *testing.T) {
	id := knowledgePolicyTestID("a")
	rawID, memoryID, summaryID := knowledgePolicyTestID("b"), knowledgePolicyTestID("c"), knowledgePolicyTestID("d")
	raw := knowledge.Observation{ID: rawID, Payload: id}
	memory := knowledge.Observation{ID: memoryID, Payload: id, PayloadKind: "memory"}
	summary := knowledge.Observation{ID: summaryID, Payload: id, PayloadKind: "summary"}
	packet := knowledge.ContextPacket{
		GoalRef: id,
		Items: []knowledge.ContextItem{
			{Observation: rawID, Content: id},
			{Observation: memoryID, Content: id},
			{Observation: summaryID, Content: id},
		},
	}
	for _, observations := range [][]knowledge.Observation{
		{raw, memory, summary}, {summary, memory, raw}, {memory, raw, summary},
	} {
		got := packetPolicyDependencies(packet, knowledge.Snapshot{Observations: observations})
		if got[1] != (policyDependency{kind: policyNode, id: id}) {
			t.Fatalf("ambiguous goal = %#v", got[1])
		}
		for i, want := range []policyDependencyKind{policyObject, policyMemory, policySummary} {
			if got[3+i*4] != (policyDependency{kind: want, id: id}) {
				t.Fatalf("item %d content = %#v, want kind %s", i, got[3+i*4], want)
			}
		}
	}
	// Repeated observations in the same namespace do resolve the goal.
	got := packetPolicyDependencies(packet, knowledge.Snapshot{Observations: []knowledge.Observation{memory, memory}})
	if got[1] != (policyDependency{kind: policyMemory, id: id}) {
		t.Fatalf("unambiguous repeated goal = %#v", got[1])
	}
}

func TestKnowledgePolicyDependenciesLegacyOrder(t *testing.T) {
	a, b, c, d := knowledgePolicyTestID("a"), knowledgePolicyTestID("b"), knowledgePolicyTestID("c"), knowledgePolicyTestID("d")
	packet := knowledge.ContextPacket{
		ID: d, Input: &knowledge.RecordedInput{},
		Items: []knowledge.ContextItem{
			{Observation: a, Content: b},
			{Observation: c},
			{Observation: a, Content: b},
		},
	}
	snapshot := knowledge.Snapshot{
		Observations: []knowledge.Observation{
			{ID: a, Payload: b},
			{ID: a, Payload: c, PayloadKind: "summary", ImportedFrom: d},
		},
		Packets: []knowledge.ContextPacket{packet, {}},
	}
	// The legacy map uses the last observation for repeated IDs, while the
	// snapshot prefix still includes every observation in its original order.
	wantPacket := []model.ObjectID{d, "", "", a, b, c, d, c, "", a, b, c, d}
	if got := packetPolicyIDs(packet, snapshot); !reflect.DeepEqual(got, wantPacket) {
		t.Fatalf("packet IDs = %v, want %v", got, wantPacket)
	}
	wantKnowledge := []model.ObjectID{a, b, "", a, c, d, d, "", "", a, b, c, d, c, "", a, b, c, d, "", ""}
	if got := knowledgePolicyIDs(snapshot); !reflect.DeepEqual(got, wantKnowledge) {
		t.Fatalf("knowledge IDs = %v, want %v", got, wantKnowledge)
	}
	if got := knowledgePolicyIDs(knowledge.Snapshot{}); got == nil || len(got) != 0 {
		t.Fatalf("empty snapshot IDs = %#v, want non-nil empty slice", got)
	}
	if got, want := packetPolicyIDs(knowledge.ContextPacket{}, knowledge.Snapshot{}), []model.ObjectID{"", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("empty packet IDs = %v, want %v", got, want)
	}
}

func TestSummaryPolicyDependenciesLegacyOrderAndKinds(t *testing.T) {
	a, b, c, d := knowledgePolicyTestID("a"), knowledgePolicyTestID("b"), knowledgePolicyTestID("c"), knowledgePolicyTestID("d")
	summary := knowledge.Summary{
		ID: b,
		Generation: knowledge.SummaryGeneration{
			Input: knowledge.RecordedContent{Object: d}, Output: knowledge.RecordedContent{Object: b},
		},
		Validation: knowledge.SummaryValidation{Evidence: []knowledge.RecordedContent{
			{Object: c}, {Object: d}, {}, {Object: a},
		}},
		Sources: []knowledge.SummarySource{
			{Observation: a, Content: b, PayloadKind: "memory"},
			{Observation: b, Content: b, PayloadKind: "experience_path"},
			{Observation: c, Content: b, PayloadKind: "summary"},
			{Observation: d, Content: b},
			{Observation: a, Content: b, PayloadKind: "future-kind"},
			{},
		},
		Omissions: []knowledge.SummaryOmission{{Observation: c}, {}, {Observation: c}},
	}
	want := []policyDependency{
		{kind: policySummary, id: b}, {kind: policySummary},
		{kind: policyObject, id: a}, {kind: policyObject, id: b},
		{kind: policyObject, id: c}, {kind: policyObject, id: d},
		{kind: policyObservation, id: a}, {kind: policyMemory, id: b},
		{kind: policyObservation, id: b}, {kind: policyPath, id: b},
		{kind: policyObservation, id: c}, {kind: policySummary, id: b},
		{kind: policyObservation, id: d}, {kind: policyObject, id: b},
		{kind: policyObservation, id: a}, {kind: policyNode, id: b},
		{kind: policyObservation}, {kind: policyObject},
		{kind: policyObservation, id: c}, {kind: policyObservation}, {kind: policyObservation, id: c},
	}
	if got := summaryPolicyDependencies(summary); !reflect.DeepEqual(got, want) {
		t.Fatalf("dependencies = %#v, want %#v", got, want)
	}
	wantIDs := []model.ObjectID{b, "", a, b, c, d, a, b, b, b, c, b, d, b, a, b, "", "", c, "", c}
	if got := summaryPolicyIDs(summary); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("summary IDs = %v, want %v", got, wantIDs)
	}
	if got, want := summaryPolicyIDs(knowledge.Summary{}), []model.ObjectID{"", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("empty summary IDs = %v, want %v", got, want)
	}
}

func TestSummaryPolicyDependenciesTypedSourcesStayLogical(t *testing.T) {
	a, b, c := knowledgePolicyTestID("a"), knowledgePolicyTestID("b"), knowledgePolicyTestID("c")
	summary := knowledge.Summary{
		ID: a, Replaces: b,
		Sources: []knowledge.SummarySource{
			{Observation: a, Content: c, PayloadKind: "memory"},
			{Observation: b, Content: c, PayloadKind: "experience_path"},
			{Observation: c, Content: c, PayloadKind: "summary"},
		},
	}
	want := []policyDependency{
		{kind: policySummary, id: a}, {kind: policySummary, id: b},
		{kind: policyObservation, id: a}, {kind: policyMemory, id: c},
		{kind: policyObservation, id: b}, {kind: policyPath, id: c},
		{kind: policyObservation, id: c}, {kind: policySummary, id: c},
	}
	if got := summaryPolicyDependencies(summary); !reflect.DeepEqual(got, want) {
		t.Fatalf("dependencies = %#v, want %#v", got, want)
	}
}
