package knowledge

import (
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestReferenceClassesAndOwnedHistoricalSelectors(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	k := o.Key
	o.Source.Anchor.Knowledge = &k
	o.Source.Anchor.Cursor = &model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(dig('a'))}
	s := Snapshot{Observations: []Observation{o}, Packets: []ContextPacket{{ID: dig('c'), GoalRef: dig('d'), Items: []ContextItem{{Observation: dig('e'), Content: o.Payload}}, Source: SourceAnchor{ViewDigest: dig('f'), Frontier: dig('a')}}}}
	if got, want := ContentReferences(s), []model.ObjectID{o.Payload, dig('d')}; !reflect.DeepEqual(got, want) {
		t.Fatalf("actual content=%+v want=%+v", got, want)
	}
	anchors := SourceAnchors(s)
	anchors[0].Anchor.Knowledge.Execution = "changed"
	anchors[0].Anchor.Cursor.Revision = "changed"
	if o.Source.Anchor.Knowledge.Execution == "changed" || o.Source.Anchor.Cursor.Revision == "changed" {
		t.Fatal("source selectors were not owned")
	}
}

func TestTypedSummaryGoalRetainsRawInputWithoutInventingObjectAddress(t *testing.T) {
	// Summary reuse makes its logical ID both an observation payload and a
	// packet GoalRef. Only the exact provider input has a raw codec address.
	// Reopen/compaction must not try to load the SummaryID from the object store.
	summary, observation, input, rawGoal := dig('a'), dig('b'), dig('c'), dig('d')
	s := Snapshot{
		Observations: []Observation{{ID: observation, Payload: summary, PayloadKind: "summary"}},
		Packets: []ContextPacket{
			{GoalRef: summary, Items: []ContextItem{{Observation: observation, Content: summary}}, Input: &RecordedInput{Object: input}},
			{GoalRef: rawGoal},
		},
	}
	if got, want := ContentReferences(s), []model.ObjectID{input, rawGoal}; !reflect.DeepEqual(got, want) {
		t.Fatalf("physical references=%v want=%v", got, want)
	}
}
