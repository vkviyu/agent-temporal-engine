package graph

import (
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestReferencesKeepLogicalGraphIDsSeparateFromCodecContent(t *testing.T) {
	r := Revision{ID: "not-a-raw-object", StateRoot: oid('b'), StateParent: model.RevisionID(oid('c')), JoinParents: []model.RevisionID{model.RevisionID(oid('d')), model.RevisionID(oid('c'))}, Events: []model.EventID{model.EventID(oid('e'))}, Entries: []Entry{{Value: oid('a')}, {Value: oid('b')}}}
	if got, want := ContentReferences(r), []model.ObjectID{oid('a'), oid('b'), oid('e')}; !reflect.DeepEqual(got, want) {
		t.Fatalf("content=%+v want=%+v", got, want)
	}
	parents := RevisionReferences(r)
	if want := []model.RevisionID{model.RevisionID(oid('c')), model.RevisionID(oid('d'))}; !reflect.DeepEqual(parents, want) {
		t.Fatalf("logical parents=%+v", parents)
	}
	parents[0] = "changed"
	if r.JoinParents[0] == "changed" || r.JoinParents[1] == "changed" {
		t.Fatal("extractor aliases graph metadata")
	}
}
