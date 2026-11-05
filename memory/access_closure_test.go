package memory

import (
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestAccessPolicyDependencyClosureIsCanonicalAndOwned(t *testing.T) {
	a, b, c, d := digest('a'), digest('b'), digest('c'), digest('d')
	p := AccessPolicy{dependencies: map[model.ObjectID][]model.ObjectID{a: {b, c}, b: {d}, c: {d}, d: {a}}}
	want := []model.ObjectID{a, b, c, d}
	got, err := p.DependencyClosure([]model.ObjectID{c, a, a, ""})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("closure=%v %v", got, err)
	}
	got[0] = d
	again, err := p.DependencyClosure([]model.ObjectID{a})
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("caller mutated policy closure: %v %v", again, err)
	}
	if _, err := p.DependencyClosure([]model.ObjectID{"invalid"}); err == nil {
		t.Fatal("invalid identity admitted")
	}
}
