package model_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestNodeRefTaggedUnion(t *testing.T) {
	entity := model.EntityKey{Profile: "agent", Kind: "task", ID: "t"}
	digest := model.ObjectID(strings.Repeat("a", 64))
	valid := []model.NodeRef{
		{Kind: model.NodeEntity, Entity: entity},
		{Kind: model.NodeEvent, Event: model.EventID(strings.Repeat("b", 64))},
		{Kind: model.NodeRevision, Revision: model.RevisionID(strings.Repeat("c", 64))},
		{Kind: model.NodeObject, Object: digest},
	}
	for _, ref := range valid {
		if err := ref.Validate(); err != nil || !ref.Valid() || ref.String() == "" {
			t.Fatalf("valid reference rejected: %+v, %v", ref, err)
		}
	}
	invalid := []model.NodeRef{
		{}, {Kind: model.NodeEntity},
		{Kind: model.NodeEntity, Entity: model.EntityKey{Kind: "task", ID: "t"}},
		{Kind: model.NodeEvent, Revision: model.RevisionID(strings.Repeat("c", 64))},
		{Kind: model.NodeEvent, Event: model.EventID(strings.Repeat("b", 64)), Revision: model.RevisionID(strings.Repeat("c", 64))},
		{Kind: model.NodeEntity, Entity: entity, Object: digest},
		{Kind: "unknown", Event: model.EventID(strings.Repeat("b", 64))},
		{Kind: model.NodeObject, Object: "short"},
		{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("A", 64))},
		{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("z", 64))},
	}
	for _, ref := range invalid {
		if err := ref.Validate(); !errors.Is(err, model.ErrInvalidNodeRef) || ref.Valid() || ref.String() != "" {
			t.Fatalf("invalid reference accepted: %+v, %v", ref, err)
		}
	}
}

func anchor() model.SnapshotAnchor {
	return model.SnapshotAnchor{
		Fence: model.CommitFence{Store: "store", LSN: 7, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))},
		Space: "space", Cursor: &model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(strings.Repeat("d", 64))},
		Knowledge: &model.KnowledgeKey{Actor: "actor", Execution: "execution"},
	}
}

func TestAnchorStructuralBoundaries(t *testing.T) {
	valid := anchor()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	fenceOnly := valid
	fenceOnly.Cursor = nil
	fenceOnly.Knowledge = nil
	if err := fenceOnly.Validate(); err != nil {
		t.Fatalf("fence-only anchor rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*model.SnapshotAnchor)
		want   error
	}{
		{"missing store", func(a *model.SnapshotAnchor) { a.Fence.Store = "" }, model.ErrInvalidFence},
		{"invalid digest", func(a *model.SnapshotAnchor) { a.Fence.CommitDigest = "bad" }, model.ErrInvalidFence},
		{"missing space", func(a *model.SnapshotAnchor) { a.Space = "" }, model.ErrAnchorMismatch},
		{"cross store", func(a *model.SnapshotAnchor) { a.Cursor.Store = "other" }, model.ErrAnchorMismatch},
		{"cross space", func(a *model.SnapshotAnchor) { a.Cursor.Space = "other" }, model.ErrAnchorMismatch},
		{"missing revision", func(a *model.SnapshotAnchor) { a.Cursor.Revision = "" }, model.ErrAnchorMismatch},
		{"missing actor", func(a *model.SnapshotAnchor) { a.Knowledge.Actor = "" }, model.ErrAnchorMismatch},
		{"missing execution", func(a *model.SnapshotAnchor) { a.Knowledge.Execution = "" }, model.ErrAnchorMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := anchor()
			tc.mutate(&a)
			if err := a.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAnchorCloneOwnsSelectors(t *testing.T) {
	original := anchor()
	clone := original.Clone()
	if !reflect.DeepEqual(original, clone) {
		t.Fatal("clone changed anchor value")
	}
	clone.Cursor.Revision = "changed"
	clone.Knowledge.Execution = "changed"
	if original.Cursor.Revision != model.RevisionID(strings.Repeat("d", 64)) || original.Knowledge.Execution != "execution" {
		t.Fatal("clone aliases original selectors")
	}
	original.Cursor.Space = "another"
	original.Knowledge.Actor = "another"
	if clone.Cursor.Space != "space" || clone.Knowledge.Actor != "actor" {
		t.Fatal("original aliases cloned selectors")
	}
	empty := model.SnapshotAnchor{}
	if got := empty.Clone(); got.Cursor != nil || got.Knowledge != nil {
		t.Fatal("clone invented selectors")
	}
}

func TestRootSetStructuralValidation(t *testing.T) {
	if err := (model.RootSet{}).Validate(); err != nil {
		t.Fatal(err)
	}
	valid := model.RootSet{DomainStates: model.ObjectID(strings.Repeat("a", 64)), BranchRefs: model.ObjectID(strings.Repeat("b", 64))}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []model.RootSet{{DomainStates: "short"}, {LiveControl: model.ObjectID(strings.Repeat("0", 64))}, {CommandReceipts: model.ObjectID(strings.Repeat("A", 64))}} {
		if err := bad.Validate(); !errors.Is(err, model.ErrInvalidRootSet) {
			t.Fatalf("bad root set accepted: %+v err=%v", bad, err)
		}
	}
}
