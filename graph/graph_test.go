package graph

import (
	"bytes"
	"errors"
	"github.com/vkviyu/agent-temporal-engine/model"
	"strings"
	"testing"
)

func oid(ch byte) model.ObjectID { return model.ObjectID(strings.Repeat(string(ch), 64)) }
func TestRevisionForkDiffJoinDeterministic(t *testing.T) {
	s := New()
	root, err := s.AddRevision("s", "", nil, oid('a'), nil, []Entry{{[]byte("a"), oid('1')}})
	if err != nil {
		t.Fatal(err)
	}
	left, err := s.AddRevision("s", root.ID, nil, oid('b'), nil, []Entry{{[]byte("a"), oid('2')}, {[]byte("l"), oid('3')}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := s.AddRevision("s", root.ID, nil, oid('c'), nil, []Entry{{[]byte("a"), oid('2')}, {[]byte("r"), oid('4')}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Fork("main", root.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdvanceBranch("main", 0, left.ID); err != nil {
		t.Fatal(err)
	}
	d, err := s.Diff(root.ID, left.ID)
	if err != nil || len(d.Changes) != 2 || d.Changes[0].Kind != ChangeUpdate {
		t.Fatalf("diff=%+v err=%v", d, err)
	}
	p, err := s.PlanJoin(left.ID, right.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Conflicts) != 0 || len(p.State) != 3 {
		t.Fatalf("plan=%+v", p)
	}
	joined, err := s.CommitJoin(p, oid('d'), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(joined.JoinParents) != 1 || joined.StateParent != left.ID {
		t.Fatalf("joined=%+v", joined)
	}
	if _, err := s.AddRevision("s", root.ID, []model.RevisionID{root.ID}, oid('e'), nil, nil); !errors.Is(err, ErrRevisionCycle) {
		t.Fatal(err)
	}
}
func TestJoinConflictMustResolveExplicitly(t *testing.T) {
	s := New()
	root, _ := s.AddRevision("s", "", nil, oid('a'), nil, []Entry{{[]byte("k"), oid('1')}})
	l, _ := s.AddRevision("s", root.ID, nil, oid('b'), nil, []Entry{{[]byte("k"), oid('2')}})
	r, _ := s.AddRevision("s", root.ID, nil, oid('c'), nil, []Entry{{[]byte("k"), oid('3')}})
	p, err := s.PlanJoin(l.ID, r.ID)
	if err != nil || len(p.Conflicts) != 1 {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err = s.CommitJoin(p, oid('d'), nil); !errors.Is(err, ErrJoinConflict) {
		t.Fatal(err)
	}
	if err := p.Resolve([]byte("k"), oid('4')); err != nil {
		t.Fatal(err)
	}
	if len(p.Conflicts) != 0 || len(p.State) != 1 || !bytes.Equal(p.State[0].Key, []byte("k")) {
		t.Fatalf("resolved=%+v", p)
	}
	if _, err = s.CommitJoin(p, oid('e'), nil); err != nil {
		t.Fatal(err)
	}
}

func TestJoinUsesUnchangedSideForDeletes(t *testing.T) {
	s := New()
	root, _ := s.AddRevision("s", "", nil, oid('a'), nil, []Entry{{[]byte("k"), oid('1')}})
	left, _ := s.AddRevision("s", root.ID, nil, oid('b'), nil, nil)
	right, _ := s.AddRevision("s", root.ID, nil, oid('c'), nil, []Entry{{[]byte("k"), oid('1')}})
	p, err := s.PlanJoin(left.ID, right.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Conflicts) != 0 || len(p.State) != 0 {
		t.Fatalf("delete merge not deterministic: %+v", p)
	}
}

func TestRevisionCodecOwnsBytesAndRejectsIdentityMutation(t *testing.T) {
	s := New()
	r, err := s.AddRevision("space", "", nil, oid('a'), []model.EventID{model.EventID(oid('e'))}, []Entry{{Key: []byte("z"), Value: oid('1')}, {Key: []byte("a"), Value: oid('2')}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRevision(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRevision(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != r.ID || len(decoded.Entries) != 2 || string(decoded.Entries[0].Key) != "a" {
		t.Fatalf("decoded=%+v", decoded)
	}
	raw[0] ^= 1
	if _, err := DecodeRevision(raw); err == nil {
		t.Fatal("mutated schema accepted")
	}
	r.ID = model.RevisionID(oid('f'))
	if _, err := EncodeRevision(r); !errors.Is(err, ErrInvalidRevision) {
		t.Fatal(err)
	}
}
func TestBranchCodecRoundTrip(t *testing.T) {
	want := Branch{ID: "main", Head: model.RevisionID(oid('a')), Version: 7}
	raw, err := EncodeBranch(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBranch(raw)
	if err != nil || got != want {
		t.Fatalf("%+v %v", got, err)
	}
}
