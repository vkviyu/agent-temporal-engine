package graph

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestCommandCodecCanonicalIntent(t *testing.T) {
	cmds := []Command{
		{AddRevision: &AddRevisionCommand{}},
		{AddRevision: &AddRevisionCommand{Root: oid('a'), Entries: []Entry{{Key: []byte("z"), Value: oid('b')}, {Key: []byte("a"), Value: oid('c')}}}},
		{Fork: &ForkCommand{Branch: "main", Source: model.RevisionID(oid('d'))}},
		{Advance: &AdvanceBranchCommand{Branch: "main", Expected: 19, Head: model.RevisionID(oid('e'))}},
		{Join: &JoinCommand{Branch: "main", Expected: 3, Left: model.RevisionID(oid('a')), Right: model.RevisionID(oid('b')), ExpectedDomainRoot: oid('c'), Resolutions: []Resolution{{Key: []byte("z"), Value: oid('d')}, {Key: []byte("a"), Value: ""}}}},
		{Prune: &PruneBranchCommand{Branch: "main", Expected: 4, Head: model.RevisionID(oid('a')), Reason: "retired exploration"}},
	}
	for _, c := range cmds {
		raw, err := EncodeCommand("space", c)
		if err != nil {
			t.Fatal(err)
		}
		space, decoded, err := DecodeCommand(raw)
		if err != nil || space != "space" {
			t.Fatalf("decode: %s %v", space, err)
		}
		again, err := EncodeCommand(space, decoded)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatalf("canonical: %v", err)
		}
		for n := 0; n < len(raw); n++ {
			if _, _, err = DecodeCommand(raw[:n]); err == nil {
				t.Fatalf("accepted truncation %d", n)
			}
		}
		if _, _, err = DecodeCommand(append(bytes.Clone(raw), 0)); err == nil {
			t.Fatal("accepted trailing data")
		}
	}
	c := cmds[1]
	raw, err := EncodeCommand("space", c)
	if err != nil {
		t.Fatal(err)
	}
	if string(c.AddRevision.Entries[0].Key) != "z" {
		t.Fatal("encoder sorted caller-owned entries")
	}
	_, owned, err := DecodeCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if owned.AddRevision.Entries[0].Value != oid('c') {
		t.Fatal("decode aliases input")
	}
}

func TestJoinIntentDoesNotAliasOrAcceptDuplicateResolutionSlots(t *testing.T) {
	c := Command{Join: &JoinCommand{Branch: "main", Left: model.RevisionID(oid('a')), Right: model.RevisionID(oid('b')), Resolutions: []Resolution{{Key: []byte("z"), Value: oid('c')}, {Key: []byte("a"), Value: oid('d')}}}}
	raw, err := EncodeCommand("s", c)
	if err != nil {
		t.Fatal(err)
	}
	if string(c.Join.Resolutions[0].Key) != "z" {
		t.Fatal("encoder reordered caller intent")
	}
	_, got, err := DecodeCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-4] ^= 1
	if string(got.Join.Resolutions[0].Key) != "a" {
		t.Fatal("decoder aliases wire bytes")
	}
	c.Join.Resolutions[1].Key = []byte("z")
	if _, err := EncodeCommand("s", c); !errors.Is(err, ErrCommandShape) {
		t.Fatal(err)
	}
	c.Join.Resolutions = nil
	c.Prune = &PruneBranchCommand{Branch: "main", Head: model.RevisionID(oid('a')), Reason: "x"}
	if _, err := EncodeCommand("s", c); !errors.Is(err, ErrCommandShape) {
		t.Fatal(err)
	}
}

func TestCommandCodecRejectsHiddenUnionAndRawStateBody(t *testing.T) {
	if _, err := EncodeCommand("s", Command{AddRevision: &AddRevisionCommand{}, Fork: &ForkCommand{Branch: "main", Source: model.RevisionID(oid('a'))}}); !errors.Is(err, ErrCommandShape) {
		t.Fatal(err)
	}
	if _, err := EncodeCommand("s", Command{}); !errors.Is(err, ErrCommandShape) {
		t.Fatal(err)
	}
	if _, _, err := DecodeCommand([]byte{1, 0, 1, 0, 1}); err == nil {
		t.Fatal("accepted raw mutation bytes")
	}
	raw := codec.AppendUint([]byte(commandDomain), 99)
	if _, _, err := DecodeCommand(raw); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatal(err)
	}
}
