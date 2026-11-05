package knowledge

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func snapshotObservation(space model.SpaceID, execution model.ExecutionID, sequence uint64) Observation {
	o := Observation{
		Key:     model.KnowledgeKey{Actor: "same-actor", Execution: execution},
		Source:  SourceAnchor{Anchor: model.SnapshotAnchor{Space: space, Fence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(dig('a'))}}},
		Payload: dig('b'), Sequence: sequence, Label: "read-result",
	}
	o.ID = ObservationID(o)
	return o
}

func snapshotPacket(o Observation) ContextPacket {
	key := o.Key
	anchor := o.Source.Anchor.Clone()
	anchor.Knowledge = &key
	anchor.Fence.LSN = 2
	anchor.Fence.CommitDigest = model.CommitDigest(dig('c'))
	p := ContextPacket{
		Space: o.Source.Anchor.Space, Key: &key,
		Source:  SourceAnchor{Anchor: anchor, Frontier: dig('d'), ViewDigest: dig('e')},
		GoalRef: dig('f'), Items: []ContextItem{{Ordinal: 0, Observation: o.ID, Content: o.Payload, Label: "evidence"}},
		Renderer: "renderer/v1", Tokenizer: "tokenizer/v1", EstimatedTokens: 3,
	}
	p.ID = PacketID(p)
	return p
}

func TestSnapshotRejectsForgedIdentityAndNoncontiguousSequence(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	badID := o
	badID.ID = dig('f')
	cases := []struct {
		name     string
		snapshot Snapshot
		want     error
	}{
		{"forged observation ID", Snapshot{Observations: []Observation{badID}}, ErrInvalid},
		{"duplicate observation", Snapshot{Observations: []Observation{o, o}}, ErrDuplicateItem},
		{"missing first sequence", Snapshot{Observations: []Observation{snapshotObservation("space", "one", 2)}}, ErrObservationSequence},
		{"sequence gap", Snapshot{Observations: []Observation{o, snapshotObservation("space", "one", 3)}}, ErrObservationSequence},
	}
	duplicateSequence := o
	duplicateSequence.Label = "different observation same slot"
	duplicateSequence.ID = ObservationID(duplicateSequence)
	cases = append(cases, struct {
		name     string
		snapshot Snapshot
		want     error
	}{"same slot different content", Snapshot{Observations: []Observation{o, duplicateSequence}}, ErrObservationSequence})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EncodeSnapshot(tc.snapshot); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	// Space and Execution are both in the sequence namespace.
	if err := (Snapshot{Observations: []Observation{o, snapshotObservation("space", "two", 1), snapshotObservation("other-space", "one", 1)}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPacketRequiresObservedMatchingEvidence(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	p := snapshotPacket(o)
	cases := []struct {
		name         string
		mutate       func(*ContextPacket)
		observations []Observation
		want         error
	}{
		{"missing evidence", func(*ContextPacket) {}, nil, ErrUnknownObservation},
		{"payload mismatch", func(p *ContextPacket) { p.Items[0].Content = dig('9') }, []Observation{o}, ErrInvalid},
		{"other execution", func(p *ContextPacket) { p.Key.Execution = "two"; p.Source.Anchor.Knowledge.Execution = "two" }, []Observation{o}, ErrKnowledgeMismatch},
		{"other space", func(p *ContextPacket) { p.Space = "other"; p.Source.Anchor.Space = "other" }, []Observation{o}, ErrInvalid},
		{"future source", func(p *ContextPacket) { p.Source.Anchor.Fence.LSN = 0 }, []Observation{o}, ErrFutureSource},
		{"other store", func(p *ContextPacket) { p.Source.Anchor.Fence.Store = "other" }, []Observation{o}, ErrFutureSource},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := clonePacket(p)
			tc.mutate(&candidate)
			candidate.ID = PacketID(candidate)
			_, err := ApplyCommand(Snapshot{Observations: tc.observations}, Command{Packet: &candidate})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	p.ID = dig('1')
	if _, err := ApplyCommand(Snapshot{Observations: []Observation{o}}, Command{Packet: &p}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forged packet accepted: %v", err)
	}
}

func TestSnapshotAndCommandDoNotAliasOrSortCallerData(t *testing.T) {
	o1, o2 := snapshotObservation("space", "one", 1), snapshotObservation("space", "one", 2)
	observations := []Observation{o1, o2}
	if observations[0].ID < observations[1].ID {
		observations[0], observations[1] = observations[1], observations[0]
	}
	original := append([]Observation(nil), observations...)
	s := Snapshot{Observations: observations}
	if _, err := EncodeSnapshot(s); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, observations) {
		t.Fatal("EncodeSnapshot reordered caller records")
	}
	p := snapshotPacket(o1)
	next, err := ApplyCommand(s, Command{Packet: &p})
	if err != nil {
		t.Fatal(err)
	}
	next.Packets[0].Key.Execution = "changed"
	next.Packets[0].Source.Anchor.Knowledge.Execution = "changed"
	next.Packets[0].Items[0].Label = "changed"
	if p.Key.Execution != "one" || p.Source.Anchor.Knowledge.Execution != "one" || p.Items[0].Label != "evidence" {
		t.Fatal("candidate aliases caller packet")
	}
	duplicate, err := ApplyCommand(s, Command{Observation: &o1})
	if err != nil {
		t.Fatal(err)
	}
	duplicate.Observations[0].Source.Anchor.Cursor = &model.Cursor{Revision: model.RevisionID(dig('1'))}
	if s.Observations[0].Source.Anchor.Cursor != nil {
		t.Fatal("idempotent return aliases original snapshot")
	}
	before, err := EncodeSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	invalid := snapshotObservation("space", "one", 4)
	if _, err = ApplyCommand(s, Command{Observation: &invalid}); !errors.Is(err, ErrObservationSequence) {
		t.Fatal(err)
	}
	after, err := EncodeSnapshot(s)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected candidate changed source")
	}
}

func TestKnowledgeCommandClosedCanonicalRoundTrip(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	p := snapshotPacket(o)
	for _, c := range []Command{{Observation: &o}, {Packet: &p}} {
		raw, err := EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := EncodeCommand(got)
		if err != nil || !bytes.Equal(raw, canonical) {
			t.Fatal("command changed on roundtrip")
		}
		for n := 0; n < len(raw); n++ {
			if _, err = DecodeCommand(raw[:n]); err == nil {
				t.Fatalf("truncation accepted at %d", n)
			}
		}
		if _, err = DecodeCommand(append(raw, 0)); err == nil {
			t.Fatal("trailing command byte accepted")
		}
	}
	for _, c := range []Command{{}, {Observation: &o, Packet: &p}} {
		if _, err := EncodeCommand(c); err == nil {
			t.Fatal("open command union accepted")
		}
	}
	for _, op := range []uint64{0, 3} {
		b := codec.AppendUint([]byte(commandPrefix), commandVersion)
		b = codec.AppendUint(b, op)
		b, _ = codec.AppendBytes(b, nil)
		if _, err := DecodeCommand(b); err == nil {
			t.Fatal("unknown operation accepted")
		}
	}
	if _, err := DecodeCommand([]byte(`{"Observation":{}}`)); err == nil {
		t.Fatal("JSON command used as identity")
	}
}

func TestKnowledgeSnapshotCanonicalAndBounded(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	p := snapshotPacket(o)
	p.Items = []ContextItem{} // retain legacy JSON empty-array representation
	p.ID = PacketID(p)
	s := Snapshot{Observations: []Observation{o}, Packets: []ContextPacket{p}}
	expectedJSON, err := json.Marshal(snapshotWire{Observations: s.Observations, Packets: s.Packets})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, append([]byte(snapshotPrefix), expectedJSON...)) {
		t.Fatal("snapshot byte compatibility changed")
	}
	if _, err = DecodeSnapshot(raw); err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeSnapshot(append(raw, ' ')); err == nil {
		t.Fatal("noncanonical whitespace accepted")
	}
	if _, err = DecodeSnapshot(make([]byte, MaxSnapshotBytes+1)); err == nil {
		t.Fatal("oversize snapshot accepted")
	}
}

func TestSelectSnapshotAndFrontierIsolateExecutions(t *testing.T) {
	one, two := snapshotObservation("space", "one", 1), snapshotObservation("space", "two", 1)
	packet := snapshotPacket(one)
	s := Snapshot{Observations: []Observation{two, one}, Packets: []ContextPacket{packet}}
	selected, err := SelectSnapshot(s, one.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Observations) != 1 || selected.Observations[0].ID != one.ID || len(selected.Packets) != 1 {
		t.Fatalf("unexpected projection: %+v", selected)
	}
	frontier, err := Frontier(s, one.Key)
	if err != nil {
		t.Fatal(err)
	}
	withoutPacket, err := Frontier(Snapshot{Observations: []Observation{one}}, one.Key)
	if err != nil || frontier != withoutPacket {
		t.Fatal("packet changed observation frontier")
	}
	other, err := Frontier(s, two.Key)
	if err != nil || frontier == other {
		t.Fatal("frontier did not bind execution")
	}
	selected.Packets[0].Key.Execution = "mutated"
	if packet.Key.Execution != "one" {
		t.Fatal("projection aliases packet")
	}
	next := snapshotObservation("space", "one", 2)
	s.Observations = append(s.Observations, next)
	changed, err := Frontier(s, one.Key)
	if err != nil || changed == frontier {
		t.Fatal("new observation did not advance frontier")
	}
}
