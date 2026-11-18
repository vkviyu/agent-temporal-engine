package knowledge

import (
	"bytes"
	"errors"
	"fmt"
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
			if err := tc.snapshot.Validate(); !errors.Is(err, tc.want) {
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
			err := (Snapshot{Observations: tc.observations, Packets: []ContextPacket{candidate}}).Validate()
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	p.ID = dig('1')
	if err := (Snapshot{Observations: []Observation{o}, Packets: []ContextPacket{p}}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forged packet accepted: %v", err)
	}
}

func TestSnapshotValidationAndCloneOwnCallerData(t *testing.T) {
	o1, o2 := snapshotObservation("space", "one", 1), snapshotObservation("space", "one", 2)
	sourceKey := o1.Key
	o1.Source.Anchor.Knowledge = &sourceKey
	o1.Source.Anchor.Cursor = &model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(dig('1'))}
	o1.ID = ObservationID(o1)
	observations := []Observation{o1, o2}
	if observations[0].ID < observations[1].ID {
		observations[0], observations[1] = observations[1], observations[0]
	}
	original := append([]Observation(nil), observations...)
	p := snapshotPacket(o1)
	s := Snapshot{Observations: observations, Packets: []ContextPacket{p}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, observations) {
		t.Fatal("Validate reordered caller records")
	}
	next := s.Clone()
	if !reflect.DeepEqual(next, s) {
		t.Fatal("Clone changed snapshot values")
	}
	next.Packets[0].Key.Execution = "changed"
	next.Packets[0].Source.Anchor.Knowledge.Execution = "changed"
	next.Packets[0].Items[0].Label = "changed"
	if p.Key.Execution != "one" || p.Source.Anchor.Knowledge.Execution != "one" || p.Items[0].Label != "evidence" {
		t.Fatal("clone aliases caller packet")
	}
	next.Packets[0].Source.Anchor.Cursor.Revision = model.RevisionID(dig('2'))
	if p.Source.Anchor.Cursor.Revision != model.RevisionID(dig('1')) {
		t.Fatal("clone aliases packet cursor")
	}
	for i := range next.Observations {
		if next.Observations[i].ID == o1.ID {
			next.Observations[i].Source.Anchor.Knowledge.Execution = "changed"
			next.Observations[i].Source.Anchor.Cursor.Revision = model.RevisionID(dig('3'))
		}
	}
	next.Observations[0].Label = "changed"
	if o1.Source.Anchor.Knowledge.Execution != "one" || o1.Source.Anchor.Cursor.Revision != model.RevisionID(dig('1')) || !reflect.DeepEqual(original, observations) {
		t.Fatal("clone aliases caller observations")
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
		if raw[len(commandPrefix)] != 2 {
			t.Fatal("command encoder did not emit v2")
		}
		got, err := DecodeCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatal("command changed values on roundtrip")
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
	for _, op := range []uint64{0, 4, ^uint64(0)} {
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

func TestKnowledgeCommandRejectsUnsupportedVersions(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	p := snapshotPacket(o)
	source := o.Source.clone()
	source.Anchor.Knowledge = &o.Key
	source.Anchor.Cursor = &model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(dig('a'))}
	importCommand := ImportCommand{Target: model.KnowledgeKey{Actor: "receiver", Execution: "target"}, Source: source, Observations: []model.ObjectID{o.ID}}
	for name, command := range map[string]Command{"observation": {Observation: &o}, "packet": {Packet: &p}, "import": {Import: &importCommand}} {
		t.Run(name, func(t *testing.T) {
			raw, err := EncodeCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			for _, version := range []uint64{0, 1, 3, 127, 128, ^uint64(0)} {
				t.Run(fmt.Sprint(version), func(t *testing.T) {
					header := codec.AppendUint([]byte(commandPrefix), version)
					unsupported := append(append([]byte(nil), header...), raw[len(commandPrefix)+1:]...)
					for _, input := range [][]byte{header, unsupported} {
						got, err := DecodeCommand(input)
						if !errors.Is(err, ErrUnsupportedSchema) || !reflect.DeepEqual(got, Command{}) {
							t.Fatalf("unsupported command returned %+v, %v", got, err)
						}
					}
				})
			}
		})
	}
	// Missing, truncated, and noncanonical version encodings are malformed,
	// rather than successfully decoded unsupported schemas.
	for _, versionBytes := range [][]byte{nil, {0x80}, {0x82, 0x00}} {
		raw := append([]byte(commandPrefix), versionBytes...)
		if _, err := DecodeCommand(raw); !errors.Is(err, ErrInvalid) {
			t.Fatalf("malformed version returned %v", err)
		}
	}
}

func TestSnapshotDTOBoundsAndDuplicatePackets(t *testing.T) {
	if err := (Snapshot{}).Validate(); err != nil {
		t.Fatal("empty DTO rejected", err)
	}
	for _, s := range []Snapshot{
		{Observations: make([]Observation, MaxSnapshotRecords+1)},
		{Packets: make([]ContextPacket, MaxSnapshotRecords+1)},
	} {
		if err := s.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("oversized DTO returned %v", err)
		}
	}
	o := snapshotObservation("space", "one", MaxSnapshotRecords+1)
	if err := (Snapshot{Observations: []Observation{o}}).Validate(); !errors.Is(err, ErrObservationSequence) {
		t.Fatalf("out-of-range sequence returned %v", err)
	}
	o = snapshotObservation("space", "one", 1)
	p := snapshotPacket(o)
	if err := (Snapshot{Observations: []Observation{o}, Packets: []ContextPacket{p, p}}).Validate(); !errors.Is(err, ErrDuplicateItem) {
		t.Fatalf("duplicate packet returned %v", err)
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
