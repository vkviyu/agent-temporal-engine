package knowledge

import (
	"bytes"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestTypedObservationCanonicalRoundTrip(t *testing.T) {
	fence := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digestString('a'))}
	o := Observation{
		Key:         model.KnowledgeKey{Actor: "actor", Execution: "execution"},
		Source:      SourceAnchor{Anchor: model.SnapshotAnchor{Space: "space", Fence: fence}},
		Payload:     model.ObjectID(digestString('b')),
		PayloadKind: "experience_path",
		Sequence:    1,
		Label:       "promoted path",
	}
	o.ID = ObservationID(o)
	raw, err := o.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != 3 {
		t.Fatalf("typed observation schema=%d", raw[0])
	}
	got, err := DecodeObservation(raw)
	if err != nil || got.PayloadKind != o.PayloadKind || got.ID != o.ID {
		t.Fatalf("round trip=%+v %v", got, err)
	}
	if again, err := got.Canonical(); err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("canonical bytes changed: %x %v", again, err)
	}
}

func TestTypedImportPreservesKindAndCanonicalIdentity(t *testing.T) {
	for _, kind := range []string{"memory", "experience_path"} {
		t.Run(kind, func(t *testing.T) {
			origin := snapshotObservation("space", "source", 1)
			origin.PayloadKind = kind
			origin.ID = ObservationID(origin)
			before := Snapshot{Observations: []Observation{origin}}
			chain, err := Revisions(before, "space", origin.Key)
			if err != nil {
				t.Fatal(err)
			}
			anchor := origin.Source.Anchor.Clone()
			key := origin.Key
			anchor.Knowledge = &key
			anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: "space", Revision: chain[len(chain)-1].ID}
			command := Command{Import: &ImportCommand{Target: model.KnowledgeKey{Actor: "receiver", Execution: "target"}, Source: SourceAnchor{Anchor: anchor}, Observations: []model.ObjectID{origin.ID}}}
			after, err := ApplyCommand(before, command)
			if err != nil {
				t.Fatal(err)
			}
			imported := after.Observations[len(after.Observations)-1]
			if imported.PayloadKind != kind || imported.ImportedFrom != origin.ID || imported.Payload != origin.Payload || imported.Key != command.Import.Target {
				t.Fatalf("import lost typed provenance: %+v", imported)
			}
			raw, err := imported.Canonical()
			if err != nil || len(raw) == 0 || raw[0] != 4 {
				t.Fatalf("typed import encoding: %x %v", raw, err)
			}
			decoded, err := DecodeObservation(raw)
			if err != nil || decoded.ID != imported.ID || decoded.PayloadKind != kind || decoded.ImportedFrom != origin.ID {
				t.Fatalf("typed import cannot round-trip: %+v %v", decoded, err)
			}
			for n := range len(raw) {
				if _, err := DecodeObservation(raw[:n]); err == nil {
					t.Fatalf("accepted typed import truncated at %d", n)
				}
			}
			image, err := EncodeSnapshot(after)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := DecodeSnapshot(image)
			if err != nil || len(restored.Observations) != 2 {
				t.Fatalf("typed import snapshot restore: %+v %v", restored, err)
			}
		})
	}
}

func TestTypedPayloadIsNotTreatedAsRawContentReference(t *testing.T) {
	fence := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digestString('a'))}
	o := Observation{Key: model.KnowledgeKey{Actor: "actor", Execution: "execution"}, Source: SourceAnchor{Anchor: model.SnapshotAnchor{Space: "space", Fence: fence}}, Payload: model.ObjectID(digestString('b')), PayloadKind: "memory", Sequence: 1, Label: "memory"}
	o.ID = ObservationID(o)
	packet := ContextPacket{Space: "space", Key: &o.Key, Source: SourceAnchor{Anchor: model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "store", LSN: 2, CommitDigest: model.CommitDigest(digestString('c'))}, Knowledge: &o.Key}}, GoalRef: model.ObjectID(digestString('d')), Items: []ContextItem{{Ordinal: 0, Observation: o.ID, Content: o.Payload, Label: "memory"}}, Renderer: "r", Tokenizer: "t"}
	packet.ID = PacketID(packet)
	refs := ContentReferences(Snapshot{Observations: []Observation{o}, Packets: []ContextPacket{packet}})
	for _, id := range refs {
		if id == o.Payload {
			t.Fatal("typed logical ID leaked into raw content closure")
		}
	}
}

func digestString(ch byte) string {
	return string(bytes.Repeat([]byte{ch}, 64))
}
