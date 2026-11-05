package knowledge

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestKnowledgeRevisionIndependentVectors(t *testing.T) {
	// Independently computed using Python hashlib and shortest unsigned
	// varints, not the implementation's encoder.
	key := model.KnowledgeKey{Actor: "actor", Execution: "execution"}
	genesis := makeRevision("space", key, Revision{}, "", 0)
	if genesis.ID != "fe8367e3675b639133d37d0598ca63b54c3b7240973d11c7fb5d8ac3c700b6d6" || genesis.StateRoot != "4e270fb8ce865f78c535f2c0558c113a87e8e35daf040172543cc7d38141c239" {
		t.Fatalf("genesis vector: %+v", genesis)
	}
	one := makeRevision("space", key, genesis, dig('b'), 1)
	if one.ID != "45fca3ce0b5b1521e05be6e9e36b2487402b9af550282118883c484cb520d1f8" || one.StateRoot != "26e53fc1eabaa1dfc56c062c171a11e2ca5eb09418b80d3adc863fee68321c81" {
		t.Fatalf("first vector: %+v", one)
	}
}

func TestKnowledgeRevisionExactCutScopeAndPacketIndependence(t *testing.T) {
	one, two := snapshotObservation("space", "one", 1), snapshotObservation("space", "one", 2)
	two.Payload = dig('c')
	two.ID = ObservationID(two)
	other := snapshotObservation("space", "two", 1)
	s := Snapshot{Observations: []Observation{two, other, one}}
	chain, err := Revisions(s, "space", one.Key)
	if err != nil || len(chain) != 3 || chain[2].Parent != chain[1].ID || chain[1].Parent != chain[0].ID {
		t.Fatalf("chain=%+v %v", chain, err)
	}
	packet := snapshotPacket(one)
	packet.GoalRef = one.Payload
	packet.Source.Anchor.Cursor = &model.Cursor{Store: "store", Space: "space", Revision: chain[1].ID}
	packet.ID = PacketID(packet)
	s.Packets = []ContextPacket{packet}
	after, err := Revisions(s, "space", one.Key)
	if err != nil || !reflect.DeepEqual(chain, after) {
		t.Fatal("rendering a packet advanced knowledge")
	}
	selected, r, err := SelectRevision(s, "space", one.Key, chain[1].ID)
	if err != nil || r != chain[1] || len(selected.Observations) != 1 || selected.Observations[0].ID != one.ID || len(selected.Packets) != 1 {
		t.Fatalf("exact prefix=%+v %+v %v", selected, r, err)
	}
	selected.Observations[0].Label = "changed"
	selected.Packets[0].Items[0].Label = "changed"
	if s.Observations[2].Label == "changed" || s.Packets[0].Items[0].Label == "changed" {
		t.Fatal("selected snapshot retained mutable aliases")
	}
	if _, _, err = SelectRevision(s, "space", other.Key, chain[1].ID); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("another execution selected this cursor: %v", err)
	}
	if _, _, err = SelectRevision(s, "other-space", one.Key, chain[1].ID); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("another space selected this cursor: %v", err)
	}
	if _, _, err = SelectRevision(Snapshot{Observations: []Observation{one}}, "space", one.Key, chain[2].ID); !errors.Is(err, model.ErrAnchorMismatch) {
		t.Fatalf("future cursor accepted at old cut: %v", err)
	}
	// A packet with a later source revision is not disclosed at the old
	// knowledge position, even if it happens to include only old payloads.
	s.Packets[0].Source.Anchor.Cursor.Revision = chain[2].ID
	s.Packets[0].ID = PacketID(s.Packets[0])
	selected, _, err = SelectRevision(s, "space", one.Key, chain[1].ID)
	if err != nil || len(selected.Packets) != 0 {
		t.Fatalf("future source metadata leaked=%+v %v", selected, err)
	}
}

func TestKnowledgeRevisionDeterministicEmptyScope(t *testing.T) {
	k := model.KnowledgeKey{Actor: "actor", Execution: "one"}
	_, first, err := SelectRevision(Snapshot{}, "space", k, "")
	if err != nil || first.Sequence != 0 || first.Parent != "" || first.Observation != "" {
		t.Fatalf("empty=%+v %v", first, err)
	}
	_, same, err := SelectRevision(Snapshot{}, "space", k, first.ID)
	if err != nil || first != same {
		t.Fatal("empty cursor failed to resolve")
	}
	k.Execution = "two"
	_, other, err := SelectRevision(Snapshot{}, "space", k, "")
	if err != nil || other.ID == first.ID || other.StateRoot == first.StateRoot {
		t.Fatal("empty knowledge root did not bind Execution")
	}
}
