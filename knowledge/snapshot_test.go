package knowledge

import (
	"bytes"
	"reflect"
	"testing"
)

func TestSnapshotDTOFromCanonicalRecords(t *testing.T) {
	o := snapshotObservation("space", "one", 1)
	p := snapshotPacket(o)
	observationRaw, err := o.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	packetRaw, err := p.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	observation, err := DecodeObservation(observationRaw)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := DecodePacket(packetRaw)
	if err != nil {
		t.Fatal(err)
	}
	got := Snapshot{Observations: []Observation{observation}, Packets: []ContextPacket{packet}}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Snapshot{Observations: []Observation{o}, Packets: []ContextPacket{p}}) {
		t.Fatal("canonical records changed DTO values")
	}
	if again, err := observation.Canonical(); err != nil || !bytes.Equal(observationRaw, again) {
		t.Fatalf("observation canonical bytes changed: %v", err)
	}
	if again, err := packet.Canonical(); err != nil || !bytes.Equal(packetRaw, again) {
		t.Fatalf("packet canonical bytes changed: %v", err)
	}
}
