package eventlog

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func sampleEvent() model.Event {
	return model.Event{Space: "space", Transaction: "00112233445566778899aabbccddeeff", Stream: model.EventStreamKey{Kind: "principal", Owner: "owner"}, Sequence: 1, RecordedBy: "owner", Kind: KindCommandApplied, Method: "core.state.mutate/v1", SchemaVersion: 1, Payload: model.ObjectID(strings.Repeat("a", 64)), Command: model.ObjectID(strings.Repeat("b", 64))}
}

func TestEventIndependentGoldenAndOccurrenceIdentity(t *testing.T) {
	// Golden payload and ObjectID were produced independently with Python's
	// minimal unsigned-varint/length codec and hashlib, not this package.
	const payload = "4154452d4556454e540001203030313132323333343435353636373738383939616162626363646465656666097072696e636970616c056f776e65720001056f776e657214636f72652e636f6d6d616e645f6170706c69656414636f72652e73746174652e6d75746174652f7631014061616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161406262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626200"
	want, _ := hex.DecodeString(payload)
	e := sampleEvent()
	o, err := Object(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(o.Payload, want) {
		t.Fatal("independent canonical payload changed")
	}
	id, err := codec.ObjectID(o)
	if err != nil || id != "197520cf23bc6a46554e84d1d024138f87b4a925738c6af4fb86137f29058d2a" {
		t.Fatalf("independent identity=%s %v", id, err)
	}
	decoded, err := Decode(o)
	if err != nil || decoded.ID != model.EventID(id) {
		t.Fatalf("roundtrip=%+v %v", decoded, err)
	}
	e.Transaction = "11112233445566778899aabbccddeeff"
	another, err := Object(e)
	if err != nil {
		t.Fatal(err)
	}
	otherID, _ := codec.ObjectID(another)
	if id == otherID {
		t.Fatal("identical payload merged two occurrences")
	}
	e = sampleEvent()
	e.Space = "other-space"
	another, _ = Object(e)
	otherID, _ = codec.ObjectID(another)
	if id == otherID {
		t.Fatal("event identity crosses security scope")
	}
}

func TestEventRejectsForgedRefsAndSchemaAndOwnsCanonicalOrder(t *testing.T) {
	e := sampleEvent()
	e.CausalParents = []model.EventID{model.EventID(strings.Repeat("d", 64)), model.EventID(strings.Repeat("c", 64))}
	o, err := Object(e)
	if err != nil {
		t.Fatal(err)
	}
	if e.CausalParents[0] != model.EventID(strings.Repeat("d", 64)) {
		t.Fatal("encoder sorted caller-owned slice")
	}
	d, err := Decode(o)
	if err != nil || d.CausalParents[0] >= d.CausalParents[1] {
		t.Fatalf("parents=%+v %v", d, err)
	}
	for _, mutate := range []func(*codec.Object){func(o *codec.Object) { o.Refs = o.Refs[:1] }, func(o *codec.Object) { o.Version++ }, func(o *codec.Object) { o.Payload = append(o.Payload, 0) }} {
		bad := o
		bad.Payload = bytes.Clone(o.Payload)
		bad.Refs = append([]model.ObjectID(nil), o.Refs...)
		mutate(&bad)
		if _, err := Decode(bad); err == nil {
			t.Fatal("malformed event accepted")
		}
	}
	e = sampleEvent()
	e.RecordedBy = "spoof"
	if _, err := Object(e); err == nil {
		t.Fatal("stream/principal mismatch accepted")
	}
	e = sampleEvent()
	e.Sequence = 0
	if _, err := Object(e); err == nil {
		t.Fatal("zero stream sequence accepted")
	}
	e = sampleEvent()
	e.CausalParents = []model.EventID{model.EventID(e.Payload), model.EventID(e.Payload)}
	if _, err := Object(e); err == nil {
		t.Fatal("duplicate causal parent accepted")
	}
}
