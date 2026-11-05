package knowledge

import (
	"bytes"
	"github.com/vkviyu/agent-temporal-engine/model"
	"testing"
)

func TestSnapshotRoundTripAndCommand(t *testing.T) {
	f := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}
	a := model.SnapshotAnchor{Fence: f, Space: "space"}
	o := Observation{ID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Key: model.KnowledgeKey{Actor: "actor", Execution: "exec"}, Source: SourceAnchor{Anchor: a}, Payload: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Sequence: 1, Label: "tool"}
	// IDs are content identities in the public contract; build the canonical ID.
	o.ID = ObservationID(o)
	s, e := ApplyCommand(Snapshot{}, Command{Observation: &o})
	if e != nil {
		t.Fatal(e)
	}
	raw, e := EncodeSnapshot(s)
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeSnapshot(raw)
	if e != nil {
		t.Fatal(e)
	}
	again, e := EncodeSnapshot(got)
	if e != nil || !bytes.Equal(raw, again) {
		t.Fatalf("canonical: %v", e)
	}
}
