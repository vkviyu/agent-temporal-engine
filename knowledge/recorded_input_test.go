package knowledge

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestRecordedPacketInputCanonicalVersionAndOwnership(t *testing.T) {
	p := ContextPacket{Space: "space", Source: SourceAnchor{Anchor: model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "s", LSN: 2, CommitDigest: model.CommitDigest(dig('f'))}}}, Renderer: "render/v1", Tokenizer: "tokens/v1"}
	p.ID = PacketID(p)
	legacy, err := p.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if legacy[0] != 4 {
		t.Fatal("legacy packet bytes changed")
	}
	p.Input = &RecordedInput{Object: dig('a'), Anchor: model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "s", LSN: 3, CommitDigest: model.CommitDigest(dig('e'))}, Cursor: &model.Cursor{Store: "s", Space: "space", Revision: model.RevisionID(dig('b'))}}, Encoding: "application/vnd.model-request+json;v=1"}
	p.ID = PacketID(p)
	recorded, err := p.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if recorded[0] != 4 || bytes.Equal(legacy, recorded) {
		t.Fatal("recorded request is not covered by versioned identity")
	}
	got, err := DecodePacket(recorded)
	if err != nil || got.ID != p.ID || !reflect.DeepEqual(got.Input, p.Input) {
		t.Fatal(got, err)
	}
	clone := clonePacket(p)
	clone.Input.Anchor.Cursor.Revision = model.RevisionID(dig('c'))
	clone.Input.Encoding = "changed"
	if p.Input.Encoding == "changed" || p.Input.Anchor.Cursor.Revision != model.RevisionID(dig('b')) {
		t.Fatal("recording alias leaked")
	}
	if err = p.ValidateBefore(model.CommitFence{Store: "s", LSN: 3, CommitDigest: model.CommitDigest(dig('e'))}); !errors.Is(err, ErrFutureSource) {
		t.Fatal("own/future recording accepted", err)
	}
	if err = p.ValidateBefore(model.CommitFence{Store: "s", LSN: 4, CommitDigest: model.CommitDigest(dig('e'))}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(recorded); i++ {
		if _, err = DecodePacket(recorded[:i]); err == nil {
			t.Fatal("truncated recording accepted", i)
		}
	}
	legacyAgain, err := DecodePacket(legacy)
	if err != nil || legacyAgain.Input != nil {
		t.Fatal("legacy recording invented", err)
	}
}
