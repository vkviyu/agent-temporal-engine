package knowledge

import (
	"errors"
	"github.com/vkviyu/agent-temporal-engine/model"
	"strings"
	"testing"
)

func dig(c byte) model.ObjectID { return model.ObjectID(strings.Repeat(string(c), 64)) }
func source(lsn uint64) SourceAnchor {
	return SourceAnchor{Anchor: model.SnapshotAnchor{Fence: model.CommitFence{Store: "s", LSN: lsn, CommitDigest: model.CommitDigest(dig('f'))}, Space: "space", Cursor: &model.Cursor{Store: "s", Space: "space", Revision: model.RevisionID(dig('a'))}, Knowledge: &model.KnowledgeKey{Actor: "actor", Execution: "exec"}}, ViewDigest: dig('b'), Frontier: dig('f')}
}
func TestObservationAndPacketCanonicalRoundTrip(t *testing.T) {
	o := Observation{Key: model.KnowledgeKey{Actor: "actor", Execution: "exec"}, Source: source(3), Payload: dig('d'), Sequence: 1, Label: "result"}
	o.ID = ObservationID(o)
	raw, err := o.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeObservation(raw)
	if err != nil || got.ID != o.ID || got.Source.Anchor.Cursor == nil {
		t.Fatalf("%+v %v", got, err)
	}
	p := ContextPacket{Space: "space", Key: &model.KnowledgeKey{Actor: "actor", Execution: "exec"}, Source: source(3), GoalRef: dig('e'), Items: []ContextItem{{Ordinal: 0, Observation: o.ID, Content: dig('f'), Label: "result"}}, Renderer: "renderer/v1", Tokenizer: "tokenizer/v1", EstimatedTokens: 42}
	p.ID = PacketID(p)
	raw, err = p.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePacket(raw)
	if err != nil || decoded.ID != p.ID || decoded.Items[0].Content != p.Items[0].Content {
		t.Fatalf("%+v %v", decoded, err)
	}
}
func TestFutureSourceAndKnowledgeIsolation(t *testing.T) {
	o := Observation{Key: model.KnowledgeKey{Actor: "a", Execution: "e1"}, Source: source(4), Payload: dig('d'), Sequence: 1, Label: "x"}
	if err := o.Source.ValidateBefore(model.CommitFence{Store: "s", LSN: 4, CommitDigest: model.CommitDigest(dig('a'))}); !errors.Is(err, ErrFutureSource) {
		t.Fatal(err)
	}
	bad := o
	bad.Source.Anchor.Knowledge = &model.KnowledgeKey{Actor: "a", Execution: "e2"}
	if !errors.Is(bad.Validate(), ErrKnowledgeMismatch) {
		t.Fatal(bad.Validate())
	}
	p := ContextPacket{Space: "space", Key: &model.KnowledgeKey{Actor: "actor", Execution: "exec"}, Source: source(3), GoalRef: dig('e'), Items: nil, Renderer: "r", Tokenizer: "t"}
	if err := p.ValidateBefore(model.CommitFence{Store: "s", LSN: 3, CommitDigest: model.CommitDigest(dig('a'))}); !errors.Is(err, ErrFutureSource) {
		t.Fatal(err)
	}
}
func TestPacketRejectsDuplicateAndNonContiguousItems(t *testing.T) {
	p := ContextPacket{Space: "s", Source: source(1), Renderer: "r", Tokenizer: "t", Items: []ContextItem{{Ordinal: 1, Observation: dig('a'), Content: dig('b'), Label: "x"}}}
	if err := p.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
