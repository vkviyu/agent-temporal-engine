package knowledge

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestExplicitImportOwnsProvenanceAndCASWithoutSharingKnowledge(t *testing.T) {
	one, two := snapshotObservation("space", "source", 1), snapshotObservation("space", "source", 2)
	s := Snapshot{Observations: []Observation{one, two}}
	chain, err := Revisions(s, "space", one.Key)
	if err != nil {
		t.Fatal(err)
	}
	anchor := one.Source.Anchor.Clone()
	key := one.Key
	anchor.Knowledge = &key
	anchor.Cursor = &model.Cursor{Store: "store", Space: "space", Revision: chain[1].ID}
	c := ImportCommand{Target: model.KnowledgeKey{Actor: "same-actor", Execution: "fresh"}, Source: SourceAnchor{Anchor: anchor}, Observations: []model.ObjectID{one.ID}}
	raw, err := EncodeCommand(Command{Import: &c})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCommand(raw)
	if err != nil || decoded.Import == nil || !reflect.DeepEqual(*decoded.Import, c) {
		t.Fatalf("import codec=%+v %v", decoded, err)
	}
	next, err := ApplyCommand(s, decoded)
	if err != nil {
		t.Fatal(err)
	}
	target, _, err := SelectRevision(next, "space", c.Target, "")
	if err != nil || len(target.Observations) != 1 || len(s.Observations) != 2 {
		t.Fatalf("import did not create isolated target=%+v %v", target, err)
	}
	imported := target.Observations[0]
	if imported.ImportedFrom != one.ID || imported.Key != c.Target || imported.Payload != one.Payload || !reflect.DeepEqual(imported.Source.Anchor, anchor) || imported.Sequence != 1 {
		t.Fatalf("origin provenance lost=%+v", imported)
	}
	encoded, err := imported.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeObservation(encoded); err != nil || !reflect.DeepEqual(got, imported) {
		t.Fatalf("imported observation codec=%+v %v", got, err)
	}
	if _, err = EncodeCommand(Command{Observation: &imported}); !errors.Is(err, ErrKnowledgeMismatch) {
		t.Fatalf("ordinary observation forged an import: %v", err)
	}
	if _, err = ApplyCommand(next, Command{Import: &c}); !errors.Is(err, ErrObservationSequence) {
		t.Fatalf("stale target CAS accepted: %v", err)
	}
	c.Observations[0] = two.ID
	if _, err = ApplyCommand(s, Command{Import: &c}); !errors.Is(err, ErrUnknownObservation) {
		t.Fatalf("old source cursor imported future member: %v", err)
	}
	for i := 0; i < len(raw); i++ {
		if _, err = DecodeCommand(raw[:i]); err == nil {
			t.Fatalf("accepted truncated import at %d", i)
		}
	}
}
