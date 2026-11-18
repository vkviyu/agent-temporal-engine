package knowledge

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestExplicitImportRecordsOwnProvenanceAndCanonicalIdentity(t *testing.T) {
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
	if again, err := EncodeCommand(decoded); err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("import command canonical bytes changed: %v", err)
	}
	// The caller supplies origins authenticated at the selected source revision.
	origins := map[model.ObjectID]Observation{one.ID: one}
	records, err := ImportedObservations(*decoded.Import, origins)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("imported record count=%d", len(records))
	}
	imported := records[0]
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
	dto := Snapshot{Observations: []Observation{one, two, imported}}
	if err := dto.Validate(); err != nil {
		t.Fatal("import DTO rejected", err)
	}
	target, _, err := SelectRevision(dto, "space", c.Target, "")
	if err != nil || len(target.Observations) != 1 || target.Observations[0].ID != imported.ID {
		t.Fatalf("imported target revision=%+v %v", target, err)
	}
	imported.Source.Anchor.Knowledge.Execution = "mutated"
	imported.Source.Anchor.Cursor.Revision = model.RevisionID(dig('f'))
	if !reflect.DeepEqual(c.Source, decoded.Import.Source) || !reflect.DeepEqual(origins[one.ID], one) {
		t.Fatal("imported record aliases command or origin")
	}
	c.Observations[0] = two.ID
	if _, err = ImportedObservations(c, origins); !errors.Is(err, ErrUnknownObservation) {
		t.Fatalf("import accepted an origin absent from the authenticated set: %v", err)
	}
	for i := 0; i < len(raw); i++ {
		if _, err = DecodeCommand(raw[:i]); err == nil {
			t.Fatalf("accepted truncated import at %d", i)
		}
	}
}
