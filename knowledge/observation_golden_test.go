package knowledge

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestObservationSchemasIndependentGolden(t *testing.T) {
	raw, err := os.ReadFile("../testdata/knowledge-observations.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Version  uint64 `json:"version"`
		Kind     string `json:"kind"`
		Imported bool   `json:"imported"`
		ID       string `json:"id"`
		Hex      string `json:"hex"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil || len(fixtures) != 6 {
		t.Fatalf("independent fixtures: %v", err)
	}
	for _, fixture := range fixtures {
		name := fixture.Kind
		if fixture.Imported {
			name += "/import"
		}
		t.Run(name, func(t *testing.T) {
			anchor := model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "store", LSN: 7, CommitDigest: model.CommitDigest(digestString('a'))}}
			o := Observation{Key: model.KnowledgeKey{Actor: "target-actor", Execution: "target-execution"}, Source: SourceAnchor{Anchor: anchor}, Payload: model.ObjectID(digestString('c')), PayloadKind: fixture.Kind, Sequence: 1, Label: "fixture"}
			if fixture.Imported {
				o.Source.Anchor.Cursor = &model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(digestString('b'))}
				o.Source.Anchor.Knowledge = &model.KnowledgeKey{Actor: "source-actor", Execution: "source-execution"}
				o.ImportedFrom = model.ObjectID(digestString('d'))
			}
			o.ID = ObservationID(o)
			if o.ID != model.ObjectID(fixture.ID) {
				t.Fatalf("independent identity changed: got %s want %s", o.ID, fixture.ID)
			}
			want, err := hex.DecodeString(fixture.Hex)
			if err != nil {
				t.Fatal(err)
			}
			got, err := o.Canonical()
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("independent schema %d bytes differ: %x %v", fixture.Version, got, err)
			}
			decoded, err := DecodeObservation(want)
			if err != nil || decoded.ID != o.ID || decoded.PayloadKind != o.PayloadKind || decoded.ImportedFrom != o.ImportedFrom {
				t.Fatalf("independent schema %d decode: %+v %v", fixture.Version, decoded, err)
			}
		})
	}
}
