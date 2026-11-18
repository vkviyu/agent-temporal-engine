package engine

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestPhysicalContentClosureExcludesLogicalPayloads(t *testing.T) {
	for _, kind := range []string{"memory", "experience_path", "summary", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			reader := &closureObjectReader{objects: map[model.ObjectID][]byte{}}
			add := func(object codec.Object) model.ObjectID {
				t.Helper()
				id, err := codec.ObjectID(object)
				if err != nil {
					t.Fatal(err)
				}
				reader.objects[id], err = codec.EncodeObject(object)
				if err != nil {
					t.Fatal(err)
				}
				return id
			}
			logical := model.ObjectID(strings.Repeat("a", 64))
			leaf := add(codec.Object{Type: "test.content", Space: "space", Version: 1, Payload: []byte("leaf")})
			root := add(codec.Object{
				Type: "test.catalog", Space: "space", Version: 1,
				Refs: []model.ObjectID{leaf}, Payload: []byte(logical),
			})
			// A typed payload can equal a real physical object's digest without
			// granting permission to use that identity as an object address.
			snapshot := knowledge.Snapshot{
				Observations: []knowledge.Observation{
					{ID: leaf, Payload: root, PayloadKind: kind},
					{ID: logical, Payload: logical, PayloadKind: kind},
				},
				Packets: []knowledge.ContextPacket{{
					ID: root, GoalRef: root,
					Items: []knowledge.ContextItem{
						{Observation: leaf, Content: root},
						{Observation: logical, Content: logical},
					},
					Source: knowledge.SourceAnchor{Frontier: leaf, ViewDigest: logical},
				}},
			}
			refs := physicalReferences(knowledge.ContentReferences(snapshot))
			if len(refs.ids) != 0 {
				t.Fatalf("logical-only snapshot produced physical references: %v", refs.ids)
			}
			if err := verifyContentClosure(context.Background(), reader, refs); err != nil || reader.reads != 0 {
				t.Fatalf("logical-only closure performed IO: reads=%d err=%v", reader.reads, err)
			}

			// Explicit physical fields retain the same digest despite its
			// logical use above. Duplicate roots are canonicalized by the
			// extractor; only codec Refs extend the transitive closure.
			snapshot.Observations = append(snapshot.Observations,
				knowledge.Observation{Payload: root},
				knowledge.Observation{Payload: root},
			)
			snapshot.Packets[0].Input = &knowledge.RecordedInput{Object: root}
			refs = physicalReferences(knowledge.ContentReferences(snapshot))
			if !slices.Equal(refs.ids, []model.ObjectID{root}) {
				t.Fatalf("mixed snapshot references=%v want=[%s]", refs.ids, root)
			}
			if err := verifyContentClosure(context.Background(), reader, refs); err != nil {
				t.Fatal(err)
			}
			if want := []model.ObjectID{root, leaf}; !slices.Equal(reader.readIDs, want) {
				t.Fatalf("physical closure reads=%v want=%v", reader.readIDs, want)
			}
		})
	}
}
