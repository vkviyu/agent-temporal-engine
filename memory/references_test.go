package memory

import (
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestReferencesRetainProvenanceVersionAndOmissionClasses(t *testing.T) {
	k := model.KnowledgeKey{Actor: "actor", Execution: "execution"}
	a := model.SnapshotAnchor{Space: "space", Knowledge: &k, Cursor: &model.Cursor{Space: "space", Revision: "knowledge-revision"}}
	source := Source{SourceID: "embedded-observation", Anchor: a}
	node := model.NodeRef{Kind: model.NodeRevision, Revision: "domain-revision"}
	s := Snapshot{Memories: []Memory{{Sources: []Source{source}, Evidence: []Evidence{{Source: source}}, Validation: &Validation{Counterexamples: []Source{source}}, Supersedes: []model.ObjectID{"older-memory"}, Retraction: &Retraction{SourceID: "withdrawn-source"}}}, Paths: []ExperiencePath{{SourceAnchor: a, Steps: []Step{{Node: node, Memory: "step-memory"}}, Omitted: []model.NodeRef{node}, DerivedFrom: []model.ObjectID{"older-path"}, Validation: Validation{Evidence: []Source{source}}}}, Bans: []SourceBan{{Source: "banned-source"}}, Successors: []VersionLink{{Before: "before", After: "after"}}}
	refs := References(s)
	if len(refs.Sources) != 4 || len(refs.Anchors) != 5 || len(refs.Nodes) != 2 || len(refs.Versions) != 5 || len(refs.SourceIDs) != 2 {
		t.Fatalf("lost reference classes: %+v", refs)
	}
	refs.Sources[0].Anchor.Knowledge.Execution = "changed"
	refs.Anchors[0].Cursor.Revision = "changed"
	if source.Anchor.Knowledge.Execution == "changed" || source.Anchor.Cursor.Revision == "changed" {
		t.Fatal("retained source selectors alias caller")
	}
}
