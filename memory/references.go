package memory

import "github.com/vkviyu/agent-temporal-engine/model"

// ReferenceSet keeps distinct namespaces explicit. SourceIDs may identify
// domain/event codec objects or embedded Observation/Packet/Memory records.
// They require source-anchor/schema resolution, never a blind raw-object read.
// Versions and Nodes are logical dependencies. Anchors retain history even
// when no current domain tree points to that history anymore.
type ReferenceSet struct {
	Sources   []Source
	Versions  []model.ObjectID
	Nodes     []model.NodeRef
	Anchors   []model.SnapshotAnchor
	SourceIDs []model.ObjectID
}

func References(s Snapshot) ReferenceSet {
	out := ReferenceSet{}
	addSource := func(source Source) {
		out.Sources = append(out.Sources, cloneSource(source))
		out.Anchors = append(out.Anchors, source.Anchor.Clone())
	}
	addRetraction := func(retraction *Retraction) {
		if retraction != nil && retraction.SourceID != "" {
			out.SourceIDs = append(out.SourceIDs, retraction.SourceID)
		}
	}
	for _, item := range s.Memories {
		for _, source := range allSources(item) {
			addSource(source)
		}
		out.Versions = append(out.Versions, item.Supersedes...)
		out.Versions = append(out.Versions, item.Retracts...)
		addRetraction(item.Retraction)
	}
	for _, path := range s.Paths {
		out.Anchors = append(out.Anchors, path.SourceAnchor.Clone())
		for _, source := range path.Validation.Evidence {
			addSource(source)
		}
		for _, source := range path.Validation.Counterexamples {
			addSource(source)
		}
		out.Versions = append(out.Versions, path.DerivedFrom...)
		for _, step := range path.Steps {
			out.Versions = append(out.Versions, step.Memory)
			out.Nodes = append(out.Nodes, step.Node)
		}
		out.Nodes = append(out.Nodes, path.Omitted...)
		addRetraction(path.Retraction)
	}
	for _, link := range s.Successors {
		out.Versions = append(out.Versions, link.Before, link.After)
	}
	for _, link := range s.PathSuccessors {
		out.Versions = append(out.Versions, link.Before, link.After)
	}
	for _, ban := range s.Bans {
		out.SourceIDs = append(out.SourceIDs, ban.Source)
		addRetraction(&ban.Retraction)
	}
	return out
}

// SourceReferences is the exact historical source subset of References.
// The result owns all Anchor selectors and may be retained independently.
func SourceReferences(s Snapshot) []Source { return References(s).Sources }
