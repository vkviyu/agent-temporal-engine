package graph

import (
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// ContentReferences extracts the schema's strong codec-object references.
// StateRoot may be the host's distinguished empty root. Revision/Branch IDs
// identify embedded graph records and are deliberately a different class.
// Callers authenticate the revision before using this extractor for retention.
func ContentReferences(r Revision) []model.ObjectID {
	out := make([]model.ObjectID, 0, 1+len(r.Entries)+len(r.Events))
	out = append(out, r.StateRoot)
	for _, item := range r.Entries {
		out = append(out, item.Value)
	}
	for _, event := range r.Events {
		out = append(out, model.ObjectID(event))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	unique := out[:0]
	for _, id := range out {
		if id != "" && (len(unique) == 0 || unique[len(unique)-1] != id) {
			unique = append(unique, id)
		}
	}
	return unique
}

// RevisionReferences returns logical graph dependencies, never raw codec
// ObjectIDs. The storage adapter resolves them in the authenticated graph
// partition; casting them to ObjectID would lose their namespace and schema.
func RevisionReferences(r Revision) []model.RevisionID {
	out := append([]model.RevisionID(nil), r.JoinParents...)
	if r.StateParent != "" {
		out = append(out, r.StateParent)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	unique := out[:0]
	for _, id := range out {
		if id != "" && (len(unique) == 0 || unique[len(unique)-1] != id) {
			unique = append(unique, id)
		}
	}
	return unique
}
