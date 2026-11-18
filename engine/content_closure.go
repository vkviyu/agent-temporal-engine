package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

// physicalObjectReferences contains only codec object addresses extracted by
// registered content schemas. A struct prevents implicit assignment from raw
// ID slices, which may contain logical policy identities.
type physicalObjectReferences struct {
	ids []model.ObjectID
}

// physicalReferences marks the physical boundary at registered ContentReferences
// extractors (or known physical test fixtures). Never pass policy identities or
// logical record IDs here, even when their digests equal physical object IDs.
func physicalReferences(ids []model.ObjectID) physicalObjectReferences {
	return physicalObjectReferences{ids: ids}
}

// verifyContentClosure authenticates actual codec content dependencies from
// registered schema extractors. Embedded record IDs and source anchors are
// resolved by their typed reducer, not cast into raw object references here.
// Full typed replay invokes these same checks before physical compaction.
// This verifies retention, never authorizes selective history deletion.
func verifyContentClosure(ctx context.Context, source state.ObjectReader, roots physicalObjectReferences) error {
	reader := &cursorReader{source: source}
	pending := append([]model.ObjectID(nil), roots.ids...)
	seen := make(map[model.ObjectID]bool)
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id == state.EmptyRoot || seen[id] {
			continue
		}
		if len(seen) >= cursorMaxReads {
			return state.ErrLimit
		}
		seen[id] = true
		object, err := readObject(ctx, reader, id)
		if err != nil {
			return err
		}
		// A system catalog can strongly reference partitions in many Spaces.
		// Traversal proves retained physical content, not output authority.
		// The registered reducer separately validates scope at every semantic
		// edge before this check and never returns this physical closure.
		if len(object.Refs) > cursorMaxReads-len(pending) {
			return state.ErrLimit
		}
		pending = append(pending, object.Refs...)
	}
	return nil
}
