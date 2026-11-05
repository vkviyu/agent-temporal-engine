package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// selectedKnowledge resolves against the owner's authenticated snapshot at
// anchor.Fence. The caller separately checks current and historical authority.
// Cursor IDs are execution-knowledge IDs when Knowledge is present; they are
// never interpreted as a domain StateRoot or resolved against today's graph.
func (e *Engine) selectedKnowledge(ctx context.Context, roots model.RootSet, anchor model.SnapshotAnchor, principal model.PrincipalID) (knowledge.Snapshot, knowledge.Revision, model.ObjectID, bool, error) {
	if anchor.Validate() != nil || anchor.Knowledge == nil {
		return knowledge.Snapshot{}, knowledge.Revision{}, "", false, model.ErrAnchorMismatch
	}
	snapshot, root, _, err := e.loadKnowledge(ctx, roots, anchor.Space, principal)
	if err != nil {
		return knowledge.Snapshot{}, knowledge.Revision{}, "", false, err
	}
	var id model.RevisionID
	if anchor.Cursor != nil {
		id = anchor.Cursor.Revision
	}
	selected, revision, err := knowledge.SelectRevision(snapshot, anchor.Space, *anchor.Knowledge, id)
	if err != nil {
		return knowledge.Snapshot{}, knowledge.Revision{}, "", false, err
	}
	return selected, revision, root, len(selected.Observations)+len(selected.Packets) > 0, nil
}
