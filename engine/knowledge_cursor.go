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
	return e.selectKnowledge(ctx, roots, anchor, principal, true)
}

func (e *Engine) selectedKnowledgeObservations(ctx context.Context, roots model.RootSet, anchor model.SnapshotAnchor, principal model.PrincipalID) (knowledge.Snapshot, knowledge.Revision, model.ObjectID, bool, error) {
	return e.selectKnowledge(ctx, roots, anchor, principal, false)
}

func (e *Engine) currentExecutionKnowledge(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, key model.KnowledgeKey) (knowledge.Snapshot, knowledge.Revision, error) {
	anchor := model.SnapshotAnchor{Space: space, Fence: e.agentPublishedAnchor(space).Fence, Knowledge: &key}
	snapshot, revision, _, _, err := e.selectedKnowledgeObservations(ctx, roots, anchor, principal)
	return snapshot, revision, err
}

func (e *Engine) selectKnowledge(ctx context.Context, roots model.RootSet, anchor model.SnapshotAnchor, principal model.PrincipalID, packets bool) (knowledge.Snapshot, knowledge.Revision, model.ObjectID, bool, error) {
	if anchor.Validate() != nil || anchor.Knowledge == nil {
		return knowledge.Snapshot{}, knowledge.Revision{}, "", false, model.ErrAnchorMismatch
	}
	index, root, err := e.knowledgeIndexAt(ctx, roots, anchor.Space, principal)
	if err != nil {
		return knowledge.Snapshot{}, knowledge.Revision{}, "", false, err
	}
	snapshot, err := index.snapshot(ctx, anchor.Knowledge, true, packets)
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
