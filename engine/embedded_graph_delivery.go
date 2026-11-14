package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// LoadGraph returns a mutable, independently owned graph. Authorize immutable
// Revision DTOs first, then rebuild that graph from the consumed delivery bytes.
func (e *Engine) embeddedLoadGraph(ctx context.Context, space model.SpaceID, head model.RevisionID, maxRevisions int) (*graph.Store, error) {
	read := func(scoped context.Context) (map[model.RevisionID]graph.Revision, error) {
		return e.loadGraphReadSnapshot(scoped, space, head, maxRevisions)
	}
	var records map[model.RevisionID]graph.Revision
	var err error
	if ctx.Value(readDeliveryKey{}) == nil {
		request := struct {
			Space        model.SpaceID
			Head         model.RevisionID
			MaxRevisions int
		}{space, head, maxRevisions}
		records, err = embeddedReadDelivery(ctx, e, "load_graph", "embedded-host", request, read)
	} else {
		records, err = read(ctx)
	}
	if err != nil {
		return nil, err
	}
	store := graph.New()
	if len(records) == 0 {
		return store, nil
	}
	visited := make(map[model.RevisionID]bool, len(records))
	var restore func(model.RevisionID) error
	restore = func(id model.RevisionID) error {
		if visited[id] {
			return nil
		}
		revision := records[id]
		if revision.StateParent != "" {
			if err := restore(revision.StateParent); err != nil {
				return err
			}
		}
		for _, parent := range revision.JoinParents {
			if err := restore(parent); err != nil {
				return err
			}
		}
		if err := store.PutRevision(revision); err != nil {
			return err
		}
		visited[id] = true
		return nil
	}
	if err := restore(head); err != nil {
		return nil, err
	}
	return store, nil
}

func (e *Engine) loadGraphReadSnapshot(ctx context.Context, space model.SpaceID, head model.RevisionID, maxRevisions int) (map[model.RevisionID]graph.Revision, error) {
	if maxRevisions <= 0 {
		maxRevisions = graph.MaxWalkNodes
	}
	if maxRevisions > graph.MaxWalkNodes || !model.ValidDigest(string(head)) {
		return nil, api.ErrInvalidRequest
	}
	if err := e.graphReadStart(ctx, space); err != nil {
		return nil, err
	}
	reader := &cursorReader{source: e.reader(ctx)}
	_, records, err := loadGraphParents(ctx, reader, e.readRoots(ctx), space, []model.RevisionID{head})
	if err != nil {
		return nil, graphReadError(ctx, e, err)
	}
	if len(records) > maxRevisions {
		return nil, graph.ErrLimit
	}
	var dependencies []model.ObjectID
	revisions := make([]model.RevisionID, 0, len(records))
	for id, revision := range records {
		revisions = append(revisions, id)
		dependencies = append(dependencies, revision.StateRoot)
		for _, event := range revision.Events {
			dependencies = append(dependencies, model.ObjectID(event))
		}
		for _, entry := range revision.Entries {
			dependencies = append(dependencies, entry.Value)
		}
	}
	anchor := model.SnapshotAnchor{Space: space, Fence: e.readPublication(ctx).Fence()}
	return finishGraphRead(ctx, e, reader, "load_graph", anchor, e.readRoots(ctx), records, revisions, nil, dependencies)
}
