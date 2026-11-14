package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func finishNodeRead[T any](ctx context.Context, e *Engine, entry *viewEntry, method string, value T, catalog nodeCatalog, ids []model.ObjectID, details ...nodeProjection) (T, error) {
	if readDeliveryPrincipal(ctx) != "" {
		dependencies, err := e.nodeReadDeliveryDependencies(ctx, entry, catalog, ids)
		if err != nil {
			var zero T
			return zero, e.nodeReadError(ctx, err)
		}
		ids = dependencies
	}
	return finishViewReadDelivery(ctx, e, entry, method, value, ids, details...)
}

// The whole bounded candidate catalog can contribute labels, ordering,
// relations or continuation metadata even when only part of it is emitted.
func (e *Engine) nodeReadDeliveryDependencies(ctx context.Context, entry *viewEntry, catalog nodeCatalog, ids []model.ObjectID) ([]model.ObjectID, error) {
	dependencies, err := e.selectedReadDependencies(ctx, entry)
	if err != nil {
		return nil, err
	}
	dependencies = append(dependencies, ids...)
	for id := range catalog.objects {
		dependencies = append(dependencies, id)
	}
	for ref, node := range catalog.nodes {
		dependencies = append(dependencies, nodeDisclosureIDs(node)...)
		if id := nodeReadRefID(ref); id != "" {
			dependencies = append(dependencies, id)
		}
	}
	return dependencies, nil
}

func nodeReadRefID(ref model.NodeRef) model.ObjectID {
	switch ref.Kind {
	case model.NodeObject:
		return ref.Object
	case model.NodeRevision:
		return model.ObjectID(ref.Revision)
	case model.NodeEvent:
		return model.ObjectID(ref.Event)
	case model.NodeEntity:
		if model.ValidDigest(string(ref.Entity.ID)) {
			return model.ObjectID(ref.Entity.ID)
		}
	}
	return ""
}
