package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type workAgentImageKey struct{}

// One sequential Work owns this slot. Retaining only the latest record catalog
// bounds its lifetime and avoids keeping every intermediate state alive.
// Lookups still resolve the requested root's profile ID before using this
// image; an unabsorbed candidate cannot masquerade as installed private state.
type workAgentImage struct {
	id      model.ObjectID
	space   model.SpaceID
	owner   model.PrincipalID
	records agentRecordCatalog
}

func workAgentRecordsAt(ctx context.Context, id model.ObjectID, space model.SpaceID, owner model.PrincipalID) (agentprofile.RecordImage, bool) {
	catalog, ok := workAgentRecordCatalogAt(ctx, id, space, owner)
	if !ok {
		return agentprofile.RecordImage{}, false
	}
	return catalog.image, true
}

func workAgentRecordCatalogAt(ctx context.Context, id model.ObjectID, space model.SpaceID, owner model.PrincipalID) (agentRecordCatalog, bool) {
	slot, _ := ctx.Value(workAgentImageKey{}).(*workAgentImage)
	if slot == nil || slot.id != id || slot.space != space || slot.owner != owner ||
		slot.records.id != id || slot.records.space != space || slot.records.owner != owner {
		return agentRecordCatalog{}, false
	}
	return slot.records, true
}

func retainWorkAgentRecords(ctx context.Context, catalog agentRecordCatalog) {
	if slot, _ := ctx.Value(workAgentImageKey{}).(*workAgentImage); slot != nil {
		*slot = workAgentImage{id: catalog.id, space: catalog.space, owner: catalog.owner, records: catalog}
	}
}
