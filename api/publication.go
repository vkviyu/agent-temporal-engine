package api

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type ResolvePublicationRequest struct {
	Principal model.PrincipalID
	Source    model.SourcePublicationRef
}

type SourcePublication struct {
	Source  model.SourcePublicationRef
	Anchor  model.SnapshotAnchor
	RootSet model.ObjectID
}

type PublicationResolver interface {
	ResolvePublication(context.Context, ResolvePublicationRequest) (SourcePublication, error)
}
