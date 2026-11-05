package service

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) resolvePublication(ctx context.Context, raw []byte) (api.SourcePublication, error) {
	var request protocol.ResolvePublicationRequest
	if err := protocol.DecodeStrict(raw, &request); err != nil {
		return api.SourcePublication{}, err
	}
	if !ss.allowed(request.Source.Space) {
		return api.SourcePublication{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(api.PublicationResolver)
	if !ok {
		return api.SourcePublication{}, apiError(api.ErrorUnsupported)
	}
	return backend.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: ss.principal, Source: request.Source})
}
