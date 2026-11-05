package service

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) getQueryProgress(ctx context.Context, raw []byte) (api.QueryProgressResult, error) {
	var w protocol.ReadQueryProgressRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.QueryProgressResult{}, err
	}
	if !ss.allowed(w.Space) {
		return api.QueryProgressResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(api.QueryCommands)
	if !ok {
		return api.QueryProgressResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetQueryProgress(ctx, api.ReadQueryProgressRequest{Space: w.Space, Principal: ss.principal, Query: w.Query, Anchor: w.Anchor})
}
