package service

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) getQueryIntake(ctx context.Context, raw []byte) (api.QueryIntakeResult, error) {
	var w protocol.ReadQueryIntakeRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.QueryIntakeResult{}, err
	}
	if !ss.allowed(w.Space) {
		return api.QueryIntakeResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(api.WorkCommands)
	if !ok {
		return api.QueryIntakeResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetQueryIntake(ctx, api.ReadQueryIntakeRequest{Space: w.Space, Principal: ss.principal, Query: w.Query, Anchor: w.Anchor})
}
