package service

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type graphQueryBackend interface{ api.GraphQueries }

func (ss *session) diffGraph(ctx context.Context, raw []byte) (api.GraphDiffResult, error) {
	var r api.DiffGraphRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return api.GraphDiffResult{}, err
	}
	if !ss.allowed(r.Space) {
		return api.GraphDiffResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(graphQueryBackend)
	if !ok {
		return api.GraphDiffResult{}, apiError(api.ErrorUnsupported)
	}
	return b.DiffGraph(ctx, r)
}
func (ss *session) planGraphJoin(ctx context.Context, raw []byte) (api.GraphJoinPlanResult, error) {
	var r api.PlanGraphJoinRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return api.GraphJoinPlanResult{}, err
	}
	if !ss.allowed(r.Space) {
		return api.GraphJoinPlanResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(graphQueryBackend)
	if !ok {
		return api.GraphJoinPlanResult{}, apiError(api.ErrorUnsupported)
	}
	return b.PlanGraphJoin(ctx, r)
}
func (ss *session) traceGraph(ctx context.Context, raw []byte) (api.GraphTraceResult, error) {
	var r api.TraceGraphRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return api.GraphTraceResult{}, err
	}
	if !ss.allowed(r.Space) {
		return api.GraphTraceResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(graphQueryBackend)
	if !ok {
		return api.GraphTraceResult{}, apiError(api.ErrorUnsupported)
	}
	return b.TraceGraph(ctx, r)
}
