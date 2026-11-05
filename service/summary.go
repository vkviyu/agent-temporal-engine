package service

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type summaryBackend interface{ api.SummaryCommands }

func (ss *session) applySummary(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var wire protocol.ApplySummaryRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, err
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: wire.RequestID}
	if !ss.allowed(wire.Space) {
		return out, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(summaryBackend)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	summary, err := knowledge.DecodeSummary(wire.Summary)
	if err != nil || summary.Space != wire.Space {
		return out, apiError(api.ErrorInvalidRequest)
	}
	return backend.ApplySummary(ctx, api.ApplySummaryRequest{Key: model.RequestKey{Space: wire.Space, Principal: ss.principal, RequestID: wire.RequestID}, Summary: summary, ExpectedFence: wire.ExpectedFence})
}

func (ss *session) getSummary(ctx context.Context, raw []byte) (api.SummaryResult, error) {
	var wire protocol.ReadSummaryRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.SummaryResult{}, err
	}
	if !ss.allowed(wire.Space) {
		return api.SummaryResult{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(summaryBackend)
	if !ok {
		return api.SummaryResult{}, apiError(api.ErrorUnsupported)
	}
	return backend.GetSummary(ctx, api.ReadSummaryRequest{Space: wire.Space, Principal: ss.principal, ID: wire.ID, Anchor: wire.Anchor})
}
