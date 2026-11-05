package service

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) applyEmission(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var w protocol.ApplyEmissionRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}, err
	}
	if !ss.allowed(w.Space) {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(api.EmissionCommands)
	if !ok {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}, apiError(api.ErrorUnsupported)
	}
	cmd, err := api.DecodeEmissionCommand(w.Command)
	if err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}, apiError(api.ErrorInvalidRequest)
	}
	return b.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID}, Command: cmd, ExpectedFence: w.ExpectedFence})
}
func (ss *session) getEmission(ctx context.Context, raw []byte) (api.EmissionResult, error) {
	var w protocol.ReadEmissionRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.EmissionResult{}, err
	}
	if !ss.allowed(w.Space) {
		return api.EmissionResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(api.EmissionCommands)
	if !ok {
		return api.EmissionResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetEmission(ctx, api.ReadEmissionRequest{Space: w.Space, Principal: ss.principal, ID: w.ID, Anchor: w.Anchor})
}
