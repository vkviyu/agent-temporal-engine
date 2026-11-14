package service

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type investigationBackend interface {
	api.InvestigationCommands
	api.ReadDeliveryPreparer
}

func (ss *session) updateInvestigation(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var wire protocol.UpdateInvestigationRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, err
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: wire.RequestID}
	if !ss.allowed(wire.Space) {
		return out, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(investigationBackend)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	command, err := investigation.DecodeUpdate(wire.Command)
	switch {
	case errors.Is(err, investigation.ErrLimit):
		out.Code = api.CodeResourceLimit
		return out, apiError(api.ErrorResourceLimit)
	case errors.Is(err, investigation.ErrUnsupported):
		return out, apiError(api.ErrorUnsupported)
	case err != nil:
		return out, apiError(api.ErrorInvalidRequest)
	}
	key := model.RequestKey{Space: wire.Space, Principal: ss.principal, RequestID: wire.RequestID}
	return backend.UpdateInvestigation(ctx, api.UpdateInvestigationRequest{Key: key, Command: command})
}

func (ss *session) getInvestigation(ctx context.Context, raw []byte) (api.InvestigationResult, error) {
	var wire protocol.GetInvestigationRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.InvestigationResult{}, err
	}
	if !ss.allowed(wire.Space) {
		return api.InvestigationResult{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(investigationBackend)
	if !ok {
		return api.InvestigationResult{}, apiError(api.ErrorUnsupported)
	}
	return backend.GetInvestigation(ctx, api.GetInvestigationRequest{Space: wire.Space, Principal: ss.principal, ID: wire.ID, Anchor: wire.Anchor})
}
