package service

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func (ss *session) exportTransfer(ctx context.Context, raw []byte) (transfer.Manifest, error) {
	var wire protocol.ExportTransferRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return transfer.Manifest{}, err
	}
	if !ss.allowed(wire.Space) {
		return transfer.Manifest{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(api.TransferCommands)
	if !ok {
		return transfer.Manifest{}, apiError(api.ErrorUnsupported)
	}
	return backend.ExportTransfer(ctx, api.ExportTransferRequest{Space: wire.Space, Principal: ss.principal, Anchor: wire.Anchor, Roots: wire.Roots, Omit: wire.Omit, Policy: wire.Policy})
}
func (ss *session) importTransfer(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var wire protocol.ImportTransferRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, err
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: wire.RequestID}
	if !ss.allowed(wire.Space) {
		return out, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(api.TransferCommands)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	manifest, err := transfer.DecodeManifest(wire.Manifest)
	if err != nil {
		return out, apiError(api.ErrorInvalidRequest)
	}
	return backend.ImportTransfer(ctx, api.ImportTransferRequest{Key: model.RequestKey{Space: wire.Space, Principal: ss.principal, RequestID: wire.RequestID}, Manifest: manifest, ApprovedDigest: wire.ApprovedDigest, ExpectedFence: wire.ExpectedFence})
}
func (ss *session) getTransfer(ctx context.Context, raw []byte) (api.TransferResult, error) {
	var wire protocol.ReadTransferRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.TransferResult{}, err
	}
	if !ss.allowed(wire.Space) {
		return api.TransferResult{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(api.TransferCommands)
	if !ok {
		return api.TransferResult{}, apiError(api.ErrorUnsupported)
	}
	return backend.GetTransfer(ctx, api.ReadTransferRequest{Space: wire.Space, Principal: ss.principal, ID: wire.ID, Anchor: wire.Anchor})
}
