package service

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) applyAnnotation(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var wire protocol.ApplyAnnotationRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, err
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: wire.RequestID}
	if !ss.allowed(wire.Space) {
		return out, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(api.AnnotationCommands)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	record, err := annotation.DecodeRecord(wire.Command)
	if err != nil || record.Source.Space != wire.Space {
		return out, apiError(api.ErrorInvalidRequest)
	}
	return backend.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: model.RequestKey{Space: wire.Space, Principal: ss.principal, RequestID: wire.RequestID}, Command: record, ExpectedFence: wire.ExpectedFence})
}

func (ss *session) getAnnotations(ctx context.Context, raw []byte) (api.AnnotationsResult, error) {
	var wire protocol.ReadAnnotationsRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.AnnotationsResult{}, err
	}
	if !ss.allowed(wire.Space) {
		return api.AnnotationsResult{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(api.AnnotationCommands)
	if !ok {
		return api.AnnotationsResult{}, apiError(api.ErrorUnsupported)
	}
	return backend.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: wire.Space, Principal: ss.principal, Anchor: wire.Anchor})
}
