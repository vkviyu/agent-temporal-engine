package service

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type taskTraceBackend interface {
	api.TaskTraceCommands
	TaskTraceEnabled() bool
}

func (ss *session) taskTrace(space model.SpaceID) (taskTraceBackend, error) {
	if !ss.allowed(space) {
		return nil, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(taskTraceBackend)
	if !ok || !backend.TaskTraceEnabled() {
		return nil, apiError(api.ErrorUnsupported)
	}
	return backend, nil
}

func (ss *session) applyTaskTrace(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var wire protocol.ApplyTaskTraceRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.CommitResult{}, err
	}
	backend, err := ss.taskTrace(wire.Space)
	if err != nil {
		return api.CommitResult{}, err
	}
	edit, err := api.DecodeTaskTraceEdit(wire.Edit)
	if err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: wire.RequestID}, apiError(api.ErrorInvalidRequest)
	}
	key := model.RequestKey{Space: wire.Space, Principal: ss.principal, RequestID: wire.RequestID}
	return backend.ApplyTaskTrace(ctx, api.ApplyTaskTraceRequest{Key: key, Edit: edit})
}

func (ss *session) traceRead(wire protocol.GetTaskTraceRequest) api.TaskTraceRequest {
	return api.TaskTraceRequest{Space: wire.Space, Principal: ss.principal, Subject: wire.Subject, Session: wire.Session, Anchor: wire.Anchor, Limit: wire.Limit, Continuation: wire.Continuation}
}

func (ss *session) getTaskTrace(ctx context.Context, raw []byte) (api.TaskTraceResult, error) {
	var wire protocol.GetTaskTraceRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.TaskTraceResult{}, err
	}
	backend, err := ss.taskTrace(wire.Space)
	if err != nil {
		return api.TaskTraceResult{}, err
	}
	return backend.GetTaskTrace(ctx, ss.traceRead(wire))
}

func (ss *session) getTaskTraceEvidence(ctx context.Context, raw []byte) (api.TaskTraceEvidenceResult, error) {
	var wire protocol.GetTaskTraceEvidenceRequest
	if err := protocol.DecodeStrict(raw, &wire); err != nil {
		return api.TaskTraceEvidenceResult{}, err
	}
	backend, err := ss.taskTrace(wire.Space)
	if err != nil {
		return api.TaskTraceEvidenceResult{}, err
	}
	return backend.GetTaskTraceEvidence(ctx, api.TaskTraceEvidenceRequest{TaskTraceRequest: ss.traceRead(wire.GetTaskTraceRequest), From: wire.From, To: wire.To, Relation: wire.Relation})
}
