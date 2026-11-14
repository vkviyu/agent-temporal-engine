package service

import (
	"context"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) dispatchCore(
	ctx context.Context,
	req protocol.Request,
	fail responseBuilder,
	ok successBuilder,
) protocol.Response {
	switch req.Method {
	case "commit":
		var w protocol.CommitRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.CommitResult{Code: api.CodeNotSubmitted})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID})
		}
		r, err := ss.server.backend.Commit(ctx, api.CommitRequest{
			Space:         w.Space,
			Principal:     ss.principal,
			RequestID:     w.RequestID,
			Method:        w.Method,
			Body:          w.Body,
			ExpectedFence: w.ExpectedFence,
		})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "mutate":
		var w protocol.MutateRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.CommitResult{Code: api.CodeNotSubmitted})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID})
		}
		r, err := ss.server.backend.Mutate(ctx, api.MutateRequest{
			Key:           model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID},
			Batch:         w.Batch,
			ExpectedFence: w.ExpectedFence,
		})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "lookup_request":
		var w protocol.LookupRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.LookupRequestResult{Code: api.CodeIndeterminate})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.LookupRequestResult{Code: api.CodeIndeterminate})
		}
		r, err := ss.server.backend.LookupRequest(ctx, api.LookupRequestRequest{
			Space:     w.Space,
			Principal: ss.principal,
			RequestID: w.RequestID,
		})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_state":
		var w protocol.ReadStateRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.StateValueResult{})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.StateValueResult{})
		}
		r, err := ss.server.backend.GetState(ctx, api.ReadStateRequest{Space: w.Space, Key: w.Key})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "snapshot":
		var empty protocol.Empty
		if err := protocol.DecodeStrict(req.Body, &empty); err != nil {
			return fail(err, api.StateSnapshot{})
		}
		if !ss.globalSnapshot {
			return fail(ErrUnauthorized, api.StateSnapshot{})
		}
		r, err := ss.server.backend.Snapshot(ctx)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	default:
		return fail(fmt.Errorf("%w: method", apiError(api.ErrorUnsupported)), struct{}{})
	}
}
