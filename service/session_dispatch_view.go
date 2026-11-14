package service

import (
	"context"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) dispatchView(
	ctx context.Context,
	req protocol.Request,
	fail responseBuilder,
	ok successBuilder,
) protocol.Response {
	switch req.Method {
	case "open_view":
		var w protocol.OpenViewRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.ViewDescriptor{})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.ViewDescriptor{})
		}
		r, err := ss.server.backend.OpenView(ctx, api.OpenViewRequest{
			Space:     w.Space,
			Principal: ss.principal,
			Anchor:    w.Anchor,
			Knowledge: w.Knowledge,
			Mode:      w.Mode,
		})
		if err != nil {
			return fail(err, r)
		}
		ss.views[r.Ref] = struct{}{}
		return ok(r)
	case "close_view":
		var w protocol.CloseViewRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, struct{}{})
		}
		if _, owned := ss.views[w.Ref]; !owned {
			return fail(ErrUnauthorized, struct{}{})
		}
		if err := ss.server.backend.CloseView(ctx, api.CloseViewRequest{Ref: w.Ref}); err != nil {
			return fail(err, struct{}{})
		}
		delete(ss.views, w.Ref)
		return ok(struct{}{})
	case "get_view_state":
		var w protocol.ReadViewRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.StateValueResult{})
		}
		if _, owned := ss.views[w.View]; !owned {
			return fail(ErrUnauthorized, api.StateValueResult{})
		}
		r, err := ss.server.backend.GetViewState(ctx, api.ReadViewStateRequest{View: w.View, Key: w.Key})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_view_entries":
		var w protocol.ReadViewEntriesRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.StateEntriesResult{})
		}
		if _, owned := ss.views[w.View]; !owned {
			return fail(ErrUnauthorized, struct{}{})
		}
		backend, hasEntries := ss.server.backend.(entriesBackend)
		if !hasEntries {
			return fail(fmt.Errorf("%w: view entries", apiError(api.ErrorUnsupported)), api.StateEntriesResult{})
		}
		r, err := backend.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: w.View, Limits: w.Limits})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	default:
		return fail(fmt.Errorf("%w: method", apiError(api.ErrorUnsupported)), struct{}{})
	}
}
