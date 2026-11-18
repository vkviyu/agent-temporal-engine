package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func (e *Engine) LookupRequest(ctx context.Context, req api.LookupRequestRequest) (api.LookupRequestResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "lookup_request", req.Principal, req, func(scoped context.Context) (api.LookupRequestResult, error) {
			return e.LookupRequest(scoped, req)
		})
	}
	if err := req.Validate(); err != nil {
		return api.LookupRequestResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	if err := e.publicGate(); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	if got, found, err := e.inFlight(req); found {
		if err == nil && ctx.Value(readDeliveryKey{}) != nil {
			captured, captureErr := finishReadDelivery(ctx, e, "lookup_request", req.Principal, got, model.SnapshotAnchor{Space: req.Space, Fence: got.ObservedFence}, "", "", nil)
			return captured, captureErr
		}
		return got, err
	}
	result, found, err := e.lookup(ctx, req.Key())
	if err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, e.readFailureAt(ctx, err)
	}
	if !found {
		if got, queued, err := e.inFlight(req); queued {
			if err == nil && ctx.Value(readDeliveryKey{}) != nil {
				captured, captureErr := finishReadDelivery(ctx, e, "lookup_request", req.Principal, got, model.SnapshotAnchor{Space: req.Space, Fence: got.ObservedFence}, "", "", nil)
				return captured, captureErr
			}
			return got, err
		}
		result := api.LookupRequestResult{Code: api.CodeNotFoundAtFence, ObservedFence: e.readPublication(ctx).Fence()}
		return finishReadDelivery(ctx, e, "lookup_request", req.Principal, result, model.SnapshotAnchor{Space: req.Space, Fence: result.ObservedFence}, "", "", nil)
	}
	out := api.LookupRequestResult{Code: api.CodeDurableAck, Result: result, ObservedFence: e.readPublication(ctx).Fence()}
	return finishReadDelivery(ctx, e, "lookup_request", req.Principal, out, model.SnapshotAnchor{Space: req.Space, Fence: out.ObservedFence}, "", "", nil)
}

func (e *Engine) GetState(ctx context.Context, req api.ReadStateRequest) (api.StateValueResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_state", "embedded-host", req, func(scoped context.Context) (api.StateValueResult, error) {
			return e.GetState(scoped, req)
		})
	}
	return e.getState(ctx, req)
}

func (e *Engine) getState(ctx context.Context, req api.ReadStateRequest) (api.StateValueResult, error) {
	if err := ctx.Err(); err != nil {
		return api.StateValueResult{}, err
	}
	if err := validSpace(req.Space); err != nil {
		return api.StateValueResult{}, err
	}
	if len(req.Key) > api.MaxStateKeyBytes {
		return api.StateValueResult{}, state.ErrInvalidKey
	}
	if err := e.publicGate(); err != nil {
		return api.StateValueResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.StateValueResult{}, err
	}
	root, err := domainRoot(ctx, e.reader(ctx), e.readRoots(ctx), req.Space)
	if err != nil {
		return api.StateValueResult{}, e.readFailureAt(ctx, err)
	}
	out, id, err := e.readStateAtRoot(ctx, req, root)
	if err != nil {
		return api.StateValueResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.StateValueResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.StateValueResult{}, err
	}
	var dependencies []model.ObjectID
	if out.Found {
		dependencies = []model.ObjectID{id}
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: out.Fence}
	return finishReadDelivery(ctx, e, "get_state", readDeliveryPrincipal(ctx), out, anchor, "", "", dependencies)
}

func (e *Engine) readStateAtRoot(ctx context.Context, req api.ReadStateRequest, root model.ObjectID) (api.StateValueResult, model.ObjectID, error) {
	id, found, err := state.Get(ctx, e.reader(ctx), req.Space, root, req.Key)
	if err != nil {
		return api.StateValueResult{}, "", e.readFailureAt(ctx, err)
	}
	result := api.StateValueResult{Found: found, StateRoot: root, RootSet: e.readRootSet(ctx), Fence: e.readPublication(ctx).Fence()}
	if found {
		if err := e.checkDisclosureIDs(ctx, req.Space, []model.ObjectID{id}); err != nil {
			return api.StateValueResult{}, "", err
		}
		obj, err := readObject(ctx, e.reader(ctx), id)
		if err != nil {
			return api.StateValueResult{}, "", e.readFailureAt(ctx, err)
		}
		if obj.Space != req.Space {
			return api.StateValueResult{}, "", e.readFailureAt(ctx, ErrInvalidStateData)
		}
		result.Value, err = readValue(obj)
		if err != nil {
			return api.StateValueResult{}, "", e.readFailureAt(ctx, err)
		}
	}
	return result, id, nil
}

func (e *Engine) Snapshot(ctx context.Context) (api.StateSnapshot, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "snapshot", "embedded-host", struct{}{}, func(scoped context.Context) (api.StateSnapshot, error) {
			return e.Snapshot(scoped)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.StateSnapshot{}, err
	}
	if err := e.publicGate(); err != nil {
		return api.StateSnapshot{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.StateSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.StateSnapshot{}, err
	}
	result := api.StateSnapshot{Roots: e.readRoots(ctx), RootSet: e.readRootSet(ctx), Fence: e.readPublication(ctx).Fence()}
	return finishReadDelivery(ctx, e, "snapshot", readDeliveryPrincipal(ctx), result, model.SnapshotAnchor{Fence: result.Fence}, "", "", nil)
}
