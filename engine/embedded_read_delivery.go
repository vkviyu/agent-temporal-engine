package engine

import (
	"context"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Embedded calls are trusted host deliveries. They use the same request-bound
// one-shot grant as the Unix transport, then reconstruct the public result from
// the exact authorized bytes. Request hashing and response encoding stay outside
// the publication lock. A transport capture already owns its delivery boundary.
func embeddedReadDelivery[Q, T any](ctx context.Context, e *Engine, method string, principal model.PrincipalID, request Q, read func(context.Context) (T, error)) (T, error) {
	var zero T
	if lookup, ok := any(&zero).(*api.LookupRequestResult); ok {
		// Failure to issue a delivery cannot establish receipt absence, even
		// when the captured read had observed an in-flight or durable request.
		lookup.Code = api.CodeIndeterminate
	}
	if principal == "" {
		principal = "embedded-host"
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return zero, err
	}
	binding := api.ReadDeliveryBinding{Destination: embeddedReadDestination(principal), Method: method, RequestDigest: emissionDigest(raw)}
	var captured T
	var readErr error
	delivered := false
	defer func() {
		if descriptor, ok := any(captured).(api.ViewDescriptor); ok && !delivered && descriptor.Ref != "" {
			_ = e.CloseView(context.Background(), api.CloseViewRequest{Ref: descriptor.Ref})
		}
	}()
	grant, err := e.PrepareReadDelivery(ctx, binding, func(scoped context.Context) error {
		captured, readErr = read(scoped)
		return readErr
	})
	if err != nil {
		if readErr != nil {
			return captured, err
		}
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	body, err := grant.Consume(binding)
	if err != nil {
		return zero, err
	}
	var result T
	if err := json.Unmarshal(body, &result); err != nil {
		return zero, err
	}
	delivered = true
	return result, nil
}

func embeddedViewReadDelivery[Q, T any](ctx context.Context, e *Engine, method string, view model.ViewRef, request Q, read func(context.Context) (T, error)) (T, error) {
	entry, err := e.acquireView(ctx, view)
	if err != nil {
		var zero T
		return zero, err
	}
	principal := entry.principal
	e.releaseView(view, entry)
	return embeddedReadDelivery(ctx, e, method, principal, request, read)
}
