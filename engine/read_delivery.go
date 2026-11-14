package engine

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type readDeliveryKey struct{}

// A capture context is used by the trusted transport to invoke one ordinary
// read method. The method transfers ownership of its result to the capture.
type readDeliveryCapture struct {
	engine  *Engine
	binding api.ReadDeliveryBinding
	mu      sync.Mutex
	closed  bool
	invalid bool
	result  *readDeliveryResult
}

type readDeliveryResult struct {
	value        any
	anchor       model.SnapshotAnchor
	view         model.ViewRef
	viewDigest   model.ObjectID
	policyFence  model.CommitFence
	policyRoot   model.ObjectID
	controlRoot  model.ObjectID
	dependencies []model.ObjectID
}

type readDeliveryGrant struct {
	binding          api.ReadDeliveryBinding
	anchor           model.SnapshotAnchor
	view             model.ViewRef
	viewDigest       model.ObjectID
	policyFence      model.CommitFence
	policyRoot       model.ObjectID
	controlRoot      model.ObjectID
	dependencyDigest model.ObjectID
	contentDigest    model.ObjectID
	body             []byte
	expires          time.Time
	used             atomic.Bool
}

func readDeliveryPrincipal(ctx context.Context) model.PrincipalID {
	if capture, _ := ctx.Value(readDeliveryKey{}).(*readDeliveryCapture); capture != nil {
		return capture.binding.Destination.Principal
	}
	return ""
}

// finishReadDelivery is called with the publication lock held, after the read
// authorizes its result and dependencies against the current cut. The transport
// later serializes the result and compares this cut before issuing the grant.
func finishReadDelivery[T any](ctx context.Context, e *Engine, method string, principal model.PrincipalID, value T, anchor model.SnapshotAnchor, view model.ViewRef, viewDigest model.ObjectID, deps []model.ObjectID) (T, error) {
	var zero T
	capture, _ := ctx.Value(readDeliveryKey{}).(*readDeliveryCapture)
	if capture == nil {
		return zero, api.ErrReadEmissionDenied
	}
	var dependencies []model.ObjectID
	if len(deps) != 0 {
		policy, err := e.readPolicy(ctx, anchor.Space)
		if err != nil {
			return zero, err
		}
		dependencies, err = e.policyDependencyIDs(ctx, anchor.Space, policy, deps)
		if err != nil {
			return zero, err
		}
		if err := policy.Check(dependencies, memory.AccessAudit); err != nil {
			return zero, err
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := capture.record(e, method, principal, &readDeliveryResult{
		value: value, anchor: anchor.Clone(), view: view, viewDigest: viewDigest,
		policyFence: e.readPublication(ctx).Fence(), policyRoot: e.readRoots(ctx).ExecutionKnowledge, controlRoot: e.readRoots(ctx).LiveControl,
		dependencies: dependencies,
	}); err != nil {
		return zero, err
	}
	if method == "open_view" || method == "lookup_request" {
		return value, nil
	}
	return zero, nil
}

func finishViewReadDelivery[T any](ctx context.Context, e *Engine, entry *viewEntry, method string, value T, deps []model.ObjectID, details ...nodeProjection) (T, error) {
	var zero T
	if err := e.nodeContextOutputGate(ctx, entry, deps, details...); err != nil {
		return zero, err
	}
	return finishReadDelivery(ctx, e, method, entry.principal, value, entry.descriptor.Anchor, entry.descriptor.Ref, entry.descriptor.ViewDigest, deps)
}

func (g *readDeliveryGrant) Consume(binding api.ReadDeliveryBinding) ([]byte, error) {
	if g.binding != binding || !time.Now().Before(g.expires) || !g.used.CompareAndSwap(false, true) {
		return nil, api.ErrReadEmissionDenied
	}
	if emissionDigest(g.body) != g.contentDigest {
		return nil, api.ErrReadEmissionDenied
	}
	return g.body, nil
}

func (c *readDeliveryCapture) record(e *Engine, method string, principal model.PrincipalID, result *readDeliveryResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.invalid || c.result != nil || c.engine != e || c.binding.Method != method || c.binding.Destination.Principal != principal {
		c.invalid = true
		return api.ErrReadEmissionDenied
	}
	c.result = result
	return nil
}

func (c *readDeliveryCapture) finish() *readDeliveryResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	result := c.result
	c.result = nil
	if c.invalid {
		return nil
	}
	return result
}

func (e *Engine) PrepareReadDelivery(ctx context.Context, binding api.ReadDeliveryBinding, read func(context.Context) error) (api.ReadDeliveryGrant, error) {
	if !validReadDestination(binding.Destination) || binding.Method == "" || !model.ValidDigest(string(binding.RequestDigest)) || read == nil || ctx.Value(readDeliveryKey{}) != nil {
		return nil, api.ErrReadEmissionDenied
	}
	ctx, release, err := e.beginReadCut(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	capture := &readDeliveryCapture{engine: e, binding: binding}
	err = read(context.WithValue(ctx, readDeliveryKey{}, capture))
	result := capture.finish()
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, api.ErrReadEmissionDenied
	}
	body, err := json.Marshal(result.value)
	if err != nil {
		return nil, err
	}
	grant := &readDeliveryGrant{
		binding: binding, anchor: result.anchor, view: result.view, viewDigest: result.viewDigest,
		policyFence: result.policyFence, policyRoot: result.policyRoot, controlRoot: result.controlRoot,
		body: body, contentDigest: emissionDigest(body),
	}
	if len(result.dependencies) != 0 {
		grant.dependencyDigest, err = api.EmissionDependencyDigest(result.dependencies)
		if err != nil {
			return nil, err
		}
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	if err := e.checkReadCutLocked(ctx); err != nil {
		return nil, err
	}
	// The authorization and the outgoing bytes are now complete. Comparing
	// the publication fence orders actual issuance with every control change.
	if e.publication.Fence() != grant.policyFence {
		return nil, api.ErrReadAuthorizationChanged
	}
	grant.expires = time.Now().Add(readEmissionGrantTTL)
	return grant, nil
}

var _ api.ReadDeliveryPreparer = (*Engine)(nil)
