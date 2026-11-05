package engine

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func queryProgressKey(space model.SpaceID, owner model.PrincipalID, query string) []byte {
	b, _ := codec.AppendString(nil, "query/progress/v1")
	for _, s := range []string{string(space), string(owner), query} {
		b, _ = codec.AppendString(b, s)
	}
	return b
}
func queryTurnReservationKey(key model.RequestKey) []byte {
	b, _ := codec.AppendString(nil, "query/action/v1")
	return append(b, receiptKey(key)...)
}
func (e *Engine) readQueryProgress(ctx context.Context, key []byte) (api.QueryProgress, bool, error) {
	id, found, err := state.Get(ctx, e.index, systemSpace, e.roots.LiveControl, key)
	if err != nil || !found {
		return api.QueryProgress{}, found, err
	}
	o, err := readObject(ctx, e.index, id)
	if err != nil {
		return api.QueryProgress{}, false, err
	}
	p, err := decodeQueryProgressValue(o)
	return p, err == nil, err
}

func decodeQueryProgressValue(o codec.Object) (api.QueryProgress, error) {
	v, err := readValue(o)
	if err != nil || v.Kind != api.ValueBytes {
		return api.QueryProgress{}, ErrInvalidStateData
	}
	var p api.QueryProgress
	if err := json.Unmarshal(v.Data, &p); err != nil {
		return p, ErrInvalidStateData
	}
	raw, err := json.Marshal(p)
	if err != nil || !bytes.Equal(raw, v.Data) || p.Version == 0 || p.Source.Space != o.Space || p.Source.Validate() != nil || p.Source.Cursor != nil || p.Source.Knowledge != nil {
		return p, ErrInvalidStateData
	}
	return p, nil
}
func (e *Engine) loadQueryProgress(ctx context.Context, req api.CommitRequest, query string) (api.QueryProgress, bool, error) {
	p, found, err := e.readQueryProgress(ctx, queryProgressKey(req.Space, req.Principal, query))
	if err == nil && found && (p.Query != query || p.Source.Space != req.Space || (p.Next != nil && (p.Next.Space != req.Space || p.Next.Principal != req.Principal))) {
		return p, false, ErrInvalidStateData
	}
	return p, found, err
}
func (e *Engine) checkQueryTurnReservation(ctx context.Context, req api.CommitRequest) error {
	p, found, err := e.readQueryProgress(ctx, queryTurnReservationKey(req.Key()))
	if err != nil || !found {
		return err
	}
	if p.Next == nil || !sameQueryRequest(*p.Next, req) {
		return ErrActionReservation
	}
	return nil
}
func sameQueryRequest(a, b api.CommitRequest) bool {
	return a.Key() == b.Key() && a.Method == b.Method && bytes.Equal(a.Body, b.Body) && sameExpectedFence(a.ExpectedFence, b.ExpectedFence)
}
func queryDefinition(l *agentprofile.Ledger, id string) (agentprofile.Query, bool) {
	for _, q := range l.Snapshot().Lifecycle.Queries {
		if q.ID == id {
			return q, true
		}
	}
	return agentprofile.Query{}, false
}

func (e *Engine) queryDefinitionAt(ctx context.Context, l *agentprofile.Ledger, space model.SpaceID, principal model.PrincipalID, id string) (agentprofile.Query, bool, error) {
	if q, found := queryDefinition(l, id); found {
		return q, true, nil
	}
	intake, found, err := e.loadQueryIntake(ctx, space, principal, id)
	if err != nil || !found {
		return agentprofile.Query{}, found, err
	}
	return intake.Intake.Query, true, nil
}

func queryActivitySpec(l *agentprofile.Ledger, id string) (agentprofile.ActivitySpec, error) {
	r := l.Snapshot().Lifecycle.Recovery
	if r != nil {
		for _, spec := range r.ActivitySpecs {
			if spec.Activity == id {
				return spec, nil
			}
		}
	}
	return agentprofile.ActivitySpec{}, agentprofile.ErrUnknown
}
func queryActivityControl(l *agentprofile.Ledger, id string) (agentprofile.ActivityControl, error) {
	r := l.Snapshot().Lifecycle.Recovery
	if r != nil {
		for _, ctl := range r.ActivityControls {
			if ctl.Activity == id {
				return ctl, nil
			}
		}
	}
	return agentprofile.ActivityControl{}, agentprofile.ErrUnknown
}
func queryToolsDone(p api.QueryProgress) bool {
	for _, d := range p.Decisions {
		for _, inv := range d.Invocations {
			if inv.AcceptedResult == "" {
				return false
			}
		}
	}
	return true
}
func queryInvocation(p api.QueryProgress, activity string) (api.QueryInvocation, bool) {
	for _, d := range p.Decisions {
		for _, i := range d.Invocations {
			if i.Activity == activity {
				return i, true
			}
		}
	}
	return api.QueryInvocation{}, false
}

// GetQueryProgress returns a controller position at one authenticated historical
// cut. It does not replace the ledger's Query or expose another principal's state.
func (e *Engine) GetQueryProgress(ctx context.Context, r api.ReadQueryProgressRequest) (api.QueryProgressResult, error) {
	if err := validAgentScope(r.Space, r.Principal); err != nil {
		return api.QueryProgressResult{}, err
	}
	if r.Query == "" || len(r.Query) > model.MaxIdentityBytes {
		return api.QueryProgressResult{}, api.ErrInvalidRequest
	}
	if !e.options.AgentProfile {
		return api.QueryProgressResult{}, ErrUnsupportedMethod
	}
	if err := e.publicGate(); err != nil {
		return api.QueryProgressResult{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.QueryProgressResult{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.QueryProgressResult{}, err
	}
	a := model.SnapshotAnchor{Space: r.Space, Fence: e.publication.Fence()}
	if r.Anchor != nil {
		a = r.Anchor.Clone()
	}
	if a.Validate() != nil || a.Space != r.Space || a.Cursor != nil || a.Knowledge != nil {
		return api.QueryProgressResult{}, model.ErrAnchorMismatch
	}
	roots, _, err := e.rootsAtFence(ctx, a.Fence, false)
	if err != nil {
		return api.QueryProgressResult{}, e.readFailure(err)
	}
	v := e.workView()
	v.roots = roots
	p, found, err := v.loadQueryProgress(ctx, api.CommitRequest{Space: r.Space, Principal: r.Principal}, r.Query)
	if err != nil {
		return api.QueryProgressResult{}, e.readFailure(err)
	}
	var ids []model.ObjectID
	if found {
		l, _, _, err := loadAgentLedger(ctx, e.index, roots.LiveControl, r.Space, r.Principal)
		if err != nil {
			return api.QueryProgressResult{}, e.readFailure(err)
		}
		q, ok, qErr := e.queryDefinitionAt(ctx, l, r.Space, r.Principal, r.Query)
		if qErr != nil {
			return api.QueryProgressResult{}, e.readFailure(qErr)
		}
		if !ok || q.Owner != r.Principal || q.Space != r.Space {
			return api.QueryProgressResult{}, ErrInvalidStateData
		}
		for _, d := range p.Decisions {
			for _, inv := range d.Invocations {
				ids = append(ids, inv.Definition)
			}
		}
		provenance, err := e.queryProgressDisclosureIDs(ctx, r.Space, r.Principal, l, r.Query)
		if err != nil {
			return api.QueryProgressResult{}, e.readFailure(err)
		}
		ids = append(ids, provenance...)
		if err := e.checkDisclosureIDs(ctx, r.Space, ids); err != nil {
			return api.QueryProgressResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return api.QueryProgressResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.QueryProgressResult{}, err
	}
	out := api.QueryProgressResult{Found: found, Progress: p, Anchor: a}
	return finishReadDelivery(ctx, e, "get_query_progress", r.Principal, out, a, "", "", ids)
}

// Query arguments and pending request bytes may derive from recorded model
// input. Track that declared provenance at its original input cut; never infer
// it from latest knowledge or treat a copied byte string as a new authorization.
func (e *Engine) queryProgressDisclosureIDs(ctx context.Context, space model.SpaceID, principal model.PrincipalID, l *agentprofile.Ledger, query string) ([]model.ObjectID, error) {
	var ids []model.ObjectID
	r := l.Snapshot().Lifecycle.Recovery
	if r == nil {
		return ids, nil
	}
	for _, spec := range r.ActivitySpecs {
		if spec.Query != query {
			continue
		}
		dependencies, err := e.activityInputDisclosureIDs(ctx, space, principal, spec)
		if err != nil {
			return nil, err
		}
		ids = append(ids, dependencies...)
	}
	return ids, nil
}
