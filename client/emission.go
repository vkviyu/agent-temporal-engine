package client

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

var _ api.EmissionCommands = (*Client)(nil)

func (c *Client) ApplyEmission(ctx context.Context, r api.ApplyEmissionRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	raw, err := api.EncodeEmissionCommand(r.Command)
	if err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	err = c.call(ctx, "apply_emission", protocol.ApplyEmissionRequest{Space: key.Space, RequestID: key.RequestID, Command: raw, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, err
}
func (c *Client) GetEmission(ctx context.Context, r api.ReadEmissionRequest) (api.EmissionResult, error) {
	var out api.EmissionResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	err := c.call(ctx, "get_emission", protocol.ReadEmissionRequest{Space: r.Space, ID: r.ID, Anchor: r.Anchor}, &out, nil)
	if err != nil {
		return out, err
	}
	if !validEmissionResult(out, r, c.identity.Principal) {
		_ = c.Close()
		return api.EmissionResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func validEmissionResult(out api.EmissionResult, req api.ReadEmissionRequest, principal model.PrincipalID) bool {
	if out.Anchor.Validate() != nil || out.Anchor.Space != req.Space || out.Anchor.Fence != out.Fence ||
		out.Anchor.Knowledge != nil || out.Anchor.Cursor != nil || !optionalID(out.RootSet) ||
		(req.Anchor != nil && !equalAnchor(*req.Anchor, out.Anchor)) {
		return false
	}
	if !out.Found {
		return reflect.DeepEqual(out.Record, api.EmissionRecord{})
	}
	r := out.Record
	if out.RootSet == "" || r.Spec.ID != req.ID || r.Spec.Anchor.Space != req.Space ||
		!emissionFenceBefore(r.Spec.Anchor.Fence, out.Fence) || r.Version == 0 ||
		(r.Status != "issued" && r.Status != "claimed" && r.Status != "revoked") {
		return false
	}
	if _, err := api.EncodeEmissionCommand(api.EmissionCommand{Issue: &r.Spec}); err != nil {
		return false
	}
	if r.Proof == nil {
		return false
	}
	if p := r.Proof; p != nil {
		projection := r.Spec.Projection
		typed := r.Spec.ContentKind == "packet" || r.Spec.ContentKind == "summary"
		if typed {
			projection = "value_bytes"
		} else if projection == "" {
			projection = "payload"
		}
		if (!typed && (r.Spec.Anchor.Cursor != nil || r.Spec.Anchor.Knowledge != nil)) || p.Projection != projection ||
			(projection != "payload" && projection != "value_bytes") ||
			!model.ValidDigest(string(p.ContentDigest)) || !model.ValidDigest(string(p.RangeDigest)) ||
			r.Spec.Offset > p.TotalBytes || r.Spec.Length > p.TotalBytes-r.Spec.Offset ||
			(r.Spec.Offset == 0 && r.Spec.Length == p.TotalBytes && p.ContentDigest != p.RangeDigest) {
			return false
		}
		digest, err := api.EmissionDependencyDigest(p.Dependencies)
		if err != nil || digest != p.DependencyDigest || p.Space != req.Space || p.Principal != principal ||
			!optionalID(p.PolicyRoot) || !emissionFenceBefore(r.Spec.Anchor.Fence, p.PolicyFence) ||
			!emissionFenceBefore(p.PolicyFence, out.Fence) {
			return false
		}
		contentFound := false
		for _, id := range p.Dependencies {
			contentFound = contentFound || id == r.Spec.Content
		}
		if !contentFound {
			return false
		}
	}
	return true
}

func emissionFenceBefore(earlier, later model.CommitFence) bool {
	return earlier.Validate() == nil && earlier.Store == later.Store &&
		(earlier.LSN < later.LSN || earlier == later)
}
