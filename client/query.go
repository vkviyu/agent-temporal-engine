package client

import (
	"context"
	"reflect"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

var _ api.QueryCommands = (*Client)(nil)

func (c *Client) GetQueryProgress(ctx context.Context, r api.ReadQueryProgressRequest) (api.QueryProgressResult, error) {
	var out api.QueryProgressResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "get_query_progress", protocol.ReadQueryProgressRequest{Space: r.Space, Query: r.Query, Anchor: r.Anchor}, &out, nil); err != nil {
		return out, err
	}
	if !validQueryProgress(out, r, c.identity.Principal) {
		_ = c.Close()
		return api.QueryProgressResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func queryIdentity(s string) bool {
	return s != "" && len(s) <= api.MaxIdentityBytes && utf8.ValidString(s)
}

// Activity/Result identities retain the generic Agent profile's text bound;
// they are not narrowed to a newly introduced Query DTO identity limit.
func queryProfileIdentity(s string) bool { return s != "" && len(s) <= 1024 && utf8.ValidString(s) }

func validQueryProgress(out api.QueryProgressResult, r api.ReadQueryProgressRequest, principal model.PrincipalID) bool {
	if out.Anchor.Validate() != nil || out.Anchor.Space != r.Space || out.Anchor.Knowledge != nil || out.Anchor.Cursor != nil || (r.Anchor != nil && !equalAnchor(out.Anchor, *r.Anchor)) {
		return false
	}
	if !out.Found {
		return reflect.DeepEqual(out.Progress, api.QueryProgress{})
	}
	p := out.Progress
	if p.Query != r.Query || !queryIdentity(p.Query) || p.Version == 0 || !queryIdentity(p.AdapterVersion) || !queryIdentity(p.Phase) || p.Source.Validate() != nil || p.Source.Space != r.Space || p.Source.Cursor != nil || p.Source.Knowledge != nil || !emissionFenceBefore(p.Source.Fence, out.Anchor.Fence) || p.Completed != (p.Next == nil) || len(p.Decisions) > agentprofile.MaxLifecycleRecords || p.Sequence != uint64(len(p.Decisions)) || p.Version <= p.Sequence || (p.Completed && p.Sequence == 0) {
		return false
	}
	if next := p.Next; next != nil && (next.Validate() != nil || next.Space != r.Space || next.Principal != principal || (next.Method != api.MethodApplyAgent && next.Method != api.MethodApplyWork)) {
		return false
	}
	identities, invocations, results := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, d := range p.Decisions {
		if d.Sequence != uint64(i+1) || !queryProfileIdentity(d.Activity) || !queryProfileIdentity(d.AcceptedResult) || identities[d.Activity] || results[d.AcceptedResult] || len(d.Invocations) > api.MaxWorkSteps {
			return false
		}
		identities[d.Activity], results[d.AcceptedResult] = true, true
		calls := map[string]bool{}
		for j, inv := range d.Invocations {
			if !queryIdentity(inv.ID) || !queryIdentity(inv.Activity) || !queryIdentity(inv.ProviderCallKey) || !queryIdentity(inv.Tool) || !queryIdentity(inv.ToolVersion) || !model.ValidDigest(string(inv.Definition)) || len(inv.Arguments) > api.MaxCommandBody || (j > 0 && d.Invocations[j-1].ID >= inv.ID) || invocations[inv.ID] || identities[inv.Activity] || calls[inv.ProviderCallKey] || (inv.AcceptedResult != "" && (!queryProfileIdentity(inv.AcceptedResult) || results[inv.AcceptedResult])) || (p.Completed && inv.AcceptedResult == "") {
				return false
			}
			if inv.DispatchClass != "pure" && inv.DispatchClass != "idempotent" && inv.DispatchClass != "non_idempotent" {
				return false
			}
			invocations[inv.ID], identities[inv.Activity], calls[inv.ProviderCallKey] = true, true, true
			if inv.AcceptedResult != "" {
				results[inv.AcceptedResult] = true
			}
		}
	}
	return true
}
