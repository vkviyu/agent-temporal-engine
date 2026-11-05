package client

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

var _ api.WorkCommands = (*Client)(nil)

func (c *Client) ApplyWork(ctx context.Context, r api.ApplyWorkRequest) (api.CommitResult, error) {
	b, err := api.EncodeWorkUnit(r.Unit)
	if err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}, err
	}
	return c.Commit(ctx, api.CommitRequest{Space: r.Key.Space, Principal: r.Key.Principal, RequestID: r.Key.RequestID, Method: api.MethodApplyWork, Body: b, ExpectedFence: r.ExpectedFence})
}
func (c *Client) GetQueryIntake(ctx context.Context, r api.ReadQueryIntakeRequest) (api.QueryIntakeResult, error) {
	var out api.QueryIntakeResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	err := c.call(ctx, "get_query_intake", protocol.ReadQueryIntakeRequest{Space: r.Space, Query: r.Query, Anchor: r.Anchor}, &out, nil)
	if err != nil {
		return out, err
	}
	if out.Anchor.Validate() != nil || out.Anchor.Space != r.Space || (r.Anchor != nil && !equalAnchor(out.Anchor, *r.Anchor)) {
		_ = c.Close()
		return api.QueryIntakeResult{}, protocol.ErrProtocol
	}
	if out.Found {
		q := out.State.Intake
		if q.Query.ID != r.Query || q.Query.Space != r.Space || q.Query.Owner != c.identity.Principal || q.Next.Space != r.Space || q.Next.Principal != c.identity.Principal {
			_ = c.Close()
			return api.QueryIntakeResult{}, protocol.ErrProtocol
		}
		if _, err := api.EncodeWorkUnit(api.WorkUnit{Kind: "query_intake", Query: r.Query, Intake: &q}); err != nil {
			_ = c.Close()
			return api.QueryIntakeResult{}, protocol.ErrProtocol
		}
	}
	return out, nil
}
