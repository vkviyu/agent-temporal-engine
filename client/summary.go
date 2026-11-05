package client

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (c *Client) ApplySummary(ctx context.Context, req api.ApplySummaryRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: req.Key.RequestID}
	if err := c.principal(req.Key.Principal); err != nil {
		return out, err
	}
	key := req.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, err := req.Summary.Canonical()
	if err != nil || req.Summary.Space != key.Space {
		if err == nil {
			err = api.ErrInvalidRequest
		}
		return out, err
	}
	err = c.call(ctx, "apply_summary", protocol.ApplySummaryRequest{Space: key.Space, RequestID: key.RequestID, Summary: raw, ExpectedFence: req.ExpectedFence}, &out, &key)
	return out, err
}

func (c *Client) GetSummary(ctx context.Context, req api.ReadSummaryRequest) (api.SummaryResult, error) {
	var out api.SummaryResult
	if err := c.principal(req.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "get_summary", protocol.ReadSummaryRequest{Space: req.Space, ID: req.ID, Anchor: req.Anchor}, &out, nil); err != nil {
		return out, err
	}
	invalid := out.Anchor.Validate() != nil || out.Anchor.Space != req.Space || out.Anchor.Fence != out.Fence || out.Anchor.Knowledge != nil || out.Anchor.Cursor != nil || !optionalID(out.RootSet)
	if req.Anchor != nil && !reflect.DeepEqual(*req.Anchor, out.Anchor) {
		invalid = true
	}
	if out.Found {
		if _, err := out.Summary.Canonical(); err != nil || out.Summary.ID != req.ID || out.Summary.Space != req.Space || !model.ValidDigest(string(out.Record)) || out.RootSet == "" || out.Summary.ValidateBefore(out.Fence) != nil {
			invalid = true
		}
	} else if out.Record != "" || !reflect.DeepEqual(out.Summary, knowledge.Summary{}) {
		invalid = true
	}
	if invalid {
		_ = c.Close()
		return api.SummaryResult{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.SummaryCommands = (*Client)(nil)
