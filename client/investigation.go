package client

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (c *Client) UpdateInvestigation(ctx context.Context, req api.UpdateInvestigationRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: req.Key.RequestID}
	key := req.Key
	if err := c.principal(key.Principal); err != nil {
		return out, err
	}
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, err := investigation.EncodeUpdate(req.Command)
	if err != nil {
		return out, err
	}
	citations := req.Command.Citations()
	for _, citation := range citations {
		if citation.Anchor.Space != key.Space {
			return out, model.ErrAnchorMismatch
		}
	}
	wire := protocol.UpdateInvestigationRequest{Space: key.Space, RequestID: key.RequestID, Command: raw}
	err = c.call(ctx, "update_investigation", wire, &out, &key)
	if err == nil && out.Code == api.CodeDurableAck && !investigationSourcesPrecede(citations, key.Space, out.Fence) {
		_ = c.Close()
		return out, &OutcomeUnknownError{Key: key, Cause: protocol.ErrProtocol}
	}
	return out, err
}

func (c *Client) GetInvestigation(ctx context.Context, req api.GetInvestigationRequest) (api.InvestigationResult, error) {
	var out api.InvestigationResult
	if err := c.principal(req.Principal); err != nil {
		return out, err
	}
	wire := protocol.GetInvestigationRequest{Space: req.Space, ID: req.ID, Anchor: req.Anchor}
	if err := c.call(ctx, "get_investigation", wire, &out, nil); err != nil {
		return out, err
	}
	if !validInvestigationResult(req, out) {
		_ = c.Close()
		return api.InvestigationResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func validInvestigationResult(req api.GetInvestigationRequest, out api.InvestigationResult) bool {
	anchor := out.Anchor
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Fence != out.Fence || anchor.Knowledge != nil || anchor.Cursor != nil || !optionalID(out.RootSet) {
		return false
	}
	if req.Anchor != nil && !reflect.DeepEqual(*req.Anchor, anchor) {
		return false
	}
	if !out.Found {
		return reflect.DeepEqual(out.Checkpoint, investigation.Checkpoint{})
	}
	checkpoint := out.Checkpoint
	_, err := investigation.EncodeCheckpoint(checkpoint)
	return out.RootSet != "" && checkpoint.ID == req.ID && err == nil &&
		investigationSourcesPrecede(checkpoint.Citations(), req.Space, out.Fence)
}

func investigationSourcesPrecede(citations []investigation.Citation, space model.SpaceID, fence model.CommitFence) bool {
	for _, citation := range citations {
		source := citation.Anchor
		if source.Space != space || source.Fence.Store != fence.Store || source.Fence.LSN >= fence.LSN {
			return false
		}
	}
	return true
}

var _ api.InvestigationCommands = (*Client)(nil)
