package client

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (c *Client) ApplyTaskTrace(ctx context.Context, req api.ApplyTaskTraceRequest) (api.CommitResult, error) {
	key := req.Key
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: key.RequestID}
	if err := c.principal(key.Principal); err != nil {
		return out, err
	}
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	if req.Edit.Source.Space != key.Space {
		return out, model.ErrAnchorMismatch
	}
	edit, err := api.EncodeTaskTraceEdit(req.Edit)
	if err != nil {
		return out, err
	}
	wire := protocol.ApplyTaskTraceRequest{Space: key.Space, RequestID: key.RequestID, Edit: edit}
	err = c.call(ctx, "apply_task_trace", wire, &out, &key)
	return out, err
}

func wireTaskTrace(req api.TaskTraceRequest) protocol.GetTaskTraceRequest {
	return protocol.GetTaskTraceRequest{Space: req.Space, Subject: req.Subject, Session: req.Session, Anchor: req.Anchor, Limit: req.Limit, Continuation: req.Continuation}
}

func validTaskTraceAnchor(req api.TaskTraceRequest, anchor model.SnapshotAnchor) bool {
	return anchor.Validate() == nil && anchor.Space == req.Space && anchor.Cursor == nil && anchor.Knowledge == nil &&
		(req.Anchor == nil || reflect.DeepEqual(*req.Anchor, anchor))
}

func (c *Client) GetTaskTrace(ctx context.Context, req api.TaskTraceRequest) (api.TaskTraceResult, error) {
	var out api.TaskTraceResult
	if err := c.principal(req.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "get_task_trace", wireTaskTrace(req), &out, nil); err != nil {
		return out, err
	}
	if out.Subject != req.Subject || !validTaskTraceAnchor(req, out.Anchor) {
		_ = c.Close()
		return api.TaskTraceResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func (c *Client) GetTaskTraceEvidence(ctx context.Context, req api.TaskTraceEvidenceRequest) (api.TaskTraceEvidenceResult, error) {
	var out api.TaskTraceEvidenceResult
	if err := c.principal(req.Principal); err != nil {
		return out, err
	}
	wire := protocol.GetTaskTraceEvidenceRequest{GetTaskTraceRequest: wireTaskTrace(req.TaskTraceRequest), From: req.From, To: req.To, Relation: req.Relation}
	if err := c.call(ctx, "get_task_trace_evidence", wire, &out, nil); err != nil {
		return out, err
	}
	if !validTaskTraceAnchor(req.TaskTraceRequest, out.Anchor) {
		_ = c.Close()
		return api.TaskTraceEvidenceResult{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.TaskTraceCommands = (*Client)(nil)
