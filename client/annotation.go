package client

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (c *Client) ApplyAnnotation(ctx context.Context, req api.ApplyAnnotationRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: req.Key.RequestID}
	if err := c.principal(req.Key.Principal); err != nil {
		return out, err
	}
	key := req.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, err := annotation.EncodeRecord(req.Command)
	if err != nil || req.Command.Source.Space != key.Space {
		if err == nil {
			err = api.ErrInvalidRequest
		}
		return out, err
	}
	err = c.call(ctx, "apply_annotation", protocol.ApplyAnnotationRequest{Space: key.Space, RequestID: key.RequestID, Command: raw, ExpectedFence: req.ExpectedFence}, &out, &key)
	if err == nil && out.Code == api.CodeDurableAck && (req.Command.Source.Fence.Store != out.Fence.Store || req.Command.Source.Fence.LSN >= out.Fence.LSN) {
		_ = c.Close()
		return out, &OutcomeUnknownError{Key: key, Cause: protocol.ErrProtocol}
	}
	return out, err
}

func (c *Client) GetAnnotations(ctx context.Context, req api.ReadAnnotationsRequest) (api.AnnotationsResult, error) {
	var out api.AnnotationsResult
	if err := c.principal(req.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "get_annotations", protocol.ReadAnnotationsRequest{Space: req.Space, Anchor: req.Anchor}, &out, nil); err != nil {
		return out, err
	}
	invalid := out.Anchor.Validate() != nil || out.Anchor.Space != req.Space || out.Anchor.Fence != out.Fence || out.Anchor.Knowledge != nil || out.Anchor.Cursor != nil || !optionalID(out.RootSet)
	if req.Anchor != nil && !reflect.DeepEqual(*req.Anchor, out.Anchor) {
		invalid = true
	}
	if _, err := annotation.EncodeSnapshot(out.Snapshot); err != nil {
		invalid = true
	}
	if len(out.Snapshot.Records) > 0 && out.RootSet == "" {
		invalid = true
	}
	for _, record := range out.Snapshot.Records {
		source := record.Source
		if source.Space != req.Space || source.Fence.Store != out.Fence.Store || source.Fence.LSN >= out.Fence.LSN {
			invalid = true
		}
	}
	if invalid {
		_ = c.Close()
		return api.AnnotationsResult{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.AnnotationCommands = (*Client)(nil)
