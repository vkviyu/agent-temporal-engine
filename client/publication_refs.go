package client

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
	"reflect"
)

func (c *Client) ResolvePublication(ctx context.Context, request api.ResolvePublicationRequest) (api.SourcePublication, error) {
	var out api.SourcePublication
	if err := c.principal(request.Principal); err != nil {
		return out, err
	}
	if err := request.Source.Validate(); err != nil {
		return out, err
	}
	if err := c.call(ctx, "resolve_publication", protocol.ResolvePublicationRequest{Source: request.Source}, &out, nil); err != nil {
		return out, err
	}
	expected := model.SnapshotAnchor{Space: request.Source.Space, Fence: out.Anchor.Fence, Cursor: request.Source.Cursor, Knowledge: request.Source.Knowledge}
	if !reflect.DeepEqual(out.Source, request.Source) || out.Source.Validate() != nil || out.Anchor.Validate() != nil || out.Anchor.Fence.Store != request.Source.Store || out.Anchor.Fence.LSN == 0 || !reflect.DeepEqual(out.Anchor, expected) || !model.ValidDigest(string(out.RootSet)) {
		_ = c.Close()
		return api.SourcePublication{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.PublicationResolver = (*Client)(nil)
