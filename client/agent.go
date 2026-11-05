package client

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

var _ api.AgentCommands = (*Client)(nil)

func (c *Client) ApplyAgent(ctx context.Context, r api.ApplyAgentRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	command, err := agentprofile.EncodeCommand(r.Command)
	if err != nil {
		return out, err
	}
	err = c.call(ctx, "apply_agent", protocol.ApplyAgentRequest{Space: key.Space, RequestID: key.RequestID, Command: command, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, err
}

func (c *Client) GetAgentState(ctx context.Context, r api.ReadAgentStateRequest) (api.AgentStateResult, error) {
	var out api.AgentStateResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	err := c.call(ctx, "get_agent_state", protocol.ReadAgentStateRequest{Space: r.Space, Anchor: r.Anchor}, &out, nil)
	if err != nil {
		return out, err
	}
	if out.Anchor.Space != r.Space {
		_ = c.Close()
		return api.AgentStateResult{}, protocol.ErrProtocol
	}
	for _, task := range out.Snapshot.Tasks {
		if task.Space != r.Space || task.Owner != c.identity.Principal {
			_ = c.Close()
			return api.AgentStateResult{}, protocol.ErrProtocol
		}
	}
	if r.Anchor != nil && !equalAnchor(*r.Anchor, out.Anchor) {
		_ = c.Close()
		return api.AgentStateResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func equalAnchor(a, b model.SnapshotAnchor) bool {
	if a.Fence != b.Fence || a.Space != b.Space || (a.Cursor == nil) != (b.Cursor == nil) || (a.Knowledge == nil) != (b.Knowledge == nil) {
		return false
	}
	if a.Cursor != nil && *a.Cursor != *b.Cursor {
		return false
	}
	return a.Knowledge == nil || *a.Knowledge == *b.Knowledge
}

func validateAgentState(r api.AgentStateResult) error {
	if r.Anchor.Validate() != nil || r.Anchor.Fence != r.Fence || r.Anchor.Cursor != nil || r.Anchor.Knowledge != nil || !optionalID(r.RootSet) || !optionalID(r.LiveControlRoot) {
		return protocol.ErrProtocol
	}
	if r.Found && (r.RootSet == "" || r.LiveControlRoot == "") {
		return protocol.ErrProtocol
	}
	if !r.Found && (len(r.Snapshot.Tasks) != 0 || len(r.Snapshot.Executions) != 0 || len(r.Snapshot.Steps) != 0 || len(r.Snapshot.Invocations) != 0 || len(r.Snapshot.Actions) != 0) {
		return protocol.ErrProtocol
	}
	if _, err := agentprofile.EncodeSnapshot(r.Snapshot); err != nil {
		return protocol.ErrProtocol
	}
	return nil
}
