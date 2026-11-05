package client

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (c *Client) ApplyGraph(ctx context.Context, r api.ApplyGraphRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, e := graph.EncodeCommand(key.Space, r.Command)
	if e != nil {
		return out, e
	}
	e = c.call(ctx, "apply_graph", protocol.ApplyGraphRequest{Space: key.Space, RequestID: key.RequestID, Command: raw, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, e
}
func (c *Client) GetGraphRevision(ctx context.Context, r api.ReadGraphRevisionRequest) (api.GraphRevisionResult, error) {
	var out api.GraphRevisionResult
	if err := c.call(ctx, "get_graph_revision", protocol.ReadGraphRevisionRequest{Space: r.Space, ID: r.ID, Anchor: r.Anchor}, &out, nil); err != nil {
		return out, err
	}
	if out.Fence.Validate() != nil {
		_ = c.Close()
		return api.GraphRevisionResult{}, protocol.ErrProtocol
	}
	if err := validateGraphAnchor(r.Space, r.Anchor, out.Anchor, out.RootSet); err != nil {
		_ = c.Close()
		return api.GraphRevisionResult{}, err
	}
	if out.Found {
		if _, err := graph.EncodeRevision(out.Value); err != nil || out.Value.Space != r.Space || out.Value.ID != r.ID {
			_ = c.Close()
			return api.GraphRevisionResult{}, protocol.ErrProtocol
		}
	} else if out.Value.ID != "" || out.Value.Space != "" || out.Value.StateRoot != "" || out.Value.StateParent != "" || len(out.Value.JoinParents)+len(out.Value.Events)+len(out.Value.Entries) != 0 {
		_ = c.Close()
		return api.GraphRevisionResult{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) GetGraphBranch(ctx context.Context, r api.ReadGraphBranchRequest) (api.GraphBranchResult, error) {
	var out api.GraphBranchResult
	if err := c.call(ctx, "get_graph_branch", protocol.ReadGraphBranchRequest{Space: r.Space, ID: r.ID, Anchor: r.Anchor}, &out, nil); err != nil {
		return out, err
	}
	if out.Fence.Validate() != nil {
		_ = c.Close()
		return api.GraphBranchResult{}, protocol.ErrProtocol
	}
	if err := validateGraphAnchor(r.Space, r.Anchor, out.Anchor, out.RootSet); err != nil {
		_ = c.Close()
		return api.GraphBranchResult{}, err
	}
	if out.Pruned != (out.Prune != nil) {
		_ = c.Close()
		return api.GraphBranchResult{}, protocol.ErrProtocol
	}
	if out.Prune != nil {
		p := out.Prune
		if _, err := graph.EncodePrune(*p); err != nil || p.Branch != out.Value.ID || p.Head != out.Value.Head || p.Version != out.Value.Version {
			_ = c.Close()
			return api.GraphBranchResult{}, protocol.ErrProtocol
		}
	}
	if out.Found {
		if _, err := graph.EncodeBranch(out.Value); err != nil || out.Value.ID != r.ID {
			_ = c.Close()
			return api.GraphBranchResult{}, protocol.ErrProtocol
		}
	} else if out.Value != (graph.Branch{}) {
		_ = c.Close()
		return api.GraphBranchResult{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) ApplyMemory(ctx context.Context, r api.ApplyMemoryRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, e := memory.EncodeCommand(r.Command)
	if e != nil {
		return out, e
	}
	e = c.call(ctx, "apply_memory", protocol.ApplyMemoryRequest{Space: key.Space, RequestID: key.RequestID, Command: raw, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, e
}
func (c *Client) GetMemoryState(ctx context.Context, r api.ReadMemoryStateRequest) (api.MemoryStateResult, error) {
	var out api.MemoryStateResult
	if err := c.call(ctx, "get_memory", protocol.ReadMemoryRequest{Space: r.Space}, &out, nil); err != nil {
		return out, err
	}
	invalid := out.Fence.Validate() != nil || !optionalID(out.RootSet) || !optionalID(out.StateRoot)
	if _, err := memory.EncodeSnapshot(out.Snapshot); err != nil {
		invalid = true
	}
	for _, m := range out.Snapshot.Memories {
		if m.Space != r.Space {
			invalid = true
		}
	}
	for _, p := range out.Snapshot.Paths {
		if p.Space != r.Space {
			invalid = true
		}
	}
	if !out.Found && len(out.Snapshot.Memories)+len(out.Snapshot.Paths)+len(out.Snapshot.Bans)+len(out.Snapshot.Successors)+len(out.Snapshot.PathSuccessors) != 0 {
		invalid = true
	}
	if out.Found && (out.RootSet == "" || out.StateRoot == "") {
		invalid = true
	}
	if invalid {
		_ = c.Close()
		return api.MemoryStateResult{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) ApplyKnowledge(ctx context.Context, r api.ApplyKnowledgeRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, e := knowledge.EncodeCommand(r.Command)
	if e != nil {
		return out, e
	}
	e = c.call(ctx, "apply_knowledge", protocol.ApplyKnowledgeRequest{Space: key.Space, RequestID: key.RequestID, Command: raw, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, e
}
func (c *Client) GetKnowledgeState(ctx context.Context, r api.ReadKnowledgeStateRequest) (api.KnowledgeStateResult, error) {
	var out api.KnowledgeStateResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "get_knowledge", protocol.ReadKnowledgeRequest{Space: r.Space, Anchor: r.Anchor, Knowledge: r.Knowledge}, &out, nil); err != nil {
		return out, err
	}
	if out.Anchor.Validate() != nil || out.Anchor.Space != r.Space || out.Anchor.Fence != out.Fence || !optionalID(out.RootSet) || !optionalID(out.StateRoot) {
		_ = c.Close()
		return api.KnowledgeStateResult{}, protocol.ErrProtocol
	}
	if r.Anchor != nil && (!sameOptionalCursor(r.Anchor.Cursor, out.Anchor.Cursor) || out.Anchor.Fence != r.Anchor.Fence) {
		_ = c.Close()
		return api.KnowledgeStateResult{}, protocol.ErrProtocol
	}
	want := r.Knowledge
	if want == nil && r.Anchor != nil {
		want = r.Anchor.Knowledge
	}
	if want != nil {
		if out.Anchor.Knowledge == nil || *out.Anchor.Knowledge != *want {
			_ = c.Close()
			return api.KnowledgeStateResult{}, protocol.ErrProtocol
		}
		for _, o := range out.Snapshot.Observations {
			if o.Key != *want {
				_ = c.Close()
				return api.KnowledgeStateResult{}, protocol.ErrProtocol
			}
		}
		for _, p := range out.Snapshot.Packets {
			if p.Key == nil || *p.Key != *want {
				_ = c.Close()
				return api.KnowledgeStateResult{}, protocol.ErrProtocol
			}
		}
		frontier, e := knowledge.Frontier(out.Snapshot, *want)
		if e != nil || frontier != out.Frontier {
			_ = c.Close()
			return api.KnowledgeStateResult{}, protocol.ErrProtocol
		}
		_, revision, e := knowledge.SelectRevision(out.Snapshot, r.Space, *want, "")
		if e != nil || out.Revision == nil || *out.Revision != revision || (out.Anchor.Cursor != nil && out.Anchor.Cursor.Revision != revision.ID) {
			_ = c.Close()
			return api.KnowledgeStateResult{}, protocol.ErrProtocol
		}
	} else if out.Revision != nil || out.Frontier != "" {
		_ = c.Close()
		return api.KnowledgeStateResult{}, protocol.ErrProtocol
	}
	if err := out.Snapshot.Validate(); err != nil {
		_ = c.Close()
		return api.KnowledgeStateResult{}, protocol.ErrProtocol
	}
	for _, observation := range out.Snapshot.Observations {
		fence := observation.Source.Anchor.Fence
		if observation.Source.Anchor.Space != r.Space || fence.Store != out.Fence.Store || fence.LSN > out.Fence.LSN || (fence.LSN == out.Fence.LSN && fence.CommitDigest != out.Fence.CommitDigest) {
			_ = c.Close()
			return api.KnowledgeStateResult{}, protocol.ErrProtocol
		}
	}
	for _, packet := range out.Snapshot.Packets {
		if packet.Space != r.Space || packet.Source.Anchor.Fence.Store != out.Fence.Store || packet.Source.Anchor.Fence.LSN > out.Fence.LSN {
			_ = c.Close()
			return api.KnowledgeStateResult{}, protocol.ErrProtocol
		}
	}
	return out, nil
}

func sameOptionalCursor(a, b *model.Cursor) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

var _ api.GraphCommands = (*Client)(nil)
var _ api.MemoryCommands = (*Client)(nil)
var _ api.KnowledgeCommands = (*Client)(nil)
