package client

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func validateGraphAnchor(space model.SpaceID, want *model.SnapshotAnchor, got model.SnapshotAnchor, root model.ObjectID) error {
	if got.Validate() != nil || got.Space != space || got.Knowledge != nil || !optionalID(root) {
		return protocol.ErrProtocol
	}
	if want != nil && !equalAnchor(*want, got) {
		return protocol.ErrProtocol
	}
	return nil
}

func (c *Client) DiffGraph(ctx context.Context, r api.DiffGraphRequest) (api.GraphDiffResult, error) {
	var out api.GraphDiffResult
	if err := c.call(ctx, "diff_graph", r, &out, nil); err != nil {
		return out, err
	}
	invalid := validateGraphAnchor(r.Space, r.Anchor, out.Anchor, out.RootSet) != nil || out.Diff.From != r.From || out.Diff.To != r.To || (!out.Truncated && out.Continuation != "")
	for i, x := range out.Diff.Changes {
		if len(x.Key) > graph.MaxEntryKeyBytes || !optionalID(x.Before) || !optionalID(x.After) || x.Before == x.After || (i > 0 && bytes.Compare(out.Diff.Changes[i-1].Key, x.Key) >= 0) {
			invalid = true
		}
		switch x.Kind {
		case graph.ChangeAdd:
			invalid = invalid || x.Before != "" || x.After == ""
		case graph.ChangeRemove:
			invalid = invalid || x.Before == "" || x.After != ""
		case graph.ChangeUpdate:
			invalid = invalid || x.Before == "" || x.After == ""
		default:
			invalid = true
		}
	}
	if invalid {
		_ = c.Close()
		return api.GraphDiffResult{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) PlanGraphJoin(ctx context.Context, r api.PlanGraphJoinRequest) (api.GraphJoinPlanResult, error) {
	var out api.GraphJoinPlanResult
	if err := c.call(ctx, "plan_graph_join", r, &out, nil); err != nil {
		return out, err
	}
	if validateGraphAnchor(r.Space, r.Anchor, out.Anchor, out.RootSet) != nil || out.Plan.Left != r.Left || out.Plan.Right != r.Right || !model.ValidDigest(string(out.Plan.Base)) {
		_ = c.Close()
		return api.GraphJoinPlanResult{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) TraceGraph(ctx context.Context, r api.TraceGraphRequest) (api.GraphTraceResult, error) {
	var out api.GraphTraceResult
	if err := c.call(ctx, "trace_graph", r, &out, nil); err != nil {
		return out, err
	}
	invalid := validateGraphAnchor(r.Space, r.Anchor, out.Anchor, out.RootSet) != nil || (!out.Truncated && out.Continuation != "")
	seen := map[model.RevisionID]bool{}
	for i, n := range out.Nodes {
		if !model.ValidDigest(string(n.ID)) || n.Depth < 0 || seen[n.ID] {
			invalid = true
		}
		seen[n.ID] = true
		if i > 0 {
			p := out.Nodes[i-1]
			if p.Depth > n.Depth || (p.Depth == n.Depth && p.ID >= n.ID) {
				invalid = true
			}
		}
	}
	for _, edge := range out.Edges {
		if !model.ValidDigest(string(edge.Child)) || !model.ValidDigest(string(edge.Parent)) {
			invalid = true
		}
	}
	if invalid {
		_ = c.Close()
		return api.GraphTraceResult{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.GraphQueries = (*Client)(nil)
