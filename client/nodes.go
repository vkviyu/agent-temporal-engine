package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func validateNodeSummary(s api.NodeSummary) error {
	if s.Ref.Validate() != nil || s.Kind == "" {
		return protocol.ErrProtocol
	}
	return nil
}
func (c *Client) ListNodes(ctx context.Context, r api.ListNodesRequest) (api.NodePage, error) {
	var out api.NodePage
	if err := c.call(ctx, "list_nodes", r, &out, nil); err != nil {
		return out, err
	}
	if out.Anchor.Validate() != nil || out.Truncated != (out.Continuation != "") {
		_ = c.Close()
		return api.NodePage{}, protocol.ErrProtocol
	}
	seen := map[model.NodeRef]bool{}
	for _, s := range out.Items {
		if validateNodeSummary(s) != nil || seen[s.Ref] {
			_ = c.Close()
			return api.NodePage{}, protocol.ErrProtocol
		}
		seen[s.Ref] = true
	}
	return out, nil
}
func (c *Client) GetNode(ctx context.Context, r api.GetNodeRequest) (api.NodeDetail, error) {
	var out api.NodeDetail
	if err := c.call(ctx, "get_node", r, &out, nil); err != nil {
		return out, err
	}
	if out.Ref != r.Ref || out.Summary.Ref != out.Ref || validateNodeSummary(out.Summary) != nil || out.Anchor.Validate() != nil || !optionalID(out.Payload) || out.Encoding == "" {
		_ = c.Close()
		return api.NodeDetail{}, protocol.ErrProtocol
	}
	for _, source := range out.Sources {
		if source.Validate() != nil {
			_ = c.Close()
			return api.NodeDetail{}, protocol.ErrProtocol
		}
	}
	_, edgeLimit, byteLimit := nodeExplorerLimits(r.Limit)
	if jsonBytesForNodes(out) > byteLimit || len(out.Sources) > edgeLimit || !validNodeAnchors(out.Anchors, out.Anchor, false) {
		_ = c.Close()
		return api.NodeDetail{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) GetNodeContext(ctx context.Context, r api.NodeContextRequest) (api.NodeContext, error) {
	var out api.NodeContext
	if err := c.call(ctx, "get_node_context", r, &out, nil); err != nil {
		return out, err
	}
	if out.Ref != r.Ref || out.Anchor.Validate() != nil || out.Truncated != (out.Continuation != "") {
		_ = c.Close()
		return api.NodeContext{}, protocol.ErrProtocol
	}
	for _, ss := range out.Sections {
		seen := map[model.NodeRef]bool{}
		for _, s := range ss {
			if validateNodeSummary(s) != nil || seen[s.Ref] {
				_ = c.Close()
				return api.NodeContext{}, protocol.ErrProtocol
			}
			seen[s.Ref] = true
		}
	}
	if !validNodeContext(r, out) {
		_ = c.Close()
		return api.NodeContext{}, protocol.ErrProtocol
	}
	return out, nil
}

func jsonBytesForNodes(value any) int { raw, _ := json.Marshal(value); return len(raw) }

func validNodeAnchors(anchors []api.NodeAnchor, selected model.SnapshotAnchor, required bool) bool {
	if len(anchors) > 8 || (required && len(anchors) == 0) {
		return false
	}
	seen := map[string]bool{}
	for i, a := range anchors {
		switch a.Relation {
		case "selected_view", "knowledge_source", "recorded_input", "observation_source", "activity_input", "summary_source":
		default:
			return false
		}
		if a.Anchor.Validate() != nil || a.Anchor.Space != selected.Space || a.Anchor.Fence.Store != selected.Fence.Store || a.Anchor.Fence.LSN > selected.Fence.LSN {
			return false
		}
		if (i == 0 && a.Relation != "selected_view") || (a.Relation == "selected_view" && (i != 0 || !equalAnchor(a.Anchor, selected))) {
			return false
		}
		raw, _ := json.Marshal(a)
		if seen[string(raw)] {
			return false
		}
		seen[string(raw)] = true
	}
	return true
}

func validNodeContext(r api.NodeContextRequest, out api.NodeContext) bool {
	nodeLimit, edgeLimit, byteLimit := nodeExplorerLimits(r.Limits)
	if jsonBytesForNodes(out) > byteLimit {
		return false
	}
	count := len(out.StateChanges)
	for _, ss := range out.Sections {
		count += len(ss)
	}
	if count > nodeLimit || (out.Mode != "" && count > edgeLimit) || (out.Mode == "" && count > edgeLimit+1) {
		return false
	}
	if out.Mode == "" {
		return len(r.Sections) == 0 && (r.Mode == "" || r.Mode == "neighbors" || r.Mode == "parents" || r.Mode == "children") && out.Coverage == "" && len(out.Gaps) == 0 && len(out.Anchors) == 0 && len(out.StateChanges) == 0 && !out.DepthLimited
	}
	wantMode := r.Mode
	if wantMode == "" && len(r.Sections) > 0 {
		wantMode = "evidence_neighborhood"
	}
	if out.Mode != wantMode || !validNodeAnchors(out.Anchors, out.Anchor, true) {
		return false
	}
	coverage := "bounded_explicit_relations"
	switch out.Mode {
	case "recorded_input", "evidence_neighborhood":
	case "as_known_at_anchor":
		coverage = "execution_knowledge_at_anchor"
		if out.Anchor.Knowledge == nil {
			return false
		}
	case "retrospective":
		coverage = "retrospective_at_view_fence"
	default:
		return false
	}
	if out.Coverage != coverage {
		return false
	}
	wanted := map[string]bool{}
	if len(r.Sections) > 0 {
		for _, section := range r.Sections {
			wanted[section] = true
		}
	} else if out.Mode == "recorded_input" {
		wanted["inputs"] = true
	} else {
		for _, section := range []string{"node", "inputs", "upstream", "downstream", "stateDiff", "messages", "activityReceipts", "artifacts"} {
			wanted[section] = true
		}
	}
	if len(out.Sections) != len(wanted) {
		return false
	}
	for section, ss := range out.Sections {
		if !wanted[section] {
			return false
		}
		if section == "node" && (len(ss) > 1 || len(ss) == 1 && ss[0].Ref != r.Ref) {
			return false
		}
		if section == "stateDiff" && len(ss) != 0 {
			return false
		}
	}
	seenGaps := map[string]bool{}
	for _, gap := range out.Gaps {
		if !wanted[gap.Section] || seenGaps[gap.Section+"/"+gap.Reason] {
			return false
		}
		switch gap.Reason {
		case "unavailable_in_view":
		case "no_recorded_input":
			if gap.Section != "inputs" {
				return false
			}
		case "no_state_revision":
			if gap.Section != "stateDiff" {
				return false
			}
		default:
			return false
		}
		seenGaps[gap.Section+"/"+gap.Reason] = true
	}
	for i, change := range out.StateChanges {
		if !wanted["stateDiff"] || r.Ref.Kind != model.NodeRevision || len(change.Key) > api.MaxNodeStateKeyBytes || !optionalID(change.Before) || !optionalID(change.After) || change.Before == change.After || (i > 0 && bytes.Compare(out.StateChanges[i-1].Key, change.Key) >= 0) {
			return false
		}
	}
	return true
}

var _ api.BasicNodeReader = (*Client)(nil)

func (c *Client) SearchNodes(ctx context.Context, r api.SearchNodesRequest) (api.NodeSearchResult, error) {
	var out api.NodeSearchResult
	if err := c.call(ctx, "search_nodes", r, &out, nil); err != nil {
		return out, err
	}
	nodeLimit, _, byteLimit := nodeExplorerLimits(r.Limits)
	if r.Limit > 0 && r.Limit < nodeLimit {
		nodeLimit = r.Limit
	}
	raw, _ := json.Marshal(out)
	invalid := out.Anchor.Validate() != nil || out.Truncated != (out.Continuation != "") || out.SearchVersion != "summary-text/v1" || out.Coverage != "complete_layer" || len(out.Items) > nodeLimit || len(raw) > byteLimit
	field, match := r.TextField, r.Match
	if field == "" {
		field = "label"
	}
	if match == "" {
		match = "contains"
	}
	seen := map[model.NodeRef]bool{}
	previous := ""
	for _, x := range out.Items {
		invalid = invalid || validateNodeSummary(x.Node) != nil || seen[x.Node.Ref] || r.EntityKind != "" && x.Node.Kind != r.EntityKind
		key, _ := json.Marshal(x.Node.Ref)
		invalid = invalid || previous != "" && string(key) <= previous
		previous = string(key)
		seen[x.Node.Ref] = true
		value := x.Node.Label
		if field == "kind" {
			value = string(x.Node.Kind)
		}
		matched := false
		switch match {
		case "contains":
			matched = strings.Contains(value, r.Text)
		case "prefix":
			matched = strings.HasPrefix(value, r.Text)
		case "exact":
			matched = value == r.Text
		}
		invalid = invalid || !matched
		expected := []string{}
		if r.EntityKind != "" {
			expected = append(expected, "kind:exact")
		}
		if r.Text != "" {
			expected = append(expected, field+":"+match)
		}
		if len(expected) != len(x.Reasons) {
			invalid = true
		} else {
			for i := range expected {
				invalid = invalid || expected[i] != x.Reasons[i]
			}
		}
	}
	if invalid {
		_ = c.Close()
		return api.NodeSearchResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func (c *Client) Trace(ctx context.Context, r api.TraceRequest) (api.NodeTrace, error) {
	var out api.NodeTrace
	if err := c.call(ctx, "trace_nodes", r, &out, nil); err != nil {
		return out, err
	}
	nodeLimit, edgeLimit, byteLimit := nodeExplorerLimits(r.Limits)
	raw, _ := json.Marshal(out)
	invalid := out.Start != r.Start || out.Anchor.Validate() != nil || out.Truncated != (out.Continuation != "") || len(out.Nodes) > nodeLimit || len(out.Edges) > edgeLimit || len(raw) > byteLimit
	nodes := map[model.NodeRef]bool{}
	for _, n := range out.Nodes {
		invalid = invalid || validateNodeSummary(n) != nil || nodes[n.Ref]
		nodes[n.Ref] = true
	}
	invalid = invalid || !nodes[r.Start]
	allowed := map[string]bool{}
	for _, x := range r.Relations {
		allowed[x] = true
	}
	if len(allowed) == 0 {
		for _, x := range []string{"parent", "source", "revision_parent", "causal_parent", "stream_previous"} {
			allowed[x] = true
		}
	}
	edges := map[api.NodeTraceEdge]bool{}
	for _, edge := range out.Edges {
		invalid = invalid || edge.From.Validate() != nil || edge.To.Validate() != nil || !nodes[edge.From] || !nodes[edge.To] || !allowed[edge.Relation] || edges[edge]
		edges[edge] = true
	}
	if invalid {
		_ = c.Close()
		return api.NodeTrace{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.NodeExplorer = (*Client)(nil)

func (c *Client) ReadContent(ctx context.Context, r api.ReadContentRequest) (api.ContentRange, error) {
	var out api.ContentRange
	if err := c.call(ctx, "read_content", r, &out, nil); err != nil {
		return out, err
	}
	projection := r.Projection
	if projection == "" {
		projection = "payload"
	}
	length := r.Length
	if length == 0 {
		length = api.DefaultContentChunkBytes
	}
	maxBytes := r.MaxBytes
	if maxBytes == 0 {
		maxBytes = 1 << 20
	}
	raw, _ := json.Marshal(out)
	invalid := out.Ref != r.Ref || out.Ref.Kind != model.NodeObject || out.Ref.Validate() != nil || out.Anchor.Validate() != nil || out.ObjectType == "" || out.ObjectVersion == 0 || out.Projection != projection || out.Offset != r.Offset || out.Offset > out.TotalBytes || !model.ValidDigest(string(out.Digest)) || len(raw) > maxBytes
	if !invalid {
		expected := uint64(length)
		if expected > out.TotalBytes-out.Offset {
			expected = out.TotalBytes - out.Offset
		}
		invalid = uint64(len(out.Data)) != expected || out.EOF != (out.Offset+expected == out.TotalBytes)
	}
	if !invalid && out.Offset == 0 && uint64(len(out.Data)) == out.TotalBytes {
		sum := sha256.Sum256(out.Data)
		invalid = model.ObjectID(hex.EncodeToString(sum[:])) != out.Digest
	}
	if invalid {
		_ = c.Close()
		return api.ContentRange{}, protocol.ErrProtocol
	}
	return out, nil
}

var _ api.ContentReader = (*Client)(nil)

func nodeExplorerLimits(q api.QueryLimits) (int, int, int) {
	nodes, edges, bytes := q.MaxNodes, q.MaxEdges, q.MaxBytes
	if nodes == 0 {
		nodes = 1024
	}
	if edges == 0 {
		edges = 2048
	}
	if bytes == 0 {
		bytes = 4 << 20
	}
	return nodes, edges, bytes
}
