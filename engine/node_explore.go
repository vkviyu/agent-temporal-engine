package engine

import (
	"context"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const nodeSearchVersion = "summary-text/v1"

func normalizeNodeSearch(req api.SearchNodesRequest, l nodeQueryLimits) (api.SearchNodesRequest, error) {
	if req.Layer == "" {
		req.Layer = "entities"
	}
	if req.Layer != "entities" && req.Layer != "content" && req.Layer != "events" {
		return req, ErrUnsupportedMethod
	}
	if req.EventOperation != "" && req.Layer != "events" {
		return req, api.ErrInvalidRequest
	}
	if req.SearchVersion == "" {
		req.SearchVersion = nodeSearchVersion
	}
	if req.SearchVersion != nodeSearchVersion {
		return req, ErrUnsupportedMethod
	}
	if req.TextField == "" {
		req.TextField = "label"
	}
	if req.TextField != "label" && req.TextField != "kind" {
		return req, api.ErrInvalidRequest
	}
	if req.Match == "" {
		req.Match = "contains"
	}
	if req.Match != "contains" && req.Match != "exact" && req.Match != "prefix" {
		return req, api.ErrInvalidRequest
	}
	if len(req.Text) > 1024 || !utf8.ValidString(req.Text) {
		return req, api.ErrInvalidRequest
	}
	if req.Limit < 0 || req.Limit > l.nodes {
		return req, storage.ErrLimit
	}
	if req.Limit == 0 {
		req.Limit = l.nodes
	}
	return req, nil
}

func (e *Engine) SearchNodes(ctx context.Context, req api.SearchNodesRequest) (api.NodeSearchResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedViewReadDelivery(ctx, e, "search_nodes", req.View, req, func(scoped context.Context) (api.NodeSearchResult, error) {
			return e.SearchNodes(scoped, req)
		})
	}
	l, err := checkedNodeLimits(req.Limits)
	if err != nil {
		return api.NodeSearchResult{}, err
	}
	req, err = normalizeNodeSearch(req, l)
	if err != nil {
		return api.NodeSearchResult{}, err
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.NodeSearchResult{}, err
	}
	defer e.releaseView(req.View, entry)
	scope := nodePageScope(entry, "search", struct {
		Layer                                       string
		Kind                                        model.EntityKind
		Text, Field, Match, EventOperation, Version string
		IncludeHidden                               bool
	}{req.Layer, req.EntityKind, req.Text, req.TextField, req.Match, req.EventOperation, req.SearchVersion, req.IncludeHidden})
	after, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return api.NodeSearchResult{}, err
	}
	c, err := e.nodeCatalog(ctx, entry, req.Layer)
	if err != nil {
		return api.NodeSearchResult{}, e.nodeReadError(ctx, err)
	}
	hidden, err := e.hiddenNodes(ctx, entry, req.IncludeHidden)
	if err != nil {
		return api.NodeSearchResult{}, e.nodeReadError(ctx, err)
	}
	items := make([]api.NodeSearchMatch, 0, len(c.nodes))
	for ref, p := range c.nodes {
		if hidden[ref] {
			continue
		}
		if err = ctx.Err(); err != nil {
			return api.NodeSearchResult{}, err
		}
		n := p.detail.Summary
		if req.EntityKind != "" && n.Kind != req.EntityKind {
			continue
		}
		if req.EventOperation != "" && n.Label != req.EventOperation {
			continue
		}
		value := n.Label
		if req.TextField == "kind" {
			value = string(n.Kind)
		}
		matched := false
		switch req.Match {
		case "contains":
			matched = strings.Contains(value, req.Text)
		case "prefix":
			matched = strings.HasPrefix(value, req.Text)
		case "exact":
			matched = value == req.Text
		}
		if !matched {
			continue
		}
		reasons := []string{}
		if req.EntityKind != "" {
			reasons = append(reasons, "kind:exact")
		}
		if req.Text != "" {
			reasons = append(reasons, req.TextField+":"+req.Match)
		}
		items = append(items, api.NodeSearchMatch{Node: n, Reasons: reasons})
	}
	sort.Slice(items, func(i, j int) bool { return nodeSortKey(items[i].Node.Ref) < nodeSortKey(items[j].Node.Ref) })
	out := api.NodeSearchResult{Anchor: entry.descriptor.Anchor.Clone(), Items: []api.NodeSearchMatch{}, SearchVersion: nodeSearchVersion, Coverage: "complete_layer"}
	start := sort.Search(len(items), func(i int) bool { return nodeSortKey(items[i].Node.Ref) > after })
	used := jsonBytes(out)
	last := ""
	for i := start; i < len(items); i++ {
		cost := jsonBytes(items[i])
		if len(out.Items) > 0 {
			cost++
		}
		tokenBytes := 0
		if i+1 < len(items) {
			tokenBytes = len(writeNodeToken(scope, nodeSortKey(items[i].Node.Ref)))
		}
		if len(out.Items) >= req.Limit || used+cost+tokenBytes > l.bytes {
			out.Truncated = true
			break
		}
		out.Items = append(out.Items, items[i])
		used += cost
		last = nodeSortKey(items[i].Node.Ref)
	}
	if out.Truncated {
		if last == "" {
			return api.NodeSearchResult{}, storage.ErrLimit
		}
		out.Continuation = writeNodeToken(scope, last)
	}
	if jsonBytes(out) > l.bytes {
		return api.NodeSearchResult{}, storage.ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return api.NodeSearchResult{}, err
	}
	return finishNodeRead(ctx, e, entry, "search_nodes", out, c, nil)
}

func nodeRelation(child, parent model.NodeRef) string {
	if child.Kind == model.NodeEvent && parent.Kind == model.NodeEvent {
		return "causal_parent"
	}
	if child.Kind == model.NodeRevision && parent.Kind == model.NodeRevision {
		return "revision_parent"
	}
	if child.Kind == model.NodeEntity && parent.Kind == model.NodeEntity {
		return "parent"
	}
	return "source"
}
func traceEdgeKey(x api.NodeTraceEdge) string {
	return nodeSortKey(x.From) + ":" + nodeSortKey(x.To) + ":" + x.Relation
}
func normalizeTrace(req api.TraceRequest) (api.TraceRequest, error) {
	if req.Start.Validate() != nil {
		return req, model.ErrInvalidNodeRef
	}
	if req.Direction == "" {
		req.Direction = "parents"
	}
	if req.Direction != "parents" && req.Direction != "children" && req.Direction != "both" {
		return req, api.ErrInvalidRequest
	}
	if len(req.Relations) > 5 {
		return req, api.ErrInvalidRequest
	}
	if len(req.Relations) == 0 {
		req.Relations = []string{"causal_parent", "parent", "revision_parent", "source", "stream_previous"}
	} else {
		req.Relations = append([]string(nil), req.Relations...)
		sort.Strings(req.Relations)
	}
	for i, r := range req.Relations {
		if r != "parent" && r != "source" && r != "revision_parent" && r != "causal_parent" && r != "stream_previous" {
			return req, api.ErrInvalidRequest
		}
		if i > 0 && req.Relations[i-1] == r {
			return req, api.ErrInvalidRequest
		}
	}
	return req, nil
}

// traceNeighborhood builds only the selected layer's authenticated adjacency.
// Every edge needs both endpoints in the fixed View, so an opaque source cannot
// become a payload grant merely because its identifier occurs in a record.
func traceNeighborhood(ctx context.Context, c nodeCatalog, req api.TraceRequest, depth int) ([]api.NodeTraceEdge, bool, error) {
	parents := map[model.NodeRef][]api.NodeTraceEdge{}
	children := map[model.NodeRef][]api.NodeTraceEdge{}
	allowed := map[string]bool{}
	for _, r := range req.Relations {
		allowed[r] = true
	}
	edgeCount := 0
	for ref, p := range c.nodes {
		if p.streamPrevious != "" && allowed["stream_previous"] {
			previous := model.NodeRef{Kind: model.NodeEvent, Event: p.streamPrevious}
			if _, ok := c.nodes[previous]; ok {
				edgeCount++
				if edgeCount > maxNodeQueryRecords {
					return nil, false, storage.ErrLimit
				}
				edge := api.NodeTraceEdge{From: ref, To: previous, Relation: "stream_previous"}
				parents[ref] = append(parents[ref], edge)
				children[previous] = append(children[previous], edge)
			}
		}
		for _, parent := range p.parents {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if _, ok := c.nodes[parent]; !ok {
				continue
			}
			relation := nodeRelation(ref, parent)
			if !allowed[relation] {
				continue
			}
			edgeCount++
			if edgeCount > maxNodeQueryRecords {
				return nil, false, storage.ErrLimit
			}
			edge := api.NodeTraceEdge{From: ref, To: parent, Relation: relation}
			parents[ref] = append(parents[ref], edge)
			children[parent] = append(children[parent], edge)
		}
	}
	for _, adj := range []map[model.NodeRef][]api.NodeTraceEdge{parents, children} {
		for k := range adj {
			sort.Slice(adj[k], func(i, j int) bool { return traceEdgeKey(adj[k][i]) < traceEdgeKey(adj[k][j]) })
		}
	}
	selected := map[string]api.NodeTraceEdge{}
	depthLimited := false
	for _, direction := range []string{"parents", "children"} {
		if req.Direction != "both" && req.Direction != direction {
			continue
		}
		adj := parents
		if direction == "children" {
			adj = children
		}
		seen := map[model.NodeRef]bool{req.Start: true}
		frontier := []model.NodeRef{req.Start}
		for level := 0; len(frontier) > 0; level++ {
			next := []model.NodeRef{}
			for _, ref := range frontier {
				for _, edge := range adj[ref] {
					if err := ctx.Err(); err != nil {
						return nil, false, err
					}
					other := edge.To
					if direction == "children" {
						other = edge.From
					}
					if level == depth {
						if _, included := selected[traceEdgeKey(edge)]; !included {
							depthLimited = true
						}
						continue
					}
					selected[traceEdgeKey(edge)] = edge
					if !seen[other] {
						seen[other] = true
						next = append(next, other)
					}
				}
			}
			frontier = next
		}
	}
	out := make([]api.NodeTraceEdge, 0, len(selected))
	for _, edge := range selected {
		out = append(out, edge)
	}
	sort.Slice(out, func(i, j int) bool { return traceEdgeKey(out[i]) < traceEdgeKey(out[j]) })
	return out, depthLimited, nil
}

func (e *Engine) Trace(ctx context.Context, req api.TraceRequest) (api.NodeTrace, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedViewReadDelivery(ctx, e, "trace_nodes", req.View, req, func(scoped context.Context) (api.NodeTrace, error) {
			return e.Trace(scoped, req)
		})
	}
	l, err := checkedNodeLimits(req.Limits)
	if err != nil {
		return api.NodeTrace{}, err
	}
	req, err = normalizeTrace(req)
	if err != nil {
		return api.NodeTrace{}, err
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.NodeTrace{}, err
	}
	defer e.releaseView(req.View, entry)
	start, c, err := e.projectedNode(ctx, entry, req.Start)
	if err != nil {
		if err == model.ErrInvalidNodeRef {
			return api.NodeTrace{}, err
		}
		return api.NodeTrace{}, e.nodeReadError(ctx, err)
	}
	scope := nodePageScope(entry, "trace", struct {
		Start     model.NodeRef
		Direction string
		Relations []string
		Depth     int
	}{req.Start, req.Direction, req.Relations, l.depth})
	after, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return api.NodeTrace{}, err
	}
	edges, depthLimited, err := traceNeighborhood(ctx, c, req, l.depth)
	if err != nil {
		return api.NodeTrace{}, e.nodeReadError(ctx, err)
	}
	out := api.NodeTrace{Anchor: entry.descriptor.Anchor.Clone(), Start: req.Start, Nodes: []api.NodeSummary{start.detail.Summary}, Edges: []api.NodeTraceEdge{}, DepthLimited: depthLimited}
	nodes := map[model.NodeRef]bool{req.Start: true}
	last := ""
	used := jsonBytes(out)
	for i, edge := range edges {
		key := traceEdgeKey(edge)
		if key <= after {
			continue
		}
		added := []api.NodeSummary{}
		for _, ref := range []model.NodeRef{edge.From, edge.To} {
			if !nodes[ref] {
				if len(added) == 0 || added[0].Ref != ref {
					added = append(added, c.nodes[ref].detail.Summary)
				}
			}
		}
		cost := jsonBytes(edge)
		if len(out.Edges) > 0 {
			cost++
		}
		for _, n := range added {
			cost += jsonBytes(n) + 1
		}
		tokenBytes := 0
		if i+1 < len(edges) {
			tokenBytes = len(writeNodeToken(scope, key))
		}
		if len(out.Edges) >= l.edges || len(out.Nodes)+len(added) > l.nodes || used+cost+tokenBytes > l.bytes {
			out.Truncated = true
			break
		}
		out.Edges = append(out.Edges, edge)
		for _, n := range added {
			out.Nodes = append(out.Nodes, n)
			nodes[n.Ref] = true
		}
		used += cost
		last = key
	}
	if out.Truncated {
		if last == "" {
			return api.NodeTrace{}, storage.ErrLimit
		}
		out.Continuation = writeNodeToken(scope, last)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return nodeSortKey(out.Nodes[i].Ref) < nodeSortKey(out.Nodes[j].Ref) })
	if jsonBytes(out) > l.bytes {
		return api.NodeTrace{}, storage.ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return api.NodeTrace{}, err
	}
	return finishNodeRead(ctx, e, entry, "trace_nodes", out, c, nil)
}

var _ api.NodeExplorer = (*Engine)(nil)
