package engine

import (
	"context"
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) ListNodes(ctx context.Context, req api.ListNodesRequest) (api.NodePage, error) {
	l, err := checkedNodeLimits(req.Limits)
	if err != nil {
		return api.NodePage{}, err
	}
	if req.Limit < 0 || req.Limit > l.nodes {
		return api.NodePage{}, storage.ErrLimit
	}
	size := req.Limit
	if size == 0 {
		size = l.nodes
	}
	layer := req.Layer
	if layer == "" {
		layer = "entities"
	}
	if layer != "entities" && layer != "content" && layer != "events" {
		return api.NodePage{}, ErrUnsupportedMethod
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.NodePage{}, err
	}
	defer e.releaseView(req.View, entry)
	scope := nodePageScope(entry, "list", struct {
		Layer         string
		Kind          model.EntityKind
		IncludeHidden bool
	}{layer, req.EntityKind, req.IncludeHidden})
	after, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return api.NodePage{}, err
	}
	c, err := e.nodeCatalog(ctx, entry, layer)
	if err != nil {
		return api.NodePage{}, e.nodeReadError(err)
	}
	hidden, err := e.hiddenNodes(ctx, entry, req.IncludeHidden)
	if err != nil {
		return api.NodePage{}, e.nodeReadError(err)
	}
	items := make([]api.NodeSummary, 0, len(c.nodes))
	for ref, n := range c.nodes {
		if hidden[ref] {
			continue
		}
		if req.EntityKind == "" || n.detail.Summary.Kind == req.EntityKind {
			items = append(items, n.detail.Summary)
		}
	}
	sort.Slice(items, func(i, j int) bool { return nodeSortKey(items[i].Ref) < nodeSortKey(items[j].Ref) })
	p := api.NodePage{Anchor: entry.descriptor.Anchor.Clone(), Items: []api.NodeSummary{}}
	start := sort.Search(len(items), func(i int) bool { return nodeSortKey(items[i].Ref) > after })
	used := jsonBytes(p)
	for i := start; i < len(items); i++ {
		if len(p.Items) == size {
			p.Truncated = true
			break
		}
		cost := jsonBytes(items[i])
		if len(p.Items) > 0 {
			cost++
		}
		// Reserve the exact token bytes whenever another item follows. This
		// makes the page bound include pagination metadata without reencoding
		// an ever-growing page for every candidate.
		tokenBytes := 0
		if i+1 < len(items) {
			tokenBytes = len(writeNodeToken(scope, nodeSortKey(items[i].Ref)))
		}
		if used+cost+tokenBytes > l.bytes {
			if len(p.Items) == 0 {
				return api.NodePage{}, storage.ErrLimit
			}
			p.Truncated = true
			break
		}
		p.Items = append(p.Items, items[i])
		used += cost
	}
	if p.Truncated {
		p.Continuation = writeNodeToken(scope, nodeSortKey(p.Items[len(p.Items)-1].Ref))
		if jsonBytes(p) > l.bytes {
			return api.NodePage{}, storage.ErrLimit
		}
	}
	if err = ctx.Err(); err != nil {
		return api.NodePage{}, err
	}
	return finishNodeRead(ctx, e, entry, "list_nodes", p, c, nil)
}
func (e *Engine) projectedNode(ctx context.Context, entry *viewEntry, ref model.NodeRef) (nodeProjection, nodeCatalog, error) {
	if ref.Validate() != nil {
		return nodeProjection{}, nodeCatalog{}, model.ErrInvalidNodeRef
	}
	if ref.Kind == model.NodeEntity {
		if n, handled, err := domainSubjectNode(ctx, &nodeQueryReader{source: e.index}, entry, ref); handled {
			c := nodeCatalog{nodes: map[model.NodeRef]nodeProjection{ref: n}, objects: map[model.ObjectID]codec.Object{}}
			return n, c, err
		}
	}
	if ref.Kind == model.NodeEntity && entry.descriptor.Anchor.Knowledge != nil && (ref.Entity.Profile == "memory" || (ref.Entity.Profile == "knowledge" && ref.Entity.Kind == "summary")) {
		c, err := e.nodeCatalog(ctx, entry, "content")
		if err != nil {
			return nodeProjection{}, c, err
		}
		if n, ok := c.nodes[ref]; ok {
			return n, c, nil
		}
		return nodeProjection{}, c, ErrNodeNotFound
	}
	layer := "content"
	if ref.Kind == model.NodeEntity {
		layer = "entities"
	} else if ref.Kind == model.NodeEvent {
		layer = "events"
	} else if ref.Kind != model.NodeObject && ref.Kind != model.NodeRevision {
		return nodeProjection{}, nodeCatalog{}, ErrUnsupportedMethod
	}
	c, err := e.nodeCatalog(ctx, entry, layer)
	if err != nil {
		return nodeProjection{}, c, err
	}
	if n, ok := c.nodes[ref]; ok {
		return n, c, nil
	}
	if ref.Kind == model.NodeObject {
		if n, handled, err := domainSubjectNode(ctx, &nodeQueryReader{source: e.index}, entry, ref); handled {
			c.nodes[ref] = n
			return n, c, err
		}
	}
	if ref.Kind == model.NodeRevision || ref.Kind == model.NodeEvent {
		return nodeProjection{}, c, ErrNodeNotFound
	}
	if obj, ok := c.objects[ref.Object]; ok {
		n := api.NodeSummary{Ref: ref, Kind: "record", Label: obj.Type}
		return nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: n, Payload: ref.Object, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}, c, nil
	}
	if ref.Kind == model.NodeObject && e.options.AgentProfile && entry.descriptor.Anchor.Knowledge == nil {
		profile, err := e.nodeCatalog(ctx, entry, "entities")
		if err != nil {
			return nodeProjection{}, c, err
		}
		if obj, ok := profile.objects[ref.Object]; ok {
			n := api.NodeSummary{Ref: ref, Kind: "profile_snapshot", Label: "agent snapshot"}
			return nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: n, Payload: ref.Object, Data: append([]byte(nil), obj.Payload...), Encoding: obj.Type + "/v1", Anchor: entry.descriptor.Anchor.Clone()}}, profile, nil
		}
		if err := e.addDeclaredNodeInputs(ctx, entry, &profile); err != nil {
			return nodeProjection{}, c, err
		}
		if n, ok := profile.nodes[ref]; ok {
			return n, profile, nil
		}
	}
	return nodeProjection{}, c, ErrNodeNotFound
}
func (e *Engine) GetNode(ctx context.Context, req api.GetNodeRequest) (api.NodeDetail, error) {
	l, err := checkedNodeLimits(req.Limit)
	if err != nil {
		return api.NodeDetail{}, err
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.NodeDetail{}, err
	}
	defer e.releaseView(req.View, entry)
	n, c, err := e.projectedNode(ctx, entry, req.Ref)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) || errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, model.ErrInvalidNodeRef) {
			return api.NodeDetail{}, err
		}
		return api.NodeDetail{}, e.nodeReadError(err)
	}
	n.detail.Anchors, err = e.nodeDeclaredAnchors(ctx, entry, n)
	if err != nil {
		return api.NodeDetail{}, e.nodeReadError(err)
	}
	if jsonBytes(n.detail) > l.bytes || len(n.detail.Sources) > l.edges {
		return api.NodeDetail{}, storage.ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return api.NodeDetail{}, err
	}
	return finishNodeRead(ctx, e, entry, "get_node", n.detail, c, nodeDisclosureIDs(n), n)
}

type contextNeighbor struct {
	section string
	node    api.NodeSummary
}

func (e *Engine) GetNodeContext(ctx context.Context, req api.NodeContextRequest) (api.NodeContext, error) {
	if typedContextMode(req.Mode) || len(req.Sections) > 0 {
		return e.getTypedNodeContext(ctx, req)
	}
	l, err := checkedNodeLimits(req.Limits)
	if err != nil {
		return api.NodeContext{}, err
	}
	if req.Mode != "" && req.Mode != "neighbors" && req.Mode != "parents" && req.Mode != "children" {
		return api.NodeContext{}, ErrUnsupportedMethod
	}
	if req.Projection != "" && req.Projection != "summary" {
		return api.NodeContext{}, ErrUnsupportedMethod
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.NodeContext{}, err
	}
	defer e.releaseView(req.View, entry)
	n, c, err := e.projectedNode(ctx, entry, req.Ref)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) || errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, model.ErrInvalidNodeRef) {
			return api.NodeContext{}, err
		}
		return api.NodeContext{}, e.nodeReadError(err)
	}
	scope := nodePageScope(entry, "context", struct {
		Ref              model.NodeRef
		Mode, Projection string
		Depth            int
	}{req.Ref, req.Mode, req.Projection, l.depth})
	after, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return api.NodeContext{}, err
	}
	neighbors := []contextNeighbor{}
	seen := map[model.NodeRef]bool{req.Ref: true}
	children := map[model.NodeRef][]model.NodeRef{}
	for ref, p := range c.nodes {
		for _, parent := range p.parents {
			children[parent] = append(children[parent], ref)
		}
	}
	for k := range children {
		sort.Slice(children[k], func(i, j int) bool { return nodeSortKey(children[k][i]) < nodeSortKey(children[k][j]) })
	}
	// Traverse each direction independently: going up to a Task then down
	// into a sibling Execution must not import sibling context implicitly.
	for _, section := range []string{"parents", "children"} {
		if req.Mode == "parents" && section == "children" || req.Mode == "children" && section == "parents" {
			continue
		}
		frontier := []model.NodeRef{req.Ref}
		for depth := 0; depth < l.depth && len(frontier) > 0; depth++ {
			next := []model.NodeRef{}
			for _, ref := range frontier {
				if err = ctx.Err(); err != nil {
					return api.NodeContext{}, err
				}
				refs := c.nodes[ref].parents
				if section == "children" {
					refs = children[ref]
				}
				for _, other := range refs {
					if seen[other] {
						continue
					}
					if p, ok := c.nodes[other]; ok {
						seen[other] = true
						neighbors = append(neighbors, contextNeighbor{section, p.detail.Summary})
						next = append(next, other)
					}
				}
			}
			frontier = next
		}
	}
	key := func(x contextNeighbor) string { return x.section + ":" + nodeSortKey(x.node.Ref) }
	sort.Slice(neighbors, func(i, j int) bool { return key(neighbors[i]) < key(neighbors[j]) })
	out := api.NodeContext{Ref: req.Ref, Anchor: entry.descriptor.Anchor.Clone(), Sections: map[string][]api.NodeSummary{"node": {n.detail.Summary}}}
	count := 1
	last := ""
	used := jsonBytes(out)
	for i, x := range neighbors {
		if key(x) <= after {
			continue
		}
		if count >= l.nodes || count-1 >= l.edges {
			out.Truncated = true
			break
		}
		cost := jsonBytes(x.node) + 1
		if _, exists := out.Sections[x.section]; !exists {
			cost += jsonBytes(x.section) + 3
		}
		tokenBytes := 0
		if i+1 < len(neighbors) {
			tokenBytes = len(writeNodeToken(scope, key(x)))
		}
		if used+cost+tokenBytes > l.bytes {
			out.Truncated = true
			break
		}
		out.Sections[x.section] = append(out.Sections[x.section], x.node)
		used += cost
		last = key(x)
		count++
	}
	if out.Truncated {
		if last == "" {
			return api.NodeContext{}, storage.ErrLimit
		}
		out.Continuation = writeNodeToken(scope, last)
	}
	if jsonBytes(out) > l.bytes {
		return api.NodeContext{}, storage.ErrLimit
	}
	return finishNodeRead(ctx, e, entry, "get_node_context", out, c, nil)
}

var _ api.BasicNodeReader = (*Engine)(nil)
