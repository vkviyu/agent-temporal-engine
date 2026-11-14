package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func graphReadAnchor(e *Engine, ctx context.Context, space model.SpaceID, request *model.SnapshotAnchor) (model.SnapshotAnchor, model.RootSet, model.ObjectID, error) {
	anchor := model.SnapshotAnchor{Space: space, Fence: e.readPublication(ctx).Fence()}
	if request != nil {
		anchor = request.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != space {
		return model.SnapshotAnchor{}, model.RootSet{}, "", model.ErrAnchorMismatch
	}
	if anchor.Knowledge != nil {
		return model.SnapshotAnchor{}, model.RootSet{}, "", ErrUnsupportedMethod
	}
	if request == nil {
		return anchor, e.readRoots(ctx), e.readRootSet(ctx), nil
	}
	roots, id, err := e.rootsAtFence(ctx, anchor.Fence, false)
	return anchor, roots, id, err
}

func graphReadError(ctx context.Context, e *Engine, err error) error {
	if graphCommandRejection(err) || isLimit(err) || errors.Is(err, model.ErrAnchorMismatch) || errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, api.ErrInvalidRequest) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return e.readFailureAt(ctx, err)
}

func (e *Engine) graphReadStart(ctx context.Context, space model.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validSpace(space); err != nil {
		return err
	}
	return e.readReady(ctx)
}

type graphReadSession struct {
	engine  *Engine
	ctx     context.Context
	reader  *cursorReader
	space   model.SpaceID
	anchor  model.SnapshotAnchor
	roots   model.RootSet
	rootSet model.ObjectID
}

func (e *Engine) openGraphRead(ctx context.Context, space model.SpaceID, requested *model.SnapshotAnchor) (graphReadSession, error) {
	if err := e.graphReadStart(ctx, space); err != nil {
		return graphReadSession{}, err
	}
	anchor, roots, rootSet, err := graphReadAnchor(e, ctx, space, requested)
	if err != nil {
		return graphReadSession{}, graphReadError(ctx, e, err)
	}
	return graphReadSession{
		engine: e, ctx: ctx, reader: &cursorReader{source: e.reader(ctx)},
		space: space, anchor: anchor, roots: roots, rootSet: rootSet,
	}, nil
}

func (s graphReadSession) allow(ids ...model.RevisionID) error {
	return graphReadError(s.ctx, s.engine, graphAllowedAtCursor(s.ctx, s.reader, s.roots, s.anchor, ids...))
}

func graphAllowedAtCursor(ctx context.Context, reader *cursorReader, roots model.RootSet, anchor model.SnapshotAnchor, ids ...model.RevisionID) error {
	if anchor.Cursor == nil {
		return nil
	}
	g, _, err := loadGraphParents(ctx, reader, roots, anchor.Space, []model.RevisionID{anchor.Cursor.Revision})
	if err != nil {
		if errors.Is(err, graph.ErrUnknownRevision) {
			return model.ErrAnchorMismatch
		}
		return err
	}
	for _, id := range ids {
		ok, err := g.IsAncestor(id, anchor.Cursor.Revision)
		if err != nil || !ok {
			if err == nil || errors.Is(err, graph.ErrUnknownRevision) {
				return model.ErrAnchorMismatch
			}
			return err
		}
	}
	return nil
}

// finishGraphRead checks the policy for the returned page and for the graph
// records that produced it. Read their stored identities from the same
// authenticated BranchRefs cut that was used to compute the response.
func finishGraphRead[T any](ctx context.Context, e *Engine, reader *cursorReader, method string, anchor model.SnapshotAnchor, roots model.RootSet, out T, revisions []model.RevisionID, keys [][]byte, deps []model.ObjectID) (T, error) {
	var zero T
	seen := make(map[model.RevisionID]bool, len(revisions))
	for _, revision := range revisions {
		if revision != "" && !seen[revision] {
			seen[revision] = true
			keys = append(keys, graphRevisionKey(revision))
			deps = append(deps, model.ObjectID(revision))
		}
	}
	if len(keys) != 0 {
		root, err := slotSpaceRoot(ctx, reader, roots.BranchRefs, anchor.Space)
		if err != nil {
			return zero, graphReadError(ctx, e, err)
		}
		for _, key := range keys {
			id, found, err := state.Get(ctx, reader, anchor.Space, root, key)
			if err != nil {
				return zero, graphReadError(ctx, e, err)
			}
			if !found {
				return zero, graphReadError(ctx, e, ErrInvalidStateData)
			}
			deps = append(deps, id)
		}
	}
	if len(deps) != 0 {
		if err := e.checkDisclosureIDs(ctx, anchor.Space, deps); err != nil {
			return zero, err
		}
	}
	if err := e.viewOutputGate(); err != nil {
		return zero, err
	}
	return finishReadDelivery(ctx, e, method, readDeliveryPrincipal(ctx), out, anchor, "", "", deps)
}

func (e *Engine) GetGraphRevision(ctx context.Context, req api.ReadGraphRevisionRequest) (api.GraphRevisionResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_graph_revision", "embedded-host", req, func(scoped context.Context) (api.GraphRevisionResult, error) {
			return e.GetGraphRevision(scoped, req)
		})
	}
	if !model.ValidDigest(string(req.ID)) {
		return api.GraphRevisionResult{}, api.ErrInvalidRequest
	}
	session, err := e.openGraphRead(ctx, req.Space, req.Anchor)
	if err != nil {
		return api.GraphRevisionResult{}, err
	}
	if err = session.allow(req.ID); err != nil {
		return api.GraphRevisionResult{}, err
	}
	r, err := graphRevisionAt(ctx, session.reader, session.roots, req.Space, req.ID)
	found := true
	if errors.Is(err, graph.ErrUnknownRevision) {
		found = false
		err = nil
	}
	if err != nil {
		return api.GraphRevisionResult{}, graphReadError(ctx, e, err)
	}
	var revisions []model.RevisionID
	var deps []model.ObjectID
	if found {
		if err = verifyGraphEntries(ctx, session.reader, r); err != nil {
			return api.GraphRevisionResult{}, graphReadError(ctx, e, err)
		}
		revisions = []model.RevisionID{r.ID}
		deps = []model.ObjectID{r.StateRoot, model.ObjectID(r.StateParent)}
		for _, parent := range r.JoinParents {
			deps = append(deps, model.ObjectID(parent))
		}
		for _, event := range r.Events {
			deps = append(deps, model.ObjectID(event))
		}
		for _, entry := range r.Entries {
			deps = append(deps, entry.Value)
		}
	}
	out := api.GraphRevisionResult{Found: found, Value: r, Fence: session.anchor.Fence, Anchor: session.anchor, RootSet: session.rootSet}
	return finishGraphRead(ctx, e, session.reader, "get_graph_revision", session.anchor, session.roots, out, revisions, nil, deps)
}

func (e *Engine) GetGraphBranch(ctx context.Context, req api.ReadGraphBranchRequest) (api.GraphBranchResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_graph_branch", "embedded-host", req, func(scoped context.Context) (api.GraphBranchResult, error) {
			return e.GetGraphBranch(scoped, req)
		})
	}
	if _, err := graph.EncodeBranch(graph.Branch{ID: req.ID, Head: model.RevisionID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}); err != nil {
		return api.GraphBranchResult{}, api.ErrInvalidRequest
	}
	session, err := e.openGraphRead(ctx, req.Space, req.Anchor)
	if err != nil {
		return api.GraphBranchResult{}, err
	}
	b, prune, err := graphBranchAt(ctx, session.reader, session.roots, req.Space, req.ID)
	found := true
	if errors.Is(err, graph.ErrUnknownBranch) {
		found = false
		err = nil
	}
	if err != nil {
		return api.GraphBranchResult{}, graphReadError(ctx, e, err)
	}
	var keys [][]byte
	var deps []model.ObjectID
	if found {
		if err = session.allow(b.Head); err != nil {
			return api.GraphBranchResult{}, err
		}
		keys = append(keys, graphBranchKey(b.ID))
		deps = append(deps, model.ObjectID(b.Head))
		if b.Scope != nil {
			deps = append(deps, model.ObjectID(b.Scope.Base), model.ObjectID(b.PublishedHead))
		}
		if prune != nil {
			keys = append(keys, graphPrunedKey(b.ID))
		}
	}
	out := api.GraphBranchResult{Found: found, Value: b, Pruned: prune != nil, Prune: prune, Fence: session.anchor.Fence, Anchor: session.anchor, RootSet: session.rootSet}
	return finishGraphRead(ctx, e, session.reader, "get_graph_branch", session.anchor, session.roots, out, nil, keys, deps)
}

type graphQueryLimits struct{ nodes, edges, bytes, depth int }

func checkedGraphLimits(l api.QueryLimits) (graphQueryLimits, error) {
	x := graphQueryLimits{nodes: l.MaxNodes, edges: l.MaxEdges, bytes: l.MaxBytes, depth: l.MaxDepth}
	if x.nodes == 0 {
		x.nodes = 1024
	}
	if x.edges == 0 {
		x.edges = 2048
	}
	if x.bytes == 0 {
		x.bytes = 4 << 20
	}
	if x.depth == 0 {
		x.depth = 32
	}
	if x.nodes < 1 || x.nodes > graph.MaxWalkNodes || x.edges < 1 || x.edges > graph.MaxWalkEdges || x.bytes < 1 || x.bytes > 8<<20 || x.depth < 1 || x.depth > graph.MaxWalkNodes {
		return x, graph.ErrLimit
	}
	return x, nil
}

type graphPageToken struct {
	Digest string
	After  int
}

func graphQueryDigest(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(append([]byte("ATE-GRAPH-QUERY\x00"), b...))
	return hex.EncodeToString(sum[:])
}
func graphPageAfter(token, digest string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	if len(token) > 256 {
		return 0, api.ErrInvalidRequest
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, api.ErrInvalidRequest
	}
	var p graphPageToken
	if err = json.Unmarshal(raw, &p); err != nil || p.Digest != digest || p.After < 1 || p.After >= total {
		return 0, api.ErrInvalidRequest
	}
	canonical, _ := json.Marshal(p)
	if string(raw) != string(canonical) {
		return 0, api.ErrInvalidRequest
	}
	return p.After, nil
}
func graphNextPage(digest string, after int) string {
	b, _ := json.Marshal(graphPageToken{Digest: digest, After: after})
	return base64.RawURLEncoding.EncodeToString(b)
}
func graphPageLimit(n int, l graphQueryLimits) (int, error) {
	if n == 0 {
		n = l.nodes
	}
	if n < 1 || n > l.nodes {
		return 0, graph.ErrLimit
	}
	return n, nil
}

func (e *Engine) DiffGraph(ctx context.Context, req api.DiffGraphRequest) (api.GraphDiffResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "diff_graph", "embedded-host", req, func(scoped context.Context) (api.GraphDiffResult, error) {
			return e.DiffGraph(scoped, req)
		})
	}
	l, err := checkedGraphLimits(req.Limits)
	if err != nil {
		return api.GraphDiffResult{}, err
	}
	limit, err := graphPageLimit(req.Limit, l)
	if err != nil {
		return api.GraphDiffResult{}, err
	}
	if !model.ValidDigest(string(req.From)) || !model.ValidDigest(string(req.To)) {
		return api.GraphDiffResult{}, api.ErrInvalidRequest
	}
	session, err := e.openGraphRead(ctx, req.Space, req.Anchor)
	if err != nil {
		return api.GraphDiffResult{}, err
	}
	if err = session.allow(req.From, req.To); err != nil {
		return api.GraphDiffResult{}, err
	}
	g, records, err := loadGraphParents(ctx, session.reader, session.roots, req.Space, []model.RevisionID{req.From, req.To})
	if err != nil {
		return api.GraphDiffResult{}, graphReadError(ctx, e, err)
	}
	for _, id := range []model.RevisionID{req.From, req.To} {
		if err = verifyGraphEntries(ctx, session.reader, records[id]); err != nil {
			return api.GraphDiffResult{}, graphReadError(ctx, e, err)
		}
	}
	diff, err := g.Diff(req.From, req.To)
	if err != nil {
		return api.GraphDiffResult{}, graphReadError(ctx, e, err)
	}
	bound := req
	bound.Anchor = &session.anchor
	bound.Continuation = ""
	digest := graphQueryDigest(bound)
	start, err := graphPageAfter(req.Continuation, digest, len(diff.Changes))
	if err != nil {
		return api.GraphDiffResult{}, err
	}
	end := start + limit
	if end > len(diff.Changes) {
		end = len(diff.Changes)
	}
	out := api.GraphDiffResult{Anchor: session.anchor, RootSet: session.rootSet, Diff: graph.Diff{From: diff.From, To: diff.To, Changes: diff.Changes[start:end]}}
	for {
		if end < len(diff.Changes) {
			out.Truncated = true
			out.Continuation = graphNextPage(digest, end)
		}
		raw, _ := json.Marshal(out)
		if len(raw) <= l.bytes {
			break
		}
		if end <= start+1 {
			return api.GraphDiffResult{}, graph.ErrLimit
		}
		end--
		out.Diff.Changes = diff.Changes[start:end]
	}
	deps := make([]model.ObjectID, 0, len(out.Diff.Changes)*2)
	for _, change := range out.Diff.Changes {
		deps = append(deps, change.Before, change.After)
	}
	return finishGraphRead(ctx, e, session.reader, "diff_graph", session.anchor, session.roots, out, []model.RevisionID{req.From, req.To}, nil, deps)
}

func (e *Engine) PlanGraphJoin(ctx context.Context, req api.PlanGraphJoinRequest) (api.GraphJoinPlanResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "plan_graph_join", "embedded-host", req, func(scoped context.Context) (api.GraphJoinPlanResult, error) {
			return e.PlanGraphJoin(scoped, req)
		})
	}
	l, err := checkedGraphLimits(req.Limits)
	if err != nil {
		return api.GraphJoinPlanResult{}, err
	}
	if !model.ValidDigest(string(req.Left)) || !model.ValidDigest(string(req.Right)) {
		return api.GraphJoinPlanResult{}, api.ErrInvalidRequest
	}
	session, err := e.openGraphRead(ctx, req.Space, req.Anchor)
	if err != nil {
		return api.GraphJoinPlanResult{}, err
	}
	if err = session.allow(req.Left, req.Right); err != nil {
		return api.GraphJoinPlanResult{}, err
	}
	g, records, err := loadGraphParents(ctx, session.reader, session.roots, req.Space, []model.RevisionID{req.Left, req.Right})
	if err != nil {
		return api.GraphJoinPlanResult{}, graphReadError(ctx, e, err)
	}
	plan, err := g.PlanJoin(req.Left, req.Right)
	if err != nil {
		return api.GraphJoinPlanResult{}, graphReadError(ctx, e, err)
	}
	for _, id := range []model.RevisionID{plan.Base, req.Left, req.Right} {
		if err = verifyGraphEntries(ctx, session.reader, records[id]); err != nil {
			return api.GraphJoinPlanResult{}, graphReadError(ctx, e, err)
		}
	}
	out := api.GraphJoinPlanResult{Anchor: session.anchor, RootSet: session.rootSet, Plan: plan}
	raw, _ := json.Marshal(out)
	if len(plan.State)+len(plan.Conflicts) > l.nodes || len(plan.Changes) > l.edges || len(raw) > l.bytes {
		return api.GraphJoinPlanResult{}, graph.ErrLimit
	}
	deps := make([]model.ObjectID, 0, len(plan.State)+3*len(plan.Conflicts)+2*len(plan.Changes))
	for _, entry := range plan.State {
		deps = append(deps, entry.Value)
	}
	for _, conflict := range plan.Conflicts {
		deps = append(deps, conflict.Base, conflict.Left, conflict.Right)
	}
	for _, change := range plan.Changes {
		deps = append(deps, change.Before, change.After)
	}
	return finishGraphRead(ctx, e, session.reader, "plan_graph_join", session.anchor, session.roots, out, []model.RevisionID{plan.Base, req.Left, req.Right}, nil, deps)
}

func (e *Engine) TraceGraph(ctx context.Context, req api.TraceGraphRequest) (api.GraphTraceResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "trace_graph", "embedded-host", req, func(scoped context.Context) (api.GraphTraceResult, error) {
			return e.TraceGraph(scoped, req)
		})
	}
	l, err := checkedGraphLimits(req.Limits)
	if err != nil {
		return api.GraphTraceResult{}, err
	}
	limit, err := graphPageLimit(req.Limit, l)
	if err != nil {
		return api.GraphTraceResult{}, err
	}
	if !model.ValidDigest(string(req.Start)) {
		return api.GraphTraceResult{}, api.ErrInvalidRequest
	}
	direction := req.Direction
	if direction == "" {
		direction = "ancestors"
	}
	if direction != "ancestors" && direction != "descendants" {
		return api.GraphTraceResult{}, api.ErrInvalidRequest
	}
	session, err := e.openGraphRead(ctx, req.Space, req.Anchor)
	if err != nil {
		return api.GraphTraceResult{}, err
	}
	if err = session.allow(req.Start); err != nil {
		return api.GraphTraceResult{}, err
	}
	_, records, err := loadGraphParents(ctx, session.reader, session.roots, req.Space, []model.RevisionID{req.Start})
	if err != nil {
		return api.GraphTraceResult{}, graphReadError(ctx, e, err)
	}
	if direction == "descendants" {
		if session.anchor.Cursor != nil {
			_, records, err = loadGraphParents(ctx, session.reader, session.roots, req.Space, []model.RevisionID{session.anchor.Cursor.Revision})
		} else {
			records, err = graphAllRevisions(ctx, session.reader, session.roots, req.Space)
		}
		if err != nil {
			return api.GraphTraceResult{}, graphReadError(ctx, e, err)
		}
	}
	adj := map[model.RevisionID][]model.RevisionID{}
	for id, r := range records {
		parents := append([]model.RevisionID(nil), r.JoinParents...)
		if r.StateParent != "" {
			parents = append(parents, r.StateParent)
		}
		for _, p := range parents {
			if direction == "ancestors" {
				adj[id] = append(adj[id], p)
			} else {
				adj[p] = append(adj[p], id)
			}
		}
	}
	for id := range adj {
		sort.Slice(adj[id], func(i, j int) bool { return adj[id][i] < adj[id][j] })
	}
	nodes := []api.GraphTraceNode{{ID: req.Start, Depth: 0}}
	seen := map[model.RevisionID]bool{req.Start: true}
	edges := 0
	traversalTruncated := false
	for i := 0; i < len(nodes); i++ {
		if err = ctx.Err(); err != nil {
			return api.GraphTraceResult{}, err
		}
		n := nodes[i]
		if n.Depth >= l.depth {
			if len(adj[n.ID]) > 0 {
				traversalTruncated = true
			}
			continue
		}
		for _, next := range adj[n.ID] {
			edges++
			if edges > graph.MaxWalkEdges {
				return api.GraphTraceResult{}, graph.ErrLimit
			}
			if seen[next] {
				continue
			}
			if len(nodes) >= graph.MaxWalkNodes {
				return api.GraphTraceResult{}, graph.ErrLimit
			}
			seen[next] = true
			nodes = append(nodes, api.GraphTraceNode{ID: next, Depth: n.Depth + 1})
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Depth != nodes[j].Depth {
			return nodes[i].Depth < nodes[j].Depth
		}
		return nodes[i].ID < nodes[j].ID
	})
	bound := req
	bound.Anchor = &session.anchor
	bound.Direction = direction
	bound.Continuation = ""
	digest := graphQueryDigest(bound)
	start, err := graphPageAfter(req.Continuation, digest, len(nodes))
	if err != nil {
		return api.GraphTraceResult{}, err
	}
	end := start + limit
	if end > len(nodes) {
		end = len(nodes)
	}
	out := api.GraphTraceResult{Anchor: session.anchor, RootSet: session.rootSet}
	for {
		out.Nodes = nodes[start:end]
		out.Edges = nil
		for _, n := range out.Nodes {
			r := records[n.ID]
			if r.StateParent != "" && seen[r.StateParent] {
				out.Edges = append(out.Edges, api.GraphTraceEdge{Child: n.ID, Parent: r.StateParent})
			}
			for _, p := range r.JoinParents {
				if seen[p] {
					out.Edges = append(out.Edges, api.GraphTraceEdge{Child: n.ID, Parent: p})
				}
			}
		}
		out.Truncated = traversalTruncated || end < len(nodes)
		out.Continuation = ""
		if end < len(nodes) {
			out.Continuation = graphNextPage(digest, end)
		}
		raw, _ := json.Marshal(out)
		if len(out.Edges) <= l.edges && len(raw) <= l.bytes {
			break
		}
		if end <= start+1 {
			return api.GraphTraceResult{}, graph.ErrLimit
		}
		end--
	}
	revisions := make([]model.RevisionID, 1, 1+len(out.Nodes)+len(out.Edges))
	revisions[0] = req.Start
	for _, node := range out.Nodes {
		revisions = append(revisions, node.ID)
	}
	for _, edge := range out.Edges {
		revisions = append(revisions, edge.Parent)
	}
	return finishGraphRead(ctx, e, session.reader, "trace_graph", session.anchor, session.roots, out, revisions, nil, nil)
}

func graphAllRevisions(ctx context.Context, reader *cursorReader, roots model.RootSet, space model.SpaceID) (map[model.RevisionID]graph.Revision, error) {
	root, err := slotSpaceRoot(ctx, reader, roots.BranchRefs, space)
	if err != nil {
		return nil, err
	}
	entries, err := stateEntriesForGraph(ctx, reader, space, root)
	if err != nil {
		return nil, err
	}
	out := map[model.RevisionID]graph.Revision{}
	for _, id := range entries {
		r, err := graphRevisionAt(ctx, reader, roots, space, id)
		if err != nil {
			return nil, err
		}
		out[id] = r
	}
	return out, nil
}

func stateEntriesForGraph(ctx context.Context, reader *cursorReader, space model.SpaceID, root model.ObjectID) ([]model.RevisionID, error) {
	entries, err := state.Entries(ctx, reader, space, root, state.ScanLimits{MaxEntries: graph.MaxWalkNodes, MaxReadBytes: cursorMaxReadBytes})
	if err != nil {
		return nil, err
	}
	ids := make([]model.RevisionID, 0)
	for _, item := range entries {
		if !bytes.HasPrefix(item.Key, []byte("ate/graph/revision/")) {
			continue
		}
		id := model.RevisionID(item.Key[len("ate/graph/revision/"):])
		if !model.ValidDigest(string(id)) {
			return nil, ErrInvalidStateData
		}
		ids = append(ids, id)
	}
	return ids, nil
}

var _ api.GraphCommands = (*Engine)(nil)
var _ api.GraphQueries = (*Engine)(nil)

// LoadGraph captures latest exactly once. Every parent comes from that cut;
// the old facade's repeated latest reads could mix unrelated publications.
func (e *Engine) LoadGraph(ctx context.Context, space model.SpaceID, head model.RevisionID, maxRevisions int) (*graph.Store, error) {
	return e.embeddedLoadGraph(ctx, space, head, maxRevisions)
}
