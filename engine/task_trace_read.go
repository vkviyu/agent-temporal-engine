package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const (
	defaultTaskTracePage = 32
	maxTaskTracePage     = 128
	maxTaskTraceEdges    = 512
)

type taskTraceOccurrence struct {
	event model.Event
	lsn   uint64
}

type taskTraceRead struct {
	anchor     model.SnapshotAnchor
	entry      *viewEntry
	reader     *nodeQueryReader
	edit       api.TaskTraceEdit
	record     model.ObjectID
	keys       []traceEventKey
	events     map[model.EventID]taskTraceOccurrence
	boundary   map[model.EventID]model.Event
	order      []model.EventID
	provenance traceProvenance
	loadEvent  func(context.Context, model.EventID) (model.Event, error)
	inScope    func(context.Context, model.EventID) (bool, error)
	metadata   func(context.Context, model.EventID) (eventMetadataItem, bool, error)
	allItems   func(context.Context) ([]eventMetadataItem, error)
	children   func(context.Context, model.EventID) ([]eventMetadataItem, error)
}

func (e *Engine) TaskTraceEnabled() bool {
	return e.options.AgentProfile && e.options.EventProfile
}

func (e *Engine) readTaskTrace(ctx context.Context, req api.TaskTraceRequest) (taskTraceRead, error) {
	if !e.TaskTraceEnabled() {
		return taskTraceRead{}, ErrUnsupportedMethod
	}
	if req.Subject.Validate() != nil || validAgentScope(req.Space, req.Principal) != nil ||
		req.Limit < 0 || req.Limit > maxTaskTracePage || (req.Continuation != "" && req.Anchor == nil) ||
		(req.Session != "" && (req.Subject.Kind != "query" || len(req.Session) > model.MaxIdentityBytes || !utf8.ValidString(req.Session))) {
		return taskTraceRead{}, api.ErrInvalidRequest
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
		return taskTraceRead{}, model.ErrAnchorMismatch
	}
	entry, err := e.historicalSourceEntry(ctx, req.Space, req.Principal, anchor, "audit_snapshot")
	if err != nil {
		if errors.Is(err, model.ErrAnchorMismatch) {
			return taskTraceRead{}, err
		}
		return taskTraceRead{}, e.nodeReadError(ctx, err)
	}
	edit, record, err := e.loadTaskTrace(ctx, entry.roots, req)
	if err != nil {
		return taskTraceRead{}, e.nodeReadError(ctx, err)
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.reader(ctx), entry.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return taskTraceRead{}, e.nodeReadError(ctx, err)
	}
	snapshot := ledger.Snapshot()
	keys, err := taskTraceScope(req, ledger, snapshot)
	if err != nil {
		return taskTraceRead{}, err
	}
	reader := &nodeQueryReader{source: e.reader(ctx)}
	result := taskTraceRead{anchor: anchor, entry: entry, reader: reader, edit: edit, record: record,
		events: make(map[model.EventID]taskTraceOccurrence), boundary: map[model.EventID]model.Event{},
		provenance: newTraceProvenance(entry.roots, snapshot), keys: keys}
	result.metadata = e.traceEventMetadata
	result.allItems = func(loadCtx context.Context) ([]eventMetadataItem, error) {
		return e.traceEventIDs(loadCtx, keys, anchor.Fence)
	}
	result.inScope = func(loadCtx context.Context, id model.EventID) (bool, error) {
		return e.traceEventInScope(loadCtx, id, keys, anchor.Fence)
	}
	result.children = func(loadCtx context.Context, id model.EventID) ([]eventMetadataItem, error) {
		return e.traceEventChildren(loadCtx, id, anchor.Fence)
	}
	result.loadEvent = func(loadCtx context.Context, id model.EventID) (model.Event, error) {
		if occurrence, found := result.events[id]; found {
			return occurrence.event, nil
		}
		meta, found, err := e.traceEventMetadata(loadCtx, id)
		if err != nil {
			return model.Event{}, err
		}
		if !found || meta.entry.Space != req.Space || meta.entry.Principal != req.Principal || meta.entry.LSN > anchor.Fence.LSN {
			return model.Event{}, ErrNodeNotFound
		}
		event, _, err := loadViewEvent(loadCtx, reader, entry, id)
		if err != nil {
			return model.Event{}, err
		}
		if event.Attribution == nil {
			return model.Event{}, ErrStoredObject
		}
		result.events[id] = taskTraceOccurrence{event: event, lsn: meta.entry.LSN}
		return event, nil
	}
	return result, nil
}

func (r taskTraceRead) node(id model.EventID, milestone api.TraceMilestone) api.TaskTraceNode {
	event := r.occurrence(id)
	node := api.TaskTraceNode{
		At:        api.TraceCitation{Ref: model.NodeRef{Kind: model.NodeEvent, Event: id}, Anchor: r.anchor.Clone()},
		Operation: event.Method, Boundary: r.boundary[id].ID != "", Label: milestone.Label, Evidence: milestone.Evidence,
	}
	if a := event.Attribution; a != nil {
		node.Operation, node.Subject, node.Execution = a.Operation, a.Subject, a.Execution
		node.Task, node.Query, node.Session = a.Task, a.Query, a.Session
	}
	return node
}

func (r taskTraceRead) occurrence(id model.EventID) model.Event {
	if event, ok := r.events[id]; ok {
		return event.event
	}
	return r.boundary[id]
}

// A boundary node is an actual causal parent reached from this trace. Load
// that node alone without importing the foreign task's history.
func (e *Engine) loadTraceBoundary(ctx context.Context, r taskTraceRead, id model.EventID) error {
	if _, ok := r.boundary[id]; ok {
		return nil
	}
	meta, found, err := e.readEventEntry(ctx, id)
	if err != nil {
		return err
	}
	if !found || meta.Space != r.anchor.Space || meta.Principal != r.entry.principal || meta.LSN > r.anchor.Fence.LSN {
		return ErrNodeNotFound
	}
	event, _, err := loadViewEvent(ctx, r.reader, r.entry, id)
	if err != nil {
		return err
	}
	r.boundary[id] = event
	return nil
}

func isTraceMilestone(event model.Event) bool {
	return isTraceMilestoneOperation(event.Attribution.Operation)
}

func isTraceMilestoneOperation(operation string) bool {
	switch operation {
	case "Work.query_intake", "Lifecycle.PutQuery", "Control.StartWork", "CreateTask",
		"Control.SpawnChild", "Spawn", "Control.CompleteTask", "Control.JoinExecutions",
		"Control.AnswerQuery", "Control.ConsumeMessage", "Control.ReconcileAttempt",
		"Graph.ForkLocal", "Graph.JoinLocal", "Graph.PublishBranch", "Graph.Prune":
		return true
	}
	return false
}

func (r taskTraceRead) overview(ctx context.Context, ids []model.EventID, milestones map[model.EventID]api.TraceMilestone) ([]api.TaskTraceEdge, []model.EventID, error) {
	// Condense only recorded dependencies, stopping at the next selected
	// predecessor on each traversal path. Chronology supplies display order.
	var edges []api.TaskTraceEdge
	relevant := make(map[model.EventID]bool, len(ids))
	totalVisits := 0
	for _, id := range ids {
		relevant[id] = true
		seen := map[model.EventID]bool{}
		event, err := r.loadEvent(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		queue := append([]model.EventID(nil), event.CausalParents...)
		for i := 0; i < len(queue); i++ {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			parent := queue[i]
			if seen[parent] {
				continue
			}
			seen[parent] = true
			totalVisits++
			if totalVisits > maxDerivationEdges {
				return nil, nil, storage.ErrLimit
			}
			if _, found := milestones[parent]; found {
				if _, err := r.loadEvent(ctx, parent); err != nil {
					return nil, nil, err
				}
				edges = append(edges, api.TaskTraceEdge{From: parent, To: id, Relation: "causal"})
				relevant[parent] = true
				continue
			}
			meta, found, err := r.metadata(ctx, parent)
			if err != nil {
				return nil, nil, err
			}
			if found && isTraceMilestoneOperation(meta.operation) {
				if _, err := r.loadEvent(ctx, parent); err != nil {
					return nil, nil, err
				}
				edges = append(edges, api.TaskTraceEdge{From: parent, To: id, Relation: "causal"})
				relevant[parent] = true
				continue
			}
			inside, err := r.inScope(ctx, parent)
			if err != nil {
				return nil, nil, err
			}
			if inside {
				parentEvent, err := r.loadEvent(ctx, parent)
				if err != nil {
					return nil, nil, err
				}
				relevant[parent] = true
				queue = append(queue, parentEvent.CausalParents...)
			} else {
				edges = append(edges, api.TaskTraceEdge{From: parent, To: id, Relation: "causal", Boundary: true})
				relevant[parent] = true
			}
			if len(edges) > maxTaskTraceEdges {
				return nil, nil, storage.ErrLimit
			}
		}
	}
	page := make(map[model.EventID]bool, len(ids))
	for _, id := range ids {
		page[id] = true
	}
	for _, assertion := range r.edit.Assertions {
		if page[assertion.To] {
			edges = append(edges, api.TaskTraceEdge{From: assertion.From, To: assertion.To, Relation: assertion.Relation, Reason: assertion.Reason, Evidence: assertion.Evidence})
			relevant[assertion.From] = true
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		a, b := edges[i], edges[j]
		if a.To != b.To {
			return a.To < b.To
		}
		if a.From != b.From {
			return a.From < b.From
		}
		return a.Relation < b.Relation
	})
	deps := make([]model.EventID, 0, len(relevant))
	for id := range relevant {
		deps = append(deps, id)
	}
	return edges, deps, nil
}

func tracePage(req api.TaskTraceRequest, anchor model.SnapshotAnchor, filter any, count int) (start, end int, next string, err error) {
	rawScope, err := json.Marshal(struct {
		Anchor    model.SnapshotAnchor
		Principal model.PrincipalID
		Subject   api.TraceSubject
		Session   string
		Limit     int
		Filter    any
	}{anchor, req.Principal, req.Subject, req.Session, req.Limit, filter})
	if err != nil {
		return 0, 0, "", err
	}
	scope := string(emissionDigest(rawScope))
	if req.Continuation != "" {
		raw, err := readNodeToken(req.Continuation, scope)
		if err != nil {
			return 0, 0, "", err
		}
		start, err = strconv.Atoi(raw)
		if err != nil || start < 1 || start >= count {
			return 0, 0, "", api.ErrInvalidRequest
		}
	}
	size := req.Limit
	if size == 0 {
		size = defaultTaskTracePage
	}
	end = min(start+size, count)
	if end < count {
		next = writeNodeToken(scope, strconv.Itoa(end))
	}
	return start, end, next, nil
}

func tracePositionScope(req api.TaskTraceRequest, anchor model.SnapshotAnchor) (string, error) {
	raw, err := json.Marshal(struct {
		Anchor    model.SnapshotAnchor
		Principal model.PrincipalID
		Subject   api.TraceSubject
		Session   string
		Limit     int
		Filter    string
	}{anchor, req.Principal, req.Subject, req.Session, req.Limit, "overview-position-v1"})
	if err != nil {
		return "", err
	}
	return string(emissionDigest(raw)), nil
}

func readTracePosition(req api.TaskTraceRequest, anchor model.SnapshotAnchor) (tracePosition, string, error) {
	scope, err := tracePositionScope(req, anchor)
	if err != nil {
		return tracePosition{}, "", err
	}
	if req.Continuation == "" {
		return tracePosition{}, scope, nil
	}
	raw, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return tracePosition{}, "", err
	}
	var position tracePosition
	if json.Unmarshal([]byte(raw), &position) != nil || position.ID == "" {
		return tracePosition{}, "", api.ErrInvalidRequest
	}
	return position, scope, nil
}

func (e *Engine) GetTaskTrace(ctx context.Context, req api.TaskTraceRequest) (api.TaskTraceResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_task_trace", req.Principal, req, func(scoped context.Context) (api.TaskTraceResult, error) {
			return e.GetTaskTrace(scoped, req)
		})
	}
	r, err := e.readTaskTrace(ctx, req)
	if err != nil {
		return api.TaskTraceResult{}, err
	}
	milestones := make(map[model.EventID]api.TraceMilestone, len(r.edit.Milestones))
	for _, milestone := range r.edit.Milestones {
		milestones[milestone.Event] = milestone
	}
	after, scope, err := readTracePosition(req, r.anchor)
	if err != nil {
		return api.TaskTraceResult{}, err
	}
	size := req.Limit
	if size == 0 {
		size = defaultTaskTracePage
	}
	items, last, more, err := e.traceEventPage(ctx, r.keys, r.anchor.Fence, after, size, func(item eventMetadataItem) bool {
		return milestones[item.id].Event != "" || isTraceMilestoneOperation(item.operation)
	})
	if err != nil {
		return api.TaskTraceResult{}, err
	}
	var ids []model.EventID
	for _, item := range items {
		if err := func() error {
			_, err := r.loadEvent(ctx, item.id)
			return err
		}(); err != nil {
			return api.TaskTraceResult{}, e.nodeReadError(ctx, err)
		}
		ids = append(ids, item.id)
		if milestones[item.id].Event == "" {
			milestones[item.id] = api.TraceMilestone{}
		}
	}
	next := ""
	if more {
		raw, err := json.Marshal(last)
		if err != nil {
			return api.TaskTraceResult{}, err
		}
		next = writeNodeToken(scope, string(raw))
	}
	out := api.TaskTraceResult{Subject: req.Subject, Anchor: r.anchor, Continuation: next}
	if r.record != "" {
		out.Version = r.edit.ExpectedVersion + 1
	}
	for _, id := range ids {
		out.Nodes = append(out.Nodes, r.node(id, milestones[id]))
	}
	edges, deps, err := r.overview(ctx, ids, milestones)
	if err != nil {
		return api.TaskTraceResult{}, err
	}
	out.Edges = edges
	if len(out.Edges) > maxTaskTraceEdges || jsonBytes(out) > 1<<20 {
		return api.TaskTraceResult{}, storage.ErrLimit
	}
	for _, edge := range edges {
		if edge.Boundary {
			if err := e.loadTraceBoundary(ctx, r, edge.From); err != nil {
				return api.TaskTraceResult{}, e.nodeReadError(ctx, err)
			}
		}
	}
	dependencies, err := e.taskTraceDependencies(ctx, req, r, deps)
	if err != nil {
		return api.TaskTraceResult{}, err
	}
	return finishReadDelivery(ctx, e, "get_task_trace", req.Principal, out, r.anchor, "", "", dependencies)
}

func (e *Engine) GetTaskTraceEvidence(ctx context.Context, req api.TaskTraceEvidenceRequest) (api.TaskTraceEvidenceResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_task_trace_evidence", req.Principal, req, func(scoped context.Context) (api.TaskTraceEvidenceResult, error) {
			return e.GetTaskTraceEvidence(scoped, req)
		})
	}
	if !model.ValidDigest(string(req.From)) || !model.ValidDigest(string(req.To)) || req.From == req.To {
		return api.TaskTraceEvidenceResult{}, api.ErrInvalidRequest
	}
	switch req.Relation {
	case "causal", "supports", "refutes", "explains":
	default:
		return api.TaskTraceEvidenceResult{}, api.ErrInvalidRequest
	}
	r, err := e.readTaskTrace(ctx, req.TaskTraceRequest)
	if err != nil {
		return api.TaskTraceEvidenceResult{}, err
	}
	out := api.TaskTraceEvidenceResult{Anchor: r.anchor}
	var ids []model.EventID
	if req.Relation == "causal" {
		ids, err = r.between(ctx, req.From, req.To)
		if err != nil {
			return out, err
		}
		if _, inside := r.events[req.From]; !inside {
			if err := e.loadTraceBoundary(ctx, r, req.From); err != nil {
				return out, e.nodeReadError(ctx, err)
			}
		}
	} else {
		for _, edge := range r.edit.Assertions {
			if edge.From == req.From && edge.To == req.To && edge.Relation == req.Relation {
				out.Citations = edge.Evidence
				ids = []model.EventID{edge.From, edge.To}
				break
			}
		}
		if len(ids) == 0 {
			return out, ErrNodeNotFound
		}
	}
	start, end, next, err := tracePage(req.TaskTraceRequest, r.anchor, [3]string{string(req.From), string(req.To), req.Relation}, len(ids))
	if err != nil {
		return out, err
	}
	for _, id := range ids[start:end] {
		out.Events = append(out.Events, r.node(id, api.TraceMilestone{}))
	}
	out.Continuation = next
	dependencies, err := e.taskTraceDependencies(ctx, req.TaskTraceRequest, r, ids)
	if err != nil {
		return api.TaskTraceEvidenceResult{}, err
	}
	return finishReadDelivery(ctx, e, "get_task_trace_evidence", req.Principal, out, r.anchor, "", "", dependencies)
}

// between returns the intersection of From's descendants and To's ancestors
// in publication order. An intervening unrelated event never enters the page.
func (r taskTraceRead) between(ctx context.Context, from, to model.EventID) ([]model.EventID, error) {
	insideTo, err := r.inScope(ctx, to)
	if err != nil {
		return nil, err
	}
	if !insideTo {
		return nil, ErrNodeNotFound
	}
	descendants := map[model.EventID]bool{from: true}
	queue := []model.EventID{from}
	visits := 0
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		children, err := r.children(ctx, queue[i])
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			inside, err := r.inScope(ctx, child.id)
			if err != nil {
				return nil, err
			}
			if !inside || descendants[child.id] {
				continue
			}
			visits++
			if visits > maxDerivationEdges {
				return nil, storage.ErrLimit
			}
			if _, err := r.loadEvent(ctx, child.id); err != nil {
				return nil, err
			}
			descendants[child.id] = true
			queue = append(queue, child.id)
		}
	}
	if !descendants[to] {
		return nil, ErrNodeNotFound
	}
	ancestors := map[model.EventID]bool{to: true}
	queue = []model.EventID{to}
	for i := 0; i < len(queue); i++ {
		id := queue[i]
		event, err := r.loadEvent(ctx, id)
		if err != nil {
			return nil, err
		}
		for _, parent := range event.CausalParents {
			if parent == from {
				ancestors[parent] = true
				continue
			}
			inside, err := r.inScope(ctx, parent)
			if err != nil {
				return nil, err
			}
			if inside && !ancestors[parent] {
				visits++
				if visits > maxDerivationEdges {
					return nil, storage.ErrLimit
				}
				ancestors[parent] = true
				queue = append(queue, parent)
			}
		}
	}
	var ids []model.EventID
	insideFrom, err := r.inScope(ctx, from)
	if err != nil {
		return nil, err
	}
	if !insideFrom && ancestors[from] {
		ids = append(ids, from)
	}
	for id := range descendants {
		if ancestors[id] && (id != from || insideFrom) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, aFound, _ := r.metadata(ctx, ids[i])
		b, bFound, _ := r.metadata(ctx, ids[j])
		if !aFound || !bFound {
			return ids[i] < ids[j]
		}
		return compareTracePosition(traceItemPosition(a), traceItemPosition(b)) < 0
	})
	return ids, nil
}

var _ api.TaskTraceCommands = (*Engine)(nil)
