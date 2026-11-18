package engine

import (
	"context"
	"encoding/json"
	"errors"
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
	profile, err := e.traceProfiles.load(ctx, e.reader(ctx), entry.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return taskTraceRead{}, e.nodeReadError(ctx, err)
	}
	keys, err := profile.scope(req)
	if err != nil {
		return taskTraceRead{}, err
	}
	reader := &nodeQueryReader{source: e.reader(ctx)}
	result := taskTraceRead{anchor: anchor, entry: entry, reader: reader, edit: edit, record: record,
		events: make(map[model.EventID]taskTraceOccurrence), boundary: map[model.EventID]model.Event{},
		provenance: profile.provenanceAt(entry.roots), keys: keys}
	result.metadata = e.traceEventMetadata
	result.allItems = func(loadCtx context.Context) ([]eventMetadataItem, error) {
		return e.traceEventIDs(loadCtx, keys, anchor.Fence)
	}
	selected := newTraceScope(keys)
	result.inScope = func(loadCtx context.Context, id model.EventID) (bool, error) {
		return e.traceEventInScope(loadCtx, id, selected, anchor.Fence)
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
	var dependencies []model.ObjectID
	var causal *traceEvidence
	if req.Relation == "causal" {
		causal, err = e.causalTraceEvidence(ctx, req, r)
		if err != nil {
			return out, err
		}
		ids, dependencies = causal.ids, causal.dependencies
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
		if causal != nil {
			if id == req.From && causal.externalFrom {
				err = e.loadTraceBoundary(ctx, r, id)
			} else {
				_, err = r.loadEvent(ctx, id)
			}
			if err != nil {
				return api.TaskTraceEvidenceResult{}, e.nodeReadError(ctx, err)
			}
		}
		out.Events = append(out.Events, r.node(id, api.TraceMilestone{}))
	}
	out.Continuation = next
	if causal == nil {
		dependencies, err = e.taskTraceDependencies(ctx, req.TaskTraceRequest, r, ids)
		if err != nil {
			return api.TaskTraceEvidenceResult{}, err
		}
	}
	return finishReadDelivery(ctx, e, "get_task_trace_evidence", req.Principal, out, r.anchor, "", "", dependencies)
}

var _ api.TaskTraceCommands = (*Engine)(nil)
