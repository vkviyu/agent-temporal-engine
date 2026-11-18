package engine

import (
	"container/heap"
	"context"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type traceEventKey struct {
	space     model.SpaceID
	principal model.PrincipalID
	kind, id  string
}

type tracePosition struct {
	LSN     uint64        `json:"lsn"`
	Ordinal uint64        `json:"ordinal"`
	ID      model.EventID `json:"id"`
}

func traceItemPosition(item eventMetadataItem) tracePosition {
	return tracePosition{LSN: item.entry.LSN, Ordinal: item.ordinal, ID: item.id}
}

func compareTracePosition(a, b tracePosition) int {
	if a.LSN < b.LSN {
		return -1
	}
	if a.LSN > b.LSN {
		return 1
	}
	if a.Ordinal < b.Ordinal {
		return -1
	}
	if a.Ordinal > b.Ordinal {
		return 1
	}
	if a.ID < b.ID {
		return -1
	}
	if a.ID > b.ID {
		return 1
	}
	return 0
}

// The index is a projection of installed facts. Journal replay rebuilds it
// before opening a store; only the publication owner appends to it.
func (e *Engine) indexTaskTraceEvent(event model.Event, meta eventEntry) {
	if e.traceEvents == nil {
		e.traceEvents = make(map[traceEventKey][]eventMetadataItem)
	}
	a := event.Attribution
	item := eventMetadataItem{id: event.ID, entry: meta, operation: a.Operation, ordinal: a.Ordinal}
	if e.traceEventMeta == nil {
		e.traceEventMeta = make(map[model.EventID]eventMetadataItem)
	}
	if e.traceEventKeys == nil {
		e.traceEventKeys = make(map[model.EventID][]traceEventKey)
	}
	if e.traceCausalChildren == nil {
		e.traceCausalChildren = make(map[model.EventID][]eventMetadataItem)
	}
	e.traceEventMeta[event.ID] = item
	for _, parent := range event.CausalParents {
		e.traceCausalChildren[parent] = append(e.traceCausalChildren[parent], item)
	}
	for _, key := range []traceEventKey{
		{event.Space, event.RecordedBy, "query", a.Query},
		{event.Space, event.RecordedBy, "task", string(a.Task)},
		{event.Space, event.RecordedBy, "execution", string(a.Execution)},
	} {
		if key.id != "" {
			e.traceEvents[key] = append(e.traceEvents[key], item)
			e.traceEventKeys[event.ID] = append(e.traceEventKeys[event.ID], key)
		}
	}
}

type traceStreamHead struct {
	stream int
	index  int
	item   eventMetadataItem
}

type traceStreamHeap []traceStreamHead

func (h traceStreamHeap) Len() int { return len(h) }
func (h traceStreamHeap) Less(i, j int) bool {
	return compareTracePosition(traceItemPosition(h[i].item), traceItemPosition(h[j].item)) < 0
}
func (h traceStreamHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *traceStreamHeap) Push(x any)   { *h = append(*h, x.(traceStreamHead)) }
func (h *traceStreamHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (e *Engine) traceEventPage(ctx context.Context, keys []traceEventKey, fence model.CommitFence, after tracePosition, limit int, include func(eventMetadataItem) bool) ([]eventMetadataItem, tracePosition, bool, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, tracePosition{}, false, err
	}
	// Published prefixes are immutable. Copy the bounded slice headers while
	// locked and enumerate outside the publication lock. Event bodies are
	// loaded only after this metadata page has been selected.
	streams := make([][]eventMetadataItem, 0, len(keys))
	for _, key := range keys {
		stream := e.traceEvents[key]
		end := sort.Search(len(stream), func(i int) bool { return stream[i].entry.LSN > fence.LSN })
		streams = append(streams, stream[:end:end])
	}
	e.mu.Unlock()

	h := &traceStreamHeap{}
	heap.Init(h)
	for i, stream := range streams {
		start := sort.Search(len(stream), func(j int) bool {
			return compareTracePosition(traceItemPosition(stream[j]), after) > 0
		})
		if start < len(stream) {
			heap.Push(h, traceStreamHead{stream: i, index: start, item: stream[start]})
		}
	}
	seen := make(map[model.EventID]bool)
	items := make([]eventMetadataItem, 0, limit)
	var last tracePosition
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, tracePosition{}, false, err
		}
		head := heap.Pop(h).(traceStreamHead)
		stream := streams[head.stream]
		next := head.index + 1
		if next < len(stream) {
			heap.Push(h, traceStreamHead{stream: head.stream, index: next, item: stream[next]})
		}
		if seen[head.item.id] {
			continue
		}
		seen[head.item.id] = true
		last = traceItemPosition(head.item)
		if include != nil && !include(head.item) {
			continue
		}
		items = append(items, head.item)
		if len(items) == limit {
			return items, last, h.Len() > 0, nil
		}
	}
	return items, last, false, nil
}

func (e *Engine) traceEventIDs(ctx context.Context, keys []traceEventKey, fence model.CommitFence) ([]eventMetadataItem, error) {
	items, _, more, err := e.traceEventPage(ctx, keys, fence, tracePosition{}, maxNodeQueryRecords, nil)
	if err != nil {
		return nil, err
	}
	if more {
		return nil, storage.ErrLimit
	}
	return items, nil
}

func (e *Engine) traceEventMetadata(ctx context.Context, id model.EventID) (eventMetadataItem, bool, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return eventMetadataItem{}, false, err
	}
	item, found := e.traceEventMeta[id]
	e.mu.Unlock()
	return item, found, nil
}

// traceScope is immutable after construction and belongs to one trace request.
// Reuse it throughout traversal instead of rebuilding it under the engine lock.
type traceScope map[traceEventKey]struct{}

func newTraceScope(keys []traceEventKey) traceScope {
	selected := make(traceScope, len(keys))
	for _, key := range keys {
		selected[key] = struct{}{}
	}
	return selected
}

func (e *Engine) traceEventInScope(ctx context.Context, id model.EventID, selected traceScope, fence model.CommitFence) (bool, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return false, err
	}
	defer e.mu.Unlock()
	item, found := e.traceEventMeta[id]
	if !found || item.entry.LSN > fence.LSN {
		return false, nil
	}
	for _, key := range e.traceEventKeys[id] {
		if _, found := selected[key]; found {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) traceEventChildren(ctx context.Context, id model.EventID, fence model.CommitFence) ([]eventMetadataItem, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	children := e.traceCausalChildren[id]
	end := sort.Search(len(children), func(i int) bool { return children[i].entry.LSN > fence.LSN })
	// Return owned metadata from the historical prefix only. Later children
	// must not increase the copy cost or become visible to this request.
	return append([]eventMetadataItem(nil), children[:end]...), nil
}

func taskTraceScope(req api.TaskTraceRequest, ledger *agentprofile.Ledger, snapshot agentprofile.Snapshot) ([]traceEventKey, error) {
	key := traceEventKey{req.Space, req.Principal, req.Subject.Kind, req.Subject.ID}
	keys := []traceEventKey{key}
	if key.kind == "query" {
		query, err := ledger.GetQuery(key.id)
		if err != nil || (req.Session != "" && req.Session != query.Session) {
			return nil, ErrNodeNotFound
		}
		return keys, nil
	}
	if key.kind == "task" {
		if _, err := ledger.GetTask(model.EntityID(key.id)); err != nil {
			return nil, ErrNodeNotFound
		}
		children := make(map[model.ExecutionID][]model.ExecutionID)
		tasks := make(map[model.ExecutionID]model.EntityID)
		var queue []model.ExecutionID
		for _, execution := range snapshot.Executions {
			children[execution.Parent] = append(children[execution.Parent], execution.ID)
			tasks[execution.ID] = execution.Task
			if execution.Task == model.EntityID(key.id) {
				queue = append(queue, execution.ID)
			}
		}
		seen := make(map[model.ExecutionID]bool)
		seenTasks := map[model.EntityID]bool{model.EntityID(key.id): true}
		for i := 0; i < len(queue); i++ {
			id := queue[i]
			if seen[id] {
				continue
			}
			seen[id] = true
			keys = append(keys, traceEventKey{req.Space, req.Principal, "execution", string(id)})
			if task := tasks[id]; !seenTasks[task] {
				seenTasks[task] = true
				keys = append(keys, traceEventKey{req.Space, req.Principal, "task", string(task)})
			}
			queue = append(queue, children[id]...)
		}
	}
	return keys, nil
}
