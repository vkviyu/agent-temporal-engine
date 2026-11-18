package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/state"
)

const taskTraceProfileEntries = 32
const taskTraceProfileBytes = 16 << 20

type taskTraceProfileKey struct {
	space     model.SpaceID
	principal model.PrincipalID
	object    model.ObjectID
}

// taskTraceProfile owns immutable, payload-free lookup tables. In particular it
// retains neither a Ledger/Snapshot nor roots, policy, context, reader or pin.
type taskTraceProfile struct {
	queries        map[string]string // query -> session
	tasks          map[model.EntityID]struct{}
	taskExecutions map[model.EntityID][]model.ExecutionID
	executionTasks map[model.ExecutionID]model.EntityID
	children       map[model.ExecutionID][]model.ExecutionID
	provenance     traceProvenance // roots remain zero; bind per read
	bytes          int
}

func traceProfileString[T ~string](s T) T { return T(strings.Clone(string(s))) }

func newTaskTraceProfile(snapshot agentprofile.Snapshot) *taskTraceProfile {
	p := &taskTraceProfile{
		queries: make(map[string]string), tasks: make(map[model.EntityID]struct{}),
		taskExecutions: make(map[model.EntityID][]model.ExecutionID),
		executionTasks: make(map[model.ExecutionID]model.EntityID),
		children:       make(map[model.ExecutionID][]model.ExecutionID),
		provenance:     newTraceProvenance(model.RootSet{}, snapshot),
	}
	for _, query := range snapshot.Lifecycle.Queries {
		p.queries[traceProfileString(query.ID)] = traceProfileString(query.Session)
	}
	for _, task := range snapshot.Tasks {
		p.tasks[traceProfileString(task.ID)] = struct{}{}
	}
	for _, execution := range snapshot.Executions {
		id, task, parent := traceProfileString(execution.ID), traceProfileString(execution.Task), traceProfileString(execution.Parent)
		p.taskExecutions[task] = append(p.taskExecutions[task], id)
		p.executionTasks[id] = task
		p.children[parent] = append(p.children[parent], id)
	}
	p.bytes = traceProfileRetainedBytes(reflect.ValueOf(p).Elem())
	return p
}

func (p *taskTraceProfile) provenanceAt(roots model.RootSet) traceProvenance {
	provenance := p.provenance
	provenance.roots = roots
	return provenance
}

// Preserve taskTraceScope's execution-subtree semantics, including cross-task
// children. Discovering another task adds its event key, not all its executions.
func (p *taskTraceProfile) scope(req api.TaskTraceRequest) ([]traceEventKey, error) {
	key := traceEventKey{req.Space, req.Principal, req.Subject.Kind, req.Subject.ID}
	keys := []traceEventKey{key}
	if key.kind == "query" {
		session, found := p.queries[key.id]
		if !found || (req.Session != "" && req.Session != session) {
			return nil, ErrNodeNotFound
		}
		return keys, nil
	}
	if key.kind == "task" {
		task := model.EntityID(key.id)
		if _, found := p.tasks[task]; !found {
			return nil, ErrNodeNotFound
		}
		// Never append to a cached slice, even when it has spare capacity.
		queue := append([]model.ExecutionID(nil), p.taskExecutions[task]...)
		seen := make(map[model.ExecutionID]bool)
		seenTasks := map[model.EntityID]bool{task: true}
		for i := 0; i < len(queue); i++ {
			id := queue[i]
			if seen[id] {
				continue
			}
			seen[id] = true
			keys = append(keys, traceEventKey{req.Space, req.Principal, "execution", string(id)})
			if task := p.executionTasks[id]; !seenTasks[task] {
				seenTasks[task] = true
				keys = append(keys, traceEventKey{req.Space, req.Principal, "task", string(task)})
			}
			queue = append(queue, p.children[id]...)
		}
	}
	return keys, nil
}

// Count every retained string, including duplicates, all slice capacity and all
// map keys/values. Double inline sizes plus per-entry/map allowances deliberately
// overcharge allocation rounding, map slack and bookkeeping. Reflection happens
// once at construction, outside the lock, and also covers future struct fields.
func traceProfileRetainedBytes(v reflect.Value) int {
	size := 2 * int(v.Type().Size())
	switch v.Kind() {
	case reflect.String:
		size += v.Len()
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			size += traceProfileRetainedBytes(v.Field(i))
		}
	case reflect.Map:
		size += 256 + v.Len()*128
		iter := v.MapRange()
		for iter.Next() {
			size += traceProfileRetainedBytes(iter.Key()) + traceProfileRetainedBytes(iter.Value())
		}
	case reflect.Slice:
		size += 2 * v.Cap() * int(v.Type().Elem().Size())
		for i := 0; i < v.Len(); i++ {
			size += traceProfileRetainedBytes(v.Index(i))
		}
	}
	return size
}

type taskTraceProfileEntry struct {
	value *taskTraceProfile
	bytes int
	used  uint64
}

// The cache has its own lock; no IO or projection building runs under it.
// Entries are immutable and shared by active readers even after eviction.
type taskTraceProfileCache struct {
	observation *observation.Collector
	mu          sync.Mutex
	entries     map[taskTraceProfileKey]taskTraceProfileEntry
	bytes       int
	clock       uint64
	stopped     bool
}

func (c *taskTraceProfileCache) get(key taskTraceProfileKey) (*taskTraceProfile, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.entries[key]
	if c.stopped || !found {
		c.observe(observation.Miss)
		return nil, false
	}
	c.clock++
	entry.used = c.clock
	c.entries[key] = entry
	c.observe(observation.Hit)
	return entry.value, true
}

func (c *taskTraceProfileCache) put(key taskTraceProfileKey, value *taskTraceProfile) {
	size := 512 + len(key.space) + len(key.principal) + len(key.object) + value.bytes
	if size > taskTraceProfileBytes {
		return // Oversized reads still succeed without retaining their projection.
	}
	// Own key bytes too; identities may be substrings of a larger request.
	key = taskTraceProfileKey{traceProfileString(key.space), traceProfileString(key.principal), traceProfileString(key.object)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	if _, found := c.entries[key]; found {
		return
	}
	for len(c.entries) >= taskTraceProfileEntries || c.bytes+size > taskTraceProfileBytes {
		var oldestKey taskTraceProfileKey
		var oldest taskTraceProfileEntry
		for k, entry := range c.entries {
			if oldest.value == nil || entry.used < oldest.used {
				oldestKey, oldest = k, entry
			}
		}
		delete(c.entries, oldestKey)
		c.bytes -= oldest.bytes
		c.observe(observation.Eviction)
	}
	if c.entries == nil {
		c.entries = make(map[taskTraceProfileKey]taskTraceProfileEntry)
	}
	c.clock++
	c.entries[key] = taskTraceProfileEntry{value, size, c.clock}
	c.bytes += size
	c.observe(observation.Insert)
}

func (c *taskTraceProfileCache) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	c.entries, c.bytes = nil, 0
	c.observe(observation.Cleared)
}

func (c *taskTraceProfileCache) observe(outcome observation.Outcome) {
	c.observation.Emit(observation.Event{Kind: observation.Cache, Cache: observation.TraceProfile,
		Outcome: outcome, Count: uint64(len(c.entries)), RetainedEstimate: uint64(c.bytes)})
}

// The caller authenticates the historical anchor before arriving here. Even on
// a hit, resolve this principal's partition in that historical LiveControl.
// Only a validated immutable object ID can select a previously checked profile.
func (c *taskTraceProfileCache) load(ctx context.Context, reader state.ObjectReader, root model.ObjectID, space model.SpaceID, principal model.PrincipalID) (*taskTraceProfile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, found, err := state.Get(ctx, reader, systemSpace, root, agentPartitionKey(space, principal))
	if err != nil {
		return nil, err
	}
	if !found {
		return newTaskTraceProfile(agentprofile.Snapshot{}), nil
	}
	key := taskTraceProfileKey{space, principal, id}
	if profile, found := c.get(key); found {
		return profile, nil
	}
	obj, err := readObject(ctx, reader, id)
	if err != nil {
		return nil, err
	}
	if obj.Type != agentRecordRootType {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, ErrUnsupportedSchema)
	}
	image, owner, err := loadAgentRecordImage(ctx, reader, obj)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	snapshot, err := image.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if owner != principal || obj.Space != space {
		return nil, ErrInvalidStateData
	}
	profile := newTaskTraceProfile(snapshot)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.put(key, profile)
	return profile, nil
}
