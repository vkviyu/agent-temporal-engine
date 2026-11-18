package engine

import (
	"context"
	"slices"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
)

const traceEvidenceCacheEntries = 32
const traceEvidenceCacheBytes = 16 << 20

// traceEvidence is immutable after construction. It contains historical IDs,
// never readers, pins, event bodies, access policies or authorization results.
// Every delivery must expand dependencies against the current policy again.
type traceEvidence struct {
	ids          []model.EventID
	dependencies []model.ObjectID
	externalFrom bool
}

type traceEvidenceEntry struct {
	value *traceEvidence
	bytes int
	used  uint64
}

// A bounded, disposable accelerator: eviction and restart cause recomputation,
// not invalid continuations. Immutable values may be shared by active readers.
// There are at most 32 entries, so eviction uses a bounded scan without an
// extra linked-list allocation per entry. No IO runs while mu is held.
type traceEvidenceCache struct {
	observation *observation.Collector
	mu          sync.Mutex
	entries     map[string]traceEvidenceEntry
	bytes       int
	clock       uint64
	stopped     bool
}

func (c *traceEvidenceCache) get(key string) (*traceEvidence, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.entries[key]
	if !found || c.stopped {
		c.observe(observation.Miss)
		return nil, false
	}
	c.clock++
	entry.used = c.clock
	c.entries[key] = entry
	c.observe(observation.Hit)
	return entry.value, true
}

func traceEvidenceBytes(key string, value *traceEvidence) int {
	// Charge string headers and bytes even when their backing storage is shared.
	// The fixed allowance covers the key, entry, value and map bookkeeping.
	size := 512 + len(key)
	for _, id := range value.ids {
		size += 16 + len(id)
	}
	for _, id := range value.dependencies {
		size += 16 + len(id)
	}
	return size
}

func (c *traceEvidenceCache) put(key string, value *traceEvidence) {
	size := traceEvidenceBytes(key, value)
	if size > traceEvidenceCacheBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	if _, found := c.entries[key]; found {
		return // A concurrent builder already installed the same immutable path.
	}
	for len(c.entries) >= traceEvidenceCacheEntries || c.bytes+size > traceEvidenceCacheBytes {
		var oldestKey string
		var oldest traceEvidenceEntry
		for k, entry := range c.entries {
			if oldestKey == "" || entry.used < oldest.used {
				oldestKey, oldest = k, entry
			}
		}
		delete(c.entries, oldestKey)
		c.bytes -= oldest.bytes
		c.observe(observation.Eviction)
	}
	if c.entries == nil {
		c.entries = make(map[string]traceEvidenceEntry)
	}
	// Own the slices; callers can release their construction buffers. Cache
	// readers treat the shared result as read-only, so hits do not copy a path.
	owned := &traceEvidence{ids: slices.Clone(value.ids), dependencies: slices.Clone(value.dependencies), externalFrom: value.externalFrom}
	c.clock++
	c.entries[key] = traceEvidenceEntry{value: owned, bytes: size, used: c.clock}
	c.bytes += size
	c.observe(observation.Insert)
}

func (c *traceEvidenceCache) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	c.entries, c.bytes = nil, 0
	c.observe(observation.Cleared)
}

// Called under the cache lock; reuse maintained accounting, never rescan values.
func (c *traceEvidenceCache) observe(outcome observation.Outcome) {
	c.observation.Emit(observation.Event{Kind: observation.Cache, Cache: observation.TraceEvidence,
		Outcome: outcome, Count: uint64(len(c.entries)), RetainedEstimate: uint64(c.bytes)})
}

func traceEvidenceKey(req api.TaskTraceEvidenceRequest, r taskTraceRead) (string, error) {
	scope := req.TaskTraceRequest
	scope.Limit = 0 // Page sizing is validated by the existing continuation token.
	return taskTraceTokenScopeDigest(scope, r.anchor, [5]string{
		"causal", string(req.From), string(req.To), string(r.entry.rootSet), string(r.record),
	})
}

func (e *Engine) causalTraceEvidence(ctx context.Context, req api.TaskTraceEvidenceRequest, r taskTraceRead) (*traceEvidence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := traceEvidenceKey(req, r)
	if err != nil {
		return nil, err
	}
	if value, found := e.traceEvidence.get(key); found {
		return value, nil
	}
	ids, err := r.between(ctx, req.From, req.To)
	if err != nil {
		return nil, err
	}
	_, insideFrom := r.events[req.From]
	if !insideFrom {
		if err := e.loadTraceBoundary(ctx, r, req.From); err != nil {
			return nil, e.nodeReadError(ctx, err)
		}
	}
	dependencies, err := e.taskTraceDependencies(ctx, req.TaskTraceRequest, r, ids)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value := &traceEvidence{ids: ids, dependencies: dependencies, externalFrom: !insideFrom}
	e.traceEvidence.put(key, value)
	return value, nil
}
