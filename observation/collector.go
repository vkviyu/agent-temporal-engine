// Package observation provides an optional, lossy mailbox for numeric engine
// diagnostics. It has no dependency on engine, state or storage.
//
// Hosts own consumption: Drain copies into host memory, and never calls host
// code. No worker, callback, context sink or shutdown protocol is installed.
// A collector may be shared by stores, but events intentionally cannot identify
// stores, requests or principals. Use separate collectors when isolation matters.
package observation

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Kind uint8

const (
	Create Kind = iota + 1
	Open
	AdmissionWait
	Prepare
	P1
	Commit
	Fence
	Checkpoint
	Compaction
	Read
	Scan
	PinWait
	Close
	Cache
)

type Outcome uint8

const (
	Success Outcome = iota
	Canceled
	Limit
	Rejected
	Conflict
	Unknown
	Fenced
	Corruption
	Closed
	Failure
	Hit
	Miss
	Insert
	Eviction
	Cleared
	Skipped
)

type CacheKind uint8

const (
	NoCache CacheKind = iota
	TraceEvidence
	TraceProfile
)

// Event contains only low-cardinality enums and numeric measurements.
// Count/Bytes: Prepare/P1 = prepared transactions/encoded bytes;
// Open = physically scanned transactions/published prefix bytes;
// Scan and Read = successful object reads/encoded bytes returned by the reader;
// Compaction = retired segments/copied bytes; PinWait = initial outstanding
// owners; Cache = retained entries/zero bytes.
// RetainedEstimate is ONLY the cache's existing accounting estimate; it is not
// total heap, RSS, or memory retained by readers after eviction.
// Certified/Replayed apply to Open; CheckpointHit also handles empty checkpoints.
// On failed Open, report counters may be incomplete; Replayed counts successful
// semantic replay calls. Duration is elapsed time, not CPU time.
type Event struct {
	Kind             Kind
	Outcome          Outcome
	Cache            CacheKind
	Duration         time.Duration
	Count            uint64
	Bytes            uint64
	RetainedEstimate uint64
	Certified        uint64
	Replayed         uint64
	CheckpointHit    bool
}

const MaxCapacity = 4096

var ErrCapacity = errors.New("observation capacity outside [0, 4096]")

// Collector must not be copied after use. Nil and zero values are disabled.
// Capacity is immutable. Emit uses TryLock: full or contended mailboxes drop
// the new event and increment a separate lifetime counter. Diagnostics never
// determine operation results. No delivery or cross-producer ordering is promised.
type Collector struct {
	mu         sync.Mutex
	events     []Event
	head, size int
	full       atomic.Uint64
	contention atomic.Uint64
}

// New(0) returns nil without allocating. Positive capacities allocate once.
func New(capacity int) (*Collector, error) {
	if capacity < 0 || capacity > MaxCapacity {
		return nil, ErrCapacity
	}
	if capacity == 0 {
		return nil, nil
	}
	return &Collector{events: make([]Event, capacity)}, nil
}

func (c *Collector) Enabled() bool { return c != nil && len(c.events) != 0 }

func (c *Collector) Emit(event Event) {
	if !c.Enabled() {
		return
	}
	if !c.mu.TryLock() {
		c.contention.Add(1)
		return
	}
	if c.size == len(c.events) {
		c.full.Add(1)
	} else {
		c.events[(c.head+c.size)%len(c.events)] = event
		c.size++
	}
	c.mu.Unlock()
}

// Drain consumes at most len(dst) events in mailbox order without allocating.
// Concurrent drains are safe (use separate destination buffers). A drain holds
// the mailbox lock for at most MaxCapacity copies; producers never wait for it.
func (c *Collector) Drain(dst []Event) int {
	if !c.Enabled() || len(dst) == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := min(len(dst), c.size)
	for i := 0; i < n; i++ {
		dst[i] = c.events[c.head]
		c.events[c.head] = Event{}
		c.head = (c.head + 1) % len(c.events)
	}
	c.size -= n
	return n
}

// Drops is a non-resetting pair of independent atomic counters. It remains
// available even when no consumer runs; a concurrent snapshot is approximate.
func (c *Collector) Drops() (full, contention uint64) {
	if c == nil {
		return 0, 0
	}
	return c.full.Load(), c.contention.Load()
}

// Start avoids even reading the clock when disabled.
func (c *Collector) Start() time.Time {
	if !c.Enabled() {
		return time.Time{}
	}
	return time.Now()
}

func (c *Collector) Finish(start time.Time, event Event) {
	if start.IsZero() {
		return
	}
	event.Duration = time.Since(start)
	c.Emit(event)
}
