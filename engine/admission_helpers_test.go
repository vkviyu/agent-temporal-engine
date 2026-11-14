package engine

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// coordinatorGate is a one-shot IO boundary. Its entered notification, rather
// than elapsed time, establishes that publication is still in progress.
type coordinatorGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	err     error
	after   bool
}

func newCoordinatorGate() *coordinatorGate {
	return &coordinatorGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *coordinatorGate) unblock() { g.once.Do(func() { close(g.release) }) }

type coordinatorIO struct {
	storage.DurableIO
	mu            sync.Mutex
	writeGate     *coordinatorGate
	syncGate      *coordinatorGate
	currentGate   *coordinatorGate
	gates         []*coordinatorGate
	segmentWrites int
	segmentBytes  uint64
	segmentSyncs  int
	currentWrites int
	currentEnds   []uint64
	ownerCloses   int
}

func (d *coordinatorIO) takeGate(kind string) *coordinatorGate {
	d.mu.Lock()
	defer d.mu.Unlock()
	var slot **coordinatorGate
	switch kind {
	case "write":
		d.segmentWrites++
		slot = &d.writeGate
	case "sync":
		d.segmentSyncs++
		slot = &d.syncGate
	case "current":
		d.currentWrites++
		d.currentEnds = append(d.currentEnds, d.segmentBytes)
		slot = &d.currentGate
	}
	g := *slot
	*slot = nil
	return g
}

func (d *coordinatorIO) arm(kind string, g *coordinatorGate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.gates = append(d.gates, g)
	switch kind {
	case "write":
		d.writeGate = g
	case "sync":
		d.syncGate = g
	case "current":
		d.currentGate = g
	default:
		panic("unknown coordinator IO gate")
	}
}

func (d *coordinatorIO) counts() (writes, syncs, currents, closes int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.segmentWrites, d.segmentSyncs, d.currentWrites, d.ownerCloses
}

func (d *coordinatorIO) appendSizes() []uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	var sizes []uint64
	for i := 1; i < len(d.currentEnds); i++ {
		sizes = append(sizes, d.currentEnds[i]-d.currentEnds[i-1])
	}
	return sizes
}

func coordinatorGated(g *coordinatorGate, operation func() error) error {
	if g == nil {
		return operation()
	}
	close(g.entered)
	<-g.release
	if !g.after && g.err != nil {
		return g.err
	}
	if err := operation(); err != nil {
		return err
	}
	return g.err
}

type coordinatorFile struct {
	storage.DurableFile
	io *coordinatorIO
}

func (f *coordinatorFile) Write(p []byte) (n int, err error) {
	err = coordinatorGated(f.io.takeGate("write"), func() error {
		n, err = f.DurableFile.Write(p)
		return err
	})
	if n > 0 {
		f.io.mu.Lock()
		f.io.segmentBytes += uint64(n)
		f.io.mu.Unlock()
	}
	return n, err
}

func (f *coordinatorFile) Sync() error {
	return coordinatorGated(f.io.takeGate("sync"), f.DurableFile.Sync)
}

func (d *coordinatorIO) wrap(name string, f storage.DurableFile) storage.DurableFile {
	if f != nil && strings.HasPrefix(name, "segments/SEGMENT.") {
		return &coordinatorFile{DurableFile: f, io: d}
	}
	return f
}

func (d *coordinatorIO) CreateExclusive(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(name, f), err
}

func (d *coordinatorIO) OpenAppend(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.OpenAppend(name)
	return d.wrap(name, f), err
}

func (d *coordinatorIO) ReplaceAtomic(from, to string) error {
	if to != "CURRENT" {
		return d.DurableIO.ReplaceAtomic(from, to)
	}
	return coordinatorGated(d.takeGate("current"), func() error {
		return d.DurableIO.ReplaceAtomic(from, to)
	})
}

type coordinatorOwner struct {
	storage.OwnerLock
	io *coordinatorIO
}

func (o *coordinatorOwner) Close() error {
	o.io.mu.Lock()
	o.io.ownerCloses++
	o.io.mu.Unlock()
	return o.OwnerLock.Close()
}

func (d *coordinatorIO) AcquireOwnerLock() (storage.OwnerLock, error) {
	o, err := d.DurableIO.AcquireOwnerLock()
	if o == nil {
		return nil, err
	}
	return &coordinatorOwner{OwnerLock: o, io: d}, err
}

func coordinatorStore(t *testing.T, options Options) (*Engine, *coordinatorIO) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native durability profile requires Darwin or Linux")
	}
	d, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	io := &coordinatorIO{DurableIO: d}
	e, err := Create(io, options)
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Always release a deliberately stalled worker, even after Fatal.
		io.mu.Lock()
		for _, g := range io.gates {
			if g != nil {
				g.unblock()
			}
		}
		io.mu.Unlock()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return e, io
}

func coordinatorRequest(t *testing.T, id, key, value string, condition api.ValueCondition) api.CommitRequest {
	t.Helper()
	body, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{
		Key: []byte(key), Kind: api.MutationPut,
		Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)}, Condition: condition,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: model.RequestID(id), Method: api.MethodMutateState, Body: body}
}

type coordinatorOutcome struct {
	result api.CommitResult
	err    error
}

func coordinatorCommit(e *Engine, ctx context.Context, req api.CommitRequest) <-chan coordinatorOutcome {
	out := make(chan coordinatorOutcome, 1)
	go func() {
		r, err := e.Commit(ctx, req)
		out <- coordinatorOutcome{result: r, err: err}
	}()
	return out
}

func coordinatorWait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("coordinator did not reach the deterministic gate/result")
		var zero T
		return zero
	}
}

func coordinatorAck(t *testing.T, outcome coordinatorOutcome) api.CommitResult {
	t.Helper()
	if outcome.err != nil || outcome.result.Code != api.CodeDurableAck {
		t.Fatalf("expected durable acknowledgment, got %+v, %v", outcome.result, outcome.err)
	}
	return outcome.result
}

func coordinatorInterrupted(t *testing.T, outcome coordinatorOutcome, cause error) {
	t.Helper()
	if outcome.result.Code != api.CodeWaitInterrupted || !errors.Is(outcome.err, cause) || !errors.Is(outcome.err, ErrWaitInterrupted) {
		t.Fatalf("accepted cancellation must only interrupt waiting: %+v, %v", outcome.result, outcome.err)
	}
}

func coordinatorLookup(t *testing.T, e *Engine, req api.CommitRequest) api.LookupRequestResult {
	t.Helper()
	r, err := e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Coordinator phase assertions inspect admission directly. Public receipt
// reads also acquire an outbound grant and can be refused during publication.
func coordinatorInFlight(t *testing.T, e *Engine, req api.CommitRequest) api.LookupRequestResult {
	t.Helper()
	r, found, err := e.inFlight(api.LookupRequestRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID})
	if err != nil || !found {
		t.Fatalf("expected admitted request %s: found=%v result=%+v err=%v", req.RequestID, found, r, err)
	}
	return r
}

// The first Err result is captured before notifying the test. Cancellation
// after checked therefore cannot turn the initial pre-check into an early
// return; the operation must make its subsequent lock wait cancellable.
type coordinatorObservedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

type coordinatorWaitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *coordinatorWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func (c *coordinatorObservedContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

// Return nil from the initial pre-check, but cancel before digesting and
// admission finish. This reproduces the boundary without a hash-speed race.
type coordinatorCancelAfterCheck struct {
	context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func (c *coordinatorCancelAfterCheck) Err() error {
	err := c.Context.Err()
	c.once.Do(c.cancel)
	return err
}

func coordinatorUntil(t *testing.T, predicate func() bool) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for !predicate() {
		select {
		case <-timer.C:
			t.Fatal("coordinator did not reach the required admission state")
		default:
			runtime.Gosched()
		}
	}
}

func coordinatorFlightWaiters(t *testing.T, e *Engine, req api.CommitRequest, n int) {
	t.Helper()
	coordinatorUntil(t, func() bool {
		e.admission.mu.Lock()
		defer e.admission.mu.Unlock()
		f := e.admission.flights[req.Key()]
		return f != nil && f.waiters == n
	})
}

func coordinatorQueued(t *testing.T, e *Engine, requests ...api.CommitRequest) {
	t.Helper()
	coordinatorUntil(t, func() bool {
		e.admission.mu.Lock()
		defer e.admission.mu.Unlock()
		for _, req := range requests {
			found := false
			for _, f := range e.admission.queue {
				if f.request.Key() == req.Key() {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
}
