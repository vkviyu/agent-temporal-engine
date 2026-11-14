package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type readCutKey struct{}

// readCut owns one immutable published bundle and physical layout. It is not
// an Engine copy: mutable lifecycle, catalogs and publication remain owned by
// the coordinator. Only bounded metadata point lookups consult that owner.
type readCut struct {
	owner       *Engine
	roots       model.RootSet
	rootSet     model.ObjectID
	publication storage.Publication
	pin         *storage.PinnedReader
	reader      state.ObjectReader
	closed      atomic.Bool
	release     sync.Once
	policies    sync.Map // SpaceID -> *readPolicy
	events      *eventMetadataBlock
}

type readPolicy struct {
	once   sync.Once
	policy memory.AccessPolicy
	err    error
}

func (e *Engine) readCut(ctx context.Context) *readCut {
	cut, _ := ctx.Value(readCutKey{}).(*readCut)
	if cut == nil || cut.owner != e {
		return nil
	}
	return cut
}

// beginReadCut reserves physical ownership before releasing the publication
// lock. Read operations have a separate lifetime count from the View quota.
func (e *Engine) beginReadCut(ctx context.Context) (context.Context, func(), error) {
	if cut := e.readCut(ctx); cut != nil {
		if cut.closed.Load() {
			return ctx, nil, api.ErrReadEmissionDenied
		}
		return ctx, func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	if err := e.publicGate(); err != nil {
		return ctx, nil, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return ctx, nil, err
	}
	if err := e.ready(); err != nil {
		e.mu.Unlock()
		return ctx, nil, err
	}
	v := &e.views
	v.mu.Lock()
	v.init()
	if v.stopping || e.draining.Load() {
		v.mu.Unlock()
		e.mu.Unlock()
		return ctx, nil, storage.ErrClosing
	}
	if v.readers >= storage.MaxReaderPins {
		v.mu.Unlock()
		e.mu.Unlock()
		return ctx, nil, storage.ErrLimit
	}
	v.readers++
	v.mu.Unlock()
	pin, err := e.journal.PinReader()
	cut := &readCut{owner: e, roots: e.roots, rootSet: e.rootSet, publication: e.publication, pin: pin, events: e.eventBlocks}
	e.mu.Unlock()
	if err != nil {
		v.mu.Lock()
		v.readers--
		v.notify()
		v.mu.Unlock()
		return ctx, nil, err
	}
	cut.reader = &fixedObjectReader{index: e.index, source: pin, canonical: e.journal}
	closeCut := func() {
		cut.release.Do(func() {
			cut.closed.Store(true)
			_ = pin.Close()
			v.mu.Lock()
			v.readers--
			v.notify()
			v.mu.Unlock()
		})
	}
	return context.WithValue(ctx, readCutKey{}, cut), closeCut, nil
}

func (e *Engine) readReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cut := e.readCut(ctx)
	if cut == nil || cut.closed.Load() {
		return api.ErrReadEmissionDenied
	}
	return e.publicGate()
}

// Issuance is the sole short critical section of a prepared read. The roots,
// dependency proof, bytes and hashes were computed before entering it.
func (e *Engine) checkReadCutLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cut := e.readCut(ctx)
	if cut == nil || cut.closed.Load() {
		return api.ErrReadEmissionDenied
	}
	if e.closed || e.journal == nil {
		return storage.ErrClosed
	}
	if e.fenced != nil {
		return fmt.Errorf("%w: %w", storage.ErrStoreFenced, e.fenced)
	}
	if e.publicationPending.Load() || cut.publication.Fence() != e.publication.Fence() || cut.roots != e.roots || cut.rootSet != e.rootSet {
		return api.ErrReadAuthorizationChanged
	}
	// Journal.Published is metadata-only: it neither reads files nor acquires
	// the physical publisher gate, and catches a physical fence not yet mirrored.
	_, err := e.journal.Published()
	return err
}

func (e *Engine) readRoots(ctx context.Context) model.RootSet {
	if cut := e.readCut(ctx); cut != nil {
		return cut.roots
	}
	return e.roots // private reducer/startup, whose state is exclusively owned
}

func (e *Engine) readRootSet(ctx context.Context) model.ObjectID {
	if cut := e.readCut(ctx); cut != nil {
		return cut.rootSet
	}
	return e.rootSet
}

func (e *Engine) readPublication(ctx context.Context) storage.Publication {
	if cut := e.readCut(ctx); cut != nil {
		return cut.publication
	}
	return e.publication
}

func (e *Engine) reader(ctx context.Context) state.ObjectReader {
	if cut := e.readCut(ctx); cut != nil {
		return cut.reader
	}
	return e.index
}

func (e *Engine) readPolicy(ctx context.Context, space model.SpaceID) (memory.AccessPolicy, error) {
	load := func() (memory.AccessPolicy, error) {
		store, _, _, err := e.loadMemory(ctx, e.readRoots(ctx), space)
		if err != nil {
			return memory.AccessPolicy{}, err
		}
		return store.AccessPolicy(), nil
	}
	cut := e.readCut(ctx)
	if cut == nil {
		return load()
	}
	value, _ := cut.policies.LoadOrStore(space, &readPolicy{})
	cached := value.(*readPolicy)
	cached.once.Do(func() { cached.policy, cached.err = load() })
	return cached.policy, cached.err
}

func (e *Engine) readFailureAt(ctx context.Context, err error) error {
	if e.readCut(ctx) == nil {
		return e.readFailure(err) // private/previous locked reducer caller
	}
	if err == nil || isLimit(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, api.ErrReadAuthorizationChanged) {
		return err
	}
	return e.fenceWriter(err) // marks the engine first; physical Fence is lock-free
}

func (e *Engine) readEnvelope(ctx context.Context, id model.TxID) (commitEnvelope, bool, error) {
	cut := e.readCut(ctx)
	if cut == nil {
		proof, ok := e.envelopes[id]
		if !ok {
			proof, ok = e.baseEnvelopes[id]
		}
		return proof, ok, nil
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return commitEnvelope{}, false, err
	}
	proof, found := e.envelopes[id]
	e.mu.Unlock()
	return proof, found && proof.fence.LSN <= cut.publication.Position.LSN, nil
}
