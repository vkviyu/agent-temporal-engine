package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const DefaultMaxObjects uint64 = 1_000_000
const MaxObjects uint64 = 2_000_000

var (
	ErrUnsupportedMethod  = errors.New("unsupported core command")
	ErrSemanticCorruption = errors.New("semantic journal verification failed")
)

type Options struct {
	// AgentProfile enables the statically compiled agent/v1 transitions.
	// Opening a log containing these commands also requires this option.
	AgentProfile bool
	// EventProfile records canonical command-applied occurrences atomically
	// with each accepted transaction. Reopening such a log requires this flag.
	EventProfile bool
	Journal      storage.JournalOptions
	MaxObjects   uint64
	Admission    AdmissionOptions
	Batch        BatchOptions
	Views        ViewOptions
}

func (o Options) checked() (Options, error) {
	var err error
	if o.Journal, err = storage.NormalizeJournalOptions(o.Journal); err != nil {
		return o, err
	}
	if o.Admission, err = o.Admission.checked(); err != nil {
		return o, err
	}
	if o.Batch, err = o.Batch.checked(); err != nil {
		return o, err
	}
	if o.Views, err = o.Views.checked(); err != nil {
		return o, err
	}
	if o.MaxObjects == 0 {
		o.MaxObjects = DefaultMaxObjects
	}
	if o.MaxObjects > MaxObjects {
		return o, storage.ErrLimit
	}
	return o, nil
}

type commitEnvelope struct {
	fence     model.CommitFence
	rootSet   model.ObjectID
	space     model.SpaceID
	principal model.PrincipalID
}

// Engine accepts registered core state commands and, when explicitly enabled,
// static agent/v1 transitions. Both share admission, receipts, the five-root
// candidate and the sole Journal publisher. Principal is supplied by a trusted
// embedding host; arbitrary StateOps/control-root writes are never accepted.
type Engine struct {
	// privateReduction is set only on a private WorkUnit candidate. It prevents
	// pure subreducers from manufacturing standalone receipts or publications.
	privateReduction bool
	// Replay freezes event generation to the registry recorded by the core
	// occurrence. Zero denotes legacy core-only transactions, never live opt-out.
	replayingEvents     bool
	replayEventRegistry uint64
	mu                  contextMutex
	views               viewRegistry
	history             map[uint64]commitEnvelope
	baseHistory         map[uint64]commitEnvelope
	events              map[model.EventID]eventEntry
	baseEvents          map[model.EventID]eventEntry
	eventHeads          map[string]eventHead
	baseEventHeads      map[string]eventHead
	journal             *storage.Journal
	index               *objectIndex
	options             Options
	roots               model.RootSet
	rootSet             model.ObjectID
	publication         storage.Publication
	envelopes           map[model.TxID]commitEnvelope
	baseEnvelopes       map[model.TxID]commitEnvelope
	receiptHints        map[[32]byte]struct{}
	admission           *admissionState
	draining            atomic.Bool
	retired             atomic.Bool
	fenced              error
	closing             bool
	closed              bool
}

var _ api.Committer = (*Engine)(nil)

func newEngine(o Options) *Engine {
	return &Engine{options: o, history: make(map[uint64]commitEnvelope), events: make(map[model.EventID]eventEntry), eventHeads: make(map[string]eventHead), index: newObjectIndex(), envelopes: make(map[model.TxID]commitEnvelope), receiptHints: make(map[[32]byte]struct{})}
}

// Create uses an existing empty directory IO. The caller owns that IO lifetime;
// close the Engine (retrying failures) before closing its IO.
func Create(d storage.DurableIO, options Options) (*Engine, error) {
	o, err := options.checked()
	if err != nil {
		return nil, err
	}
	j, err := storage.CreateJournal(d, o.Journal)
	if err != nil {
		return nil, err
	}
	e := newEngine(o)
	e.journal = j
	e.index.source = j
	e.publication, err = j.Published()
	if err != nil {
		return nil, storage.CloseOnError(err, j)
	}
	e.history[0] = commitEnvelope{fence: e.publication.Fence()}
	e.startCoordinator()
	return e, nil
}

// OpenExisting validates semantic replay before storage may quarantine tail or
// reestablish CURRENT. No reconstructed state is exposed on a failed open.
func OpenExisting(d storage.DurableIO, options Options) (*Engine, storage.RecoveryReport, error) {
	o, err := options.checked()
	if err != nil {
		return nil, storage.RecoveryReport{}, err
	}
	e := newEngine(o)
	j, report, err := storage.OpenJournalWithReplay(d, o.Journal, e.replay)
	if err != nil {
		return nil, report, err
	}
	e.journal = j
	e.index.source = j
	e.publication = report.Publication
	if e.publication.Position.LSN == 0 {
		e.history[0] = commitEnvelope{fence: e.publication.Fence()}
	}
	e.startCoordinator()
	return e, report, nil
}

func (e *Engine) ready() error {
	if e.journal == nil {
		return storage.ErrClosed
	}
	if e.closed {
		return storage.ErrClosed
	}
	if e.draining.Load() || e.closing {
		return storage.ErrClosing
	}
	if e.fenced != nil {
		return fmt.Errorf("%w: %w", storage.ErrStoreFenced, e.fenced)
	}
	_, err := e.journal.Published()
	return err
}

func semanticError(err error) error {
	if errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, annotation.ErrUnsupported) {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if errors.Is(err, ErrObjectIO) {
		return err
	}
	if errors.Is(err, ErrStoredObject) {
		return fmt.Errorf("%w: %w: %w", ErrSemanticCorruption, storage.ErrCorruption, err)
	}
	if errors.Is(err, ErrUnsupportedSchema) || errors.Is(err, codec.ErrObjectUnsupported) || errors.Is(err, storage.ErrUnsupported) || errors.Is(err, storage.ErrLimit) || errors.Is(err, state.ErrLimit) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w: %w", ErrSemanticCorruption, storage.ErrCorruption, err)
}

func isLimit(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrObjectIO) {
		return false
	}
	return errors.Is(err, storage.ErrLimit) || errors.Is(err, state.ErrLimit) || errors.Is(err, codec.ErrObjectLimit) || errors.Is(err, graph.ErrLimit) || errors.Is(err, knowledge.ErrContextSelectionLimit) || errors.Is(err, annotation.ErrLimit)
}

func (e *Engine) readFailure(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	cause := semanticError(err)
	e.fenced = cause
	err = e.journal.Fence(cause)
	e.stopAdmission(err)
	return err
}

func (e *Engine) lookup(ctx context.Context, key model.RequestKey) (api.CommitResult, bool, error) {
	id, found, err := state.Get(ctx, e.index, systemSpace, e.roots.CommandReceipts, receiptKey(key))
	if err != nil || !found {
		return api.CommitResult{}, false, err
	}
	obj, err := readObject(ctx, e.index, id)
	if err != nil {
		return api.CommitResult{}, false, err
	}
	receipt, err := readReceipt(obj)
	if err != nil {
		return api.CommitResult{}, false, err
	}
	proof, ok := e.lookupProof(receipt.TxID)
	if !ok || receipt.Key != key || receipt.LSN != proof.fence.LSN {
		return api.CommitResult{}, false, ErrInvalidStateData
	}
	return api.CommitResult{Code: api.CodeDurableAck, RequestID: key.RequestID, TxID: receipt.TxID, Fence: proof.fence,
		RequestDigest: receipt.RequestDigest, ResultDigest: id, StateRoot: receipt.StateRoot, RootSet: proof.rootSet}, true, nil
}

func (e *Engine) Commit(ctx context.Context, req api.CommitRequest) (api.CommitResult, error) {
	return e.commitQueued(ctx, req)
}

func (e *Engine) lookupProof(id model.TxID) (commitEnvelope, bool) {
	proof, ok := e.envelopes[id]
	if !ok {
		proof, ok = e.baseEnvelopes[id]
	}
	return proof, ok
}

func (e *Engine) Mutate(ctx context.Context, req api.MutateRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := EncodeStateBatch(req.Batch)
	if err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted, RequestID: req.Key.RequestID}, err
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodMutateState, Body: body, ExpectedFence: req.ExpectedFence})
}

func (e *Engine) LookupRequest(ctx context.Context, req api.LookupRequestRequest) (api.LookupRequestResult, error) {
	if err := req.Validate(); err != nil {
		return api.LookupRequestResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	if err := e.publicGate(); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	if got, found, err := e.inFlight(req); found {
		if err == nil && ctx.Value(readDeliveryKey{}) != nil {
			if lockErr := e.mu.LockContext(ctx); lockErr != nil {
				return api.LookupRequestResult{Code: api.CodeIndeterminate}, lockErr
			}
			captured, captureErr := finishReadDelivery(ctx, e, "lookup_request", req.Principal, got, model.SnapshotAnchor{Space: req.Space, Fence: got.ObservedFence}, "", "", nil)
			e.mu.Unlock()
			return captured, captureErr
		}
		return got, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, err
	}
	result, found, err := e.lookup(ctx, req.Key())
	if err != nil {
		return api.LookupRequestResult{Code: api.CodeIndeterminate}, e.readFailure(err)
	}
	if !found {
		if got, queued, err := e.inFlight(req); queued {
			if err == nil && ctx.Value(readDeliveryKey{}) != nil {
				captured, captureErr := finishReadDelivery(ctx, e, "lookup_request", req.Principal, got, model.SnapshotAnchor{Space: req.Space, Fence: got.ObservedFence}, "", "", nil)
				return captured, captureErr
			}
			return got, err
		}
		result := api.LookupRequestResult{Code: api.CodeNotFoundAtFence, ObservedFence: e.publication.Fence()}
		return finishReadDelivery(ctx, e, "lookup_request", req.Principal, result, model.SnapshotAnchor{Space: req.Space, Fence: result.ObservedFence}, "", "", nil)
	}
	out := api.LookupRequestResult{Code: api.CodeDurableAck, Result: result, ObservedFence: e.publication.Fence()}
	return finishReadDelivery(ctx, e, "lookup_request", req.Principal, out, model.SnapshotAnchor{Space: req.Space, Fence: out.ObservedFence}, "", "", nil)
}

func (e *Engine) GetState(ctx context.Context, req api.ReadStateRequest) (api.StateValueResult, error) {
	return e.getStateForMethodAnchor(ctx, req, api.MethodMutateState, nil, true)
}

// GetStateForMethod reads the root owned by a typed domain command. It is used
// by Graph/Memory/Knowledge facades so their reads cannot silently fall back
// to DomainStates.
func (e *Engine) GetStateForMethod(ctx context.Context, req api.ReadStateRequest, method string) (api.StateValueResult, error) {
	return e.GetStateForMethodAnchor(ctx, req, method, nil)
}

// GetStateForMethodAnchor reads a typed root at an already-published fence.
// Knowledge selectors remain separate from this control-state cut and are
// rejected here until their per-key projection is available.
func (e *Engine) GetStateForMethodAnchor(ctx context.Context, req api.ReadStateRequest, method string, anchor *model.SnapshotAnchor) (api.StateValueResult, error) {
	return e.getStateForMethodAnchor(ctx, req, method, anchor, false)
}

func (e *Engine) getStateForMethodAnchor(ctx context.Context, req api.ReadStateRequest, method string, anchor *model.SnapshotAnchor, deliver bool) (api.StateValueResult, error) {
	if err := ctx.Err(); err != nil {
		return api.StateValueResult{}, err
	}
	if err := validSpace(req.Space); err != nil {
		return api.StateValueResult{}, err
	}
	if len(req.Key) > api.MaxStateKeyBytes {
		return api.StateValueResult{}, state.ErrInvalidKey
	}
	if !isRegisteredDomainMethod(method) {
		return api.StateValueResult{}, ErrUnsupportedMethod
	}
	if err := e.publicGate(); err != nil {
		return api.StateValueResult{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.StateValueResult{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.StateValueResult{}, err
	}
	roots := e.roots
	rootSetID := e.rootSet
	if anchor != nil {
		a := anchor.Clone()
		if err := a.Validate(); err != nil || a.Space != req.Space || a.Cursor != nil || a.Knowledge != nil {
			return api.StateValueResult{}, model.ErrAnchorMismatch
		}
		proof, ok := e.history[a.Fence.LSN]
		if !ok || proof.fence != a.Fence {
			return api.StateValueResult{}, model.ErrAnchorMismatch
		}
		roots = model.RootSet{}
		if proof.rootSet != "" {
			obj, err := readObject(ctx, e.index, proof.rootSet)
			if err != nil {
				return api.StateValueResult{}, e.readFailure(err)
			}
			roots, err = readRootSet(obj)
			if err != nil {
				return api.StateValueResult{}, e.readFailure(err)
			}
		}
		rootSetID = proof.rootSet
	}
	root, err := slotSpaceRoot(ctx, e.index, methodRoot(roots, method), req.Space)
	if err != nil {
		return api.StateValueResult{}, e.readFailure(err)
	}
	out, id, err := e.readStateAtRoot(ctx, req, root)
	if err != nil {
		return api.StateValueResult{}, err
	}
	out.RootSet = rootSetID
	if anchor != nil {
		out.Fence = anchor.Fence
	}
	if err := ctx.Err(); err != nil {
		return api.StateValueResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.StateValueResult{}, err
	}
	if deliver {
		// The transport authenticates this principal and authorizes the requested
		// space before calling the principal-free core state API.
		var dependencies []model.ObjectID
		if out.Found {
			dependencies = []model.ObjectID{id}
		}
		anchor := model.SnapshotAnchor{Space: req.Space, Fence: out.Fence}
		return finishReadDelivery(ctx, e, "get_state", readDeliveryPrincipal(ctx), out, anchor, "", "", dependencies)
	}
	return out, nil
}

func (e *Engine) readStateAtRoot(ctx context.Context, req api.ReadStateRequest, root model.ObjectID) (api.StateValueResult, model.ObjectID, error) {
	id, found, err := state.Get(ctx, e.index, req.Space, root, req.Key)
	if err != nil {
		return api.StateValueResult{}, "", e.readFailure(err)
	}
	result := api.StateValueResult{Found: found, StateRoot: root, RootSet: e.rootSet, Fence: e.publication.Fence()}
	if found {
		if err := e.checkDisclosureIDs(ctx, req.Space, []model.ObjectID{id}); err != nil {
			return api.StateValueResult{}, "", err
		}
		if err := e.checkDerivedDisclosureIDs(ctx, req.Space, []model.ObjectID{id}); err != nil {
			return api.StateValueResult{}, "", err
		}
		obj, err := readObject(ctx, e.index, id)
		if err != nil {
			return api.StateValueResult{}, "", e.readFailure(err)
		}
		if obj.Space != req.Space {
			return api.StateValueResult{}, "", e.readFailure(ErrInvalidStateData)
		}
		result.Value, err = readValue(obj)
		if err != nil {
			return api.StateValueResult{}, "", e.readFailure(err)
		}
	}
	return result, id, nil
}

func (e *Engine) Snapshot(ctx context.Context) (api.StateSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return api.StateSnapshot{}, err
	}
	if err := e.publicGate(); err != nil {
		return api.StateSnapshot{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.StateSnapshot{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.StateSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.StateSnapshot{}, err
	}
	result := api.StateSnapshot{Roots: e.roots, RootSet: e.rootSet, Fence: e.publication.Fence()}
	return finishReadDelivery(ctx, e, "snapshot", readDeliveryPrincipal(ctx), result, model.SnapshotAnchor{Fence: result.Fence}, "", "", nil)
}

// CloseContext stops new admission and read grants even if its own context is
// already canceled. Accepted work keeps draining. A timeout only ends this wait;
// retry Close to finish retiring the journal and its owner lock.
func (e *Engine) CloseContext(ctx context.Context) error {
	if e.retired.Load() {
		return nil
	}
	e.draining.Store(true)
	e.stopAdmission(storage.ErrClosing)
	e.stopViews()
	if err := ctx.Err(); err != nil {
		return err
	}
	if a := e.admission; a != nil {
		select {
		case <-a.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := e.waitViews(ctx); err != nil {
		return err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closing = true
	if e.journal != nil {
		if err := e.journal.Close(); err != nil {
			return err
		}
	}
	e.closed = true
	e.retired.Store(true)
	return nil
}

func (e *Engine) Close() error { return e.CloseContext(context.Background()) }
