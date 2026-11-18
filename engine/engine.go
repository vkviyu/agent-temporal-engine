package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
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
	// Observation is an optional host-drained, bounded diagnostic mailbox.
	// Nil/zero disables it. It survives failed opens and may be shared, but
	// carries no store/principal/request identity. No callbacks or workers run.
	Observation *observation.Collector
	// AgentProfile enables the statically compiled agent/v1 transitions.
	// Opening a log containing these commands also requires this option.
	AgentProfile bool
	// EventProfile records canonical command-applied occurrences atomically
	// with each accepted transaction. Reopening such a log requires this flag.
	EventProfile bool
	// DisableRecoveryCheckpoints forces complete historical semantic replay.
	// By default only a CURRENT-selected, authenticated checkpoint is consumed.
	DisableRecoveryCheckpoints bool
	Journal                    storage.JournalOptions
	MaxObjects                 uint64
	Admission                  AdmissionOptions
	Batch                      BatchOptions
	Views                      ViewOptions
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
	// Private audit replay and batch staging need semantic event state, but
	// never serve TaskTrace queries. Zero keeps indexing enabled, including
	// for engines assembled directly by internal fixtures.
	skipTaskTraceIndex bool
	// Replay freezes event generation to the registry recorded by the core
	// occurrence. Zero denotes legacy core-only transactions, never live opt-out.
	replayingEvents     bool
	replayEventRegistry uint64
	mu                  contextMutex
	// Only coordinator, maintenance and final Close acquire writerMu. Public
	// readers use mu to capture the published roots, never this IO gate.
	writerMu            contextMutex
	publicationPending  atomic.Bool
	views               viewRegistry
	history             map[uint64]commitEnvelope
	baseHistory         map[uint64]commitEnvelope
	events              map[model.EventID]eventEntry
	baseEvents          map[model.EventID]eventEntry
	eventBlocks         *eventMetadataBlock
	traceEvents         map[traceEventKey][]eventMetadataItem
	traceEventMeta      map[model.EventID]eventMetadataItem
	traceEventKeys      map[model.EventID][]traceEventKey
	traceCausalChildren map[model.EventID][]eventMetadataItem
	traceEvidence       traceEvidenceCache
	traceProfiles       taskTraceProfileCache
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
	// TaskTrace maps are allocated lazily by indexTaskTraceEvent.
	return &Engine{
		options:       o,
		history:       make(map[uint64]commitEnvelope),
		events:        make(map[model.EventID]eventEntry),
		eventHeads:    make(map[string]eventHead),
		index:         newObjectIndex(),
		envelopes:     make(map[model.TxID]commitEnvelope),
		receiptHints:  make(map[[32]byte]struct{}),
		traceEvidence: traceEvidenceCache{observation: o.Observation},
		traceProfiles: taskTraceProfileCache{observation: o.Observation},
	}
}

// newAuditVerifier retains all replay validation state without constructing
// query-only TaskTrace indexes. It must never become a published Engine.
func newAuditVerifier(o Options) *Engine {
	o.Observation = nil
	e := newEngine(o)
	e.skipTaskTraceIndex = true
	return e
}

// Create uses an existing empty directory IO. The caller owns that IO lifetime;
// close the Engine (retrying failures) before closing its IO.
func Create(d storage.DurableIO, options Options) (_ *Engine, err error) {
	start := options.Observation.Start()
	defer func() {
		options.Observation.Finish(start, observation.Event{Kind: observation.Create, Outcome: observationOutcome(err)})
	}()
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
func OpenExisting(d storage.DurableIO, options Options) (_ *Engine, report storage.RecoveryReport, err error) {
	start := options.Observation.Start()
	var replayed uint64
	var checkpointHit bool
	defer func() {
		options.Observation.Finish(start, observation.Event{Kind: observation.Open, Outcome: observationOutcome(err),
			Count: report.ScannedTransactions, Bytes: report.PublishedBytes, Certified: report.CheckpointTransactions,
			Replayed: replayed, CheckpointHit: checkpointHit})
	}()
	o, err := options.checked()
	if err != nil {
		return nil, storage.RecoveryReport{}, err
	}
	e := newEngine(o)
	replay := e.replay
	if o.Observation.Enabled() {
		replay = func(tx storage.ScannedTransaction, source io.ReaderAt) error {
			err := e.replay(tx, source)
			if err == nil {
				replayed++
			}
			return err
		}
	}
	var load storage.CheckpointLoader
	if !o.DisableRecoveryCheckpoints {
		load = func(ref storage.RecoveryCheckpoint, input io.Reader, source io.ReaderAt) (storage.ReplayFunc, error) {
			candidate, err := decodeRecoveryCheckpoint(o, ref, input, source)
			if err != nil {
				return nil, err
			}
			e = candidate
			checkpointHit = true
			if o.Observation.Enabled() {
				return replay, nil
			}
			return candidate.replay, nil
		}
	}
	j, report, err := storage.OpenJournalWithCheckpoint(d, o.Journal, replay, load)
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
	if e.publicationPending.Load() {
		return api.ErrReadAuthorizationChanged
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
	if errors.Is(err, ErrUnsupportedSchema) || errors.Is(err, knowledge.ErrUnsupportedSchema) || errors.Is(err, codec.ErrObjectUnsupported) || errors.Is(err, storage.ErrUnsupported) || errors.Is(err, storage.ErrLimit) || errors.Is(err, state.ErrLimit) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w: %w", ErrSemanticCorruption, storage.ErrCorruption, err)
}

func isLimit(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrObjectIO) {
		return false
	}
	return errors.Is(err, agentprofile.ErrStateLimit) || errors.Is(err, storage.ErrLimit) || errors.Is(err, state.ErrLimit) || errors.Is(err, codec.ErrObjectLimit) || errors.Is(err, graph.ErrLimit) || errors.Is(err, knowledge.ErrContextSelectionLimit) || errors.Is(err, annotation.ErrLimit) || errors.Is(err, investigation.ErrLimit)
}

func (e *Engine) readFailure(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	cause := semanticError(err)
	e.traceEvidence.stop()
	e.traceProfiles.stop()
	e.fenced = cause
	err = e.journal.Fence(cause)
	e.stopAdmission(err)
	return err
}

func (e *Engine) lookup(ctx context.Context, key model.RequestKey) (api.CommitResult, bool, error) {
	id, found, err := state.Get(ctx, e.reader(ctx), systemSpace, e.readRoots(ctx).CommandReceipts, receiptKey(key))
	if err != nil || !found {
		return api.CommitResult{}, false, err
	}
	obj, err := readObject(ctx, e.reader(ctx), id)
	if err != nil {
		return api.CommitResult{}, false, err
	}
	receipt, err := readReceipt(obj)
	if err != nil {
		return api.CommitResult{}, false, err
	}
	proof, ok, err := e.readEnvelope(ctx, receipt.TxID)
	if err != nil {
		return api.CommitResult{}, false, err
	}
	if !ok || receipt.Key != key || receipt.LSN != proof.fence.LSN {
		return api.CommitResult{}, false, ErrInvalidStateData
	}
	return api.CommitResult{Code: api.CodeDurableAck, RequestID: key.RequestID, TxID: receipt.TxID, Fence: proof.fence,
		RequestDigest: receipt.RequestDigest, ResultDigest: id, StateRoot: receipt.StateRoot, RootSet: proof.rootSet}, true, nil
}

func (e *Engine) Commit(ctx context.Context, req api.CommitRequest) (api.CommitResult, error) {
	start := e.options.Observation.Start()
	result, err := e.commitQueued(ctx, req)
	e.options.Observation.Finish(start, observation.Event{Kind: observation.Commit, Outcome: commitObservation(result, err)})
	return result, err
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

// CloseContext stops new admission and read grants even if its own context is
// already canceled. Accepted work keeps draining. A timeout only ends this wait;
// retry Close to finish retiring the journal and its owner lock.
func (e *Engine) CloseContext(ctx context.Context) (err error) {
	start := e.options.Observation.Start()
	defer func() {
		e.options.Observation.Finish(start, observation.Event{Kind: observation.Close, Outcome: observationOutcome(err)})
	}()
	if e.retired.Load() {
		return nil
	}
	e.draining.Store(true)
	e.traceEvidence.stop()
	e.traceProfiles.stop()
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
	if err := e.writerMu.LockContext(ctx); err != nil {
		return err
	}
	defer e.writerMu.Unlock()
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closing = true
	e.mu.Unlock()
	if e.journal != nil {
		if err := e.journal.Close(); err != nil {
			return err
		}
	}
	e.mu.Lock()
	e.closed = true
	e.retired.Store(true)
	e.mu.Unlock()
	return nil
}

func (e *Engine) Close() error { return e.CloseContext(context.Background()) }
