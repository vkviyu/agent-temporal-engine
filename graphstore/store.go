// Package graphstore publishes experimental Revision and BranchRef records through Journal.
// Create/Open authenticate graph metadata only: they do not prove that a claimed
// StateRoot contains Revision.Entries. Use CreateVerified/OpenVerified with an
// authenticated state reader when semantic state verification is required.
// This format is a separate metadata adapter, not the engine's RootSet/profile
// authority or an authorization boundary for untrusted Agent commands.
package graphstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var ErrClosed = errors.New("graph store is closed")
var ErrInvalidGraphTransaction = errors.New("invalid graph transaction")
var ErrUnsupportedAdapterSchema = errors.New("unsupported graphstore adapter schema")

const principal = model.PrincipalID("graph-host")
const AdapterSchema uint64 = 1
const metadataDomain = "ATE-GRAPHSTORE\x00"
const requestPrefix = "revision/v1/"
const maxPublicationMetadataBytes = 128

type committedRevision struct {
	space       model.SpaceID
	digest      storage.Digest
	publication storage.PublishedTransaction
}
type committedRequest struct {
	digest      storage.Digest
	publication storage.PublishedTransaction
	branch      graph.Branch
}
type Store struct {
	mu          sync.Mutex
	journal     *storage.Journal
	graph       *graph.Store
	committed   map[model.RevisionID]committedRevision
	requests    map[model.RequestKey]committedRequest
	stateReader graph.StateReader
	closing     bool
	closed      bool
}

// Report is physical Journal evidence. Verified does not imply state-tree
// semantics were checked; OpenVerified additionally performs VerifyState in the
// semantic replay callback before any tail repair or publication witness sync.
type Report = storage.RecoveryReport

// Create initializes a metadata-only graph adapter in an empty directory.
func Create(d storage.DurableIO, o storage.JournalOptions) (*Store, error) { return create(d, o, nil) }
func CreateVerified(d storage.DurableIO, o storage.JournalOptions, reader graph.StateReader) (*Store, error) {
	if reader == nil {
		return nil, graph.ErrStateUnverified
	}
	return create(d, o, reader)
}
func create(d storage.DurableIO, o storage.JournalOptions, reader graph.StateReader) (*Store, error) {
	j, err := storage.CreateJournal(d, o)
	if err != nil {
		return nil, err
	}
	return &Store{journal: j, graph: graph.New(), committed: make(map[model.RevisionID]committedRevision), requests: make(map[model.RequestKey]committedRequest), stateReader: reader}, nil
}

// Open authenticates graph metadata, not the external state tree. Unsupported
// experimental records are rejected; no fallback or implicit migration occurs.
func Open(d storage.DurableIO, o storage.JournalOptions) (*Store, Report, error) {
	return open(d, o, nil)
}
func OpenVerified(d storage.DurableIO, o storage.JournalOptions, reader graph.StateReader) (*Store, Report, error) {
	if reader == nil {
		return nil, Report{}, graph.ErrStateUnverified
	}
	return open(d, o, reader)
}
func open(d storage.DurableIO, o storage.JournalOptions, reader graph.StateReader) (*Store, Report, error) {
	candidate := graph.New()
	committed := make(map[model.RevisionID]committedRevision)
	requests := make(map[model.RequestKey]committedRequest)
	var previousGeneration uint64
	j, report, err := storage.OpenJournalWithReplay(d, o, func(tx storage.ScannedTransaction, _ io.ReaderAt) error {
		if len(tx.Records) != 1 || len(tx.RecordLocations) != 1 || len(tx.RefUpdates) != 0 {
			return ErrInvalidGraphTransaction
		}
		generation, err := decodePublicationMetadata(tx.ControlUpdates)
		if err != nil {
			return err
		}
		if previousGeneration == ^uint64(0) || generation != previousGeneration+1 || generation != tx.LSN {
			return ErrInvalidGraphTransaction
		}
		raw := tx.Records[0].Payload
		var space model.SpaceID
		var branch graph.Branch
		switch tx.Records[0].Kind {
		case storage.KindRevision:
			r, decodeErr := graph.DecodeRevision(raw)
			if decodeErr != nil {
				return decodeErr
			}
			if tx.Request != (model.RequestKey{Space: r.Space, Principal: principal, RequestID: revisionRequestID(r.ID)}) || tx.Receipt != digestOf(string(r.ID)) || tx.PostRootSet != digestOf(string(r.ID)) || tx.RequestDigest != digestOfBytes(raw) {
				return ErrInvalidGraphTransaction
			}
			if _, duplicate := committed[r.ID]; duplicate {
				return fmt.Errorf("%w: duplicate revision request", ErrInvalidGraphTransaction)
			}
			if reader != nil {
				if decodeErr = graph.VerifyState(r, reader); decodeErr != nil {
					return decodeErr
				}
			}
			if decodeErr = candidate.PutRevision(r); decodeErr != nil {
				return decodeErr
			}
			space = r.Space
		case storage.KindEvent:
			command, decodeErr := decodeBranchCommand(raw)
			if decodeErr != nil {
				return decodeErr
			}
			if tx.Request.RequestID != branchRequestID(raw) || tx.Request.Principal != principal || tx.Receipt != branchDigest(raw) || tx.PostRootSet != branchDigest(raw) || tx.RequestDigest != branchDigest(raw) {
				return ErrInvalidGraphTransaction
			}
			if _, duplicate := requests[tx.Request]; duplicate {
				return fmt.Errorf("%w: duplicate branch request", ErrInvalidGraphTransaction)
			}
			space, decodeErr = validateBranchCommand(command, candidate)
			if decodeErr != nil {
				return decodeErr
			}
			if tx.Request.Space != space {
				return ErrInvalidGraphTransaction
			}
			if branch, decodeErr = applyBranchCommand(candidate, command); decodeErr != nil {
				return decodeErr
			}
		default:
			return ErrInvalidGraphTransaction
		}
		own := storage.Publication{Store: tx.Store, Generation: generation, Position: storage.Position{LSN: tx.LSN, Digest: tx.CommitDigest}, PostRootSet: tx.PostRootSet, PublishedEnd: uint64(tx.EndOffset), Profile: d.Profile()}
		proof := storage.PublishedTransaction{Publication: own, RecordLocations: append([]storage.RecordLocation(nil), tx.RecordLocations...)}
		requests[tx.Request] = committedRequest{digest: tx.RequestDigest, publication: clonePublication(proof), branch: branch}
		previousGeneration = generation
		if tx.Records[0].Kind == storage.KindRevision {
			r, decodeErr := graph.DecodeRevision(raw)
			if decodeErr != nil {
				return decodeErr
			}
			committed[r.ID] = committedRevision{space: space, digest: tx.RequestDigest, publication: proof}
		}
		return nil
	})
	if err != nil {
		return nil, report, err
	}
	return &Store{journal: j, graph: candidate, committed: committed, requests: requests, stateReader: reader}, report, nil
}
func revisionRequestID(id model.RevisionID) model.RequestID {
	return model.RequestID(requestPrefix + string(id))
}
func digestOf(id string) storage.Digest {
	var d storage.Digest
	raw, _ := hex.DecodeString(id)
	copy(d[:], raw)
	return d
}
func digestOfBytes(b []byte) storage.Digest { return storage.Digest(sha256.Sum256(b)) }
func encodePublicationMetadata(generation uint64) []byte {
	b := append([]byte(nil), metadataDomain...)
	b = codec.AppendUint(b, AdapterSchema)
	return codec.AppendUint(b, generation)
}
func decodePublicationMetadata(raw []byte) (uint64, error) {
	if len(raw) > maxPublicationMetadataBytes {
		return 0, storage.ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(metadataDomain)) {
		return 0, errors.Join(storage.ErrUnsupported, ErrUnsupportedAdapterSchema)
	}
	d := codec.NewDecoder(raw[len(metadataDomain):])
	schema, err := d.Uint()
	if err != nil {
		return 0, err
	}
	if schema != AdapterSchema {
		return 0, errors.Join(storage.ErrUnsupported, ErrUnsupportedAdapterSchema)
	}
	generation, err := d.Uint()
	if err != nil {
		return 0, err
	}
	if generation == 0 {
		return 0, ErrInvalidGraphTransaction
	}
	if err = d.Done(); err != nil {
		return 0, err
	}
	return generation, nil
}
func clonePublication(p storage.PublishedTransaction) storage.PublishedTransaction {
	p.RecordLocations = append([]storage.RecordLocation(nil), p.RecordLocations...)
	return p
}
func (s *Store) ready() error {
	if s.closed || s.journal == nil {
		return errors.Join(ErrClosed, storage.ErrClosed)
	}
	if s.closing {
		return storage.ErrClosing
	}
	_, err := s.journal.Published()
	return err
}
func (s *Store) validateParents(r graph.Revision) error {
	check := func(id model.RevisionID) error {
		p, ok := s.committed[id]
		if !ok {
			return graph.ErrUnknownRevision
		}
		if p.space != r.Space {
			return graph.ErrSpaceMismatch
		}
		return nil
	}
	if r.StateParent != "" {
		if err := check(r.StateParent); err != nil {
			return err
		}
	}
	for _, parent := range r.JoinParents {
		if err := check(parent); err != nil {
			return err
		}
	}
	return nil
}

// AddRevision publishes one immutable metadata record. Equal content reuses its
// stable RequestID and original publication proof, including after reopen. New
// parents and optional state verification are checked before any layout IO.
func (s *Store) AddRevision(ctx context.Context, space string, parent model.RevisionID, joins []model.RevisionID, root model.ObjectID, events []model.EventID, entries []graph.Entry) (graph.Revision, storage.PublishedTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	if err := ctx.Err(); err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	r, err := graph.BuildRevision(space, parent, joins, root, events, entries)
	if err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	return s.publishRevisionLocked(ctx, r)
}

func (s *Store) publishRevisionLocked(ctx context.Context, r graph.Revision) (graph.Revision, storage.PublishedTransaction, error) {
	raw, err := graph.EncodeRevision(r)
	if err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	requestDigest := digestOfBytes(raw)
	if existing, ok := s.committed[r.ID]; ok {
		if existing.digest != requestDigest {
			return graph.Revision{}, storage.PublishedTransaction{}, ErrInvalidGraphTransaction
		}
		return r, clonePublication(existing.publication), nil
	}
	if len(s.committed) >= graph.MaxRevisions {
		return graph.Revision{}, storage.PublishedTransaction{}, graph.ErrLimit
	}
	if err = s.validateParents(r); err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	if s.stateReader != nil {
		if err = graph.VerifyState(r, s.stateReader); err != nil {
			return graph.Revision{}, storage.PublishedTransaction{}, err
		}
	}
	p, err := s.journal.Published()
	if err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	if p.Generation == ^uint64(0) {
		return graph.Revision{}, storage.PublishedTransaction{}, storage.ErrLimit
	}
	var txidBytes [16]byte
	if _, err = rand.Read(txidBytes[:]); err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	txid := model.TxID(hex.EncodeToString(txidBytes[:]))
	tx := storage.Transaction{TxID: txid, Request: model.RequestKey{Space: r.Space, Principal: principal, RequestID: revisionRequestID(r.ID)}, RequestDigest: requestDigest, LSN: p.Position.LSN + 1, PreviousDigest: p.Position.Digest, Records: []storage.Record{{Kind: storage.KindRevision, Payload: raw}}, ControlUpdates: encodePublicationMetadata(p.Generation + 1), Receipt: digestOf(string(r.ID)), PostRootSet: digestOf(string(r.ID))}
	published, err := s.journal.PublishWithRecords(ctx, tx)
	if err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	if err = s.graph.PutRevision(r); err != nil {
		cause := errors.Join(storage.ErrCommitUnknown, err)
		_ = s.journal.Fence(cause)
		return graph.Revision{}, storage.PublishedTransaction{}, cause
	}
	s.committed[r.ID] = committedRevision{space: r.Space, digest: requestDigest, publication: clonePublication(published)}
	if s.requests == nil {
		s.requests = make(map[model.RequestKey]committedRequest)
	}
	s.requests[tx.Request] = committedRequest{digest: requestDigest, publication: clonePublication(published)}
	return r, clonePublication(published), nil
}

// CommitJoin verifies a previously computed explicit JoinPlan and publishes
// the resulting immutable Revision through the same durable path as AddRevision.
func (s *Store) CommitJoin(ctx context.Context, proposed graph.JoinPlan, root model.ObjectID, events []model.EventID) (graph.Revision, storage.PublishedTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	if err := ctx.Err(); err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	r, err := s.graph.PreviewJoin(proposed, root, events)
	if err != nil {
		return graph.Revision{}, storage.PublishedTransaction{}, err
	}
	return s.publishRevisionLocked(ctx, r)
}

// Fork creates a durable BranchRef pointing at an existing Revision. The
// branch operation is published before the in-memory graph changes; replay
// performs the same semantic check before any tail repair can be accepted.
func (s *Store) Fork(ctx context.Context, id model.BranchID, source model.RevisionID) (graph.Branch, storage.PublishedTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishBranchLocked(ctx, branchCommand{operation: branchFork, id: id, source: source})
}

// AdvanceBranch durably applies a version CAS to an existing BranchRef.
func (s *Store) AdvanceBranch(ctx context.Context, id model.BranchID, expected uint64, head model.RevisionID) (graph.Branch, storage.PublishedTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishBranchLocked(ctx, branchCommand{operation: branchAdvance, id: id, expected: expected, head: head})
}

func (s *Store) publishBranchLocked(ctx context.Context, command branchCommand) (graph.Branch, storage.PublishedTransaction, error) {
	if err := s.ready(); err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	if err := ctx.Err(); err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	raw, err := encodeBranchCommand(command)
	if err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	reference := command.source
	if command.operation == branchAdvance {
		reference = command.head
	}
	target, err := s.graph.Get(reference)
	if err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	key := model.RequestKey{Space: target.Space, Principal: principal, RequestID: branchRequestID(raw)}
	digest := branchDigest(raw)
	// Resolve the durable request before testing today's mutable branch CAS.
	// Its original typed value and proof form one immutable result.
	if old, found := s.requests[key]; found {
		if old.digest != digest || old.branch.ID == "" {
			return graph.Branch{}, storage.PublishedTransaction{}, ErrInvalidGraphTransaction
		}
		return old.branch, clonePublication(old.publication), nil
	}
	space, err := validateBranchCommand(command, s.graph)
	if err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	if space != target.Space {
		return graph.Branch{}, storage.PublishedTransaction{}, graph.ErrSpaceMismatch
	}
	p, err := s.journal.Published()
	if err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	if p.Generation == ^uint64(0) {
		return graph.Branch{}, storage.PublishedTransaction{}, storage.ErrLimit
	}
	var txidBytes [16]byte
	if _, err = rand.Read(txidBytes[:]); err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	tx := storage.Transaction{TxID: model.TxID(hex.EncodeToString(txidBytes[:])), Request: key, RequestDigest: digest, LSN: p.Position.LSN + 1, PreviousDigest: p.Position.Digest, Records: []storage.Record{{Kind: storage.KindEvent, Payload: raw}}, ControlUpdates: encodePublicationMetadata(p.Generation + 1), Receipt: digest, PostRootSet: digest}
	published, err := s.journal.PublishWithRecords(ctx, tx)
	if err != nil {
		return graph.Branch{}, storage.PublishedTransaction{}, err
	}
	branch, err := applyBranchCommand(s.graph, command)
	if err != nil {
		cause := errors.Join(storage.ErrCommitUnknown, err)
		_ = s.journal.Fence(cause)
		return graph.Branch{}, storage.PublishedTransaction{}, cause
	}
	if s.requests == nil {
		s.requests = make(map[model.RequestKey]committedRequest)
	}
	s.requests[key] = committedRequest{digest: digest, publication: clonePublication(published), branch: branch}
	return branch, clonePublication(published), nil
}

func (s *Store) Branch(id model.BranchID) (graph.Branch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return graph.Branch{}, err
	}
	return s.graph.Branch(id)
}
func (s *Store) Get(id model.RevisionID) (graph.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return graph.Revision{}, err
	}
	return s.graph.Get(id)
}
func (s *Store) Diff(a, b model.RevisionID) (graph.Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return graph.Diff{}, err
	}
	return s.graph.Diff(a, b)
}
func (s *Store) PlanJoin(a, b model.RevisionID) (graph.JoinPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return graph.JoinPlan{}, err
	}
	return s.graph.PlanJoin(a, b)
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closing = true
	if s.journal != nil {
		if err := s.journal.Close(); err != nil {
			return err
		}
	}
	s.closed = true
	return nil
}
