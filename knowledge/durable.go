package knowledge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/journalstore"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var (
	ErrDurableClosed       = errors.New("knowledge durable store is closed")
	ErrDurableClosing      = errors.New("knowledge durable store is closing")
	ErrObservationSequence = errors.New("knowledge observation sequence conflicts")
	ErrUnknownObservation  = errors.New("context packet references an unavailable observation")
)

const durableSchema uint64 = 2
const observationRecord uint64 = 1
const packetRecord uint64 = 2

// Verification contains owned values for a trusted source-catalog verifier.
// Consumer is the host-supplied publication cut; this separate adapter cannot
// derive its truth from local LSN arithmetic. Content lists exact source IDs.
type Verification struct {
	Request   model.RequestKey
	Source    SourceAnchor
	Consumer  model.CommitFence
	Knowledge *model.KnowledgeKey
	Content   []model.ObjectID
}

// SourceVerifier must verify immutable source/consumer publication and content
// membership against the embedding host's already-authenticated catalog. It
// must not mutate state, dispatch external work, or reenter Durable methods.
// Replay calls the same verifier: absence is an error, never a switch to latest.
// Current ACL/output grants remain the host's separate responsibility.
type SourceVerifier func(Verification) error

type knowledgeScope struct {
	Space model.SpaceID
	Key   model.KnowledgeKey
}

// Durable is an experimental separate-journal projection, not the five-root
// core authority. It serializes operations independently of the projection
// mutex, so prepared installation/Close cannot deadlock on the same lock.
type Durable struct {
	operation       chan struct{}
	mu              sync.RWMutex
	observations    map[model.ObjectID]Observation
	observationCuts map[model.ObjectID]model.CommitFence
	sequences       map[knowledgeScope]uint64
	packets         map[model.ObjectID]ContextPacket
	store           *journalstore.Store
	verify          SourceVerifier
	closing         atomic.Bool
	closed          atomic.Bool
}

func newDurable(verify SourceVerifier) *Durable {
	d := &Durable{operation: make(chan struct{}, 1), observations: make(map[model.ObjectID]Observation), observationCuts: make(map[model.ObjectID]model.CommitFence), sequences: make(map[knowledgeScope]uint64), packets: make(map[model.ObjectID]ContextPacket), verify: verify}
	d.operation <- struct{}{}
	return d
}

// CreateDurable is the trusted-host compatibility constructor. It verifies
// structure, scope, local observation membership and declared cuts, but cannot
// authenticate caller-supplied external publication/content proofs. Use
// CreateDurableWithVerifier when the host has a verified source catalog.
func CreateDurable(d storage.DurableIO, o storage.JournalOptions) (*Durable, error) {
	return createDurable(d, o, nil)
}
func CreateDurableWithVerifier(d storage.DurableIO, o storage.JournalOptions, verify SourceVerifier) (*Durable, error) {
	if verify == nil {
		return nil, fmt.Errorf("%w: nil source verifier", ErrInvalid)
	}
	return createDurable(d, o, verify)
}
func createDurable(d storage.DurableIO, o storage.JournalOptions, verify SourceVerifier) (*Durable, error) {
	out := newDurable(verify)
	js, err := journalstore.CreatePrepared(d, o, out.prepare)
	if err != nil {
		return nil, err
	}
	out.store = js
	return out, nil
}

// OpenDurable has the same trusted-host boundary as CreateDurable. The replay
// projection remains private until the full Journal prefix succeeds. All
// semantic validation runs before storage may repair unpublished bytes.
func OpenDurable(d storage.DurableIO, o storage.JournalOptions) (*Durable, storage.RecoveryReport, error) {
	return openDurable(d, o, nil)
}
func OpenDurableWithVerifier(d storage.DurableIO, o storage.JournalOptions, verify SourceVerifier) (*Durable, storage.RecoveryReport, error) {
	if verify == nil {
		return nil, storage.RecoveryReport{}, fmt.Errorf("%w: nil source verifier", ErrInvalid)
	}
	return openDurable(d, o, verify)
}
func openDurable(d storage.DurableIO, o storage.JournalOptions, verify SourceVerifier) (*Durable, storage.RecoveryReport, error) {
	out := newDurable(verify)
	js, report, err := journalstore.OpenPrepared(d, o, out.prepare)
	if err != nil {
		return nil, report, err
	}
	out.store = js
	return out, report, nil
}

func encodeDurable(kind uint64, consumer model.CommitFence, raw []byte) ([]byte, error) {
	if kind != observationRecord && kind != packetRecord {
		return nil, ErrInvalid
	}
	if err := consumer.Validate(); err != nil {
		return nil, err
	}
	if !validText(string(consumer.Store)) || len(raw) > MaxRecordBytes {
		return nil, ErrInvalid
	}
	b := codec.AppendUint(nil, durableSchema)
	b = codec.AppendUint(b, kind)
	var err error
	b, err = codec.AppendString(b, string(consumer.Store))
	if err != nil {
		return nil, err
	}
	b = codec.AppendUint(b, consumer.LSN)
	b, err = codec.AppendString(b, string(consumer.CommitDigest))
	if err != nil {
		return nil, err
	}
	return codec.AppendBytes(b, raw)
}
func decodeDurable(raw []byte) (uint64, model.CommitFence, []byte, error) {
	if len(raw) > MaxRecordBytes+2*model.MaxIdentityBytes+32 {
		return 0, model.CommitFence{}, nil, ErrInvalid
	}
	d := codec.NewDecoder(raw)
	version, err := d.Uint()
	if err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	if version != durableSchema {
		return 0, model.CommitFence{}, nil, fmt.Errorf("%w: knowledge durable schema", storage.ErrUnsupported)
	}
	kind, err := d.Uint()
	if err != nil || (kind != observationRecord && kind != packetRecord) {
		return 0, model.CommitFence{}, nil, ErrInvalid
	}
	store, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	lsn, err := d.Uint()
	if err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	digest, err := d.String(64)
	if err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	consumer := model.CommitFence{Store: model.StoreID(store), LSN: lsn, CommitDigest: model.CommitDigest(digest)}
	if err = consumer.Validate(); err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	if !validText(store) {
		return 0, model.CommitFence{}, nil, ErrInvalid
	}
	payload, err := d.Bytes(MaxRecordBytes)
	if err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	if err = d.Done(); err != nil {
		return 0, model.CommitFence{}, nil, err
	}
	return kind, consumer, payload, nil
}

func cloneObservation(o Observation) Observation { o.Source = o.Source.clone(); return o }
func clonePacket(p ContextPacket) ContextPacket {
	itemsPresent := p.Items != nil
	p = p.Clone()
	if itemsPresent && p.Items == nil {
		p.Items = []ContextItem{}
	}
	return p
}

func (s *Durable) verifySource(request model.RequestKey, source SourceAnchor, consumer model.CommitFence, key *model.KnowledgeKey, content []model.ObjectID) error {
	if s.verify == nil {
		return nil
	}
	proof := Verification{Request: request, Source: source.clone(), Consumer: consumer, Content: append([]model.ObjectID(nil), content...)}
	if key != nil {
		copyKey := *key
		proof.Knowledge = &copyKey
	}
	return s.verify(proof)
}

// prepare decodes and validates the exact owned command before P1. Only the
// returned closure changes the projection, under mu, after successful publish.
func (s *Durable) prepare(r journalstore.Record) (func(), error) {
	if r.Kind != storage.KindObject {
		return nil, ErrInvalid
	}
	if err := r.Request.Validate(); err != nil {
		return nil, err
	}
	kind, consumer, raw, err := decodeDurable(r.Payload)
	if err != nil {
		return nil, err
	}
	switch kind {
	case observationRecord:
		o, err := DecodeObservation(raw)
		if err != nil {
			return nil, fmt.Errorf("observation decode: %w", err)
		}
		if o.ImportedFrom != "" {
			return nil, ErrKnowledgeMismatch
		}
		if r.Request.Space != o.Source.Anchor.Space {
			return nil, ErrInvalid
		}
		if err = o.Source.ValidateBefore(consumer); err != nil {
			return nil, err
		}
		if err = s.verifySource(r.Request, o.Source, consumer, &o.Key, []model.ObjectID{o.Payload}); err != nil {
			return nil, err
		}
		scope := knowledgeScope{Space: r.Request.Space, Key: o.Key}
		s.mu.RLock()
		_, exists := s.observations[o.ID]
		last := s.sequences[scope]
		s.mu.RUnlock()
		if exists {
			return nil, nil
		}
		if last == ^uint64(0) || o.Sequence != last+1 {
			return nil, ErrObservationSequence
		}
		candidate := cloneObservation(o)
		return func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.observations[o.ID] = candidate
			s.observationCuts[o.ID] = consumer
			s.sequences[scope] = o.Sequence
		}, nil
	case packetRecord:
		p, err := DecodePacket(raw)
		if err != nil {
			return nil, fmt.Errorf("packet decode: %w", err)
		}
		if r.Request.Space != p.Space {
			return nil, ErrInvalid
		}
		if err = p.ValidateBefore(consumer); err != nil {
			return nil, err
		}
		contents := make([]model.ObjectID, 0, len(p.Items)+1)
		selectionSource := Snapshot{}
		if p.GoalRef != "" {
			contents = append(contents, p.GoalRef)
		}
		s.mu.RLock()
		if p.Selection != nil {
			for id, o := range s.observations {
				cut := s.observationCuts[id]
				if o.Source.Anchor.Space == p.Space && p.Key != nil && o.Key == *p.Key && cut.Store == p.Source.Anchor.Fence.Store && cut.LSN <= p.Source.Anchor.Fence.LSN {
					selectionSource.Observations = append(selectionSource.Observations, o.Clone())
				}
			}
		}
		for _, item := range p.Items {
			o, ok := s.observations[item.Observation]
			if !ok {
				s.mu.RUnlock()
				return nil, ErrUnknownObservation
			}
			adopted := s.observationCuts[item.Observation]
			if o.Source.Anchor.Space != p.Space || o.Payload != item.Content {
				s.mu.RUnlock()
				return nil, ErrInvalid
			}
			if p.Key != nil && o.Key != *p.Key {
				s.mu.RUnlock()
				return nil, ErrKnowledgeMismatch
			}
			if adopted.Store != p.Source.Anchor.Fence.Store || adopted.LSN > p.Source.Anchor.Fence.LSN {
				s.mu.RUnlock()
				return nil, ErrFutureSource
			}
			contents = append(contents, item.Content)
		}
		_, exists := s.packets[p.ID]
		s.mu.RUnlock()
		if err = VerifyPacketSelection(p, selectionSource); err != nil {
			return nil, err
		}
		if err = s.verifySource(r.Request, p.Source, consumer, p.Key, contents); err != nil {
			return nil, err
		}
		if p.Input != nil {
			if err = s.verifySource(r.Request, SourceAnchor{Anchor: p.Input.Anchor}, consumer, nil, []model.ObjectID{p.Input.Object}); err != nil {
				return nil, err
			}
		}
		if exists {
			return nil, nil
		}
		candidate := clonePacket(p)
		return func() { s.mu.Lock(); defer s.mu.Unlock(); s.packets[p.ID] = candidate }, nil
	}
	return nil, ErrInvalid
}

func (s *Durable) gate() error {
	if s.closed.Load() || s.operation == nil || s.store == nil {
		return ErrDurableClosed
	}
	if s.closing.Load() {
		return ErrDurableClosing
	}
	return nil
}
func (s *Durable) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.gate(); err != nil {
		return err
	}
	select {
	case <-s.operation:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		s.release()
		return err
	}
	if err := s.gate(); err != nil {
		s.release()
		return err
	}
	if err := s.store.Ready(); err != nil {
		s.release()
		return err
	}
	return nil
}
func (s *Durable) release() { s.operation <- struct{}{} }

func (s *Durable) AppendObservation(ctx context.Context, req model.RequestKey, o Observation, consumer model.CommitFence) (Observation, error) {
	if o.ImportedFrom != "" {
		return Observation{}, ErrKnowledgeMismatch
	}
	o = cloneObservation(o)
	if o.ID == "" {
		o.ID = ObservationID(o)
	}
	raw, err := o.Canonical()
	if err != nil {
		return Observation{}, err
	}
	payload, err := encodeDurable(observationRecord, consumer, raw)
	if err != nil {
		return Observation{}, err
	}
	if err = s.acquire(ctx); err != nil {
		return Observation{}, err
	}
	defer s.release()
	if _, err = s.store.Append(ctx, journalstore.Record{Kind: storage.KindObject, Payload: payload, Request: req}); err != nil {
		return Observation{}, err
	}
	s.mu.RLock()
	result, ok := s.observations[o.ID]
	s.mu.RUnlock()
	if !ok {
		return Observation{}, ErrInvalid
	}
	return cloneObservation(result), nil
}
func (s *Durable) AppendPacket(ctx context.Context, req model.RequestKey, p ContextPacket, consumer model.CommitFence) (ContextPacket, error) {
	p = clonePacket(p)
	if p.ID == "" {
		p.ID = PacketID(p)
	}
	raw, err := p.Canonical()
	if err != nil {
		return ContextPacket{}, err
	}
	payload, err := encodeDurable(packetRecord, consumer, raw)
	if err != nil {
		return ContextPacket{}, err
	}
	if err = s.acquire(ctx); err != nil {
		return ContextPacket{}, err
	}
	defer s.release()
	if _, err = s.store.Append(ctx, journalstore.Record{Kind: storage.KindObject, Payload: payload, Request: req}); err != nil {
		return ContextPacket{}, err
	}
	s.mu.RLock()
	result, ok := s.packets[p.ID]
	s.mu.RUnlock()
	if !ok {
		return ContextPacket{}, ErrInvalid
	}
	return clonePacket(result), nil
}
func (s *Durable) GetObservation(id model.ObjectID) (Observation, error) {
	if err := s.acquire(context.Background()); err != nil {
		return Observation{}, err
	}
	defer s.release()
	s.mu.RLock()
	o, ok := s.observations[id]
	s.mu.RUnlock()
	if err := s.gate(); err != nil {
		return Observation{}, err
	}
	if !ok {
		return Observation{}, ErrInvalid
	}
	return cloneObservation(o), nil
}
func (s *Durable) GetPacket(id model.ObjectID) (ContextPacket, error) {
	if err := s.acquire(context.Background()); err != nil {
		return ContextPacket{}, err
	}
	defer s.release()
	s.mu.RLock()
	p, ok := s.packets[id]
	s.mu.RUnlock()
	if err := s.gate(); err != nil {
		return ContextPacket{}, err
	}
	if !ok {
		return ContextPacket{}, ErrInvalid
	}
	return clonePacket(p), nil
}
func (s *Durable) Close() error {
	if s.closed.Load() {
		return nil
	}
	s.closing.Store(true)
	if s.operation == nil {
		s.closed.Store(true)
		return nil
	}
	<-s.operation
	defer s.release()
	if s.closed.Load() {
		return nil
	}
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			return err
		}
	}
	s.closed.Store(true)
	return nil
}
