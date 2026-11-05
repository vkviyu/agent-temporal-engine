// Package memory provides a prepared Journal adapter for its immutable memory
// and experience-path records. The adapter is deliberately separate from the
// core Engine: it gives callers durable replay/idempotency without pretending
// to be one of ATE's five RootSet authorities.
package memory

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/journalstore"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const (
	durableSchema uint64 = 1
	durableMagic         = "ATE-MEMORY-COMMAND\x00"
)

type durableOperation uint64

const (
	durablePutCandidate durableOperation = iota + 1
	durableValidate
	durablePromote
	durableTransition
	durableRetractSource
	durableBuildPath
	durableRetractPath
)

var (
	ErrDurableClosed  = errors.New("memory durable store is closed")
	ErrDurableClosing = errors.New("memory durable store is closing")
)

type durableCommand struct {
	op         durableOperation
	memory     Memory
	path       ExperiencePath
	id         model.ObjectID
	to         Status
	why        string
	retraction Retraction
	validation Validation
}

type durableResult struct {
	op      durableOperation
	memory  Memory
	path    ExperiencePath
	changed []model.ObjectID
}

func (r durableResult) clone() durableResult {
	r.memory = cloneMemory(r.memory)
	r.path = clonePath(r.path)
	r.changed = append([]model.ObjectID(nil), r.changed...)
	return r
}

// Durable persists memory lifecycle transitions as one-record prepared
// transactions. A command is applied to a private Store clone before P1; the
// install closure swaps the clone only after the Journal publication succeeds.
type Durable struct {
	mu      sync.Mutex
	index   *Store
	store   *journalstore.Store
	results map[model.RequestKey]durableResult
	closing bool
	closed  bool
	fenced  error
}

func newDurable() *Durable {
	return &Durable{index: New(), results: make(map[model.RequestKey]durableResult)}
}

func CreateDurable(d storage.DurableIO, o storage.JournalOptions) (*Durable, error) {
	out := newDurable()
	js, err := journalstore.CreatePrepared(d, o, out.prepare)
	if err != nil {
		return nil, err
	}
	out.store = js
	return out, nil
}

func OpenDurable(d storage.DurableIO, o storage.JournalOptions) (*Durable, storage.RecoveryReport, error) {
	out := newDurable()
	js, report, err := journalstore.OpenPrepared(d, o, out.prepare)
	if err != nil {
		return nil, report, err
	}
	out.store = js
	return out, report, nil
}

func (d *Durable) ready() error {
	if d.closed || d.store == nil {
		return ErrDurableClosed
	}
	if d.closing {
		return ErrDurableClosing
	}
	if d.fenced != nil {
		return errors.Join(storage.ErrStoreFenced, d.fenced)
	}
	return d.store.Ready()
}

func (d *Durable) prepare(r journalstore.Record) (func(), error) {
	if r.Kind != storage.KindObject {
		return nil, ErrInvalidMemory
	}
	if err := r.Request.Validate(); err != nil {
		return nil, err
	}
	command, err := decodeDurableCommand(r.Payload)
	if err != nil {
		return nil, err
	}
	if err := validateDurableScope(d.index, r.Request, command); err != nil {
		return nil, err
	}
	if _, exists := d.results[r.Request]; exists {
		return nil, journalstore.ErrIdempotencyConflict
	}
	candidate := d.index.clone()
	result, err := applyDurableCommand(candidate, command)
	if err != nil {
		return nil, err
	}
	result = result.clone()
	return func() {
		d.index = candidate
		d.results[r.Request] = result
	}, nil
}

func validateDurableScope(s *Store, key model.RequestKey, c durableCommand) error {
	if key.Space == "" {
		return ErrInvalidMemory
	}
	switch c.op {
	case durablePutCandidate:
		if c.memory.Space != key.Space {
			return ErrInvalidMemory
		}
	case durableBuildPath:
		if c.path.Space != key.Space {
			return ErrInvalidMemory
		}
	case durableValidate, durablePromote, durableTransition:
		m, err := s.get(c.id)
		if err != nil {
			return err
		}
		if m.Space != key.Space {
			return ErrInvalidMemory
		}
	case durableRetractPath:
		p, err := s.getPath(c.id)
		if err != nil {
			return err
		}
		if p.Space != key.Space {
			return ErrInvalidMemory
		}
	case durableRetractSource:
		// A source ban may affect several records, so its scope is proven by
		// every matching record rather than by the source ID alone. Unknown
		// source IDs retain the pure Store's explicit-ban semantics.
		for _, m := range s.memories {
			for _, src := range allSources(m) {
				if src.SourceID == c.id && m.Space != key.Space {
					return ErrInvalidMemory
				}
			}
		}
		for _, p := range s.paths {
			for _, src := range append(append([]Source(nil), p.Validation.Evidence...), p.Validation.Counterexamples...) {
				if src.SourceID == c.id && p.Space != key.Space {
					return ErrInvalidMemory
				}
			}
		}
	}
	return nil
}

func applyDurableCommand(s *Store, c durableCommand) (durableResult, error) {
	result := durableResult{op: c.op}
	var err error
	switch c.op {
	case durablePutCandidate:
		result.memory, err = s.putCandidate(c.memory)
	case durableValidate:
		result.memory, err = s.validate(c.id, c.validation)
	case durablePromote:
		result.memory, err = s.promote(c.id)
	case durableTransition:
		result.memory, err = s.transition(c.id, c.to, c.why)
	case durableRetractSource:
		result.changed, err = s.retractSource(c.id, c.retraction)
	case durableBuildPath:
		p := c.path
		result.path, err = s.buildExperiencePath(p.Space, p.SourceAnchor, p.Steps, p.Omitted, p.Validation, p.Applicability, p.DerivedFrom)
	case durableRetractPath:
		result.path, err = s.retractPath(c.id, c.retraction)
	default:
		err = storage.ErrUnsupported
	}
	return result, err
}

func (d *Durable) execute(ctx context.Context, key model.RequestKey, command durableCommand) (durableResult, error) {
	if err := ctx.Err(); err != nil {
		return durableResult{}, err
	}
	if err := d.ready(); err != nil {
		return durableResult{}, err
	}
	raw, err := encodeDurableCommand(command)
	if err != nil {
		return durableResult{}, err
	}
	if _, err = d.store.Append(ctx, journalstore.Record{Kind: storage.KindObject, Payload: raw, Request: key}); err != nil {
		return durableResult{}, err
	}
	result, ok := d.results[key]
	if !ok || result.op != command.op {
		d.fenced = errors.New("memory durable projection missing published result")
		return durableResult{}, errors.Join(storage.ErrCommitUnknown, storage.ErrStoreFenced, d.fenced)
	}
	return result.clone(), nil
}

func (d *Durable) PutCandidate(ctx context.Context, key model.RequestKey, m Memory) (Memory, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m.Status = StatusCandidate
	if m.VisibleAt.IsZero() {
		m.VisibleAt = m.RecordedAt
	}
	m.ID = ""
	r, err := d.execute(ctx, key, durableCommand{op: durablePutCandidate, memory: m})
	return r.memory, err
}
func (d *Durable) Validate(ctx context.Context, key model.RequestKey, id model.ObjectID, v Validation) (Memory, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, err := d.execute(ctx, key, durableCommand{op: durableValidate, id: id, validation: v})
	return r.memory, err
}
func (d *Durable) Promote(ctx context.Context, key model.RequestKey, id model.ObjectID) (Memory, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, err := d.execute(ctx, key, durableCommand{op: durablePromote, id: id})
	return r.memory, err
}
func (d *Durable) Transition(ctx context.Context, key model.RequestKey, id model.ObjectID, to Status, why string) (Memory, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, err := d.execute(ctx, key, durableCommand{op: durableTransition, id: id, to: to, why: why})
	return r.memory, err
}
func (d *Durable) RetractSource(ctx context.Context, key model.RequestKey, id model.ObjectID, r Retraction) ([]model.ObjectID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.execute(ctx, key, durableCommand{op: durableRetractSource, id: id, retraction: r})
	return result.changed, err
}
func (d *Durable) BuildExperiencePath(ctx context.Context, key model.RequestKey, p ExperiencePath) (ExperiencePath, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p.Status = StatusPromoted
	p.ID = ""
	r, err := d.execute(ctx, key, durableCommand{op: durableBuildPath, path: p})
	return r.path, err
}
func (d *Durable) RetractPath(ctx context.Context, key model.RequestKey, id model.ObjectID, r Retraction) (ExperiencePath, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.execute(ctx, key, durableCommand{op: durableRetractPath, id: id, retraction: r})
	return result.path, err
}

func (d *Durable) Get(id model.ObjectID) (Memory, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ready(); err != nil {
		return Memory{}, err
	}
	return d.index.Get(id)
}
func (d *Durable) GetPath(id model.ObjectID) (ExperiencePath, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ready(); err != nil {
		return ExperiencePath{}, err
	}
	return d.index.GetPath(id)
}
func (d *Durable) Search(q Query, limit int) ([]Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ready(); err != nil {
		return nil, err
	}
	return d.index.Search(q, limit)
}
func (d *Durable) ReusablePath(id model.ObjectID, at model.SnapshotAnchor) (ExperiencePath, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ready(); err != nil {
		return ExperiencePath{}, err
	}
	return d.index.ReusablePath(id, at)
}
func (d *Durable) Ready() error { d.mu.Lock(); defer d.mu.Unlock(); return d.ready() }
func (d *Durable) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closing = true
	if d.store != nil {
		if err := d.store.Close(); err != nil {
			return err
		}
	}
	d.closed = true
	return nil
}

func (s *Store) clone() *Store {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := New()
	for id, m := range s.memories {
		out.memories[id] = cloneMemory(m)
	}
	for id, p := range s.paths {
		out.paths[id] = clonePath(p)
	}
	for id, next := range s.successors {
		out.successors[id] = next
	}
	for id, next := range s.pathSuccessors {
		out.pathSuccessors[id] = next
	}
	for id, r := range s.sourceBan {
		out.sourceBan[id] = r
	}
	for token, ids := range s.tokens {
		out.tokens[token] = make(map[model.ObjectID]struct{}, len(ids))
		for id := range ids {
			out.tokens[token][id] = struct{}{}
		}
	}
	return out
}

func encodeDurableCommand(c durableCommand) ([]byte, error) {
	var body []byte
	var err error
	text := func(v string) {
		if err == nil {
			body, err = codec.AppendString(body, v)
		}
	}
	bytesField := func(v []byte) {
		if err == nil {
			body, err = codec.AppendBytes(body, v)
		}
	}
	switch c.op {
	case durablePutCandidate:
		m := cloneMemory(c.memory)
		m.ID = ""
		var raw []byte
		raw, err = Canonical(m)
		bytesField(raw)
	case durableValidate:
		text(string(c.id))
		bytesField(encodeValidation(c.validation))
	case durablePromote:
		text(string(c.id))
	case durableTransition:
		text(string(c.id))
		text(string(c.to))
		text(c.why)
	case durableRetractSource:
		text(string(c.id))
		bytesField(encodeRetraction(c.retraction))
	case durableBuildPath:
		p := clonePath(c.path)
		p.ID = ""
		var raw []byte
		raw, err = CanonicalPath(p)
		bytesField(raw)
	case durableRetractPath:
		text(string(c.id))
		bytesField(encodeRetraction(c.retraction))
	default:
		return nil, storage.ErrUnsupported
	}
	if err != nil {
		return nil, err
	}
	if len(body) > MaxRecordBytes {
		return nil, storage.ErrLimit
	}
	raw := append([]byte(nil), durableMagic...)
	raw = codec.AppendUint(raw, durableSchema)
	raw = codec.AppendUint(raw, uint64(c.op))
	raw, err = codec.AppendBytes(raw, body)
	return raw, err
}

func decodeDurableCommand(raw []byte) (durableCommand, error) {
	if len(raw) > MaxRecordBytes || !bytes.HasPrefix(raw, []byte(durableMagic)) {
		return durableCommand{}, ErrInvalidMemory
	}
	d := codec.NewDecoder(raw[len(durableMagic):])
	version, err := d.Uint()
	if err != nil || version != durableSchema {
		return durableCommand{}, storage.ErrUnsupported
	}
	op, err := d.Uint()
	if err != nil {
		return durableCommand{}, err
	}
	body, err := d.Bytes(MaxRecordBytes)
	if err != nil || d.Done() != nil {
		return durableCommand{}, ErrInvalidMemory
	}
	c := durableCommand{op: durableOperation(op)}
	r := codec.NewDecoder(body)
	text := func() string {
		v, e := r.String(model.MaxIdentityBytes)
		if err == nil {
			err = e
		}
		return v
	}
	blob := func() []byte {
		v, e := r.Bytes(MaxRecordBytes)
		if err == nil {
			err = e
		}
		return bytes.Clone(v)
	}
	switch c.op {
	case durablePutCandidate:
		c.memory, err = DecodeMemory(blob())
	case durableValidate:
		c.id = model.ObjectID(text())
		c.validation, err = decodeValidation(blob())
	case durablePromote:
		c.id = model.ObjectID(text())
	case durableTransition:
		c.id = model.ObjectID(text())
		c.to = Status(text())
		c.why = text()
	case durableRetractSource:
		c.id = model.ObjectID(text())
		c.retraction, err = decodeRetraction(blob())
	case durableBuildPath:
		c.path, err = DecodePath(blob())
	case durableRetractPath:
		c.id = model.ObjectID(text())
		c.retraction, err = decodeRetraction(blob())
	default:
		return durableCommand{}, storage.ErrUnsupported
	}
	if err == nil {
		err = r.Done()
	}
	if err != nil {
		return durableCommand{}, err
	}
	canonical, err := encodeDurableCommand(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return durableCommand{}, ErrInvalidMemory
	}
	return c, nil
}

func encodeValidation(v Validation) []byte {
	return validationBytes(nil, v)
}
func decodeValidation(raw []byte) (Validation, error) {
	r := &recordReader{d: codec.NewDecoder(raw)}
	v := r.validation()
	if r.err == nil {
		r.err = r.d.Done()
	}
	if r.err != nil {
		return Validation{}, r.err
	}
	return v, nil
}
func encodeRetraction(v Retraction) []byte { return retractionBytes(nil, &v) }
func decodeRetraction(raw []byte) (Retraction, error) {
	r := &recordReader{d: codec.NewDecoder(raw)}
	v := r.retraction()
	if r.err == nil {
		r.err = r.d.Done()
	}
	if r.err != nil || v == nil {
		return Retraction{}, ErrInvalidMemory
	}
	return *v, nil
}
