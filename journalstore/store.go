// Package journalstore adapts a typed record stream to the physical Journal.
// It is experimental infrastructure for trusted embedding code, not ATE core's
// five-root state engine, authorization system, or a model-facing API.
package journalstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var (
	ErrClosed              = errors.New("journalstore is closed")
	ErrClosing             = errors.New("journalstore is closing")
	ErrInvalidRecord       = errors.New("invalid journalstore record")
	ErrIdempotencyConflict = errors.New("journalstore request key has a different record")
	ErrProjectionFailed    = errors.New("journalstore projection failed after publication")
)

// MaxReadRecordsBytes bounds the compatibility whole-log read. Applications
// with larger streams should use RecordsPage instead of materializing the log.
const MaxReadRecordsBytes = 64 << 20
const MaxPageRecords = 256

// V2 separates the logical LSN from the physical generation saved for receipts.
// V1 metadata is rejected rather than reinterpreted or migrated during recovery.
const metadataMagic = "ATE-JOURNALSTORE-2\x00"

type Record struct {
	Kind    storage.Kind
	Payload []byte
	Request model.RequestKey
}

// Apply reconstructs a PRIVATE projection during Open, before tail repair and
// before final prefix authentication. The caller must discard that projection
// if Open fails, and must not disclose it from the callback. Append calls Apply
// only after publication; an error then fences the adapter. Use Prepare for
// semantic validation before writing. Callbacks must not reenter the Store.
type Apply func(Record) error

// Prepare checks a record against the current projection and prepares privately
// owned replacement state without changing the visible projection. Its install
// closure MUST NOT fail, panic, perform IO, or retain mutable aliases owned by a
// caller. Append runs install after P1; replay runs it on the private candidate
// to validate the next record, and only successful Open may expose that state.
// Prepare receives an owned copy and must not reenter Store methods.
type Prepare func(Record) (install func(), err error)

type receipt struct {
	digest storage.Digest
	result storage.PublishedTransaction
}

type recordEntry struct {
	kind     storage.Kind
	request  model.RequestKey
	location storage.RecordLocation
	digest   storage.Digest
}

type Store struct {
	mu       sync.Mutex
	journal  *storage.Journal
	apply    Apply
	prepare  Prepare
	records  []recordEntry
	receipts map[model.RequestKey]receipt
	closing  bool
	closed   bool
	fenced   error
}

type Report = storage.RecoveryReport

func Create(d storage.DurableIO, o storage.JournalOptions, apply Apply) (*Store, error) {
	return create(d, o, apply, nil)
}

func CreatePrepared(d storage.DurableIO, o storage.JournalOptions, prepare Prepare) (*Store, error) {
	if prepare == nil {
		return nil, fmt.Errorf("%w: nil prepare", ErrInvalidRecord)
	}
	return create(d, o, nil, prepare)
}

func create(d storage.DurableIO, o storage.JournalOptions, apply Apply, prepare Prepare) (*Store, error) {
	j, err := storage.CreateJournal(d, o)
	if err != nil {
		return nil, err
	}
	return &Store{journal: j, apply: apply, prepare: prepare, receipts: make(map[model.RequestKey]receipt)}, nil
}

func Open(d storage.DurableIO, o storage.JournalOptions, apply Apply) (*Store, Report, error) {
	return open(d, o, apply, nil)
}

func OpenPrepared(d storage.DurableIO, o storage.JournalOptions, prepare Prepare) (*Store, Report, error) {
	if prepare == nil {
		return nil, Report{}, fmt.Errorf("%w: nil prepare", ErrInvalidRecord)
	}
	return open(d, o, nil, prepare)
}

func open(d storage.DurableIO, o storage.JournalOptions, apply Apply, prepare Prepare) (*Store, Report, error) {
	s := &Store{apply: apply, prepare: prepare, receipts: make(map[model.RequestKey]receipt)}
	var generation uint64
	j, report, err := storage.OpenJournalWithReplay(d, o, func(tx storage.ScannedTransaction, _ io.ReaderAt) error {
		// Journal owns all cleanup on a callback failure. Domain validation occurs
		// here, before any quarantine, truncate, remove, or witness synchronization.
		if len(tx.Records) != 1 || len(tx.RecordLocations) != 1 {
			return fmt.Errorf("%w: expected one body record", ErrInvalidRecord)
		}
		r := Record{Kind: tx.Records[0].Kind, Payload: tx.Records[0].Payload, Request: tx.Request}
		if err := validate(r); err != nil {
			return err
		}
		next, err := decodeMetadata(tx.RefUpdates)
		if err != nil {
			return err
		}
		// Journal verifies contiguous LSNs. Layout-only publications can leave
		// gaps in Generation, which is retained solely for the original receipt.
		if next.lsn != tx.LSN || next.generation <= generation || len(tx.ControlUpdates) != 0 {
			return fmt.Errorf("%w: publication metadata", ErrInvalidRecord)
		}
		rd := requestDigest(r)
		if rd != tx.RequestDigest || recordDigest(r) != tx.PostRootSet || receiptDigest(r) != tx.Receipt {
			return fmt.Errorf("%w: record commitments", ErrInvalidRecord)
		}
		if _, exists := s.receipts[r.Request]; exists {
			return fmt.Errorf("%w: repeated durable request key", ErrInvalidRecord)
		}
		if tx.RecordLocations[0].PayloadLength != uint32(len(r.Payload)) {
			return fmt.Errorf("%w: payload location", ErrInvalidRecord)
		}
		if err := s.project(r); err != nil {
			return err
		}
		generation = next.generation
		result := storage.PublishedTransaction{Publication: storage.Publication{Store: tx.Store, Generation: generation, Position: storage.Position{LSN: tx.LSN, Digest: tx.CommitDigest}, PostRootSet: tx.PostRootSet, PublishedEnd: uint64(tx.EndOffset), Profile: d.Profile()}, RecordLocations: append([]storage.RecordLocation(nil), tx.RecordLocations...)}
		s.receipts[r.Request] = receipt{digest: rd, result: result}
		s.records = append(s.records, recordEntry{kind: r.Kind, request: r.Request, location: tx.RecordLocations[0], digest: rd})
		return nil
	})
	if err != nil {
		return nil, report, err
	}
	s.journal = j
	return s, report, nil
}

func validate(r Record) error {
	if err := r.Request.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	if r.Kind < storage.KindObject || r.Kind > storage.KindRevision {
		return fmt.Errorf("%w: body kind", ErrInvalidRecord)
	}
	if len(r.Payload) == 0 {
		return fmt.Errorf("%w: empty payload", ErrInvalidRecord)
	}
	if len(r.Payload) > storage.MaxPayload {
		return fmt.Errorf("%w: payload", storage.ErrLimit)
	}
	return nil
}

func cloneRecord(r Record) Record { r.Payload = append([]byte(nil), r.Payload...); return r }
func cloneResult(r storage.PublishedTransaction) storage.PublishedTransaction {
	r.RecordLocations = append([]storage.RecordLocation(nil), r.RecordLocations...)
	return r
}

// Both identity and kind are bound, so reuse of the same body in another
// method-kind/space/principal/request cannot alias a receipt.
func hashRecord(domain string, r Record, includeKey bool) storage.Digest {
	h := sha256.New()
	h.Write([]byte(domain))
	integer := func(n uint64) { var b [10]byte; count := binary.PutUvarint(b[:], n); h.Write(b[:count]) }
	field := func(b []byte) { integer(uint64(len(b))); h.Write(b) }
	if includeKey {
		for _, s := range []string{string(r.Request.Space), string(r.Request.Principal), string(r.Request.RequestID)} {
			field([]byte(s))
		}
	}
	integer(uint64(r.Kind))
	field(r.Payload)
	return digestHash(h)
}
func requestDigest(r Record) storage.Digest {
	return hashRecord("ATE-JOURNALSTORE-REQUEST-1\x00", r, true)
}
func recordDigest(r Record) storage.Digest {
	return hashRecord("ATE-JOURNALSTORE-RECORD-1\x00", r, false)
}
func receiptDigest(r Record) storage.Digest {
	h := sha256.New()
	h.Write([]byte("ATE-JOURNALSTORE-RECEIPT-1\x00"))
	rq, body := requestDigest(r), recordDigest(r)
	h.Write(rq[:])
	h.Write(body[:])
	return digestHash(h)
}
func digestHash(h hash.Hash) storage.Digest {
	var out storage.Digest
	copy(out[:], h.Sum(nil))
	return out
}

type publicationMetadata struct {
	lsn        uint64
	generation uint64
}

func encodeMetadata(lsn, generation uint64) []byte {
	b := make([]byte, len(metadataMagic)+16)
	copy(b, metadataMagic)
	binary.BigEndian.PutUint64(b[len(metadataMagic):], lsn)
	binary.BigEndian.PutUint64(b[len(metadataMagic)+8:], generation)
	return b
}
func decodeMetadata(b []byte) (publicationMetadata, error) {
	if len(b) != len(metadataMagic)+16 || string(b[:len(metadataMagic)]) != metadataMagic {
		return publicationMetadata{}, fmt.Errorf("%w: journalstore transaction schema", storage.ErrUnsupported)
	}
	m := publicationMetadata{
		lsn:        binary.BigEndian.Uint64(b[len(metadataMagic):]),
		generation: binary.BigEndian.Uint64(b[len(metadataMagic)+8:]),
	}
	if m.lsn == 0 || m.generation < m.lsn {
		return publicationMetadata{}, fmt.Errorf("%w: publication metadata", ErrInvalidRecord)
	}
	return m, nil
}

func nextMetadata(p storage.Publication) ([]byte, error) {
	// Each append consumes one logical LSN and one physical publication.
	// Check both before addition; compaction can exhaust Generation first.
	if p.Position.LSN == ^uint64(0) || p.Generation == ^uint64(0) {
		return nil, storage.ErrLimit
	}
	return encodeMetadata(p.Position.LSN+1, p.Generation+1), nil
}

func (s *Store) ready() error {
	if s.closed || s.journal == nil {
		return ErrClosed
	}
	if s.closing {
		return ErrClosing
	}
	if s.fenced != nil {
		return fmt.Errorf("%w: %w", storage.ErrStoreFenced, s.fenced)
	}
	_, err := s.journal.Published()
	return err
}

// Ready is a read-output gate for adapters that expose their own projection.
func (s *Store) Ready() error { s.mu.Lock(); defer s.mu.Unlock(); return s.ready() }

func (s *Store) project(r Record) error {
	if s.prepare != nil {
		install, err := prepareProjection(s.prepare, cloneRecord(r))
		if err != nil {
			return err
		}
		if install != nil {
			return installProjection(install)
		}
		return nil
	}
	if s.apply != nil {
		return applyProjection(s.apply, cloneRecord(r))
	}
	return nil
}

func prepareProjection(prepare Prepare, r Record) (install func(), err error) {
	defer func() {
		if value := recover(); value != nil {
			install = nil
			err = fmt.Errorf("%w: prepare panicked: %v", ErrProjectionFailed, value)
		}
	}()
	return prepare(r)
}

func applyProjection(apply Apply, r Record) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: apply panicked: %v", ErrProjectionFailed, value)
		}
	}()
	return apply(r)
}

func installProjection(install func()) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: install panicked: %v", ErrProjectionFailed, value)
		}
	}()
	install()
	return nil
}

func (s *Store) Append(ctx context.Context, r Record) (storage.PublishedTransaction, error) {
	if err := ctx.Err(); err != nil {
		return storage.PublishedTransaction{}, err
	}
	if err := validate(r); err != nil {
		return storage.PublishedTransaction{}, err
	}
	r = cloneRecord(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return storage.PublishedTransaction{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.PublishedTransaction{}, err
	}
	rd := requestDigest(r)
	if old, found := s.receipts[r.Request]; found {
		if old.digest != rd {
			return storage.PublishedTransaction{}, ErrIdempotencyConflict
		}
		return cloneResult(old.result), nil
	}
	var install func()
	if s.prepare != nil {
		var err error
		install, err = prepareProjection(s.prepare, cloneRecord(r))
		if err != nil {
			return storage.PublishedTransaction{}, err
		}
	}
	p, err := s.journal.Published()
	if err != nil {
		return storage.PublishedTransaction{}, err
	}
	metadata, err := nextMetadata(p)
	if err != nil {
		return storage.PublishedTransaction{}, err
	}
	var raw [16]byte
	if _, err = rand.Read(raw[:]); err != nil {
		return storage.PublishedTransaction{}, err
	}
	tx := storage.Transaction{
		TxID:           model.TxID(hex.EncodeToString(raw[:])),
		Request:        r.Request,
		RequestDigest:  rd,
		LSN:            p.Position.LSN + 1,
		PreviousDigest: p.Position.Digest,
		Records: []storage.Record{{
			Kind:    r.Kind,
			Payload: r.Payload,
		}},
		RefUpdates:  metadata,
		Receipt:     receiptDigest(r),
		PostRootSet: recordDigest(r),
	}
	// PostRootSet is the typed record commitment of this experimental adapter,
	// not a five-root ATE core state object. Core Engine must not open this log.
	published, err := s.journal.PublishWithRecords(ctx, tx)
	if err != nil {
		return storage.PublishedTransaction{}, err
	}
	if len(published.RecordLocations) != 1 || published.RecordLocations[0].PayloadLength != uint32(len(r.Payload)) {
		cause := fmt.Errorf("%w: publication record location", ErrProjectionFailed)
		s.fenced = cause
		return storage.PublishedTransaction{}, errors.Join(storage.ErrCommitUnknown, s.journal.Fence(cause))
	}
	if install != nil {
		err = installProjection(install)
	} else if s.prepare == nil && s.apply != nil {
		err = applyProjection(s.apply, cloneRecord(r))
	}
	if err != nil {
		cause := errors.Join(ErrProjectionFailed, err)
		s.fenced = cause
		fenced := s.journal.Fence(cause)
		return storage.PublishedTransaction{}, errors.Join(storage.ErrCommitUnknown, fenced)
	}
	s.receipts[r.Request] = receipt{digest: rd, result: cloneResult(published)}
	s.records = append(s.records, recordEntry{kind: r.Kind, request: r.Request, location: published.RecordLocations[0], digest: rd})
	return cloneResult(published), nil
}

// Lookup returns only a verified durable receipt; it is unavailable when the
// adapter is closing/fenced. A false result proves absence at this ready prefix.
func (s *Store) Lookup(key model.RequestKey) (storage.PublishedTransaction, bool, error) {
	if err := key.Validate(); err != nil {
		return storage.PublishedTransaction{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return storage.PublishedTransaction{}, false, err
	}
	old, ok := s.receipts[key]
	if !ok {
		return storage.PublishedTransaction{}, false, nil
	}
	return cloneResult(old.result), true, nil
}

// Records is the compatibility whole-log read. Failure returns no records;
// callers needing an error or a bounded page should use ReadRecords/RecordsPage.
func (s *Store) Records() []Record { records, _ := s.ReadRecords(); return records }
func (s *Store) ReadRecords() ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	var size uint64
	for _, entry := range s.records {
		size += uint64(entry.location.PayloadLength)
		if size > MaxReadRecordsBytes {
			return nil, storage.ErrLimit
		}
	}
	return s.readEntries(context.Background(), s.records)
}

// RecordsPage uses a zero-based record offset, and returns the next offset.
// A page is bounded by MaxPageRecords and MaxReadRecordsBytes.
func (s *Store) RecordsPage(ctx context.Context, after uint64, limit int) ([]Record, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, after, err
	}
	if limit < 1 || limit > MaxPageRecords {
		return nil, after, storage.ErrLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return nil, after, err
	}
	if after > uint64(len(s.records)) {
		return nil, after, ErrInvalidRecord
	}
	end := after
	var size uint64
	for end < uint64(len(s.records)) && end-after < uint64(limit) {
		n := uint64(s.records[end].location.PayloadLength)
		if n > MaxReadRecordsBytes-size {
			break
		}
		size += n
		end++
	}
	records, err := s.readEntries(ctx, s.records[after:end])
	if err != nil {
		return nil, after, err
	}
	return records, end, nil
}
func (s *Store) readEntries(ctx context.Context, entries []recordEntry) ([]Record, error) {
	out := make([]Record, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r := Record{Kind: entry.kind, Request: entry.request, Payload: make([]byte, int(entry.location.PayloadLength))}
		n, err := s.journal.ReadAt(r.Payload, entry.location.PayloadOffset)
		if n == len(r.Payload) && err == io.EOF {
			err = nil
		}
		if err == nil && n != len(r.Payload) {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				err = errors.Join(storage.ErrCorruption, err)
			}
			return nil, s.fenceRead(err)
		}
		if requestDigest(r) != entry.digest {
			return nil, s.fenceRead(fmt.Errorf("%w: stored record identity", storage.ErrCorruption))
		}
		out = append(out, r)
	}
	return out, nil
}
func (s *Store) fenceRead(cause error) error { s.fenced = cause; return s.journal.Fence(cause) }

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
