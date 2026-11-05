package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type Digest [sha256.Size]byte

func (d Digest) String() string { return hex.EncodeToString(d[:]) }

type Position struct {
	LSN    uint64
	Digest Digest
}

// Record is an opaque typed body record. Records may interleave object/event/
// state/revision kinds. Object schemas and dependency topology are validated by
// the future semantic replay layer, not by the physical journal parser.
type Record struct {
	Kind    Kind
	Payload []byte
}

type Transaction struct {
	TxID           model.TxID
	Request        model.RequestKey
	RequestDigest  Digest
	LSN            uint64
	PreviousDigest Digest
	Records        []Record
	RefUpdates     []byte
	ControlUpdates []byte
	Receipt        Digest
	PostRootSet    Digest
}

type ScannedTransaction struct {
	Transaction
	// Store is authenticated by Journal recovery; a raw Scanner leaves it empty.
	Store           model.StoreID
	CommitDigest    Digest
	StartOffset     int64
	EndOffset       int64
	RecordLocations []RecordLocation
}

// RecordLocation identifies an opaque body payload in the scanner's underlying
// byte stream (logical addresses when used through a Journal). Entries correspond
// one-for-one with Transaction.Records and do
// not include the begin, metadata, receipt, or commit frames.
type RecordLocation struct {
	PayloadOffset int64
	PayloadLength uint32
}

type commitRecord struct {
	lsn      uint64
	previous Digest
	count    uint64
	frames   Digest
	root     Digest
	receipt  Digest
	request  Digest
}

func appendDigest(b []byte, d Digest) []byte { b, _ = codec.AppendBytes(b, d[:]); return b }

func readDigest(d *codec.Decoder) (Digest, error) {
	var out Digest
	b, err := d.Bytes(sha256.Size)
	if err != nil {
		return out, err
	}
	if len(b) != sha256.Size {
		return out, fmt.Errorf("%w: digest length", ErrMalformed)
	}
	copy(out[:], b)
	return out, nil
}

func encodeBegin(tx Transaction) []byte {
	b := codec.AppendUint(nil, 1)
	for _, s := range []string{string(tx.Request.Space), string(tx.Request.Principal), string(tx.Request.RequestID)} {
		b, _ = codec.AppendString(b, s)
	}
	return appendDigest(b, tx.RequestDigest)
}

func decodeBegin(b []byte) (model.RequestKey, Digest, error) {
	d := codec.NewDecoder(b)
	v, err := d.Uint()
	if err != nil {
		return model.RequestKey{}, Digest{}, err
	}
	if v != 1 {
		return model.RequestKey{}, Digest{}, ErrUnsupported
	}
	fields := [3]string{}
	for i := range fields {
		fields[i], err = d.String(model.MaxIdentityBytes)
		if err != nil {
			return model.RequestKey{}, Digest{}, err
		}
	}
	k := model.RequestKey{Space: model.SpaceID(fields[0]), Principal: model.PrincipalID(fields[1]), RequestID: model.RequestID(fields[2])}
	if err = k.Validate(); err != nil {
		return k, Digest{}, err
	}
	digest, err := readDigest(d)
	if err != nil {
		return k, Digest{}, err
	}
	if err = d.Done(); err != nil {
		return k, Digest{}, err
	}
	if digest == (Digest{}) {
		return k, digest, fmt.Errorf("%w: zero request digest", ErrMalformed)
	}
	return k, digest, nil
}

func encodeCommit(c commitRecord) []byte {
	b := codec.AppendUint(nil, 1)
	b = codec.AppendUint(b, c.lsn)
	b = appendDigest(b, c.previous)
	b = codec.AppendUint(b, c.count)
	for _, d := range []Digest{c.frames, c.root, c.receipt, c.request} {
		b = appendDigest(b, d)
	}
	return b
}

func decodeCommit(b []byte) (commitRecord, error) {
	var c commitRecord
	d := codec.NewDecoder(b)
	v, err := d.Uint()
	if err != nil {
		return c, err
	}
	if v != 1 {
		return c, ErrUnsupported
	}
	if c.lsn, err = d.Uint(); err != nil {
		return c, err
	}
	if c.previous, err = readDigest(d); err != nil {
		return c, err
	}
	if c.count, err = d.Uint(); err != nil {
		return c, err
	}
	for _, out := range []*Digest{&c.frames, &c.root, &c.receipt, &c.request} {
		*out, err = readDigest(d)
		if err != nil {
			return c, err
		}
	}
	return c, d.Done()
}

func commitDigest(txID model.TxID, payload []byte) Digest {
	id, _ := physicalTxID(txID) // callers already verified the transaction ID
	h := sha256.New()
	h.Write([]byte("ATE-TX-COMMIT\x00"))
	h.Write(id[:])
	h.Write(payload)
	var out Digest
	copy(out[:], h.Sum(nil))
	return out
}

func frameHasher() hash.Hash {
	h := sha256.New()
	h.Write([]byte("ATE-TX-FRAMES\x00"))
	return h
}

func hashDigest(h hash.Hash) Digest { var d Digest; copy(d[:], h.Sum(nil)); return d }

// EncodeTransaction creates bytes only. No IO, publication, or durable ACK is
// implied. Limits bound both output size and the total number of physical frames.
func EncodeTransaction(tx Transaction, limits Limits) ([]byte, Digest, error) {
	l, err := limits.checked()
	if err != nil {
		return nil, Digest{}, err
	}
	if _, err = physicalTxID(tx.TxID); err != nil {
		return nil, Digest{}, err
	}
	if err = tx.Request.Validate(); err != nil {
		return nil, Digest{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if tx.LSN == 0 || tx.PreviousDigest == (Digest{}) || tx.RequestDigest == (Digest{}) || tx.PostRootSet == (Digest{}) || tx.Receipt == (Digest{}) {
		return nil, Digest{}, fmt.Errorf("%w: missing transaction metadata", ErrMalformed)
	}
	if uint64(len(tx.Records))+5 > uint64(l.MaxRecords) {
		return nil, Digest{}, ErrLimit
	}
	h := frameHasher()
	var out []byte
	ordinal := uint32(0)
	appendFrame := func(kind Kind, payload []byte, include bool) error {
		if uint64(len(payload)) > uint64(l.MaxPayload) {
			return ErrLimit
		}
		size := uint64(HeaderSize+TrailerSize) + uint64(len(payload)) + uint64(padding(uint32(len(payload))))
		if uint64(len(out))+size > l.MaxTransactionBytes {
			return ErrLimit
		}
		f, err := EncodeFrame(Frame{Kind: kind, TxID: tx.TxID, Ordinal: ordinal, Payload: payload}, l)
		if err != nil {
			return err
		}
		if uint64(len(out))+uint64(len(f)) > l.MaxTransactionBytes {
			return ErrLimit
		}
		out = append(out, f...)
		if include {
			h.Write(f)
		}
		ordinal++
		return nil
	}
	if err = appendFrame(KindBegin, encodeBegin(tx), true); err != nil {
		return nil, Digest{}, err
	}
	for _, record := range tx.Records {
		if !record.Kind.body() {
			return nil, Digest{}, fmt.Errorf("%w: non-body record kind", ErrMalformed)
		}
		if err = appendFrame(record.Kind, record.Payload, true); err != nil {
			return nil, Digest{}, err
		}
	}
	for _, record := range []Record{{KindRefUpdates, tx.RefUpdates}, {KindControlUpdates, tx.ControlUpdates}, {KindCommandReceipt, appendDigest(codec.AppendUint(nil, 1), tx.Receipt)}} {
		if err = appendFrame(record.Kind, record.Payload, true); err != nil {
			return nil, Digest{}, err
		}
	}
	payload := encodeCommit(commitRecord{tx.LSN, tx.PreviousDigest, uint64(ordinal), hashDigest(h), tx.PostRootSet, tx.Receipt, tx.RequestDigest})
	if err = appendFrame(KindCommit, payload, false); err != nil {
		return nil, Digest{}, err
	}
	return out, commitDigest(tx.TxID, payload), nil
}

// ScanError reports the failing physical offset and the start of its enclosing
// transaction. ErrIncomplete is a structural EOF fact, not permission to truncate.
type ScanError struct {
	Offset            int64
	TransactionOffset int64
	Err               error
}

func (e *ScanError) Error() string {
	return fmt.Sprintf("journal at %d (transaction %d): %v", e.Offset, e.TransactionOffset, e.Err)
}
func (e *ScanError) Unwrap() error { return e.Err }

// Scanner validates a sequential, unsegmented byte stream against a caller-
// supplied previous position. It is single-owner, stops permanently on an error,
// and returns complete transactions only. Position is not a publication fence;
// CURRENT/manifest verification belongs to the future recovery coordinator.
type Scanner struct {
	r              io.Reader
	limits         Limits
	position       Position
	offset         int64
	completeOffset int64
	terminal       error
}

func NewScanner(r io.Reader, previous Position, limits Limits) (*Scanner, error) {
	l, err := limits.checked()
	if err != nil {
		return nil, err
	}
	if r == nil || previous.Digest == (Digest{}) {
		return nil, fmt.Errorf("%w: missing reader or starting digest", ErrMalformed)
	}
	return &Scanner{r: r, limits: l, position: previous}, nil
}

func (s *Scanner) Position() Position        { return s.position }
func (s *Scanner) Offset() int64             { return s.offset }
func (s *Scanner) LastCompleteOffset() int64 { return s.completeOffset }

func payloadError(stage string, err error) error {
	if errors.Is(err, ErrUnsupported) {
		return fmt.Errorf("%s: %w", stage, err)
	}
	return fmt.Errorf("%w: %s: %w", ErrMalformed, stage, err)
}

func (s *Scanner) Next() (ScannedTransaction, error) {
	if s.terminal != nil {
		return ScannedTransaction{}, s.terminal
	}
	start := s.offset
	fail := func(at int64, err error) (ScannedTransaction, error) {
		s.terminal = &ScanError{Offset: at, TransactionOffset: start, Err: err}
		return ScannedTransaction{}, s.terminal
	}
	var tx Transaction
	var locations []RecordLocation
	h := frameHasher()
	ordinal := uint32(0)
	phase := KindBegin
	for {
		if ordinal >= s.limits.MaxRecords {
			return fail(s.offset, ErrLimit)
		}
		used := uint64(s.offset - start)
		if used >= s.limits.MaxTransactionBytes {
			return fail(s.offset, ErrLimit)
		}
		at := s.offset
		f, n, err := readFrame(s.r, s.limits, s.limits.MaxTransactionBytes-used)
		s.offset += n
		if err != nil {
			if err == io.EOF {
				if ordinal == 0 {
					s.terminal = io.EOF
					return ScannedTransaction{}, io.EOF
				}
				err = fmt.Errorf("%w: transaction lacks commit: %w", ErrIncomplete, io.ErrUnexpectedEOF)
			}
			return fail(at, err)
		}
		if f.Ordinal != ordinal {
			return fail(at, fmt.Errorf("%w: ordinal expected %d, got %d", ErrMalformed, ordinal, f.Ordinal))
		}
		if ordinal == 0 {
			if f.Kind != KindBegin {
				return fail(at, fmt.Errorf("%w: missing begin", ErrMalformed))
			}
			tx.TxID = f.TxID
			tx.Request, tx.RequestDigest, err = decodeBegin(f.Payload)
			if err != nil {
				return fail(at, payloadError("begin", err))
			}
		} else {
			if f.TxID != tx.TxID {
				return fail(at, fmt.Errorf("%w: transaction ID changed", ErrMalformed))
			}
			switch {
			case f.Kind.body() && phase == KindBegin:
				tx.Records = append(tx.Records, Record{f.Kind, f.Payload})
				locations = append(locations, RecordLocation{PayloadOffset: at + HeaderSize, PayloadLength: uint32(len(f.Payload))})
			case f.Kind == KindRefUpdates && phase == KindBegin:
				tx.RefUpdates = f.Payload
				phase = KindRefUpdates
			case f.Kind == KindControlUpdates && phase == KindRefUpdates:
				tx.ControlUpdates = f.Payload
				phase = KindControlUpdates
			case f.Kind == KindCommandReceipt && phase == KindControlUpdates:
				d := codec.NewDecoder(f.Payload)
				v, err := d.Uint()
				if err != nil {
					return fail(at, fmt.Errorf("%w: receipt: %w", ErrMalformed, err))
				}
				if v != 1 {
					return fail(at, ErrUnsupported)
				}
				tx.Receipt, err = readDigest(d)
				if err != nil {
					return fail(at, fmt.Errorf("%w: receipt: %w", ErrMalformed, err))
				}
				if err = d.Done(); err != nil {
					return fail(at, fmt.Errorf("%w: receipt: %w", ErrMalformed, err))
				}
				phase = KindCommandReceipt
			case f.Kind == KindCommit && phase == KindCommandReceipt:
				c, err := decodeCommit(f.Payload)
				if err != nil {
					return fail(at, payloadError("commit", err))
				}
				if s.position.LSN == math.MaxUint64 || c.lsn != s.position.LSN+1 || c.previous != s.position.Digest {
					return fail(at, fmt.Errorf("%w: transaction chain", ErrIntegrity))
				}
				if c.count != uint64(ordinal) || c.frames != hashDigest(h) || c.receipt != tx.Receipt || c.request != tx.RequestDigest {
					return fail(at, fmt.Errorf("%w: transaction commitment", ErrIntegrity))
				}
				if c.root == (Digest{}) || c.receipt == (Digest{}) {
					return fail(at, fmt.Errorf("%w: missing root or receipt", ErrMalformed))
				}
				tx.LSN = c.lsn
				tx.PreviousDigest = c.previous
				tx.PostRootSet = c.root
				digest := commitDigest(tx.TxID, f.Payload)
				s.position = Position{c.lsn, digest}
				s.completeOffset = s.offset
				return ScannedTransaction{Transaction: tx, CommitDigest: digest, StartOffset: start, EndOffset: s.offset, RecordLocations: locations}, nil
			default:
				return fail(at, fmt.Errorf("%w: frame order at kind %d", ErrMalformed, f.Kind))
			}
		}
		// Re-encoding a validated v1 frame is byte-identical (zero flags/padding).
		// This hashes only this bounded frame, never a second whole transaction.
		encoded, _ := EncodeFrame(f, s.limits)
		h.Write(encoded)
		ordinal++
	}
}
