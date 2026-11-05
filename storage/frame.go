// Package storage implements experimental journal codecs, segmented P1
// publication, and physical recovery under an explicit synchronization profile.
// Domain state replay and authorization are not implemented here. Raw scanner
// success alone does not prove publication; OpenJournal verifies CURRENT.
package storage

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	FrameVersion        uint16 = 1
	HeaderSize                 = 48
	TrailerSize                = 8
	MaxPayload                 = 16 << 20
	MaxTransactionBytes        = 64 << 20
	MaxRecords                 = 65536
)

const frameMagic = "ATEFRM01"
const endMagic = "END!"

type Kind uint16

const (
	KindBegin          Kind = 1
	KindObject         Kind = 2
	KindEvent          Kind = 3
	KindStateOps       Kind = 4
	KindRevision       Kind = 5
	KindRefUpdates     Kind = 6
	KindControlUpdates Kind = 7
	KindCommandReceipt Kind = 8
	KindCommit         Kind = 9
)

func (k Kind) valid() bool { return k >= KindBegin && k <= KindCommit }
func (k Kind) body() bool  { return k >= KindObject && k <= KindRevision }

var (
	ErrIncomplete  = errors.New("incomplete journal input")
	ErrIntegrity   = errors.New("journal integrity check failed")
	ErrMalformed   = errors.New("malformed journal input")
	ErrUnsupported = errors.New("unsupported journal format")
	ErrLimit       = errors.New("journal resource limit exceeded")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Limits may lower the built-in safety ceilings. Zero fields use defaults;
// exceeding a ceiling is rejected, never interpreted as unlimited.
type Limits struct {
	MaxPayload          uint32
	MaxTransactionBytes uint64
	MaxRecords          uint32
}

func DefaultLimits() Limits { return Limits{MaxPayload, MaxTransactionBytes, MaxRecords} }

func (l Limits) checked() (Limits, error) {
	if l.MaxPayload == 0 {
		l.MaxPayload = MaxPayload
	}
	if l.MaxTransactionBytes == 0 {
		l.MaxTransactionBytes = MaxTransactionBytes
	}
	if l.MaxRecords == 0 {
		l.MaxRecords = MaxRecords
	}
	if l.MaxPayload > MaxPayload || l.MaxTransactionBytes > MaxTransactionBytes || l.MaxRecords > MaxRecords {
		return l, ErrLimit
	}
	return l, nil
}

type Frame struct {
	Kind    Kind
	TxID    model.TxID
	Ordinal uint32
	Payload []byte
}

func physicalTxID(id model.TxID) ([16]byte, error) {
	var out [16]byte
	if len(id) != 32 {
		return out, fmt.Errorf("%w: transaction ID", ErrMalformed)
	}
	b, err := hex.DecodeString(string(id))
	if err != nil || hex.EncodeToString(b) != string(id) {
		return out, fmt.Errorf("%w: transaction ID", ErrMalformed)
	}
	copy(out[:], b)
	if out == ([16]byte{}) {
		return out, fmt.Errorf("%w: zero transaction ID", ErrMalformed)
	}
	return out, nil
}

func padding(n uint32) int { return int((8 - n%8) % 8) }

func EncodeFrame(f Frame, limits Limits) ([]byte, error) {
	l, err := limits.checked()
	if err != nil {
		return nil, err
	}
	if !f.Kind.valid() {
		return nil, fmt.Errorf("%w: frame kind %d", ErrUnsupported, f.Kind)
	}
	id, err := physicalTxID(f.TxID)
	if err != nil {
		return nil, err
	}
	if uint64(len(f.Payload)) > uint64(l.MaxPayload) {
		return nil, ErrLimit
	}
	n := uint32(len(f.Payload))
	end := HeaderSize + len(f.Payload) + padding(n)
	b := make([]byte, end+TrailerSize)
	copy(b[:8], frameMagic)
	binary.LittleEndian.PutUint16(b[8:10], FrameVersion)
	binary.LittleEndian.PutUint16(b[10:12], uint16(f.Kind))
	// Flags [12:16] are zero: v0 has no optional/mandatory flag extensions.
	binary.LittleEndian.PutUint32(b[16:20], HeaderSize)
	binary.LittleEndian.PutUint32(b[20:24], n)
	copy(b[24:40], id[:])
	binary.LittleEndian.PutUint32(b[40:44], f.Ordinal)
	binary.LittleEndian.PutUint32(b[44:48], crc32.Checksum(b[:44], crcTable))
	copy(b[HeaderSize:], f.Payload)
	binary.LittleEndian.PutUint32(b[end:end+4], crc32.Checksum(b[:end], crcTable))
	copy(b[end+4:], endMagic)
	return b, nil
}

// ReadFrame distinguishes clean EOF, incomplete EOF, invalid structure and
// checksum failure. It never decides a tail is safe to discard. Returned bytes
// consumed include an invalid/partial header; payload is independently owned.
func ReadFrame(r io.Reader, limits Limits) (Frame, int64, error) {
	l, err := limits.checked()
	if err != nil {
		return Frame{}, 0, err
	}
	return readFrame(r, l, ^uint64(0))
}

func readFrame(r io.Reader, l Limits, remaining uint64) (Frame, int64, error) {
	if remaining < HeaderSize {
		return Frame{}, 0, ErrLimit
	}
	var header [HeaderSize]byte
	n, err := io.ReadFull(r, header[:])
	if err != nil {
		if n == 0 && err == io.EOF {
			return Frame{}, 0, io.EOF
		}
		return Frame{}, int64(n), inputError("header", err)
	}
	fail := func(err error) (Frame, int64, error) { return Frame{}, HeaderSize, err }
	if string(header[:8]) != frameMagic {
		return fail(fmt.Errorf("%w: frame magic", ErrMalformed))
	}
	if crc32.Checksum(header[:44], crcTable) != binary.LittleEndian.Uint32(header[44:]) {
		return fail(fmt.Errorf("%w: header CRC32C", ErrIntegrity))
	}
	if binary.LittleEndian.Uint16(header[8:10]) != FrameVersion {
		return fail(fmt.Errorf("%w: version", ErrUnsupported))
	}
	kind := Kind(binary.LittleEndian.Uint16(header[10:12]))
	if !kind.valid() {
		return fail(fmt.Errorf("%w: kind", ErrUnsupported))
	}
	if binary.LittleEndian.Uint32(header[12:16]) != 0 {
		return fail(fmt.Errorf("%w: flags", ErrUnsupported))
	}
	if binary.LittleEndian.Uint32(header[16:20]) != HeaderSize {
		return fail(fmt.Errorf("%w: header size", ErrMalformed))
	}
	var id [16]byte
	copy(id[:], header[24:40])
	if id == ([16]byte{}) {
		return fail(fmt.Errorf("%w: zero transaction ID", ErrMalformed))
	}
	length := binary.LittleEndian.Uint32(header[20:24])
	total := uint64(HeaderSize+TrailerSize) + uint64(length) + uint64(padding(length))
	// All bounds precede payload allocation, including remaining TX budget.
	if length > l.MaxPayload || total > remaining {
		return fail(ErrLimit)
	}
	tail := make([]byte, int(length)+padding(length)+TrailerSize)
	n, err = io.ReadFull(r, tail)
	consumed := int64(HeaderSize + n)
	if err != nil {
		return Frame{}, consumed, inputError("payload/trailer", err)
	}
	end := len(tail) - TrailerSize
	if string(tail[end+4:]) != endMagic {
		return Frame{}, consumed, fmt.Errorf("%w: end magic", ErrMalformed)
	}
	crc := crc32.Update(0, crcTable, header[:])
	crc = crc32.Update(crc, crcTable, tail[:end])
	if crc != binary.LittleEndian.Uint32(tail[end:end+4]) {
		return Frame{}, consumed, fmt.Errorf("%w: record CRC32C", ErrIntegrity)
	}
	for _, b := range tail[int(length):end] {
		if b != 0 {
			return Frame{}, consumed, fmt.Errorf("%w: nonzero padding", ErrMalformed)
		}
	}
	return Frame{Kind: kind, TxID: model.TxID(hex.EncodeToString(id[:])), Ordinal: binary.LittleEndian.Uint32(header[40:44]), Payload: tail[:int(length):int(length)]}, consumed, nil
}

func inputError(stage string, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %s: %w", ErrIncomplete, stage, io.ErrUnexpectedEOF)
	}
	return fmt.Errorf("read %s: %w", stage, err)
}
