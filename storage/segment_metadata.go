package storage

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	SuccessorHeaderSize = 160
	SegmentFooterSize   = 160
	MaxSegments         = 1024
	MaxManifestBytes    = 1 << 20
	successorMagic      = "ATESEG02"
	footerMagic         = "ATESEAL1"
)

// SegmentDescriptor authenticates one immutable prefix in an ordered layout.
// DataEnd excludes the footer; LogicalStart/End exclude all layout overhead.
// Start/EndPosition are the last complete transactions at these boundaries.
// Both may be equal when one transaction continues through the entire segment.
type SegmentDescriptor struct {
	SegmentID     string
	HeaderSize    uint64
	HeaderDigest  Digest
	Sealed        bool
	DataEnd       uint64
	PrefixDigest  Digest
	SealedDigest  Digest
	LogicalStart  uint64
	LogicalEnd    uint64
	StartPosition Position
	EndPosition   Position
}

// SuccessorHeader binds the new segment to a fully sealed physical predecessor.
// Position identifies the last complete transaction, not an in-progress one.
type SuccessorHeader struct {
	Store                 model.StoreID
	SegmentID             string
	LogicalStart          uint64
	PreviousSegmentID     string
	PreviousSegmentDigest Digest
	Position              Position
}

// SegmentFooter seals the authenticated data prefix and complete-tx position.
// The physical predecessor digest includes this footer and is not a tx digest.
type SegmentFooter struct {
	Store        model.StoreID
	SegmentID    string
	DataEnd      uint64
	PrefixDigest Digest
	LogicalEnd   uint64
	Position     Position
}

func (m Manifest) validateSegments() error {
	if len(m.Segments) < 2 {
		return fmt.Errorf("%w: segmented manifest requires at least two segments", ErrMalformed)
	}
	if len(m.Segments) > MaxSegments {
		return ErrLimit
	}
	genesis, err := GenesisPosition(Identity{Store: m.Store, Profile: FileSyncExperimental})
	if err != nil {
		return err
	}
	previous := genesis
	logical := uint64(SegmentHeaderSize)
	seen := make(map[string]struct{}, len(m.Segments))
	for i, s := range m.Segments {
		if _, err := metadataID(s.SegmentID); err != nil {
			return err
		}
		if _, ok := seen[s.SegmentID]; ok {
			return fmt.Errorf("%w: duplicate segment ID", ErrMalformed)
		}
		seen[s.SegmentID] = struct{}{}
		header := uint64(SuccessorHeaderSize)
		if i == 0 {
			header = SegmentHeaderSize
		}
		sealed := i < len(m.Segments)-1
		if s.HeaderSize != header || s.HeaderDigest == (Digest{}) || s.PrefixDigest == (Digest{}) || s.Sealed != sealed || (s.SealedDigest != (Digest{})) != sealed || s.DataEnd < s.HeaderSize || s.DataEnd > math.MaxInt64-SegmentFooterSize || (sealed && s.DataEnd == s.HeaderSize) || s.LogicalStart != logical || s.LogicalEnd < s.LogicalStart || s.LogicalEnd-s.LogicalStart != s.DataEnd-s.HeaderSize || s.LogicalEnd > math.MaxInt64 || s.StartPosition != previous || s.EndPosition.Digest == (Digest{}) || s.EndPosition.LSN < s.StartPosition.LSN || (s.EndPosition.LSN == s.StartPosition.LSN && s.EndPosition != s.StartPosition) {
			return fmt.Errorf("%w: segment descriptor %d", ErrMalformed, i)
		}
		previous = s.EndPosition
		logical = s.LogicalEnd
	}
	tip := m.Segments[len(m.Segments)-1]
	if logical != m.PublishedEnd || previous != m.Position || tip.SegmentID != m.SegmentID || tip.HeaderDigest != m.SegmentHeaderDigest || tip.PrefixDigest != m.PrefixDigest {
		return fmt.Errorf("%w: manifest active segment/tip binding", ErrMalformed)
	}
	return nil
}

func appendSegmentDescriptors(b []byte, segments []SegmentDescriptor) []byte {
	b = codec.AppendUint(b, uint64(len(segments)))
	for _, s := range segments {
		b = metadataAppendID(b, s.SegmentID)
		b = codec.AppendUint(b, s.HeaderSize)
		b = appendDigest(b, s.HeaderDigest)
		flag := uint64(0)
		if s.Sealed {
			flag = 1
		}
		b = codec.AppendUint(b, flag)
		b = codec.AppendUint(b, s.DataEnd)
		b = appendDigest(b, s.PrefixDigest)
		b = appendDigest(b, s.SealedDigest)
		b = codec.AppendUint(b, s.LogicalStart)
		b = codec.AppendUint(b, s.LogicalEnd)
		b = codec.AppendUint(b, s.StartPosition.LSN)
		b = appendDigest(b, s.StartPosition.Digest)
		b = codec.AppendUint(b, s.EndPosition.LSN)
		b = appendDigest(b, s.EndPosition.Digest)
	}
	return b
}

func readSegmentDescriptors(d *codec.Decoder) ([]SegmentDescriptor, error) {
	out, err := readSegmentDescriptorsOptional(d, MaxSegments)
	if err != nil {
		return nil, err
	}
	if len(out) < 2 {
		return nil, fmt.Errorf("%w: segment count", ErrMalformed)
	}
	return out, nil
}

func readSegmentDescriptorsOptional(d *codec.Decoder, maximum uint64) ([]SegmentDescriptor, error) {
	count, err := d.Uint()
	if err != nil {
		return nil, metadataError(err)
	}
	if count > maximum {
		return nil, ErrLimit
	}
	if count == 0 {
		return nil, nil
	}
	// Check a conservative minimum encoded length before allocating the catalog.
	// This is also useful for tiny adversarial inputs declaring a large count.
	const minDescriptorBytes = 16 + 1 + 1 + 33 + 1 + 1 + 33 + 33 + 1 + 1 + 1 + 33 + 1 + 33
	if uint64(d.Remaining()) < count*minDescriptorBytes {
		return nil, metadataError(codec.ErrTruncated)
	}
	out := make([]SegmentDescriptor, int(count))
	for i := range out {
		s := &out[i]
		if s.SegmentID, err = metadataReadID(d); err != nil {
			return nil, metadataError(err)
		}
		if s.HeaderSize, err = d.Uint(); err != nil {
			return nil, metadataError(err)
		}
		if s.HeaderDigest, err = readDigest(d); err != nil {
			return nil, metadataError(err)
		}
		flag, e := d.Uint()
		if e != nil {
			return nil, metadataError(e)
		}
		if flag > 1 {
			return nil, fmt.Errorf("%w: segment state", ErrUnsupported)
		}
		s.Sealed = flag == 1
		if s.DataEnd, err = d.Uint(); err != nil {
			return nil, metadataError(err)
		}
		if s.PrefixDigest, err = readDigest(d); err != nil {
			return nil, metadataError(err)
		}
		if s.SealedDigest, err = readDigest(d); err != nil {
			return nil, metadataError(err)
		}
		if s.LogicalStart, err = d.Uint(); err != nil {
			return nil, metadataError(err)
		}
		if s.LogicalEnd, err = d.Uint(); err != nil {
			return nil, metadataError(err)
		}
		if s.StartPosition.LSN, err = d.Uint(); err != nil {
			return nil, metadataError(err)
		}
		if s.StartPosition.Digest, err = readDigest(d); err != nil {
			return nil, metadataError(err)
		}
		if s.EndPosition.LSN, err = d.Uint(); err != nil {
			return nil, metadataError(err)
		}
		if s.EndPosition.Digest, err = readDigest(d); err != nil {
			return nil, metadataError(err)
		}
	}
	return out, nil
}

func validateSegmentIdentity(store model.StoreID, id string) error {
	if _, err := metadataID(string(store)); err != nil {
		return err
	}
	_, err := metadataID(id)
	return err
}

func (h SuccessorHeader) validate() error {
	if err := validateSegmentIdentity(h.Store, h.SegmentID); err != nil {
		return err
	}
	if _, err := metadataID(h.PreviousSegmentID); err != nil {
		return err
	}
	if h.SegmentID == h.PreviousSegmentID || h.PreviousSegmentDigest == (Digest{}) || h.Position.Digest == (Digest{}) || h.Position.LSN == math.MaxUint64 || h.LogicalStart <= SegmentHeaderSize || h.LogicalStart > math.MaxInt64 {
		return fmt.Errorf("%w: successor header", ErrMalformed)
	}
	return nil
}

func EncodeSuccessorHeader(h SuccessorHeader) ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	b := fixedSegmentEnvelope(successorMagic, 2, h.Store, h.SegmentID)
	binary.LittleEndian.PutUint64(b[48:56], h.LogicalStart)
	prev, _ := metadataID(h.PreviousSegmentID)
	copy(b[56:72], prev[:])
	copy(b[72:104], h.PreviousSegmentDigest[:])
	binary.LittleEndian.PutUint64(b[104:112], h.Position.LSN)
	copy(b[112:144], h.Position.Digest[:])
	binary.LittleEndian.PutUint32(b[156:160], crc32.Checksum(b[:156], crcTable))
	return b, nil
}

func DecodeSuccessorHeader(b []byte) (SuccessorHeader, error) {
	var h SuccessorHeader
	if err := checkFixedSegmentEnvelope(b, successorMagic, 2, 144); err != nil {
		return h, err
	}
	h.Store = model.StoreID(hex.EncodeToString(b[16:32]))
	h.SegmentID = hex.EncodeToString(b[32:48])
	h.LogicalStart = binary.LittleEndian.Uint64(b[48:56])
	h.PreviousSegmentID = hex.EncodeToString(b[56:72])
	copy(h.PreviousSegmentDigest[:], b[72:104])
	h.Position.LSN = binary.LittleEndian.Uint64(b[104:112])
	copy(h.Position.Digest[:], b[112:144])
	if err := h.validate(); err != nil {
		return SuccessorHeader{}, err
	}
	return h, nil
}

func (f SegmentFooter) validate() error {
	if err := validateSegmentIdentity(f.Store, f.SegmentID); err != nil {
		return err
	}
	if f.DataEnd <= SegmentHeaderSize || f.DataEnd > math.MaxInt64-SegmentFooterSize || f.PrefixDigest == (Digest{}) || f.LogicalEnd <= SegmentHeaderSize || f.LogicalEnd > math.MaxInt64 || f.Position.Digest == (Digest{}) {
		return fmt.Errorf("%w: segment footer", ErrMalformed)
	}
	return nil
}

func EncodeSegmentFooter(f SegmentFooter) ([]byte, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	b := fixedSegmentEnvelope(footerMagic, 1, f.Store, f.SegmentID)
	binary.LittleEndian.PutUint64(b[48:56], f.DataEnd)
	copy(b[56:88], f.PrefixDigest[:])
	binary.LittleEndian.PutUint64(b[88:96], f.LogicalEnd)
	binary.LittleEndian.PutUint64(b[96:104], f.Position.LSN)
	copy(b[104:136], f.Position.Digest[:])
	binary.LittleEndian.PutUint32(b[156:160], crc32.Checksum(b[:156], crcTable))
	return b, nil
}

func DecodeSegmentFooter(b []byte) (SegmentFooter, error) {
	var f SegmentFooter
	if err := checkFixedSegmentEnvelope(b, footerMagic, 1, 136); err != nil {
		return f, err
	}
	f.Store = model.StoreID(hex.EncodeToString(b[16:32]))
	f.SegmentID = hex.EncodeToString(b[32:48])
	f.DataEnd = binary.LittleEndian.Uint64(b[48:56])
	copy(f.PrefixDigest[:], b[56:88])
	f.LogicalEnd = binary.LittleEndian.Uint64(b[88:96])
	f.Position.LSN = binary.LittleEndian.Uint64(b[96:104])
	copy(f.Position.Digest[:], b[104:136])
	if err := f.validate(); err != nil {
		return SegmentFooter{}, err
	}
	return f, nil
}

func fixedSegmentEnvelope(magic string, schema uint16, store model.StoreID, id string) []byte {
	b := make([]byte, SuccessorHeaderSize)
	copy(b[:8], magic)
	binary.LittleEndian.PutUint16(b[8:10], schema)
	s, _ := metadataID(string(store))
	copy(b[16:32], s[:])
	i, _ := metadataID(id)
	copy(b[32:48], i[:])
	return b
}

func checkFixedSegmentEnvelope(b []byte, magic string, schema uint16, reservedStart int) error {
	if len(b) != SuccessorHeaderSize {
		return fmt.Errorf("%w: segment metadata size", ErrMalformed)
	}
	if string(b[:8]) != magic {
		return fmt.Errorf("%w: segment metadata magic", ErrMalformed)
	}
	if crc32.Checksum(b[:156], crcTable) != binary.LittleEndian.Uint32(b[156:160]) {
		return fmt.Errorf("%w: segment metadata CRC32C", ErrIntegrity)
	}
	if binary.LittleEndian.Uint16(b[8:10]) != schema {
		return fmt.Errorf("%w: segment metadata schema", ErrUnsupported)
	}
	if binary.LittleEndian.Uint16(b[10:12]) != 0 {
		return fmt.Errorf("%w: segment metadata flags", ErrUnsupported)
	}
	if binary.LittleEndian.Uint32(b[12:16]) != 0 {
		return fmt.Errorf("%w: segment metadata reserved bytes", ErrMalformed)
	}
	for _, v := range b[reservedStart:156] {
		if v != 0 {
			return fmt.Errorf("%w: segment metadata padding", ErrMalformed)
		}
	}
	return nil
}
