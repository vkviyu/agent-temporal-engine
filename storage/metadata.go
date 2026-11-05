package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	SegmentHeaderSize = 96
	MaxMetadataBytes  = 4096
)

const (
	identityMagic = "ATEID001"
	manifestMagic = "ATEMAN01"
	currentMagic  = "ATECUR01"
	segmentMagic  = "ATESEG01"
)

// Identity fixes the store identity and its synchronization contract.
type Identity struct {
	Store   model.StoreID
	Profile DurabilityProfile
}

// SegmentHeader binds a segment to its store and preceding published position.
// Schema 1 supports exactly the initial segment, whose FirstLSN is 1.
type SegmentHeader struct {
	Store          model.StoreID
	SegmentID      string
	FirstLSN       uint64
	PreviousDigest Digest
}

// Manifest authenticates one published prefix. Its cross-file relationships
// are checked by the store coordinator, independently of this pure codec.
type Manifest struct {
	Store               model.StoreID
	Generation          uint64
	ManifestID          string
	SegmentID           string
	SegmentHeaderDigest Digest
	PublishedEnd        uint64
	PrefixDigest        Digest
	Position            Position
	PostRootSet         Digest
	// Segments is nil for legacy schema1; schema2 authenticates the complete
	// ordered sealed/active layout while PublishedEnd remains logical.
	Segments []SegmentDescriptor
	// LayoutSource authenticates the prior layout copied by AuditFull
	// compaction. The logical transaction prefix is unchanged. RetiredSegments
	// names only obsolete physical copies, never discardable logical history.
	LayoutSource    *LayoutSource
	RetiredSegments []SegmentDescriptor
}

type LayoutSource struct {
	ManifestID     string
	ManifestDigest Digest
	Generation     uint64
	PublishedEnd   uint64
	Position       Position
	PostRootSet    Digest
}

// Current selects an immutable manifest and authenticates the store identity.
type Current struct {
	Store          model.StoreID
	Generation     uint64
	ManifestID     string
	ManifestDigest Digest
	IdentityDigest Digest
}

func metadataID(id string) ([16]byte, error) {
	var out [16]byte
	if len(id) != 32 {
		return out, fmt.Errorf("%w: metadata ID length", ErrMalformed)
	}
	n, err := hex.Decode(out[:], []byte(id))
	if err != nil || n != len(out) || hex.EncodeToString(out[:]) != id || out == ([16]byte{}) {
		return [16]byte{}, fmt.Errorf("%w: metadata ID must be nonzero lowercase hex", ErrMalformed)
	}
	return out, nil
}

func metadataAppendID(b []byte, id string) []byte {
	raw, _ := metadataID(id) // callers validate before creating output
	b, _ = codec.AppendBytes(b, raw[:])
	return b
}

func metadataReadID(d *codec.Decoder) (string, error) {
	b, err := d.Bytes(16)
	if err != nil {
		return "", err
	}
	if len(b) != 16 {
		return "", fmt.Errorf("%w: metadata ID length", ErrMalformed)
	}
	var raw [16]byte
	copy(raw[:], b)
	if raw == ([16]byte{}) {
		return "", fmt.Errorf("%w: zero metadata ID", ErrMalformed)
	}
	return hex.EncodeToString(raw[:]), nil
}

func metadataError(err error) error {
	return fmt.Errorf("%w: metadata payload: %w", ErrMalformed, err)
}

func metadataEnvelope(magic string, payload []byte) ([]byte, error) {
	return metadataEnvelopeBounded(magic, payload, MaxMetadataBytes)
}

func metadataEnvelopeBounded(magic string, payload []byte, maximum int) ([]byte, error) {
	if len(payload) > maximum-len(magic)-sha256.Size {
		return nil, ErrLimit
	}
	b := make([]byte, 0, len(magic)+len(payload)+sha256.Size)
	b = append(b, magic...)
	b = append(b, payload...)
	sum := sha256.Sum256(b)
	return append(b, sum[:]...), nil
}

func metadataDecoder(b []byte, magic string) (*codec.Decoder, error) {
	d, _, err := metadataDecoderVersioned(b, magic, MaxMetadataBytes, 1)
	return d, err
}

func metadataDecoderVersioned(b []byte, magic string, maximum int, versions ...uint64) (*codec.Decoder, uint64, error) {
	// Reject the complete input size before allocating or parsing any field.
	if len(b) > maximum {
		return nil, 0, ErrLimit
	}
	if len(b) < len(magic)+sha256.Size {
		return nil, 0, fmt.Errorf("%w: truncated metadata envelope", ErrMalformed)
	}
	if string(b[:len(magic)]) != magic {
		return nil, 0, fmt.Errorf("%w: metadata magic", ErrMalformed)
	}
	end := len(b) - sha256.Size
	sum := sha256.Sum256(b[:end])
	var checksum Digest
	copy(checksum[:], b[end:])
	if Digest(sum) != checksum {
		return nil, 0, fmt.Errorf("%w: metadata SHA-256", ErrIntegrity)
	}
	d := codec.NewDecoder(b[len(magic):end])
	version, err := d.Uint()
	if err != nil {
		return nil, 0, metadataError(err)
	}
	supported := false
	for _, v := range versions {
		if version == v {
			supported = true
			break
		}
	}
	if !supported {
		return nil, 0, fmt.Errorf("%w: metadata schema", ErrUnsupported)
	}
	flags, err := d.Uint()
	if err != nil {
		return nil, 0, metadataError(err)
	}
	if flags != 0 {
		return nil, 0, fmt.Errorf("%w: metadata flags", ErrUnsupported)
	}
	return d, version, nil
}

func metadataPayload() []byte { return codec.AppendUint(codec.AppendUint(nil, 1), 0) }

func (id Identity) validate() error {
	if _, err := metadataID(string(id.Store)); err != nil {
		return err
	}
	if id.Profile != FileSyncExperimental {
		return fmt.Errorf("%w: durability profile", ErrUnsupported)
	}
	return nil
}

func EncodeIdentity(id Identity) ([]byte, error) {
	if err := id.validate(); err != nil {
		return nil, err
	}
	b := metadataAppendID(metadataPayload(), string(id.Store))
	b, _ = codec.AppendString(b, string(id.Profile))
	return metadataEnvelope(identityMagic, b)
}

func DecodeIdentity(b []byte) (Identity, error) {
	var id Identity
	d, err := metadataDecoder(b, identityMagic)
	if err != nil {
		return id, err
	}
	store, err := metadataReadID(d)
	if err != nil {
		return id, metadataError(err)
	}
	profile, err := d.String(MaxMetadataBytes)
	if err != nil {
		return id, metadataError(err)
	}
	if err := d.Done(); err != nil {
		return id, metadataError(err)
	}
	id = Identity{Store: model.StoreID(store), Profile: DurabilityProfile(profile)}
	if err := id.validate(); err != nil {
		return Identity{}, err
	}
	return id, nil
}

// IdentityDigest includes the entire identity file, including its checksum.
func IdentityDigest(id Identity) (Digest, error) {
	b, err := EncodeIdentity(id)
	if err != nil {
		return Digest{}, err
	}
	return Digest(sha256.Sum256(b)), nil
}

func GenesisPosition(id Identity) (Position, error) {
	digest, err := IdentityDigest(id)
	if err != nil {
		return Position{}, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("ATE-STORE-GENESIS\x00"))
	_, _ = h.Write(digest[:])
	return Position{LSN: 0, Digest: hashDigest(h)}, nil
}

func (header SegmentHeader) validate() error {
	if _, err := metadataID(string(header.Store)); err != nil {
		return err
	}
	if _, err := metadataID(header.SegmentID); err != nil {
		return err
	}
	if header.FirstLSN != 1 || header.PreviousDigest == (Digest{}) {
		return fmt.Errorf("%w: segment starting position", ErrMalformed)
	}
	return nil
}

func EncodeSegmentHeader(header SegmentHeader) ([]byte, error) {
	if err := header.validate(); err != nil {
		return nil, err
	}
	store, _ := metadataID(string(header.Store))
	segment, _ := metadataID(header.SegmentID)
	b := make([]byte, SegmentHeaderSize)
	copy(b[:8], segmentMagic)
	binary.LittleEndian.PutUint16(b[8:10], 1)
	copy(b[16:32], store[:])
	copy(b[32:48], segment[:])
	binary.LittleEndian.PutUint64(b[48:56], header.FirstLSN)
	copy(b[56:88], header.PreviousDigest[:])
	binary.LittleEndian.PutUint32(b[88:92], crc32.Checksum(b[:88], crcTable))
	return b, nil
}

func DecodeSegmentHeader(b []byte) (SegmentHeader, error) {
	var header SegmentHeader
	if len(b) != SegmentHeaderSize {
		return header, fmt.Errorf("%w: segment header size", ErrMalformed)
	}
	if string(b[:8]) != segmentMagic {
		return header, fmt.Errorf("%w: segment magic", ErrMalformed)
	}
	if crc32.Checksum(b[:88], crcTable) != binary.LittleEndian.Uint32(b[88:92]) {
		return header, fmt.Errorf("%w: segment header CRC32C", ErrIntegrity)
	}
	if binary.LittleEndian.Uint16(b[8:10]) != 1 {
		return header, fmt.Errorf("%w: segment version", ErrUnsupported)
	}
	if binary.LittleEndian.Uint16(b[10:12]) != 0 {
		return header, fmt.Errorf("%w: segment flags", ErrUnsupported)
	}
	if binary.LittleEndian.Uint32(b[12:16]) != 0 || binary.LittleEndian.Uint32(b[92:96]) != 0 {
		return header, fmt.Errorf("%w: segment reserved bytes", ErrMalformed)
	}
	header.Store = model.StoreID(hex.EncodeToString(b[16:32]))
	header.SegmentID = hex.EncodeToString(b[32:48])
	header.FirstLSN = binary.LittleEndian.Uint64(b[48:56])
	copy(header.PreviousDigest[:], b[56:88])
	if err := header.validate(); err != nil {
		return SegmentHeader{}, err
	}
	return header, nil
}

func (m Manifest) validate() error {
	for _, id := range []string{string(m.Store), m.ManifestID, m.SegmentID} {
		if _, err := metadataID(id); err != nil {
			return err
		}
	}
	if m.PublishedEnd < SegmentHeaderSize || m.SegmentHeaderDigest == (Digest{}) || m.PrefixDigest == (Digest{}) || m.Position.Digest == (Digest{}) || ((m.Position.LSN == 0) != (m.PostRootSet == (Digest{}))) {
		return fmt.Errorf("%w: manifest published position", ErrMalformed)
	}
	if m.Segments != nil {
		if err := m.validateSegments(); err != nil {
			return err
		}
	}
	return m.validateRetirement()
}

func EncodeManifest(m Manifest) ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	payload := metadataPayload()
	maximum := MaxMetadataBytes
	if m.Segments != nil {
		payload = codec.AppendUint(codec.AppendUint(nil, 2), 0)
		maximum = MaxManifestBytes
	}
	if m.LayoutSource != nil {
		payload = codec.AppendUint(codec.AppendUint(nil, 3), 0)
		maximum = MaxManifestBytes
	}
	b := metadataAppendID(payload, string(m.Store))
	b = codec.AppendUint(b, m.Generation)
	b = metadataAppendID(b, m.ManifestID)
	b = metadataAppendID(b, m.SegmentID)
	b = appendDigest(b, m.SegmentHeaderDigest)
	b = codec.AppendUint(b, m.PublishedEnd)
	b = appendDigest(b, m.PrefixDigest)
	b = codec.AppendUint(b, m.Position.LSN)
	b = appendDigest(b, m.Position.Digest)
	b = appendDigest(b, m.PostRootSet)
	if m.Segments != nil {
		b = appendSegmentDescriptors(b, m.Segments)
	}
	if m.LayoutSource != nil {
		// Schema3 supports a single physical segment as well as a segmented
		// layout. An explicit presence bit avoids an ambiguous zero-length
		// schema2 descriptor catalog.
		if m.Segments == nil {
			b = codec.AppendUint(b, 0)
		}
		b = appendLayoutSource(b, *m.LayoutSource)
		b = appendSegmentDescriptors(b, m.RetiredSegments)
	}
	return metadataEnvelopeBounded(manifestMagic, b, maximum)
}

func DecodeManifest(b []byte) (Manifest, error) {
	var m Manifest
	// A legacy manifest never gains the larger schema2 allocation budget.
	if len(b) > MaxManifestBytes || (len(b) > MaxMetadataBytes && (len(b) < 9 || (b[8] != 2 && b[8] != 3))) {
		return m, ErrLimit
	}
	d, version, err := metadataDecoderVersioned(b, manifestMagic, MaxManifestBytes, 1, 2, 3)
	if err != nil {
		return m, err
	}
	store, err := metadataReadID(d)
	if err != nil {
		return m, metadataError(err)
	}
	m.Store = model.StoreID(store)
	if m.Generation, err = d.Uint(); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.ManifestID, err = metadataReadID(d); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.SegmentID, err = metadataReadID(d); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.SegmentHeaderDigest, err = readDigest(d); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.PublishedEnd, err = d.Uint(); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.PrefixDigest, err = readDigest(d); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.Position.LSN, err = d.Uint(); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.Position.Digest, err = readDigest(d); err != nil {
		return Manifest{}, metadataError(err)
	}
	if m.PostRootSet, err = readDigest(d); err != nil {
		return Manifest{}, metadataError(err)
	}
	if version == 2 {
		if m.Segments, err = readSegmentDescriptors(d); err != nil {
			return Manifest{}, err
		}
	}
	if version == 3 {
		if m.Segments, err = readSegmentDescriptorsOptional(d, MaxSegments); err != nil {
			return Manifest{}, err
		}
		source, e := readLayoutSource(d)
		if e != nil {
			return Manifest{}, e
		}
		m.LayoutSource = &source
		if m.RetiredSegments, err = readSegmentDescriptorsOptional(d, 2*MaxSegments); err != nil {
			return Manifest{}, err
		}
	}
	if err := d.Done(); err != nil {
		return Manifest{}, metadataError(err)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (c Current) validate() error {
	for _, id := range []string{string(c.Store), c.ManifestID} {
		if _, err := metadataID(id); err != nil {
			return err
		}
	}
	if c.ManifestDigest == (Digest{}) || c.IdentityDigest == (Digest{}) {
		return fmt.Errorf("%w: current digest", ErrMalformed)
	}
	return nil
}

func EncodeCurrent(c Current) ([]byte, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	b := metadataAppendID(metadataPayload(), string(c.Store))
	b = codec.AppendUint(b, c.Generation)
	b = metadataAppendID(b, c.ManifestID)
	b = appendDigest(b, c.ManifestDigest)
	b = appendDigest(b, c.IdentityDigest)
	return metadataEnvelope(currentMagic, b)
}

func DecodeCurrent(b []byte) (Current, error) {
	var c Current
	d, err := metadataDecoder(b, currentMagic)
	if err != nil {
		return c, err
	}
	store, err := metadataReadID(d)
	if err != nil {
		return c, metadataError(err)
	}
	c.Store = model.StoreID(store)
	if c.Generation, err = d.Uint(); err != nil {
		return Current{}, metadataError(err)
	}
	if c.ManifestID, err = metadataReadID(d); err != nil {
		return Current{}, metadataError(err)
	}
	if c.ManifestDigest, err = readDigest(d); err != nil {
		return Current{}, metadataError(err)
	}
	if c.IdentityDigest, err = readDigest(d); err != nil {
		return Current{}, metadataError(err)
	}
	if err := d.Done(); err != nil {
		return Current{}, metadataError(err)
	}
	if err := c.validate(); err != nil {
		return Current{}, err
	}
	return c, nil
}
