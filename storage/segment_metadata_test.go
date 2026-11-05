package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Full vectors are independently calculated with Python varints/length prefixes,
// hashlib SHA-256 and a bitwise reflected CRC32C implementation.
const (
	successorHeaderGolden   = "41544553454730320200000000000000000102030405060708090a0b0c0d0e0f202122232425262728292a2b2c2d2e2f0004000000000000101112131415161718191a1b1c1d1e1fe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff0000000000000000011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c100000000000000000000000009e093de"
	segmentFooterGolden     = "4154455345414c310100000000000000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0004000000000000606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f00040000000000000000000000000000011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c1000000000000000000000000000000000000000037de700b"
	segmentedManifestGolden = "4154454d414e3031020010000102030405060708090a0b0c0d0e0f0110303132333435363738393a3b3c3d3e3f10202122232425262728292a2b2c2d2e2f203144337c245ebf079a75c7c2dc86b19473625bfed1e3e49d21afbae6819467cf800a20808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f0120a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf20c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf0210101112131415161718191a1b1c1d1e1f602038d431a866ebc28b274ab4c00dffe3bb7cb583056112e64b46aef4cbcf973b3b01800820606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f20e0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff6080080020011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c10020011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c110202122232425262728292a2b2c2d2e2fa001203144337c245ebf079a75c7c2dc86b19473625bfed1e3e49d21afbae6819467cf00a00320808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f2000000000000000000000000000000000000000000000000000000000000000008008800a0020011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c10120a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf3efd732c5c8e4713bce39cf974d65a3ec754f8dcc81f5d9a9a433d5d231f4251"
)

func segmentedFixtures(t testing.TB) (SuccessorHeader, SegmentFooter, Manifest) {
	t.Helper()
	id, initial, _, _ := metadataFixtures(t)
	genesis, err := GenesisPosition(id)
	if err != nil {
		t.Fatal(err)
	}
	pattern := func(start byte) Digest {
		var d Digest
		for i := range d {
			d[i] = start + byte(i)
		}
		return d
	}
	h := SuccessorHeader{Store: id.Store, SegmentID: "202122232425262728292a2b2c2d2e2f", LogicalStart: 1024, PreviousSegmentID: initial.SegmentID, PreviousSegmentDigest: pattern(224), Position: genesis}
	f := SegmentFooter{Store: id.Store, SegmentID: initial.SegmentID, DataEnd: 1024, PrefixDigest: pattern(96), LogicalEnd: 1024, Position: genesis}
	oldHeader, err := EncodeSegmentHeader(initial)
	if err != nil {
		t.Fatal(err)
	}
	hbytes, err := EncodeSuccessorHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{Store: id.Store, Generation: 1, ManifestID: "303132333435363738393a3b3c3d3e3f", SegmentID: h.SegmentID, SegmentHeaderDigest: Digest(sha256.Sum256(hbytes)), PublishedEnd: 1280, PrefixDigest: pattern(128), Position: Position{1, pattern(160)}, PostRootSet: pattern(192)}
	m.Segments = []SegmentDescriptor{
		{SegmentID: initial.SegmentID, HeaderSize: SegmentHeaderSize, HeaderDigest: Digest(sha256.Sum256(oldHeader)), Sealed: true, DataEnd: 1024, PrefixDigest: f.PrefixDigest, SealedDigest: h.PreviousSegmentDigest, LogicalStart: 96, LogicalEnd: 1024, StartPosition: genesis, EndPosition: genesis},
		{SegmentID: h.SegmentID, HeaderSize: SuccessorHeaderSize, HeaderDigest: m.SegmentHeaderDigest, DataEnd: 416, PrefixDigest: m.PrefixDigest, LogicalStart: 1024, LogicalEnd: 1280, StartPosition: genesis, EndPosition: m.Position},
	}
	return h, f, m
}

func TestSegmentedMetadataGoldenAndOwned(t *testing.T) {
	h, f, m := segmentedFixtures(t)
	for _, tc := range []struct {
		name, gold string
		encode     func() ([]byte, error)
		decode     func([]byte) error
	}{
		{"successor", successorHeaderGolden, func() ([]byte, error) { return EncodeSuccessorHeader(h) }, func(b []byte) error {
			v, e := DecodeSuccessorHeader(b)
			if e == nil && v != h {
				t.Fatal("successor differs")
			}
			return e
		}},
		{"footer", segmentFooterGolden, func() ([]byte, error) { return EncodeSegmentFooter(f) }, func(b []byte) error {
			v, e := DecodeSegmentFooter(b)
			if e == nil && v != f {
				t.Fatal("footer differs")
			}
			return e
		}},
		{"manifest", segmentedManifestGolden, func() ([]byte, error) { return EncodeManifest(m) }, func(b []byte) error {
			v, e := DecodeManifest(b)
			if e == nil && !reflect.DeepEqual(v, m) {
				t.Fatal("manifest differs")
			}
			return e
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, e := tc.encode()
			if e != nil {
				t.Fatal(e)
			}
			gold := metadataHex(t, tc.gold)
			if !bytes.Equal(b, gold) {
				t.Fatalf("got %x\nwant %x", b, gold)
			}
			if e = tc.decode(gold); e != nil {
				t.Fatal(e)
			}
		})
	}
	b := metadataHex(t, segmentedManifestGolden)
	got, err := DecodeManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	clear(b)
	if !reflect.DeepEqual(got, m) {
		t.Fatal("decode borrowed caller input")
	}
}

func TestSegmentedFixedMetadataRejectsEveryTruncationAndFlip(t *testing.T) {
	for _, tc := range []struct {
		name, gold string
		decode     func([]byte) error
	}{
		{"successor", successorHeaderGolden, func(b []byte) error { _, e := DecodeSuccessorHeader(b); return e }},
		{"footer", segmentFooterGolden, func(b []byte) error { _, e := DecodeSegmentFooter(b); return e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gold := metadataHex(t, tc.gold)
			for i := range gold {
				if err := tc.decode(gold[:i]); !errors.Is(err, ErrMalformed) {
					t.Fatalf("truncate%d: %v", i, err)
				}
				bad := bytes.Clone(gold)
				bad[i] ^= 1
				if tc.decode(bad) == nil {
					t.Fatalf("accepted flip%d", i)
				}
			}
			if err := tc.decode(append(bytes.Clone(gold), 0)); !errors.Is(err, ErrMalformed) {
				t.Fatalf("trailing: %v", err)
			}
			for _, x := range []struct {
				at   int
				want error
			}{{8, ErrUnsupported}, {10, ErrUnsupported}, {12, ErrMalformed}, {150, ErrMalformed}} {
				bad := bytes.Clone(gold)
				bad[x.at] ^= 1
				binary.LittleEndian.PutUint32(bad[156:], crc32.Checksum(bad[:156], crcTable))
				if err := tc.decode(bad); !errors.Is(err, x.want) {
					t.Fatalf("semantic byte%d: %v", x.at, err)
				}
			}
			for _, span := range [][2]int{{16, 32}, {32, 48}} {
				bad := bytes.Clone(gold)
				clear(bad[span[0]:span[1]])
				binary.LittleEndian.PutUint32(bad[156:], crc32.Checksum(bad[:156], crcTable))
				if err := tc.decode(bad); !errors.Is(err, ErrMalformed) {
					t.Fatalf("zero ID: %v", err)
				}
			}
		})
	}
}

// Build a canonical envelope while bypassing field validation to test decoding
// independently from the encoder's refusal of these invalid DTOs.
func uncheckedSegmentedManifest(m Manifest, count uint64) []byte {
	b := metadataAppendID(codec.AppendUint(codec.AppendUint(nil, 2), 0), string(m.Store))
	b = codec.AppendUint(b, m.Generation)
	b = metadataAppendID(b, m.ManifestID)
	b = metadataAppendID(b, m.SegmentID)
	b = appendDigest(b, m.SegmentHeaderDigest)
	b = codec.AppendUint(b, m.PublishedEnd)
	b = appendDigest(b, m.PrefixDigest)
	b = codec.AppendUint(b, m.Position.LSN)
	b = appendDigest(b, m.Position.Digest)
	b = appendDigest(b, m.PostRootSet)
	if count != uint64(len(m.Segments)) {
		b = codec.AppendUint(b, count)
	} else {
		b = appendSegmentDescriptors(b, m.Segments)
	}
	return metadataTestEnvelope(manifestMagic, b)
}

func TestSegmentedManifestRejectsInvalidCatalog(t *testing.T) {
	_, _, original := segmentedFixtures(t)
	cases := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"empty", func(m *Manifest) { m.Segments = []SegmentDescriptor{} }},
		{"one", func(m *Manifest) { m.Segments = m.Segments[:1] }},
		{"duplicate", func(m *Manifest) { m.Segments[1].SegmentID = m.Segments[0].SegmentID }},
		{"initial-header", func(m *Manifest) { m.Segments[0].HeaderSize = SuccessorHeaderSize }},
		{"next-header", func(m *Manifest) { m.Segments[1].HeaderSize = SegmentHeaderSize }},
		{"unsealed-predecessor", func(m *Manifest) { m.Segments[0].Sealed = false }},
		{"sealed-active", func(m *Manifest) { m.Segments[1].Sealed = true }},
		{"missing-seal", func(m *Manifest) { m.Segments[0].SealedDigest = Digest{} }},
		{"active-seal", func(m *Manifest) { m.Segments[1].SealedDigest = Digest{1} }},
		{"empty-sealed", func(m *Manifest) { m.Segments[0].DataEnd = 96; m.Segments[0].LogicalEnd = 96 }},
		{"zero-header-digest", func(m *Manifest) { m.Segments[0].HeaderDigest = Digest{} }},
		{"zero-prefix-digest", func(m *Manifest) { m.Segments[0].PrefixDigest = Digest{} }},
		{"data-before-header", func(m *Manifest) { m.Segments[1].DataEnd = 1 }},
		{"physical-overflow", func(m *Manifest) { m.Segments[1].DataEnd = ^uint64(0) }},
		{"logical-gap", func(m *Manifest) { m.Segments[1].LogicalStart++ }},
		{"logical-inversion", func(m *Manifest) { m.Segments[1].LogicalEnd = 1 }},
		{"logical-physical-width", func(m *Manifest) { m.Segments[1].DataEnd++ }},
		{"wrong-genesis", func(m *Manifest) { m.Segments[0].StartPosition.Digest[0] ^= 1 }},
		{"position-gap", func(m *Manifest) { m.Segments[1].StartPosition.Digest[0] ^= 1 }},
		{"same-lsn-new-digest", func(m *Manifest) { m.Segments[0].EndPosition.Digest[0] ^= 1 }},
		{"end-position", func(m *Manifest) { m.Segments[1].EndPosition.LSN++ }},
		{"active-id", func(m *Manifest) { m.SegmentID = m.Segments[0].SegmentID }},
		{"active-header", func(m *Manifest) { m.SegmentHeaderDigest = Digest{1} }},
		{"active-prefix", func(m *Manifest) { m.PrefixDigest = Digest{1} }},
		{"logical-tip", func(m *Manifest) { m.PublishedEnd++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := original
			m.Segments = append([]SegmentDescriptor{}, m.Segments...)
			tc.mutate(&m)
			if _, err := EncodeManifest(m); !errors.Is(err, ErrMalformed) {
				t.Fatalf("encode: %v", err)
			}
			if _, err := DecodeManifest(uncheckedSegmentedManifest(m, uint64(len(m.Segments)))); !errors.Is(err, ErrMalformed) {
				t.Fatalf("decode: %v", err)
			}
		})
	}
	for _, count := range []uint64{MaxSegments + 1, ^uint64(0)} {
		if _, err := DecodeManifest(uncheckedSegmentedManifest(original, count)); !errors.Is(err, ErrLimit) {
			t.Fatalf("count%d: %v", count, err)
		}
	}
	// No allocation of a 1024-item slice from a tiny declared-count envelope.
	if _, err := DecodeManifest(uncheckedSegmentedManifest(original, MaxSegments)); !errors.Is(err, ErrMalformed) || !errors.Is(err, codec.ErrTruncated) {
		t.Fatalf("truncated bounded count: %v", err)
	}
}

func TestSegmentedManifestBoundsAndLargeCatalog(t *testing.T) {
	_, _, m := segmentedFixtures(t)
	genesis := m.Segments[0].StartPosition
	m.Segments = make([]SegmentDescriptor, 32)
	logical := uint64(SegmentHeaderSize)
	for i := range m.Segments {
		header := uint64(SuccessorHeaderSize)
		if i == 0 {
			header = SegmentHeaderSize
		}
		d := SegmentDescriptor{SegmentID: fmt.Sprintf("%032x", i+1), HeaderSize: header, HeaderDigest: Digest{1}, Sealed: i < 31, DataEnd: header + 928, PrefixDigest: Digest{2}, LogicalStart: logical, LogicalEnd: logical + 928, StartPosition: genesis, EndPosition: genesis}
		if d.Sealed {
			d.SealedDigest = Digest{3}
		}
		m.Segments[i] = d
		logical = d.LogicalEnd
	}
	m.Segments[31].EndPosition = m.Position
	m.PublishedEnd = logical
	last := m.Segments[31]
	m.SegmentID = last.SegmentID
	m.SegmentHeaderDigest = last.HeaderDigest
	m.PrefixDigest = last.PrefixDigest
	b, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) <= MaxMetadataBytes {
		t.Fatal("fixture not larger than legacy limit")
	}
	got, err := DecodeManifest(b)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatalf("large manifest %v", err)
	}
	for _, n := range []int{MaxManifestBytes + 1, MaxMetadataBytes + 1} {
		b := make([]byte, n)
		if _, err := DecodeManifest(b); !errors.Is(err, ErrLimit) {
			t.Fatalf("oversize%d: %v", n, err)
		}
		if n > MaxMetadataBytes {
			if a := testing.AllocsPerRun(20, func() { _, _ = DecodeManifest(b) }); a != 0 {
				t.Fatalf("oversize allocated%v", a)
			}
		}
	}
	m.Segments = make([]SegmentDescriptor, MaxSegments+1)
	if _, err := EncodeManifest(m); !errors.Is(err, ErrLimit) {
		t.Fatalf("encode count bound: %v", err)
	}
}

func TestSegmentedFixedMetadataKnownFields(t *testing.T) {
	h, f, _ := segmentedFixtures(t)
	for _, mutate := range []func(*SuccessorHeader){func(h *SuccessorHeader) { h.PreviousSegmentID = h.SegmentID }, func(h *SuccessorHeader) { h.PreviousSegmentDigest = Digest{} }, func(h *SuccessorHeader) { h.Position.Digest = Digest{} }, func(h *SuccessorHeader) { h.Position.LSN = ^uint64(0) }, func(h *SuccessorHeader) { h.LogicalStart = 96 }, func(h *SuccessorHeader) { h.LogicalStart = ^uint64(0) }, func(h *SuccessorHeader) { h.Store = model.StoreID("") }} {
		bad := h
		mutate(&bad)
		if _, err := EncodeSuccessorHeader(bad); !errors.Is(err, ErrMalformed) {
			t.Fatalf("successor fields: %v", err)
		}
	}
	for _, mutate := range []func(*SegmentFooter){func(f *SegmentFooter) { f.DataEnd = 96 }, func(f *SegmentFooter) { f.DataEnd = ^uint64(0) }, func(f *SegmentFooter) { f.PrefixDigest = Digest{} }, func(f *SegmentFooter) { f.LogicalEnd = 96 }, func(f *SegmentFooter) { f.LogicalEnd = ^uint64(0) }, func(f *SegmentFooter) { f.Position.Digest = Digest{} }} {
		bad := f
		mutate(&bad)
		if _, err := EncodeSegmentFooter(bad); !errors.Is(err, ErrMalformed) {
			t.Fatalf("footer fields: %v", err)
		}
	}
}

func FuzzSegmentedMetadata(f *testing.F) {
	for _, gold := range []string{successorHeaderGolden, segmentFooterGolden, segmentedManifestGolden} {
		f.Add(metadataHex(f, gold))
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		candidates := [][]byte{b}
		if len(b) >= 40 && len(b) <= MaxManifestBytes {
			candidates = append(candidates, metadataResign(b))
		}
		if len(b) == 160 {
			c := bytes.Clone(b)
			binary.LittleEndian.PutUint32(c[156:], crc32.Checksum(c[:156], crcTable))
			candidates = append(candidates, c)
		}
		for _, c := range candidates {
			before := bytes.Clone(c)
			if v, e := DecodeSuccessorHeader(c); e == nil {
				out, e := EncodeSuccessorHeader(v)
				if e != nil || !bytes.Equal(out, c) {
					t.Fatalf("successor not canonical: %v", e)
				}
			}
			if v, e := DecodeSegmentFooter(c); e == nil {
				out, e := EncodeSegmentFooter(v)
				if e != nil || !bytes.Equal(out, c) {
					t.Fatalf("footer not canonical: %v", e)
				}
			}
			if v, e := DecodeManifest(c); e == nil {
				out, e := EncodeManifest(v)
				if e != nil || !bytes.Equal(out, c) {
					t.Fatalf("manifest not canonical: %v", e)
				}
			}
			if !bytes.Equal(c, before) {
				t.Fatal("decoder mutated input")
			}
		}
	})
}
