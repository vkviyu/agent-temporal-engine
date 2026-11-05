package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// These complete vectors were derived by an independent Python implementation:
// shortest unsigned varints, byte-length prefixes, hashlib.sha256, and a bitwise
// CRC32C loop using reflected polynomial 0x82f63b78. No Go encoder generated them.
const (
	metadataIdentityGolden       = "4154454944303031010010000102030405060708090a0b0c0d0e0f1866696c652d73796e632d6578706572696d656e74616c2d313ad6a5df113256122679f8687e58e91eacc0d3aa9cd8e2b3fefe54555f4913e6"
	metadataHeaderGolden         = "41544553454730310100000000000000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000000000011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c1ad709a9b00000000"
	metadataManifestGolden       = "4154454d414e3031010010000102030405060708090a0b0c0d0e0f800110202122232425262728292a2b2c2d2e2f10101112131415161718191a1b1c1d1e1f2038d431a866ebc28b274ab4c00dffe3bb7cb583056112e64b46aef4cbcf973b3b802020606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f7f20a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf20c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf82f58c20138aaa8db190e8e9f74b205fe13fbe20f87e81d8887abe612dc6072b"
	metadataCurrentGolden        = "4154454355523031010010000102030405060708090a0b0c0d0e0f800110202122232425262728292a2b2c2d2e2f20c59448666f8d63120b42e545f3f64fff5018a38a3eec399ff390f4534431825420b71ba3216325ad888d9c6f4400fdc073c49ca743e6f2b6df559bb6875db30f18dec722fed34ffc2d0b371f5eb386d3c36bc5be5892684320d9919150defdb8c5"
	metadataIdentityDigestGolden = "b71ba3216325ad888d9c6f4400fdc073c49ca743e6f2b6df559bb6875db30f18"
	metadataGenesisGolden        = "011bc0d93907a7b729a4fa81e4f3825674686b47c73a86fe57c4f63dce30e0c1"
)

func metadataHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func metadataDigest(t testing.TB, s string) Digest {
	t.Helper()
	var d Digest
	b := metadataHex(t, s)
	if len(b) != len(d) {
		t.Fatal("invalid test digest")
	}
	copy(d[:], b)
	return d
}

func metadataFixtures(t testing.TB) (Identity, SegmentHeader, Manifest, Current) {
	t.Helper()
	id := Identity{Store: "000102030405060708090a0b0c0d0e0f", Profile: FileSyncExperimental}
	header := SegmentHeader{Store: id.Store, SegmentID: "101112131415161718191a1b1c1d1e1f", FirstLSN: 1, PreviousDigest: metadataDigest(t, metadataGenesisGolden)}
	m := Manifest{
		Store: id.Store, Generation: 128, ManifestID: "202122232425262728292a2b2c2d2e2f", SegmentID: header.SegmentID,
		SegmentHeaderDigest: metadataDigest(t, "38d431a866ebc28b274ab4c00dffe3bb7cb583056112e64b46aef4cbcf973b3b"),
		PublishedEnd:        4096, PrefixDigest: metadataDigest(t, "606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
		Position:    Position{127, metadataDigest(t, "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf")},
		PostRootSet: metadataDigest(t, "c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf"),
	}
	c := Current{Store: id.Store, Generation: 128, ManifestID: m.ManifestID,
		ManifestDigest: metadataDigest(t, "c59448666f8d63120b42e545f3f64fff5018a38a3eec399ff390f45344318254"),
		IdentityDigest: metadataDigest(t, metadataIdentityDigestGolden)}
	return id, header, m, c
}

func TestMetadataGolden(t *testing.T) {
	id, header, manifest, current := metadataFixtures(t)
	tests := []struct {
		name, golden string
		encode       func() ([]byte, error)
		decode       func([]byte) error
	}{
		{"identity", metadataIdentityGolden, func() ([]byte, error) { return EncodeIdentity(id) }, func(b []byte) error {
			got, err := DecodeIdentity(b)
			if err == nil && got != id {
				t.Errorf("identity: %#v", got)
			}
			return err
		}},
		{"segment", metadataHeaderGolden, func() ([]byte, error) { return EncodeSegmentHeader(header) }, func(b []byte) error {
			got, err := DecodeSegmentHeader(b)
			if err == nil && got != header {
				t.Errorf("header: %#v", got)
			}
			return err
		}},
		{"manifest", metadataManifestGolden, func() ([]byte, error) { return EncodeManifest(manifest) }, func(b []byte) error {
			got, err := DecodeManifest(b)
			if err == nil && !reflect.DeepEqual(got, manifest) {
				t.Errorf("manifest: %#v", got)
			}
			return err
		}},
		{"current", metadataCurrentGolden, func() ([]byte, error) { return EncodeCurrent(current) }, func(b []byte) error {
			got, err := DecodeCurrent(b)
			if err == nil && got != current {
				t.Errorf("current: %#v", got)
			}
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := metadataHex(t, test.golden)
			got, err := test.encode()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("got %x\nwant %x", got, want)
			}
			if err := test.decode(want); err != nil {
				t.Fatal(err)
			}
		})
	}
	digest, err := IdentityDigest(id)
	if err != nil || digest != metadataDigest(t, metadataIdentityDigestGolden) {
		t.Fatalf("identity digest: %x %v", digest, err)
	}
	genesis, err := GenesisPosition(id)
	if err != nil || genesis != (Position{0, metadataDigest(t, metadataGenesisGolden)}) {
		t.Fatalf("genesis: %#v %v", genesis, err)
	}
	// Returned DTOs own their IDs and digests rather than borrowing caller bytes.
	b := metadataHex(t, metadataManifestGolden)
	got, err := DecodeManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	clear(b)
	if !reflect.DeepEqual(got, manifest) {
		t.Fatal("decoded manifest aliases input")
	}
}

func metadataTestEnvelope(magic string, payload []byte) []byte {
	b := append([]byte(magic), payload...)
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

func metadataResign(b []byte) []byte {
	b = bytes.Clone(b)
	sum := sha256.Sum256(b[:len(b)-32])
	copy(b[len(b)-32:], sum[:])
	return b
}

func TestMetadataInvalidEnvelopeAndCanonical(t *testing.T) {
	tests := []struct {
		name, golden string
		decode       func([]byte) error
	}{
		{"identity", metadataIdentityGolden, func(b []byte) error { _, err := DecodeIdentity(b); return err }},
		{"manifest", metadataManifestGolden, func(b []byte) error { _, err := DecodeManifest(b); return err }},
		{"current", metadataCurrentGolden, func(b []byte) error { _, err := DecodeCurrent(b); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gold := metadataHex(t, test.golden)
			for i := 0; i < len(gold); i++ {
				if err := test.decode(gold[:i]); err == nil {
					t.Fatalf("accepted truncation at %d", i)
				}
				mutated := bytes.Clone(gold)
				mutated[i] ^= 1
				if err := test.decode(mutated); err == nil {
					t.Fatalf("accepted flipped byte %d", i)
				}
			}
			magic, payload := string(gold[:8]), gold[8:len(gold)-32]
			invalidVersion := byte(2)
			if test.name == "manifest" {
				// Manifest schema 3 is supported by the retirement/compaction
				// protocol; use the next unknown version for this negative case.
				invalidVersion = 4
			}
			cases := []struct {
				name            string
				payload         []byte
				want, canonical error
			}{
				{"version", append([]byte{invalidVersion}, payload[1:]...), ErrUnsupported, nil},
				{"flags", append([]byte{1, 1}, payload[2:]...), ErrUnsupported, nil},
				{"overlong-version", append([]byte{0x81, 0}, payload[1:]...), ErrMalformed, codec.ErrNonCanonical},
				{"overlong-flags", append([]byte{1, 0x80, 0}, payload[2:]...), ErrMalformed, codec.ErrNonCanonical},
				{"overlong-ID-length", append([]byte{1, 0, 0x90, 0}, payload[3:]...), ErrMalformed, codec.ErrNonCanonical},
				{"integer-overflow", bytes.Repeat([]byte{0xff}, 10), ErrMalformed, codec.ErrIntegerOverflow},
				{"missing-schema", nil, ErrMalformed, codec.ErrTruncated},
				{"missing-flags", []byte{1}, ErrMalformed, codec.ErrTruncated},
				{"trailing", append(bytes.Clone(payload), 0), ErrMalformed, codec.ErrTrailingData},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					err := test.decode(metadataTestEnvelope(magic, tc.payload))
					if !errors.Is(err, tc.want) || (tc.canonical != nil && !errors.Is(err, tc.canonical)) {
						t.Fatalf("got %v; want %v / %v", err, tc.want, tc.canonical)
					}
				})
			}
			for _, n := range []byte{0, 15, 17, 0xff} {
				mutated := bytes.Clone(gold)
				mutated[10] = n
				if err := test.decode(metadataResign(mutated)); !errors.Is(err, ErrMalformed) {
					t.Fatalf("ID length %d: %v", n, err)
				}
			}
			mutated := bytes.Clone(gold)
			clear(mutated[11:27])
			if err := test.decode(metadataResign(mutated)); !errors.Is(err, ErrMalformed) {
				t.Fatalf("zero store: %v", err)
			}
			mutated = bytes.Clone(gold)
			mutated[len(mutated)-1] ^= 1
			if err := test.decode(mutated); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("checksum: %v", err)
			}
			mutated = bytes.Clone(gold)
			mutated[0] ^= 1
			if err := test.decode(metadataResign(mutated)); !errors.Is(err, ErrMalformed) {
				t.Fatalf("magic: %v", err)
			}
			oversize := make([]byte, MaxMetadataBytes+1)
			if err := test.decode(oversize); !errors.Is(err, ErrLimit) {
				t.Fatalf("oversize: %v", err)
			}
			if allocs := testing.AllocsPerRun(20, func() { _ = test.decode(oversize) }); allocs != 0 {
				t.Fatalf("oversize allocated: %v", allocs)
			}
		})
	}
}

func TestMetadataKnownSchemaValidation(t *testing.T) {
	id, header, manifest, current := metadataFixtures(t)
	for _, value := range []string{"", strings.Repeat("0", 32), "000102030405060708090A0b0c0d0e0f", "000102030405060708090g0b0c0d0e0f", strings.Repeat("a", 33)} {
		badID := id
		badID.Store = model.StoreID(value)
		if _, err := EncodeIdentity(badID); !errors.Is(err, ErrMalformed) {
			t.Fatalf("identity ID %q: %v", value, err)
		}
		badHeader := header
		badHeader.SegmentID = value
		if _, err := EncodeSegmentHeader(badHeader); !errors.Is(err, ErrMalformed) {
			t.Fatalf("segment ID %q: %v", value, err)
		}
		badManifest := manifest
		badManifest.ManifestID = value
		if _, err := EncodeManifest(badManifest); !errors.Is(err, ErrMalformed) {
			t.Fatalf("manifest ID %q: %v", value, err)
		}
		badCurrent := current
		badCurrent.ManifestID = value
		if _, err := EncodeCurrent(badCurrent); !errors.Is(err, ErrMalformed) {
			t.Fatalf("current ID %q: %v", value, err)
		}
	}
	badID := id
	badID.Profile = "future-profile"
	if _, err := EncodeIdentity(badID); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("profile: %v", err)
	}
	if _, err := IdentityDigest(badID); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("identity digest profile: %v", err)
	}
	if _, err := GenesisPosition(badID); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("genesis profile: %v", err)
	}
	b := metadataHex(t, metadataIdentityGolden)
	payload := append(bytes.Clone(b[8:27]), 1, 'x')
	if _, err := DecodeIdentity(metadataTestEnvelope("ATEID001", payload)); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("decoded profile: %v", err)
	}
	b[28] = 0xff
	if _, err := DecodeIdentity(metadataResign(b)); !errors.Is(err, ErrMalformed) || !errors.Is(err, codec.ErrInvalidUTF8) {
		t.Fatalf("profile UTF-8: %v", err)
	}

	for _, mutate := range []func(*Manifest){
		func(m *Manifest) { m.PublishedEnd = SegmentHeaderSize - 1 },
		func(m *Manifest) { m.SegmentHeaderDigest = Digest{} },
		func(m *Manifest) { m.PrefixDigest = Digest{} },
		func(m *Manifest) { m.Position.Digest = Digest{} },
		func(m *Manifest) { m.PostRootSet = Digest{} },
		func(m *Manifest) { m.Position.LSN = 0 },
	} {
		bad := manifest
		mutate(&bad)
		if _, err := EncodeManifest(bad); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid manifest: %v", err)
		}
	}
	for _, span := range [][2]int{{64, 96}, {99, 131}, {133, 165}, {166, 198}} {
		bad := metadataHex(t, metadataManifestGolden)
		clear(bad[span[0]:span[1]])
		if _, err := DecodeManifest(metadataResign(bad)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("zero manifest digest at %d: %v", span[0], err)
		}
	}
	b = metadataHex(t, metadataManifestGolden)
	b[131] = 0
	if _, err := DecodeManifest(metadataResign(b)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("genesis domain root: %v", err)
	}
	b = metadataHex(t, metadataManifestGolden)
	payload = append(bytes.Clone(b[8:96]), 95)
	payload = append(payload, b[98:len(b)-32]...)
	if _, err := DecodeManifest(metadataTestEnvelope("ATEMAN01", payload)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("published end: %v", err)
	}
	// Genesis permits a zero PostRootSet and does not couple generation to LSN.
	manifest.Generation = 999
	manifest.PublishedEnd = SegmentHeaderSize
	manifest.Position.LSN = 0
	manifest.PostRootSet = Digest{}
	b, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeManifest(b); err != nil || !reflect.DeepEqual(got, manifest) {
		t.Fatalf("genesis manifest: %#v %v", got, err)
	}
	current.Generation = 0
	if _, err := EncodeCurrent(current); err != nil {
		t.Fatalf("zero generation codec: %v", err)
	}
	for _, span := range [][2]int{{47, 79}, {80, 112}} {
		bad := metadataHex(t, metadataCurrentGolden)
		clear(bad[span[0]:span[1]])
		if _, err := DecodeCurrent(metadataResign(bad)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("zero current digest: %v", err)
		}
	}
	b = metadataHex(t, metadataCurrentGolden)
	b[46] = 31
	if _, err := DecodeCurrent(metadataResign(b)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("digest length: %v", err)
	}
}

func TestSegmentHeaderInvalid(t *testing.T) {
	gold := metadataHex(t, metadataHeaderGolden)
	for i := 0; i < len(gold); i++ {
		if _, err := DecodeSegmentHeader(gold[:i]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("truncated %d: %v", i, err)
		}
		bad := bytes.Clone(gold)
		bad[i] ^= 1
		if _, err := DecodeSegmentHeader(bad); err == nil {
			t.Fatalf("flipped %d", i)
		}
	}
	if _, err := DecodeSegmentHeader(append(bytes.Clone(gold), 0)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("trailing header: %v", err)
	}
	cases := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{"version", func(b []byte) { binary.LittleEndian.PutUint16(b[8:10], 2) }, ErrUnsupported},
		{"flags", func(b []byte) { b[10] = 1 }, ErrUnsupported},
		{"reserved", func(b []byte) { b[12] = 1 }, ErrMalformed},
		{"padding", func(b []byte) { b[92] = 1 }, ErrMalformed},
		{"store", func(b []byte) { clear(b[16:32]) }, ErrMalformed},
		{"segment", func(b []byte) { clear(b[32:48]) }, ErrMalformed},
		{"zero-first-lsn", func(b []byte) { clear(b[48:56]) }, ErrMalformed},
		{"future-first-lsn", func(b []byte) { b[48] = 2 }, ErrMalformed},
		{"previous", func(b []byte) { clear(b[56:88]) }, ErrMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(gold)
			tc.mutate(bad)
			binary.LittleEndian.PutUint32(bad[88:92], crc32.Checksum(bad[:88], crcTable))
			if _, err := DecodeSegmentHeader(bad); !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}
	_, header, _, _ := metadataFixtures(t)
	for _, lsn := range []uint64{0, 2, ^uint64(0)} {
		header.FirstLSN = lsn
		if _, err := EncodeSegmentHeader(header); !errors.Is(err, ErrMalformed) {
			t.Fatalf("first LSN %d: %v", lsn, err)
		}
	}
}

func FuzzMetadataCodecs(f *testing.F) {
	for _, s := range []string{metadataIdentityGolden, metadataHeaderGolden, metadataManifestGolden, metadataCurrentGolden} {
		f.Add(metadataHex(f, s))
	}
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, MaxMetadataBytes+1))
	f.Fuzz(func(t *testing.T, b []byte) {
		candidates := [][]byte{b}
		// Repair checksums as a second path so mutations also exercise canonical
		// payload and field validation rather than stopping at the outer hash.
		if len(b) >= 40 && len(b) <= MaxMetadataBytes {
			candidates = append(candidates, metadataResign(b))
		}
		if len(b) == SegmentHeaderSize {
			header := bytes.Clone(b)
			binary.LittleEndian.PutUint32(header[88:92], crc32.Checksum(header[:88], crcTable))
			candidates = append(candidates, header)
		}
		for _, candidate := range candidates {
			before := bytes.Clone(candidate)
			if value, err := DecodeIdentity(candidate); err == nil {
				out, err := EncodeIdentity(value)
				if err != nil || !bytes.Equal(out, candidate) {
					t.Fatalf("identity success not canonical: %v", err)
				}
			}
			if value, err := DecodeSegmentHeader(candidate); err == nil {
				out, err := EncodeSegmentHeader(value)
				if err != nil || !bytes.Equal(out, candidate) {
					t.Fatalf("header success not canonical: %v", err)
				}
			}
			if value, err := DecodeManifest(candidate); err == nil {
				out, err := EncodeManifest(value)
				if err != nil || !bytes.Equal(out, candidate) {
					t.Fatalf("manifest success not canonical: %v", err)
				}
			}
			if value, err := DecodeCurrent(candidate); err == nil {
				out, err := EncodeCurrent(value)
				if err != nil || !bytes.Equal(out, candidate) {
					t.Fatalf("current success not canonical: %v", err)
				}
			}
			if !bytes.Equal(candidate, before) {
				t.Fatal("decoder changed input")
			}
		}
	})
}
