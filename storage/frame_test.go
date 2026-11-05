package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const testFrameID model.TxID = "00112233445566778899aabbccddeeff"

// Generated independently with a reflected bitwise CRC32C (polynomial
// 0x82f63b78, initial/final xor 0xffffffff), little-endian struct packing.
// This is an experimental regression vector, not a frozen storage ABI.
const testFrameGolden = "41544546524d30310100020000000000300000000300000000112233445566778899aabbccddeeff070000008721f1ec61626300000000003a3dfa5d454e4421"

func goldenFrame(t testing.TB) []byte {
	t.Helper()
	b, err := hex.DecodeString(testFrameGolden)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFrameGolden(t *testing.T) {
	want := goldenFrame(t)
	frame := Frame{Kind: KindObject, TxID: testFrameID, Ordinal: 7, Payload: []byte("abc")}
	got, err := EncodeFrame(frame, Limits{})
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got %x %v, want %x", got, err, want)
	}
	decoded, n, err := ReadFrame(bytes.NewReader(want), Limits{})
	if err != nil || n != int64(len(want)) || !reflect.DeepEqual(frame, decoded) {
		t.Fatalf("decoded %+v %d %v", decoded, n, err)
	}
}

func TestFrameAllKindsAndPadding(t *testing.T) {
	for kind := KindBegin; kind <= KindCommit; kind++ {
		for size := 0; size <= 16; size++ {
			payload := bytes.Repeat([]byte{byte(size)}, size)
			frame := Frame{Kind: kind, TxID: testFrameID, Ordinal: 0xffffffff, Payload: payload}
			encoded, err := EncodeFrame(frame, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			wantSize := HeaderSize + size + (8-size%8)%8 + TrailerSize
			if len(encoded) != wantSize {
				t.Fatalf("kind=%d size=%d total=%d", kind, size, len(encoded))
			}
			decoded, n, err := ReadFrame(bytes.NewReader(encoded), Limits{})
			if err != nil || n != int64(wantSize) || decoded.Kind != kind || decoded.TxID != frame.TxID || decoded.Ordinal != frame.Ordinal || !bytes.Equal(decoded.Payload, payload) {
				t.Fatalf("kind=%d size=%d: %+v %d %v", kind, size, decoded, n, err)
			}
			if cap(decoded.Payload) != len(decoded.Payload) {
				t.Fatal("payload exposes padding/trailer capacity")
			}
			if size > 0 {
				encoded[HeaderSize] ^= 1
				if !bytes.Equal(decoded.Payload, payload) {
					t.Fatal("decoded payload aliases input")
				}
			}
		}
	}
}

func TestFrameEveryTruncation(t *testing.T) {
	full := goldenFrame(t)
	for end := 0; end < len(full); end++ {
		_, n, err := ReadFrame(bytes.NewReader(full[:end]), Limits{})
		want := ErrIncomplete
		if end == 0 {
			want = io.EOF
		}
		if !errors.Is(err, want) || n != int64(end) {
			t.Fatalf("cut=%d consumed=%d err=%v want=%v", end, n, err, want)
		}
	}
	r := bytes.NewReader(append(append([]byte{}, full...), full...))
	for i := 0; i < 2; i++ {
		if _, n, err := ReadFrame(r, Limits{}); err != nil || n != int64(len(full)) {
			t.Fatal("concatenated frame", n, err)
		}
	}
	if _, n, err := ReadFrame(r, Limits{}); err != io.EOF || n != 0 {
		t.Fatal("clean EOF", n, err)
	}
}

func repairFrameHeader(b []byte) {
	binary.LittleEndian.PutUint32(b[44:48], crc32.Checksum(b[:44], crcTable))
}
func repairFrameRecord(b []byte) {
	end := len(b) - TrailerSize
	binary.LittleEndian.PutUint32(b[end:end+4], crc32.Checksum(b[:end], crcTable))
}

func TestFrameRejectsCorruptionAndForgedHeaders(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func([]byte)
		want       error
		headerOnly bool
	}{
		{"magic", func(b []byte) { b[0] ^= 1 }, ErrMalformed, true},
		{"header CRC", func(b []byte) { b[40] ^= 1 }, ErrIntegrity, true},
		{"header CRC stored", func(b []byte) { b[44] ^= 1 }, ErrIntegrity, true},
		{"payload CRC", func(b []byte) { b[48] ^= 1 }, ErrIntegrity, false},
		{"record CRC stored", func(b []byte) { b[len(b)-8] ^= 1 }, ErrIntegrity, false},
		{"end magic", func(b []byte) { b[len(b)-1] ^= 1 }, ErrMalformed, false},
		{"version", func(b []byte) { binary.LittleEndian.PutUint16(b[8:10], 2); repairFrameHeader(b) }, ErrUnsupported, true},
		{"kind zero", func(b []byte) { binary.LittleEndian.PutUint16(b[10:12], 0); repairFrameHeader(b) }, ErrUnsupported, true},
		{"kind unknown", func(b []byte) { binary.LittleEndian.PutUint16(b[10:12], 65535); repairFrameHeader(b) }, ErrUnsupported, true},
		{"flags low", func(b []byte) { b[12] = 1; repairFrameHeader(b) }, ErrUnsupported, true},
		{"flags high", func(b []byte) { b[15] = 0x80; repairFrameHeader(b) }, ErrUnsupported, true},
		{"header length", func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], 47); repairFrameHeader(b) }, ErrMalformed, true},
		{"zero TxID", func(b []byte) { clear(b[24:40]); repairFrameHeader(b) }, ErrMalformed, true},
		{"nonzero padding with valid CRC", func(b []byte) { b[51] = 1; repairFrameRecord(b) }, ErrMalformed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := goldenFrame(t)
			tc.mutate(b)
			_, n, err := ReadFrame(bytes.NewReader(b), Limits{})
			expected := int64(len(b))
			if tc.headerOnly {
				expected = HeaderSize
			}
			if !errors.Is(err, tc.want) || n != expected {
				t.Fatalf("got consumed=%d err=%v want %d %v", n, err, expected, tc.want)
			}
		})
	}
}

type countedFrameReader struct {
	data                        []byte
	consumed, calls, maxRequest int
}

func (r *countedFrameReader) Read(p []byte) (int, error) {
	r.calls++
	if len(p) > r.maxRequest {
		r.maxRequest = len(p)
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	r.consumed += n
	return n, nil
}

func TestFrameLengthRejectedBeforeReadingPayload(t *testing.T) {
	for _, tc := range []struct {
		name      string
		length    uint32
		limits    Limits
		remaining uint64
		internal  bool
	}{
		{"builtin ceiling", MaxPayload + 1, Limits{}, 0, false},
		{"uint32 maximum", 0xffffffff, Limits{}, 0, false},
		{"caller ceiling", 3, Limits{MaxPayload: 2}, 0, false},
		{"transaction budget", 3, DefaultLimits(), 63, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := goldenFrame(t)
			binary.LittleEndian.PutUint32(b[20:24], tc.length)
			repairFrameHeader(b)
			r := &countedFrameReader{data: b}
			var n int64
			var err error
			if tc.internal {
				_, n, err = readFrame(r, tc.limits, tc.remaining)
			} else {
				_, n, err = ReadFrame(r, tc.limits)
			}
			if !errors.Is(err, ErrLimit) || n != HeaderSize || r.consumed != HeaderSize || r.calls != 1 || r.maxRequest != HeaderSize {
				t.Fatalf("n=%d err=%v reader=%+v", n, err, r)
			}
		})
	}
	for _, limits := range []Limits{{MaxPayload: MaxPayload + 1}, {MaxRecords: MaxRecords + 1}, {MaxTransactionBytes: MaxTransactionBytes + 1}} {
		r := &countedFrameReader{data: goldenFrame(t)}
		if _, n, err := ReadFrame(r, limits); !errors.Is(err, ErrLimit) || n != 0 || r.calls != 0 {
			t.Fatal("invalid limits read input", n, err)
		}
	}
}

func TestFrameEncoderRejectsInvalidInput(t *testing.T) {
	for _, f := range []Frame{
		{Kind: 0, TxID: testFrameID}, {Kind: 10, TxID: testFrameID},
		{Kind: KindObject, TxID: ""}, {Kind: KindObject, TxID: "00000000000000000000000000000000"},
		{Kind: KindObject, TxID: "00112233445566778899AABBCCDDEEFF"},
		{Kind: KindObject, TxID: "zz112233445566778899aabbccddeeff"},
	} {
		if b, err := EncodeFrame(f, Limits{}); err == nil || b != nil {
			t.Fatalf("invalid frame encoded %+v %v", f, err)
		}
	}
	if _, err := EncodeFrame(Frame{Kind: KindObject, TxID: testFrameID, Payload: []byte{1, 2}}, Limits{MaxPayload: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func FuzzFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("ATEFRM01"))
	f.Add(goldenFrame(f))
	f.Fuzz(func(t *testing.T, data []byte) {
		frame, n, err := ReadFrame(bytes.NewReader(data), Limits{MaxPayload: 1024})
		if n < 0 || n > int64(len(data)) {
			t.Fatal("invalid consumed count", n)
		}
		if err != nil {
			return
		}
		encoded, err := EncodeFrame(frame, Limits{MaxPayload: 1024})
		if err != nil {
			t.Fatal("decoded frame cannot encode", err)
		}
		if !bytes.Equal(encoded, data[:int(n)]) {
			t.Fatal("successful parse did not preserve canonical bytes")
		}
		again, n2, err := ReadFrame(bytes.NewReader(encoded), Limits{})
		if err != nil || n2 != n || !reflect.DeepEqual(frame, again) {
			t.Fatal("roundtrip mismatch", err)
		}
	})
}
