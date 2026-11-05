package codec_test

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
)

// These vectors pin experimental behavior for review, not a stable disk ABI.
func TestUintGolden(t *testing.T) {
	for _, tc := range []struct {
		value uint64
		wire  []byte
	}{
		{0, []byte{0}}, {1, []byte{1}}, {127, []byte{0x7f}},
		{128, []byte{0x80, 1}}, {300, []byte{0xac, 2}},
		{16384, []byte{0x80, 0x80, 1}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}},
	} {
		got := codec.AppendUint([]byte{0x55}, tc.value)
		if !bytes.Equal(got, append([]byte{0x55}, tc.wire...)) {
			t.Fatalf("%d: %x", tc.value, got)
		}
		d := codec.NewDecoder(tc.wire)
		if v, err := d.Uint(); err != nil || v != tc.value || d.Done() != nil {
			t.Fatalf("%d: %d %v", tc.value, v, err)
		}
	}
}

func TestUintRejectsAndDoesNotAdvance(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
		want error
	}{
		{"empty", nil, codec.ErrTruncated}, {"unfinished", []byte{0x80}, codec.ErrTruncated},
		{"overlong zero", []byte{0x80, 0}, codec.ErrNonCanonical},
		{"overlong one", []byte{0x81, 0}, codec.ErrNonCanonical},
		{"overlong 128", []byte{0x80, 0x81, 0}, codec.ErrNonCanonical},
		{"overflow tenth byte", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}, codec.ErrIntegerOverflow},
		{"overflow unfinished tenth byte", bytes.Repeat([]byte{0x80}, 10), codec.ErrIntegerOverflow},
		{"overflow eleven bytes", []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0}, codec.ErrIntegerOverflow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := codec.NewDecoder(append([]byte{7}, tc.wire...))
			_, _ = d.Uint()
			before := d.Remaining()
			for i := 0; i < 2; i++ {
				if _, err := d.Uint(); !errors.Is(err, tc.want) || d.Remaining() != before {
					t.Fatalf("err=%v remaining=%d want=%d", err, d.Remaining(), before)
				}
			}
		})
	}
}

func TestBytesGoldenAndLimits(t *testing.T) {
	for _, tc := range []struct{ value, wire []byte }{
		{nil, []byte{0}}, {[]byte{}, []byte{0}}, {[]byte{0, 0xff, 1}, []byte{3, 0, 0xff, 1}},
	} {
		got, err := codec.AppendBytes([]byte{9}, tc.value)
		if err != nil || !bytes.Equal(got, append([]byte{9}, tc.wire...)) {
			t.Fatalf("got %x %v", got, err)
		}
		d := codec.NewDecoder(tc.wire)
		value, err := d.Bytes(uint64(len(tc.value)))
		if err != nil || !bytes.Equal(value, tc.value) || d.Done() != nil {
			t.Fatalf("decode %x %v", value, err)
		}
	}
	maximum := bytes.Repeat([]byte{0xa5}, codec.MaxByteString)
	encoded, err := codec.AppendBytes(nil, maximum)
	// 16 MiB = 2^24, hence little-endian base-128 digits 0,0,0,8.
	if err != nil || !bytes.Equal(encoded[:4], []byte{0x80, 0x80, 0x80, 8}) {
		t.Fatalf("maximum prefix %v", err)
	}
	d := codec.NewDecoder(encoded)
	got, err := d.Bytes(codec.MaxByteString)
	if err != nil || !bytes.Equal(got, maximum) || d.Done() != nil {
		t.Fatal("maximum roundtrip", err)
	}
	dst := []byte{4, 5}
	got, err = codec.AppendBytes(dst, make([]byte, codec.MaxByteString+1))
	if !errors.Is(err, codec.ErrLengthOverflow) || !bytes.Equal(got, dst) {
		t.Fatal("overflow mutated destination", err)
	}
}

func TestDecoderBytesErrorsAreAtomic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		wire  []byte
		limit uint64
		want  error
	}{
		{"missing length", nil, 10, codec.ErrTruncated},
		{"noncanonical length", []byte{0x80, 0}, 10, codec.ErrNonCanonical},
		{"missing payload", []byte{2, 1}, 10, codec.ErrTruncated},
		{"caller limit", []byte{2, 1, 2}, 1, codec.ErrLengthOverflow},
		{"global limit before payload", []byte{0x81, 0x80, 0x80, 8}, math.MaxUint64, codec.ErrLengthOverflow},
		{"uint64 length before conversion", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}, math.MaxUint64, codec.ErrLengthOverflow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := codec.NewDecoder(append([]byte{3}, tc.wire...))
			_, _ = d.Uint()
			before := d.Remaining()
			for i := 0; i < 2; i++ {
				if _, err := d.Bytes(tc.limit); !errors.Is(err, tc.want) || d.Remaining() != before {
					t.Fatalf("%v remaining %d", err, d.Remaining())
				}
			}
		})
	}
}

func TestStringsUTF8AndTrailingData(t *testing.T) {
	got, err := codec.AppendString(nil, "中\x00")
	if err != nil || !bytes.Equal(got, []byte{4, 0xe4, 0xb8, 0xad, 0}) {
		t.Fatalf("golden %x %v", got, err)
	}
	d := codec.NewDecoder(got)
	value, err := d.String(4)
	if err != nil || value != "中\x00" || d.Done() != nil {
		t.Fatal(value, err)
	}
	dst := []byte{7}
	got, err = codec.AppendString(dst, string([]byte{0xff}))
	if !errors.Is(err, codec.ErrInvalidUTF8) || !bytes.Equal(got, dst) {
		t.Fatal("invalid UTF8 encoded")
	}
	d = codec.NewDecoder([]byte{1, 0xff, 3})
	before := d.Remaining()
	if _, err = d.String(10); !errors.Is(err, codec.ErrInvalidUTF8) || d.Remaining() != before {
		t.Fatal("UTF8 failure advanced", err)
	}
	if !errors.Is(d.Done(), codec.ErrTrailingData) {
		t.Fatal("unread data accepted")
	}
	if raw, err := d.Bytes(10); err != nil || !bytes.Equal(raw, []byte{0xff}) {
		t.Fatal("bytes must permit arbitrary data")
	}
	if !errors.Is(d.Done(), codec.ErrTrailingData) {
		t.Fatal("trailing byte accepted")
	}
}

func FuzzDecoder(f *testing.F) {
	for _, seed := range [][]byte{nil, {0}, {0x80}, {0x80, 0}, {1, 0xff}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}} {
		f.Add(seed, uint64(32))
	}
	f.Fuzz(func(t *testing.T, data []byte, limit uint64) {
		for mode := 0; mode < 3; mode++ {
			d := codec.NewDecoder(data)
			before := d.Remaining()
			var err error
			switch mode {
			case 0:
				var v uint64
				v, err = d.Uint()
				if err == nil {
					consumed := len(data) - d.Remaining()
					if !bytes.Equal(codec.AppendUint(nil, v), data[:consumed]) {
						t.Fatal("noncanonical integer accepted")
					}
				}
			case 1:
				var v []byte
				v, err = d.Bytes(limit)
				if err == nil {
					encoded, e := codec.AppendBytes(nil, v)
					if e != nil || !bytes.Equal(encoded, data[:len(data)-d.Remaining()]) {
						t.Fatal("noncanonical bytes accepted")
					}
				}
			case 2:
				var v string
				v, err = d.String(limit)
				if err == nil {
					encoded, e := codec.AppendString(nil, v)
					if e != nil || !bytes.Equal(encoded, data[:len(data)-d.Remaining()]) {
						t.Fatal("noncanonical string accepted")
					}
				}
			}
			if d.Remaining() < 0 || d.Remaining() > before || (err != nil && d.Remaining() != before) {
				t.Fatal("invalid decoder position")
			}
			if (d.Done() == nil) != (d.Remaining() == 0) {
				t.Fatal("incorrect Done")
			}
		}
	})
}
