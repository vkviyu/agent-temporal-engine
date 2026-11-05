// Package codec implements experimental ATE-CANON-1 primitives. Integers use
// shortest unsigned varints; byte strings have a varint byte-length prefix.
// Schemas explicitly order their fields; Go reflection and wire JSON are not
// canonical encodings. No on-disk compatibility promise is made in v0.
package codec

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"
)

const MaxByteString = 16 << 20

var (
	ErrLengthOverflow  = errors.New("canonical byte string exceeds limit")
	ErrTruncated       = errors.New("truncated canonical value")
	ErrNonCanonical    = errors.New("non-canonical encoding")
	ErrIntegerOverflow = errors.New("canonical integer overflow")
	ErrInvalidUTF8     = errors.New("invalid UTF-8 string")
	ErrTrailingData    = errors.New("trailing canonical data")
)

func AppendUint(dst []byte, value uint64) []byte { return binary.AppendUvarint(dst, value) }

func AppendBytes(dst []byte, value []byte) ([]byte, error) {
	if len(value) > MaxByteString {
		return dst, ErrLengthOverflow
	}
	dst = AppendUint(dst, uint64(len(value)))
	return append(dst, value...), nil
}

func AppendString(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return dst, ErrInvalidUTF8
	}
	return AppendBytes(dst, []byte(value))
}

// Decoder borrows immutable input. Bytes returns a view of that input; callers
// retaining or modifying a decoded value must clone it. Methods do not advance
// on failure and check lengths before converting them to int.
type Decoder struct {
	data   []byte
	offset int
}

func NewDecoder(data []byte) *Decoder { return &Decoder{data: data} }
func (d *Decoder) Remaining() int     { return len(d.data) - d.offset }
func (d *Decoder) Done() error {
	if d.Remaining() != 0 {
		return ErrTrailingData
	}
	return nil
}

func (d *Decoder) Uint() (uint64, error) {
	v, n := binary.Uvarint(d.data[d.offset:])
	if n == 0 {
		if d.Remaining() >= binary.MaxVarintLen64 {
			return 0, ErrIntegerOverflow
		}
		return 0, ErrTruncated
	}
	if n < 0 {
		return 0, ErrIntegerOverflow
	}
	if n > 1 && d.data[d.offset+n-1] == 0 {
		return 0, ErrNonCanonical
	}
	d.offset += n
	return v, nil
}

func (d *Decoder) Bytes(limit uint64) ([]byte, error) {
	start := d.offset
	n, err := d.Uint()
	if err != nil {
		return nil, err
	}
	if n > MaxByteString || n > limit {
		d.offset = start
		return nil, ErrLengthOverflow
	}
	if n > uint64(d.Remaining()) {
		d.offset = start
		return nil, ErrTruncated
	}
	value := d.data[d.offset : d.offset+int(n)]
	d.offset += int(n)
	return value, nil
}

func (d *Decoder) String(limit uint64) (string, error) {
	start := d.offset
	value, err := d.Bytes(limit)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(value) {
		d.offset = start
		return "", ErrInvalidUTF8
	}
	return string(value), nil
}
