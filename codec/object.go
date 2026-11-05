package codec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	MaxObjectRefs    = 4096
	MaxObjectPayload = 8 << 20
	MaxEncodedObject = 16 << 20
	maxObjectType    = 128
	objectMagic      = "ATEOBJ01"
	objectDomain     = "ATE-OBJECT\x00"
)

var (
	ErrObjectUnsupported = errors.New("unsupported object envelope")
	ErrInvalidObject     = errors.New("invalid object")
	ErrObjectLimit       = errors.New("object exceeds limit")
	ErrObjectDigest      = errors.New("object digest mismatch")
)

// Object is a generic immutable object envelope. Type and Version identify a
// payload schema that callers must validate; this codec accepts unknown schemas.
// Refs must already be strictly increasing canonical nonzero ObjectIDs.
// The caller owns all slices and must not mutate them during encoding.
type Object struct {
	Type    string
	Space   model.SpaceID
	Version uint64
	Refs    []model.ObjectID
	Payload []byte
}

// EncodeObject produces the canonical experimental envelope without changing o.
func EncodeObject(o Object) ([]byte, error) {
	if err := validateObject(o); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, len(objectMagic)+32+len(o.Payload)+33*len(o.Refs))
	encoded = append(encoded, objectMagic...)
	encoded = AppendUint(encoded, 1)
	encoded = AppendUint(encoded, 0)
	encoded, _ = AppendString(encoded, o.Type)
	encoded, _ = AppendString(encoded, string(o.Space))
	encoded = AppendUint(encoded, o.Version)
	encoded = AppendUint(encoded, uint64(len(o.Refs)))
	for _, ref := range o.Refs {
		digest, _ := hex.DecodeString(string(ref)) // validated above
		encoded, _ = AppendBytes(encoded, digest)
	}
	encoded, _ = AppendBytes(encoded, o.Payload)
	if len(encoded) > MaxEncodedObject {
		return nil, fmt.Errorf("%w: encoded bytes", ErrObjectLimit)
	}
	return encoded, nil
}

// DecodeObject rejects unsupported, malformed and noncanonical encodings. Its
// returned slices and strings are independent of encoded and of other results.
func DecodeObject(encoded []byte) (Object, error) {
	if len(encoded) > MaxEncodedObject {
		return Object{}, fmt.Errorf("%w: encoded bytes", ErrObjectLimit)
	}
	if len(encoded) < len(objectMagic) {
		return Object{}, objectDecodeError("magic", ErrTruncated)
	}
	if string(encoded[:len(objectMagic)]) != objectMagic {
		return Object{}, fmt.Errorf("%w: magic", ErrInvalidObject)
	}
	d := NewDecoder(encoded[len(objectMagic):])
	schema, err := d.Uint()
	if err != nil {
		return Object{}, objectDecodeError("schema", err)
	}
	if schema != 1 {
		return Object{}, fmt.Errorf("%w: schema %d", ErrObjectUnsupported, schema)
	}
	flags, err := d.Uint()
	if err != nil {
		return Object{}, objectDecodeError("flags", err)
	}
	if flags != 0 {
		return Object{}, fmt.Errorf("%w: flags %d", ErrObjectUnsupported, flags)
	}
	var o Object
	if o.Type, err = d.String(maxObjectType); err != nil {
		return Object{}, objectDecodeError("type", err)
	}
	space, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return Object{}, objectDecodeError("space", err)
	}
	o.Space = model.SpaceID(space)
	if o.Version, err = d.Uint(); err != nil {
		return Object{}, objectDecodeError("version", err)
	}
	count, err := d.Uint()
	if err != nil {
		return Object{}, objectDecodeError("reference count", err)
	}
	if count > MaxObjectRefs {
		return Object{}, fmt.Errorf("%w: reference count", ErrObjectLimit)
	}
	// Each reference needs a one-byte length and a 32-byte digest. Check before
	// allocating, while count is bounded and before converting it to int.
	if count > uint64(d.Remaining()/33) {
		return Object{}, objectDecodeError("references", ErrTruncated)
	}
	if count != 0 {
		o.Refs = make([]model.ObjectID, int(count))
	}
	for i := range o.Refs {
		digest, err := d.Bytes(sha256.Size)
		if err != nil {
			return Object{}, objectDecodeError("reference", err)
		}
		if len(digest) != sha256.Size {
			return Object{}, fmt.Errorf("%w: reference digest length", ErrInvalidObject)
		}
		o.Refs[i] = model.ObjectID(hex.EncodeToString(digest))
	}
	payload, err := d.Bytes(MaxObjectPayload)
	if err != nil {
		return Object{}, objectDecodeError("payload", err)
	}
	if err := d.Done(); err != nil {
		return Object{}, objectDecodeError("envelope", err)
	}
	o.Payload = bytes.Clone(payload)
	if err := validateObject(o); err != nil {
		return Object{}, err
	}
	return o, nil
}

// ObjectID hashes the domain separator and the entire canonical envelope.
func ObjectID(o Object) (model.ObjectID, error) {
	encoded, err := EncodeObject(o)
	if err != nil {
		return "", err
	}
	return encodedObjectID(encoded), nil
}

// VerifyObject validates the canonical envelope before comparing its digest to
// the external identity. There is no self-checksum trailer in the envelope.
func VerifyObject(id model.ObjectID, encoded []byte) (Object, error) {
	o, err := DecodeObject(encoded)
	if err != nil {
		return Object{}, err
	}
	if id != encodedObjectID(encoded) {
		return Object{}, ErrObjectDigest
	}
	return o, nil
}

func encodedObjectID(encoded []byte) model.ObjectID {
	h := sha256.New()
	_, _ = h.Write([]byte(objectDomain))
	_, _ = h.Write(encoded)
	return model.ObjectID(hex.EncodeToString(h.Sum(nil)))
}

func validateObject(o Object) error {
	for _, field := range []struct {
		name  string
		value string
		limit int
	}{{"type", o.Type, maxObjectType}, {"space", string(o.Space), model.MaxIdentityBytes}} {
		if len(field.value) > field.limit {
			return fmt.Errorf("%w: %s", ErrObjectLimit, field.name)
		}
		if field.value == "" {
			return fmt.Errorf("%w: empty %s", ErrInvalidObject, field.name)
		}
		if !utf8.ValidString(field.value) {
			return objectDecodeError(field.name, ErrInvalidUTF8)
		}
	}
	if o.Version == 0 {
		return fmt.Errorf("%w: zero version", ErrInvalidObject)
	}
	if len(o.Refs) > MaxObjectRefs {
		return fmt.Errorf("%w: reference count", ErrObjectLimit)
	}
	if len(o.Payload) > MaxObjectPayload {
		return fmt.Errorf("%w: payload", ErrObjectLimit)
	}
	for i, ref := range o.Refs {
		if !model.ValidDigest(string(ref)) {
			return fmt.Errorf("%w: reference %d", ErrInvalidObject, i)
		}
		if ref == "0000000000000000000000000000000000000000000000000000000000000000" {
			return fmt.Errorf("%w: zero reference %d", ErrInvalidObject, i)
		}
		if i != 0 && o.Refs[i-1] >= ref {
			return fmt.Errorf("%w: references must be strictly increasing", ErrInvalidObject)
		}
	}
	return nil
}

func objectDecodeError(field string, cause error) error {
	kind := ErrInvalidObject
	if errors.Is(cause, ErrLengthOverflow) {
		kind = ErrObjectLimit
	}
	return fmt.Errorf("%w: %s: %w", kind, field, cause)
}
