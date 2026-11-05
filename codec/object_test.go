package codec_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Independently generated with Python hashlib and a handwritten base-128
// encoder: no Go codec or object implementation generated these expectations.
const objectGoldenHex = "4154454f424a303101000c6578616d706c652e6b696e640973706163652de4b8adac02022001010101010101010101010101010101010101010101010101010101010101012002020202020202020202020202020202020202020202020202020202020202020400ff4100"
const objectGoldenID model.ObjectID = "2e71f5c6b11ff1bc17b58c8015aea0a002c7109295493edc1c253f74d2836def"

func goldenObject() codec.Object {
	return codec.Object{
		Type: "example.kind", Space: "space-中", Version: 300,
		Refs:    []model.ObjectID{model.ObjectID(strings.Repeat("01", 32)), model.ObjectID(strings.Repeat("02", 32))},
		Payload: []byte{0, 0xff, 'A', 0},
	}
}

func TestObjectGolden(t *testing.T) {
	wire, err := hex.DecodeString(objectGoldenHex)
	if err != nil {
		t.Fatal(err)
	}
	o := goldenObject()
	encoded, err := codec.EncodeObject(o)
	if err != nil || !bytes.Equal(encoded, wire) {
		t.Fatalf("golden encoding: %x, %v", encoded, err)
	}
	decoded, err := codec.DecodeObject(wire)
	if err != nil || !reflect.DeepEqual(decoded, o) {
		t.Fatalf("golden decoding: %#v, %v", decoded, err)
	}
	id, err := codec.ObjectID(o)
	if err != nil || id != objectGoldenID {
		t.Fatalf("golden identity: %s, %v", id, err)
	}
	verified, err := codec.VerifyObject(objectGoldenID, wire)
	if err != nil || !reflect.DeepEqual(verified, o) {
		t.Fatalf("golden verification: %#v, %v", verified, err)
	}
	// Pin both the domain separator and inclusion of the complete envelope.
	if rawObjectID(wire) != objectGoldenID {
		t.Fatal("independent SHA-256 disagrees")
	}
}

func TestObjectIdentityBindsEveryField(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*codec.Object)
	}{
		{"type", func(o *codec.Object) { o.Type += "2" }},
		{"space", func(o *codec.Object) { o.Space += "2" }},
		{"version", func(o *codec.Object) { o.Version++ }},
		{"references", func(o *codec.Object) { o.Refs[1] = model.ObjectID(strings.Repeat("03", 32)) }},
		{"reference count", func(o *codec.Object) { o.Refs = o.Refs[:1] }},
		{"payload", func(o *codec.Object) { o.Payload[0]++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := goldenObject()
			tc.change(&o)
			id, err := codec.ObjectID(o)
			if err != nil || id == objectGoldenID {
				t.Fatalf("field not bound: %s, %v", id, err)
			}
			wire, err := codec.EncodeObject(o)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := codec.VerifyObject(objectGoldenID, wire); !errors.Is(err, codec.ErrObjectDigest) {
				t.Fatalf("old identity accepted: %v", err)
			}
			if _, err := codec.VerifyObject(id, wire); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestObjectUnknownPayloadSchemasAndExactStrings(t *testing.T) {
	var ids []model.ObjectID
	for _, text := range []string{"é", "e\u0301", "e\x00"} {
		o := codec.Object{Type: "unknown/" + text, Space: model.SpaceID(text), Version: math.MaxUint64}
		wire, err := codec.EncodeObject(o)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.DecodeObject(wire)
		if err != nil || got.Type != o.Type || got.Space != o.Space || got.Version != o.Version {
			t.Fatalf("strings normalized or unknown schema rejected: %#v %v", got, err)
		}
		id, err := codec.ObjectID(o)
		if err != nil {
			t.Fatal(err)
		}
		for _, other := range ids {
			if id == other {
				t.Fatal("distinct UTF-8 strings share identity")
			}
		}
		ids = append(ids, id)
	}
}

func TestObjectDecodedValuesOwnTheirMemory(t *testing.T) {
	o := goldenObject()
	wire, _ := codec.EncodeObject(o)
	original := bytes.Clone(wire)
	first, err := codec.DecodeObject(wire)
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.VerifyObject(objectGoldenID, wire)
	if err != nil {
		t.Fatal(err)
	}
	first.Payload[0] = 42
	first.Refs[0] = first.Refs[1]
	if !bytes.Equal(wire, original) || !reflect.DeepEqual(second, o) {
		t.Fatal("decoded results alias input or each other")
	}
	for i := range wire {
		wire[i] = 0xff
	}
	if !reflect.DeepEqual(second, o) {
		t.Fatal("decoded strings, references or payload borrow input")
	}
}

func TestObjectEncodeRejectsInvalidValues(t *testing.T) {
	zero := model.ObjectID(strings.Repeat("0", 64))
	for _, tc := range []struct {
		name   string
		change func(*codec.Object)
		want   error
		cause  error
	}{
		{"empty type", func(o *codec.Object) { o.Type = "" }, codec.ErrInvalidObject, nil},
		{"long type", func(o *codec.Object) { o.Type = strings.Repeat("t", 129) }, codec.ErrObjectLimit, nil},
		{"invalid type UTF-8", func(o *codec.Object) { o.Type = "\xff" }, codec.ErrInvalidObject, codec.ErrInvalidUTF8},
		{"empty space", func(o *codec.Object) { o.Space = "" }, codec.ErrInvalidObject, nil},
		{"long space", func(o *codec.Object) { o.Space = model.SpaceID(strings.Repeat("s", model.MaxIdentityBytes+1)) }, codec.ErrObjectLimit, nil},
		{"invalid space UTF-8", func(o *codec.Object) { o.Space = "\xff" }, codec.ErrInvalidObject, codec.ErrInvalidUTF8},
		{"zero version", func(o *codec.Object) { o.Version = 0 }, codec.ErrInvalidObject, nil},
		{"empty reference", func(o *codec.Object) { o.Refs[0] = "" }, codec.ErrInvalidObject, nil},
		{"zero reference", func(o *codec.Object) { o.Refs[0] = zero }, codec.ErrInvalidObject, nil},
		{"short reference", func(o *codec.Object) { o.Refs[0] = "01" }, codec.ErrInvalidObject, nil},
		{"uppercase reference", func(o *codec.Object) { o.Refs[0] = model.ObjectID(strings.Repeat("AB", 32)) }, codec.ErrInvalidObject, nil},
		{"nonhex reference", func(o *codec.Object) { o.Refs[0] = model.ObjectID(strings.Repeat("gg", 32)) }, codec.ErrInvalidObject, nil},
		{"duplicate references", func(o *codec.Object) { o.Refs[1] = o.Refs[0] }, codec.ErrInvalidObject, nil},
		{"descending references", func(o *codec.Object) { o.Refs[0], o.Refs[1] = o.Refs[1], o.Refs[0] }, codec.ErrInvalidObject, nil},
		{"too many references", func(o *codec.Object) { o.Refs = make([]model.ObjectID, codec.MaxObjectRefs+1) }, codec.ErrObjectLimit, nil},
		{"large payload", func(o *codec.Object) { o.Payload = make([]byte, codec.MaxObjectPayload+1) }, codec.ErrObjectLimit, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := goldenObject()
			tc.change(&o)
			refs := append([]model.ObjectID(nil), o.Refs...)
			if encoded, err := codec.EncodeObject(o); encoded != nil || !errors.Is(err, tc.want) || (tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("encoded=%x err=%v", encoded, err)
			}
			if !reflect.DeepEqual(o.Refs, refs) {
				t.Fatal("encoder normalized references")
			}
			if id, err := codec.ObjectID(o); id != "" || !errors.Is(err, tc.want) {
				t.Fatalf("invalid object received identity %s, %v", id, err)
			}
		})
	}
}

func TestObjectLimitsInclusive(t *testing.T) {
	refs := make([]model.ObjectID, codec.MaxObjectRefs)
	for i := range refs {
		refs[i] = model.ObjectID(fmt.Sprintf("%064x", i+1))
	}
	o := codec.Object{
		Type: strings.Repeat("t", 128), Space: model.SpaceID(strings.Repeat("s", model.MaxIdentityBytes)),
		Version: math.MaxUint64, Refs: refs, Payload: bytes.Repeat([]byte{0xa5}, codec.MaxObjectPayload),
	}
	wire, err := codec.EncodeObject(o)
	if err != nil || len(wire) > codec.MaxEncodedObject {
		t.Fatalf("maximum encoding: %v", err)
	}
	got, err := codec.DecodeObject(wire)
	if err != nil || !reflect.DeepEqual(got, o) {
		t.Fatalf("maximum decoding: %v", err)
	}
	id := rawObjectID(wire)
	if _, err := codec.VerifyObject(id, wire); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.DecodeObject(make([]byte, codec.MaxEncodedObject+1)); !errors.Is(err, codec.ErrObjectLimit) {
		t.Fatalf("encoded limit: %v", err)
	}
}

func TestObjectDecodeRejectsMalformedEvenWithRecomputedDigest(t *testing.T) {
	valid := wireObject(1, 0, []byte("t"), []byte("s"), 1, nil, nil)
	ref1, ref2 := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	prefix := bytes.Clone(valid[:len(valid)-2]) // through version; replace count and payload
	longType := wireObject(1, 0, bytes.Repeat([]byte{'t'}, 129), []byte("s"), 1, nil, nil)
	for _, tc := range []struct {
		name  string
		wire  []byte
		want  error
		cause error
	}{
		{"empty", nil, codec.ErrInvalidObject, codec.ErrTruncated},
		{"bad magic", append([]byte("BADOBJ01"), valid[8:]...), codec.ErrInvalidObject, nil},
		{"missing schema", []byte("ATEOBJ01"), codec.ErrInvalidObject, codec.ErrTruncated},
		{"schema zero", wireObject(0, 0, []byte("t"), []byte("s"), 1, nil, nil), codec.ErrObjectUnsupported, nil},
		{"schema two", wireObject(2, 0, []byte("t"), []byte("s"), 1, nil, nil), codec.ErrObjectUnsupported, nil},
		{"flags", wireObject(1, 1, []byte("t"), []byte("s"), 1, nil, nil), codec.ErrObjectUnsupported, nil},
		{"noncanonical schema", append([]byte("ATEOBJ01\x81\x00"), valid[9:]...), codec.ErrInvalidObject, codec.ErrNonCanonical},
		{"noncanonical flags", append([]byte("ATEOBJ01\x01\x80\x00"), valid[10:]...), codec.ErrInvalidObject, codec.ErrNonCanonical},
		{"overflow schema", append([]byte("ATEOBJ01"), bytes.Repeat([]byte{0xff}, 11)...), codec.ErrInvalidObject, codec.ErrIntegerOverflow},
		{"empty type", wireObject(1, 0, nil, []byte("s"), 1, nil, nil), codec.ErrInvalidObject, nil},
		{"long type", longType, codec.ErrObjectLimit, codec.ErrLengthOverflow},
		{"bad type UTF-8", wireObject(1, 0, []byte{0xff}, []byte("s"), 1, nil, nil), codec.ErrInvalidObject, codec.ErrInvalidUTF8},
		{"empty space", wireObject(1, 0, []byte("t"), nil, 1, nil, nil), codec.ErrInvalidObject, nil},
		{"long space", wireObject(1, 0, []byte("t"), bytes.Repeat([]byte{'s'}, model.MaxIdentityBytes+1), 1, nil, nil), codec.ErrObjectLimit, codec.ErrLengthOverflow},
		{"bad space UTF-8", wireObject(1, 0, []byte("t"), []byte{0xff}, 1, nil, nil), codec.ErrInvalidObject, codec.ErrInvalidUTF8},
		{"zero version", wireObject(1, 0, []byte("t"), []byte("s"), 0, nil, nil), codec.ErrInvalidObject, nil},
		{"enormous reference count", codec.AppendUint(prefix, math.MaxUint64), codec.ErrObjectLimit, nil},
		{"over limit reference count", codec.AppendUint(bytes.Clone(prefix), codec.MaxObjectRefs+1), codec.ErrObjectLimit, nil},
		{"missing references", append(bytes.Clone(prefix), 1, 0), codec.ErrInvalidObject, codec.ErrTruncated},
		{"short reference", wireObject(1, 0, []byte("t"), []byte("s"), 1, [][]byte{bytes.Repeat([]byte{1}, 31)}, []byte{1}), codec.ErrInvalidObject, nil},
		{"long reference", wireObject(1, 0, []byte("t"), []byte("s"), 1, [][]byte{bytes.Repeat([]byte{1}, 33)}, nil), codec.ErrObjectLimit, codec.ErrLengthOverflow},
		{"zero reference", wireObject(1, 0, []byte("t"), []byte("s"), 1, [][]byte{make([]byte, 32)}, nil), codec.ErrInvalidObject, nil},
		{"duplicate references", wireObject(1, 0, []byte("t"), []byte("s"), 1, [][]byte{ref1, ref1}, nil), codec.ErrInvalidObject, nil},
		{"unordered references", wireObject(1, 0, []byte("t"), []byte("s"), 1, [][]byte{ref2, ref1}, nil), codec.ErrInvalidObject, nil},
		{"large declared payload", codec.AppendUint(bytes.Clone(valid[:len(valid)-1]), codec.MaxObjectPayload+1), codec.ErrObjectLimit, codec.ErrLengthOverflow},
		{"enormous declared payload", codec.AppendUint(bytes.Clone(valid[:len(valid)-1]), math.MaxUint64), codec.ErrObjectLimit, codec.ErrLengthOverflow},
		{"missing payload bytes", append(bytes.Clone(valid[:len(valid)-1]), 2, 1), codec.ErrInvalidObject, codec.ErrTruncated},
		{"noncanonical payload length", append(bytes.Clone(valid[:len(valid)-1]), 0x80, 0), codec.ErrInvalidObject, codec.ErrNonCanonical},
		{"trailing byte", append(bytes.Clone(valid), 0), codec.ErrInvalidObject, codec.ErrTrailingData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, verify := range []bool{false, true} {
				var err error
				if verify {
					_, err = codec.VerifyObject(rawObjectID(tc.wire), tc.wire)
				} else {
					_, err = codec.DecodeObject(tc.wire)
				}
				if !errors.Is(err, tc.want) || (tc.cause != nil && !errors.Is(err, tc.cause)) {
					t.Fatalf("verify=%t err=%v, want %v (cause %v)", verify, err, tc.want, tc.cause)
				}
			}
		})
	}
	wire, _ := hex.DecodeString(objectGoldenHex)
	for n := 0; n < len(wire); n++ {
		truncated := wire[:n]
		if _, err := codec.VerifyObject(rawObjectID(truncated), truncated); err == nil {
			t.Fatalf("truncation accepted at %d", n)
		}
	}
	for _, id := range []model.ObjectID{"", "not-an-id", model.ObjectID(strings.ToUpper(string(objectGoldenID))), model.ObjectID(strings.Repeat("0", 64))} {
		if _, err := codec.VerifyObject(id, wire); !errors.Is(err, codec.ErrObjectDigest) {
			t.Fatalf("wrong external identity %q: %v", id, err)
		}
	}
}

func FuzzObjectEnvelope(f *testing.F) {
	golden, _ := hex.DecodeString(objectGoldenHex)
	for _, seed := range [][]byte{nil, []byte("ATEOBJ01"), golden, append(bytes.Clone(golden), 0), wireObject(1, 0, []byte("t"), []byte("s"), 1, nil, nil), []byte("ATEOBJ01\x81\x00")} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		o, err := codec.DecodeObject(wire)
		// Recompute the external identity even for malformed inputs. A matching
		// digest must never bypass canonical, schema, or resource validation.
		id := rawObjectID(wire)
		verified, verifyErr := codec.VerifyObject(id, wire)
		if (err == nil) != (verifyErr == nil) {
			t.Fatalf("recomputed digest bypassed validation: %v / %v", err, verifyErr)
		}
		if err != nil {
			return
		}
		encoded, err := codec.EncodeObject(o)
		if err != nil || !bytes.Equal(encoded, wire) || !reflect.DeepEqual(verified, o) {
			t.Fatalf("accepted noncanonical envelope: %v", err)
		}
		gotID, err := codec.ObjectID(o)
		if err != nil || gotID != id {
			t.Fatalf("identity mismatch: %s != %s, %v", gotID, id, err)
		}
	})
}

func rawObjectID(wire []byte) model.ObjectID {
	h := sha256.New()
	_, _ = h.Write([]byte("ATE-OBJECT\x00"))
	_, _ = h.Write(wire)
	return model.ObjectID(hex.EncodeToString(h.Sum(nil)))
}

// wireObject deliberately does not enforce semantic constraints, so malformed
// tests do not rely on the encoder under test to construct their inputs.
func wireObject(schema, flags uint64, typ, space []byte, version uint64, refs [][]byte, payload []byte) []byte {
	wire := []byte("ATEOBJ01")
	wire = codec.AppendUint(wire, schema)
	wire = codec.AppendUint(wire, flags)
	wire, _ = codec.AppendBytes(wire, typ)
	wire, _ = codec.AppendBytes(wire, space)
	wire = codec.AppendUint(wire, version)
	wire = codec.AppendUint(wire, uint64(len(refs)))
	for _, ref := range refs {
		wire, _ = codec.AppendBytes(wire, ref)
	}
	wire, _ = codec.AppendBytes(wire, payload)
	return wire
}
