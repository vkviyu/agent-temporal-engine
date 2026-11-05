package engine

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Fixed vectors were computed with an independent Python unsigned-LEB128
// implementation and hashlib, rather than by round-tripping this codec.
func TestStateBatchGoldenAndCanonicalOrder(t *testing.T) {
	empty := model.ObjectID("")
	batch := api.StateBatch{ExpectedRoot: &empty, Operations: []api.StateMutation{
		{Key: []byte{0xff, 0}, Kind: api.MutationDelete, Condition: api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes}}},
		{Key: nil, Kind: api.MutationPut, Value: api.Value{Kind: api.ValueNull}, Condition: api.ValueCondition{Kind: api.ConditionAbsent}},
	}}
	const golden = "01000100020001000102ff0002020100"
	wire, err := EncodeStateBatch(batch)
	if err != nil || hex.EncodeToString(wire) != golden {
		t.Fatalf("golden: %x %v", wire, err)
	}
	if !bytes.Equal(batch.Operations[0].Key, []byte{0xff, 0}) {
		t.Fatal("encoder sorted caller's slice")
	}
	decoded, err := DecodeStateBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ExpectedRoot == nil || *decoded.ExpectedRoot != "" || len(decoded.Operations[0].Key) != 0 || decoded.Operations[1].Condition.Value.Kind != api.ValueBytes {
		t.Fatalf("lost semantic distinction: %#v", decoded)
	}
	again, err := EncodeStateBatch(decoded)
	if err != nil || !bytes.Equal(again, wire) {
		t.Fatal("canonical roundtrip", err)
	}
	batch.ExpectedRoot = nil
	without, err := EncodeStateBatch(batch)
	if err != nil || bytes.Equal(without, wire) {
		t.Fatal("nil expected-root collapsed into empty-root condition", err)
	}
}

func TestStateBatchRejectsInvalidValues(t *testing.T) {
	valid := func() api.StateBatch {
		return api.StateBatch{Operations: []api.StateMutation{{Kind: api.MutationPut, Value: api.Value{Kind: api.ValueNull}}}}
	}
	for _, tc := range []struct {
		name   string
		change func(*api.StateBatch)
	}{
		{"empty", func(b *api.StateBatch) { b.Operations = nil }},
		{"too many", func(b *api.StateBatch) { b.Operations = make([]api.StateMutation, api.MaxStateMutations+1) }},
		{"long key", func(b *api.StateBatch) { b.Operations[0].Key = make([]byte, api.MaxStateKeyBytes+1) }},
		{"duplicate", func(b *api.StateBatch) { b.Operations = append(b.Operations, b.Operations[0]) }},
		{"unknown mutation", func(b *api.StateBatch) { b.Operations[0].Kind = "unknown" }},
		{"zero put value", func(b *api.StateBatch) { b.Operations[0].Value = api.Value{} }},
		{"unknown value", func(b *api.StateBatch) { b.Operations[0].Value.Kind = "unknown" }},
		{"null data", func(b *api.StateBatch) { b.Operations[0].Value.Data = []byte{1} }},
		{"large value", func(b *api.StateBatch) {
			b.Operations[0].Value = api.Value{Kind: api.ValueBytes, Data: make([]byte, api.MaxStateValueBytes+1)}
		}},
		{"delete value", func(b *api.StateBatch) { b.Operations[0].Kind = api.MutationDelete }},
		{"delete hidden data", func(b *api.StateBatch) {
			b.Operations[0].Kind = api.MutationDelete
			b.Operations[0].Value = api.Value{Data: []byte{1}}
		}},
		{"any value", func(b *api.StateBatch) { b.Operations[0].Condition.Value = api.Value{Kind: api.ValueNull} }},
		{"absent data", func(b *api.StateBatch) {
			b.Operations[0].Condition = api.ValueCondition{Kind: api.ConditionAbsent, Value: api.Value{Data: []byte{1}}}
		}},
		{"unknown condition", func(b *api.StateBatch) { b.Operations[0].Condition.Kind = "unknown" }},
		{"zero condition value", func(b *api.StateBatch) { b.Operations[0].Condition.Kind = api.ConditionValue }},
		{"bad root", func(b *api.StateBatch) { id := model.ObjectID(strings.Repeat("A", 64)); b.ExpectedRoot = &id }},
		{"zero digest root", func(b *api.StateBatch) { id := model.ObjectID(strings.Repeat("0", 64)); b.ExpectedRoot = &id }},
		{"batch size", func(b *api.StateBatch) {
			for i := 0; i < 5; i++ {
				b.Operations = append(b.Operations, api.StateMutation{Key: []byte{byte(i + 1)}, Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: make([]byte, api.MaxStateValueBytes)}})
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := valid()
			tc.change(&b)
			if wire, err := EncodeStateBatch(b); wire != nil || !errors.Is(err, ErrInvalidStateData) {
				t.Fatalf("%x %v", wire, err)
			}
		})
	}
}

func TestStateBatchRejectsMalformed(t *testing.T) {
	// Minimal valid batch is 01 00 00 01 00 02 00: schema, flags,
	// no expected root, one operation, empty key, delete, condition-any.
	for _, tc := range []struct {
		name, wire  string
		want, cause error
	}{
		{"schema", "02000001000200", ErrUnsupportedSchema, nil},
		{"flags", "01010001000200", ErrUnsupportedSchema, nil},
		{"overlong schema", "8100000001000200", ErrInvalidStateData, codec.ErrNonCanonical},
		{"root presence", "01000201000200", ErrInvalidStateData, nil},
		{"optional root presence", "0100010201000200", ErrInvalidStateData, nil},
		{"zero operations", "01000000", ErrInvalidStateData, nil},
		{"oversize count before allocation", "0100008108", ErrInvalidStateData, nil},
		{"huge count", "010000ffffffffffffffffff01", ErrInvalidStateData, nil},
		{"missing key", "01000001", ErrInvalidStateData, codec.ErrTruncated},
		{"unknown mutation", "01000001000000", ErrInvalidStateData, nil},
		{"unknown condition", "01000001000203", ErrInvalidStateData, nil},
		{"unknown value", "0100000100010200", ErrInvalidStateData, nil},
		{"trailing", "0100000100020000", ErrInvalidStateData, codec.ErrTrailingData},
		{"unsorted", "010000020162020001610200", ErrInvalidStateData, nil},
		{"duplicate", "010000020161020001610200", ErrInvalidStateData, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire, _ := hex.DecodeString(tc.wire)
			if _, err := DecodeStateBatch(wire); !errors.Is(err, tc.want) || (tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("%v", err)
			}
		})
	}
	if _, err := DecodeStateBatch(make([]byte, api.MaxStateBatchBytes+1)); !errors.Is(err, ErrInvalidStateData) || !errors.Is(err, codec.ErrLengthOverflow) {
		t.Fatal(err)
	}
}

func TestStateBatchLimitsAndOwnership(t *testing.T) {
	id := testID(1)
	b := api.StateBatch{ExpectedRoot: &id, Operations: []api.StateMutation{{Key: bytes.Repeat([]byte{0xff}, api.MaxStateKeyBytes), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: bytes.Repeat([]byte{1}, api.MaxStateValueBytes)}, Condition: api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte{2}}}}}}
	wire, err := EncodeStateBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStateBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(wire)
	got.Operations[0].Key[0] = 0
	got.Operations[0].Value.Data[0] = 0
	got.Operations[0].Condition.Value.Data[0] = 0
	*got.ExpectedRoot = testID(2)
	if !bytes.Equal(wire, original) || *b.ExpectedRoot != id {
		t.Fatal("decode aliases source")
	}
	again, err := DecodeStateBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	for i := range wire {
		wire[i] = 0
	}
	if !bytes.Equal(again.Operations[0].Value.Data, b.Operations[0].Value.Data) || !bytes.Equal(again.Operations[0].Key, b.Operations[0].Key) {
		t.Fatal("decoded values borrow input")
	}
	many := api.StateBatch{}
	for i := 0; i < api.MaxStateMutations; i++ {
		many.Operations = append(many.Operations, api.StateMutation{Key: []byte{byte(i >> 8), byte(i)}, Kind: api.MutationDelete})
	}
	encoded, err := EncodeStateBatch(many)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeStateBatch(encoded); err != nil || len(got.Operations) != api.MaxStateMutations {
		t.Fatal(err)
	}
}

func TestCoreValueKindsAndScopes(t *testing.T) {
	null, err := makeValue("s", api.Value{Kind: api.ValueNull})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := makeValue("s", api.Value{Kind: api.ValueBytes})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := codec.ObjectID(null)
	b, _ := codec.ObjectID(empty)
	if a == b {
		t.Fatal("null collapsed into empty bytes")
	}
	for _, space := range []model.SpaceID{"", "ate:system", "ate:custom", model.SpaceID(strings.Repeat("x", model.MaxIdentityBytes+1)), "\xff"} {
		if _, err := makeValue(space, api.Value{Kind: api.ValueNull}); !errors.Is(err, ErrInvalidStateData) {
			t.Fatalf("space %q: %v", space, err)
		}
	}
	if _, err := makeValue("s", api.Value{Kind: api.ValueNull, Data: []byte{1}}); !errors.Is(err, ErrInvalidStateData) {
		t.Fatal(err)
	}
	if _, err := makeValue("s", api.Value{Kind: api.ValueBytes, Data: make([]byte, api.MaxStateValueBytes+1)}); !errors.Is(err, ErrInvalidStateData) {
		t.Fatal(err)
	}
}

func TestCanonicalCommandDecodeAndOwnership(t *testing.T) {
	r := api.CommitRequest{Space: "s", Principal: "p", RequestID: "r", Method: api.MethodMutateState, Body: []byte{1, 2}, ExpectedFence: &model.CommitFence{Store: "store", LSN: 7, CommitDigest: model.CommitDigest(testID(1))}}
	wire, err := CanonicalCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCanonicalCommand(wire)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("%#v %v", got, err)
	}
	got.Body[0] = 9
	got.ExpectedFence.LSN = 99
	if r.Body[0] != 1 || r.ExpectedFence.LSN != 7 {
		t.Fatal("decoded command aliases source")
	}
	obj, err := makeCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	read, err := readCommand(obj)
	if err != nil || !reflect.DeepEqual(read, r) {
		t.Fatal(err)
	}
	for _, b := range [][]byte{nil, []byte("bad"), append(bytes.Clone(wire), 0), append([]byte("ATE-COMMAND\x00\x81\x00"), wire[13:]...)} {
		if _, err := decodeCanonicalCommand(b); !errors.Is(err, ErrInvalidStateData) {
			t.Fatal(err)
		}
	}
	unknown := bytes.Clone(wire)
	unknown[len("ATE-COMMAND\x00")] = 2
	if _, err := decodeCanonicalCommand(unknown); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatal(err)
	}
	for n := 0; n < len(wire); n++ {
		if _, err := decodeCanonicalCommand(wire[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	obj.Space = "other"
	if _, err := readCommand(obj); !errors.Is(err, ErrInvalidStateData) {
		t.Fatal(err)
	}
	r.Space = "ate:system"
	if _, err := makeCommand(r); !errors.Is(err, ErrInvalidStateData) {
		t.Fatal(err)
	}
	// Standalone M0 decoding continues accepting its full body limit, even
	// though an object payload also needs room for the envelope's identities.
	r.Space = "s"
	r.Body = make([]byte, api.MaxCommandBody)
	wire, err = CanonicalCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeCanonicalCommand(wire); err != nil || len(got.Body) != api.MaxCommandBody {
		t.Fatal(err)
	}
}

func FuzzStateBatch(f *testing.F) {
	for _, s := range []string{"01000001000200", "01000100020001000102ff0002020100", "0200", "010000ffffffffffffffffff01"} {
		b, _ := hex.DecodeString(s)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		batch, err := DecodeStateBatch(b)
		if err != nil {
			return
		}
		encoded, err := EncodeStateBatch(batch)
		if err != nil || !bytes.Equal(encoded, b) {
			t.Fatalf("noncanonical: %v", err)
		}
	})
}

func testID(value byte) model.ObjectID {
	return model.ObjectID(hex.EncodeToString(bytes.Repeat([]byte{value}, 32)))
}
