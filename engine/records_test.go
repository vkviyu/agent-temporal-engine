package engine

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func sampleReceipt() receiptRecord {
	return receiptRecord{Key: model.RequestKey{Space: "s", Principal: "p", RequestID: "r"}, RequestDigest: testID(0x11), Command: testID(0x22), Operations: testID(0x33), TxID: model.TxID(strings.Repeat("aa", 16)), LSN: 300, StateRoot: testID(0x44)}
}

func TestCoreRecordsIndependentGolden(t *testing.T) {
	value, err := makeValue("s", api.Value{Kind: api.ValueBytes, Data: []byte{'a', 0}})
	if err != nil {
		t.Fatal(err)
	}
	root, err := makeRootSet(model.RootSet{DomainStates: testID(1), BranchRefs: testID(2), CommandReceipts: testID(1)})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := makeReceipt(sampleReceipt())
	if err != nil {
		t.Fatal(err)
	}
	// Python unsigned-LEB128 + hashlib generated all fixed bytes and digests.
	for _, tc := range []struct {
		name          string
		object        codec.Object
		payload, wire string
		id            model.ObjectID
	}{
		{"value", value, "010001026100", "4154454f424a30310100096174652e76616c75650173010006010001026100", "c98469c7be596a4b87ab53b6646e10181e9dba94108c89d80729966b5881967c"},
		{"rootset", root, "01000120010101010101010101010101010101010101010101010101010101010101010100012002020202020202020202020202020202020202020202020202020202020202020001200101010101010101010101010101010101010101010101010101010101010101", "", "027a19f5e3d51ec2d55563090997beb80476143b32b5a5c0a784f9552b38c756"},
		{"receipt", receipt, "010001730170017220111111111111111111111111111111111111111111111111111111111111111120222222222222222222222222222222222222222222222222222222222222222220333333333333333333333333333333333333333333333333333333333333333310aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac0201204444444444444444444444444444444444444444444444444444444444444444", "", "b91b49768e3011b99a3aaf4955a963fb6429e3eafc3bba29c0f2e7e2b97fcf45"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if hex.EncodeToString(tc.object.Payload) != tc.payload {
				t.Fatalf("payload %x", tc.object.Payload)
			}
			wire, err := codec.EncodeObject(tc.object)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wire != "" && hex.EncodeToString(wire) != tc.wire {
				t.Fatalf("envelope %x", wire)
			}
			id, err := codec.ObjectID(tc.object)
			if err != nil || id != tc.id {
				t.Fatalf("identity %s %v", id, err)
			}
		})
	}
	if got, err := readReceipt(receipt); err != nil || !reflect.DeepEqual(got, sampleReceipt()) {
		t.Fatalf("%#v %v", got, err)
	}
	if got, err := readRootSet(root); err != nil || got != (model.RootSet{DomainStates: testID(1), BranchRefs: testID(2), CommandReceipts: testID(1)}) {
		t.Fatal(got, err)
	}
	if slices.Contains(receipt.Refs, receiptRecord{}.RequestDigest) || slices.Contains(receipt.Refs, sampleReceipt().RequestDigest) {
		t.Fatal("request digest was treated as an object edge")
	}
	if !slices.Equal(receipt.Refs, []model.ObjectID{testID(0x22), testID(0x33), testID(0x44)}) {
		t.Fatal(receipt.Refs)
	}
	// The receipt contains no RootSet or final commit digest, avoiding cycles.
	empty, err := makeRootSet(model.RootSet{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readRootSet(empty); err != nil || got != (model.RootSet{}) || len(empty.Refs) != 0 || !bytes.Equal(empty.Payload, []byte{1, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("empty rootset: %#v %v", got, err)
	}
}

func TestOperationsRefsRoundtripAndOwnership(t *testing.T) {
	r := operationsRecord{Space: "s", BeforeRoot: testID(1), AfterRoot: testID(2), Changes: []stateChange{{Key: nil, Before: "", After: ""}, {Key: []byte{0, 0xff}, Before: testID(3), After: testID(3)}}}
	o, err := makeOperations(r)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(o.Refs, []model.ObjectID{testID(1), testID(2), testID(3)}) {
		t.Fatal(o.Refs)
	}
	wire, err := codec.EncodeObject(o)
	if err != nil {
		t.Fatal(err)
	}
	id, err := codec.ObjectID(o)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.VerifyObject(id, wire)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readOperations(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Space != r.Space || got.BeforeRoot != r.BeforeRoot || got.AfterRoot != r.AfterRoot || len(got.Changes) != 2 || got.Changes[1].Before != got.Changes[1].After || !bytes.Equal(got.Changes[1].Key, r.Changes[1].Key) {
		t.Fatalf("%#v", got)
	}
	got.Changes[1].Key[0] = 99
	if !bytes.Equal(o.Payload, decoded.Payload) || r.Changes[1].Key[0] != 0 {
		t.Fatal("operations keys alias payload")
	}
	got, err = readOperations(decoded)
	if err != nil {
		t.Fatal(err)
	}
	for i := range decoded.Payload {
		decoded.Payload[i] = 0xff
	}
	if !bytes.Equal(got.Changes[1].Key, r.Changes[1].Key) {
		t.Fatal("decoded keys borrow object payload")
	}
}

func TestCoreReadersRejectWrongEnvelopeAndUnknownFlags(t *testing.T) {
	value, _ := makeValue("s", api.Value{Kind: api.ValueNull})
	command, _ := makeCommand(api.CommitRequest{Space: "s", Principal: "p", RequestID: "r", Method: api.MethodMutateState, Body: []byte{1}})
	ops, _ := makeOperations(operationsRecord{Space: "s", Changes: []stateChange{{}}})
	receipt, _ := makeReceipt(sampleReceipt())
	root, _ := makeRootSet(model.RootSet{DomainStates: testID(1)})
	for _, tc := range []struct {
		name            string
		object          codec.Object
		read            func(codec.Object) error
		command, system bool
	}{
		{"value", value, func(o codec.Object) error { _, e := readValue(o); return e }, false, false},
		{"command", command, func(o codec.Object) error { _, e := readCommand(o); return e }, true, false},
		{"operations", ops, func(o codec.Object) error { _, e := readOperations(o); return e }, false, false},
		{"receipt", receipt, func(o codec.Object) error { _, e := readReceipt(o); return e }, false, false},
		{"rootset", root, func(o codec.Object) error { _, e := readRootSet(o); return e }, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, change := range []struct {
				name   string
				mutate func(*codec.Object)
				want   error
			}{
				{"type", func(o *codec.Object) { o.Type = "future.type" }, ErrUnsupportedSchema},
				{"version", func(o *codec.Object) { o.Version = 2 }, ErrUnsupportedSchema},
				{"scope", func(o *codec.Object) {
					if tc.system {
						o.Space = "s"
					} else {
						o.Space = systemSpace
					}
				}, ErrInvalidStateData},
				{"refs", func(o *codec.Object) { o.Refs = canonicalRefs(append(o.Refs, testID(0xfe))...) }, ErrInvalidStateData},
				{"payload empty", func(o *codec.Object) { o.Payload = nil }, ErrInvalidStateData},
				{"trailing", func(o *codec.Object) { o.Payload = append(o.Payload, 0) }, ErrInvalidStateData},
			} {
				t.Run(change.name, func(t *testing.T) {
					o := cloneCoreObject(tc.object)
					change.mutate(&o)
					if err := tc.read(o); !errors.Is(err, change.want) {
						t.Fatalf("%v", err)
					}
				})
			}
			if !tc.command {
				for _, change := range []struct {
					name  string
					at    int
					value byte
				}{{"schema", 0, 2}, {"flags", 1, 1}} {
					t.Run(change.name, func(t *testing.T) {
						o := cloneCoreObject(tc.object)
						o.Payload[change.at] = change.value
						if err := tc.read(o); !errors.Is(err, ErrUnsupportedSchema) {
							t.Fatal(err)
						}
					})
				}
			}
			// A freshly computed generic ObjectID does not bypass core validation.
			o := cloneCoreObject(tc.object)
			o.Refs = canonicalRefs(append(o.Refs, testID(0xfe))...)
			id, err := codec.ObjectID(o)
			if err != nil {
				t.Fatal(err)
			}
			wire, _ := codec.EncodeObject(o)
			verified, err := codec.VerifyObject(id, wire)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.read(verified); !errors.Is(err, ErrInvalidStateData) {
				t.Fatalf("recomputed digest bypassed refs: %v", err)
			}
		})
	}
}

func TestRecordEncodersRejectInvalidValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func() error
	}{
		{"operations empty", func() error { _, e := makeOperations(operationsRecord{Space: "s"}); return e }},
		{"operations cap", func() error {
			_, e := makeOperations(operationsRecord{Space: "s", Changes: make([]stateChange, 1025)})
			return e
		}},
		{"operations long key", func() error {
			_, e := makeOperations(operationsRecord{Space: "s", Changes: []stateChange{{Key: make([]byte, 1025)}}})
			return e
		}},
		{"operations unsorted", func() error {
			_, e := makeOperations(operationsRecord{Space: "s", Changes: []stateChange{{Key: []byte{2}}, {Key: []byte{1}}}})
			return e
		}},
		{"operations duplicate", func() error {
			_, e := makeOperations(operationsRecord{Space: "s", Changes: []stateChange{{}, {}}})
			return e
		}},
		{"operations bad ref", func() error {
			_, e := makeOperations(operationsRecord{Space: "s", Changes: []stateChange{{Before: "bad"}}})
			return e
		}},
		{"operations reserved scope", func() error {
			_, e := makeOperations(operationsRecord{Space: systemSpace, Changes: []stateChange{{}}})
			return e
		}},
		{"rootset bad id", func() error { _, e := makeRootSet(model.RootSet{DomainStates: "bad"}); return e }},
		{"rootset zero id", func() error {
			_, e := makeRootSet(model.RootSet{LiveControl: model.ObjectID(strings.Repeat("0", 64))})
			return e
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.make(); !errors.Is(err, ErrInvalidStateData) {
				t.Fatal(err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*receiptRecord)
	}{
		{"empty key", func(r *receiptRecord) { r.Key.RequestID = "" }},
		{"reserved scope", func(r *receiptRecord) { r.Key.Space = systemSpace }},
		{"bad digest", func(r *receiptRecord) { r.RequestDigest = "bad" }},
		{"zero digest", func(r *receiptRecord) { r.RequestDigest = model.ObjectID(strings.Repeat("0", 64)) }},
		{"no command", func(r *receiptRecord) { r.Command = "" }},
		{"no operations", func(r *receiptRecord) { r.Operations = "" }},
		{"no transaction", func(r *receiptRecord) { r.TxID = "" }},
		{"zero transaction", func(r *receiptRecord) { r.TxID = model.TxID(strings.Repeat("0", 32)) }},
		{"uppercase transaction", func(r *receiptRecord) { r.TxID = model.TxID(strings.Repeat("AA", 16)) }},
		{"nonhex transaction", func(r *receiptRecord) { r.TxID = model.TxID(strings.Repeat("gg", 16)) }},
		{"zero LSN", func(r *receiptRecord) { r.LSN = 0 }},
		{"bad state root", func(r *receiptRecord) { r.StateRoot = "bad" }},
	} {
		t.Run("receipt "+tc.name, func(t *testing.T) {
			r := sampleReceipt()
			tc.change(&r)
			if _, err := makeReceipt(r); !errors.Is(err, ErrInvalidStateData) {
				t.Fatal(err)
			}
		})
	}
}

func TestRecordDecodeRejectsMalformedFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		object codec.Object
		read   func(codec.Object) error
	}{
		{"root optional", codec.Object{Type: rootSetType, Space: systemSpace, Version: 1, Payload: []byte{1, 0, 2, 0, 0, 0, 0}}, func(o codec.Object) error { _, e := readRootSet(o); return e }},
		{"short root", codec.Object{Type: rootSetType, Space: systemSpace, Version: 1, Payload: []byte{1, 0, 1, 1, 1, 0, 0, 0, 0}}, func(o codec.Object) error { _, e := readRootSet(o); return e }},
		{"zero root", codec.Object{Type: rootSetType, Space: systemSpace, Version: 1, Payload: append(append([]byte{1, 0, 1, 32}, make([]byte, 32)...), 0, 0, 0, 0)}, func(o codec.Object) error { _, e := readRootSet(o); return e }},
		{"operations enormous count", codec.Object{Type: operationsType, Space: "s", Version: 1, Payload: codec.AppendUint([]byte{1, 0, 0, 0}, ^uint64(0))}, func(o codec.Object) error { _, e := readOperations(o); return e }},
		{"oversize value", codec.Object{Type: valueType, Space: "s", Version: 1, Payload: codec.AppendUint([]byte{1, 0, 1}, api.MaxStateValueBytes+1)}, func(o codec.Object) error { _, e := readValue(o); return e }},
		{"null extra data", codec.Object{Type: valueType, Space: "s", Version: 1, Payload: []byte{1, 0, 0, 0}}, func(o codec.Object) error { _, e := readValue(o); return e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.read(tc.object); !errors.Is(err, ErrInvalidStateData) {
				t.Fatal(err)
			}
		})
	}
	r := sampleReceipt()
	o, _ := makeReceipt(r)
	r.Key.Space = "t"
	other, _ := makeReceipt(r)
	other.Space = "s"
	if _, err := readReceipt(other); !errors.Is(err, ErrInvalidStateData) {
		t.Fatal(err)
	}
	for n := 0; n < len(o.Payload); n++ {
		truncated := cloneCoreObject(o)
		truncated.Payload = truncated.Payload[:n]
		if _, err := readReceipt(truncated); err == nil {
			t.Fatalf("accepted receipt truncation %d", n)
		}
	}
	v, _ := makeValue("s", api.Value{Kind: api.ValueBytes, Data: []byte{1, 2}})
	got, err := readValue(v)
	if err != nil {
		t.Fatal(err)
	}
	got.Data[0] = 99
	again, _ := readValue(v)
	if again.Data[0] != 1 {
		t.Fatal("readValue aliases payload")
	}
	before := bytes.Clone(again.Data)
	for i := range v.Payload {
		v.Payload[i] = 0
	}
	if !bytes.Equal(again.Data, before) {
		t.Fatal("value borrows payload")
	}
	refs := canonicalRefs("", testID(2), testID(1), testID(1), "")
	if !slices.Equal(refs, []model.ObjectID{testID(1), testID(2)}) {
		t.Fatal(refs)
	}
}

func FuzzCoreRecords(f *testing.F) {
	value, _ := makeValue("s", api.Value{Kind: api.ValueBytes, Data: []byte{0, 0xff}})
	ops, _ := makeOperations(operationsRecord{Space: "s", Changes: []stateChange{{}}})
	receipt, _ := makeReceipt(sampleReceipt())
	root, _ := makeRootSet(model.RootSet{})
	command, _ := makeCommand(api.CommitRequest{Space: "s", Principal: "p", RequestID: "r", Method: api.MethodMutateState})
	objects := []codec.Object{value, ops, receipt, root, command}
	for i, o := range objects {
		f.Add(o.Payload, uint8(i))
	}
	f.Fuzz(func(t *testing.T, payload []byte, kind uint8) {
		o := cloneCoreObject(objects[int(kind)%len(objects)])
		o.Payload = payload
		id, err := codec.ObjectID(o)
		if err != nil {
			return
		}
		wire, _ := codec.EncodeObject(o)
		o, err = codec.VerifyObject(id, wire)
		if err != nil {
			t.Fatal(err)
		}
		var encoded codec.Object
		switch int(kind) % len(objects) {
		case 0:
			var r api.Value
			r, err = readValue(o)
			if err == nil {
				encoded, err = makeValue(o.Space, r)
			}
		case 1:
			var r operationsRecord
			r, err = readOperations(o)
			if err == nil {
				encoded, err = makeOperations(r)
			}
		case 2:
			var r receiptRecord
			r, err = readReceipt(o)
			if err == nil {
				encoded, err = makeReceipt(r)
			}
		case 3:
			var r model.RootSet
			r, err = readRootSet(o)
			if err == nil {
				encoded, err = makeRootSet(r)
			}
		case 4:
			var r api.CommitRequest
			r, err = readCommand(o)
			if err == nil {
				encoded, err = makeCommand(r)
			}
		}
		if err != nil {
			return
		}
		got, err := codec.ObjectID(encoded)
		if err != nil || got != id {
			t.Fatalf("accepted noncanonical record: %s != %s, %v", got, id, err)
		}
	})
}

func cloneCoreObject(o codec.Object) codec.Object {
	o.Refs = slices.Clone(o.Refs)
	o.Payload = bytes.Clone(o.Payload)
	return o
}
