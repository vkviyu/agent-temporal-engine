package engine

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// EncodeStateBatch canonicalizes operation order without changing the caller's
// batch. Duplicate keys and data ignored by an operation's kind are rejected.
func EncodeStateBatch(batch api.StateBatch) ([]byte, error) {
	if len(batch.Operations) == 0 || len(batch.Operations) > api.MaxStateMutations {
		return nil, invalidState("mutation count", nil)
	}
	b := corePrefix()
	var err error
	if batch.ExpectedRoot == nil {
		b = codec.AppendUint(b, 0)
	} else {
		b = codec.AppendUint(b, 1)
		b, err = appendOptionalID(b, *batch.ExpectedRoot)
		if err != nil {
			return nil, err
		}
	}
	ops := slices.Clone(batch.Operations)
	slices.SortFunc(ops, func(a, b api.StateMutation) int { return bytes.Compare(a.Key, b.Key) })
	b = codec.AppendUint(b, uint64(len(ops)))
	for i, op := range ops {
		if len(op.Key) > api.MaxStateKeyBytes || (i > 0 && bytes.Equal(ops[i-1].Key, op.Key)) {
			return nil, invalidState("mutation key", nil)
		}
		b, _ = codec.AppendBytes(b, op.Key)
		switch op.Kind {
		case api.MutationPut:
			b = codec.AppendUint(b, 1)
			b, err = appendValue(b, op.Value)
		case api.MutationDelete:
			if !zeroValue(op.Value) {
				return nil, invalidState("delete value", nil)
			}
			b = codec.AppendUint(b, 2)
		default:
			return nil, invalidState("mutation kind", nil)
		}
		if err != nil {
			return nil, err
		}
		switch op.Condition.Kind {
		case api.ConditionAny, api.ConditionAbsent:
			if !zeroValue(op.Condition.Value) {
				return nil, invalidState("unused condition value", nil)
			}
			kind := uint64(0)
			if op.Condition.Kind == api.ConditionAbsent {
				kind = 1
			}
			b = codec.AppendUint(b, kind)
		case api.ConditionValue:
			b = codec.AppendUint(b, 2)
			b, err = appendValue(b, op.Condition.Value)
		default:
			return nil, invalidState("condition kind", nil)
		}
		if err != nil {
			return nil, err
		}
		if len(b) > api.MaxStateBatchBytes {
			return nil, invalidState("batch size", codec.ErrLengthOverflow)
		}
	}
	return b, nil
}

// DecodeStateBatch only accepts canonical byte-key order and returns owned data.
func DecodeStateBatch(b []byte) (api.StateBatch, error) {
	var batch api.StateBatch
	if len(b) > api.MaxStateBatchBytes {
		return batch, invalidState("batch size", codec.ErrLengthOverflow)
	}
	d, err := coreDecoder(b)
	if err != nil {
		return batch, err
	}
	presence, err := d.Uint()
	if err != nil {
		return batch, invalidState("expected root presence", err)
	}
	switch presence {
	case 0:
	case 1:
		id, err := readOptionalID(d)
		if err != nil {
			return api.StateBatch{}, err
		}
		batch.ExpectedRoot = &id
	default:
		return api.StateBatch{}, invalidState("expected root presence", nil)
	}
	count, err := d.Uint()
	if err != nil {
		return api.StateBatch{}, invalidState("mutation count", err)
	}
	if count == 0 || count > api.MaxStateMutations {
		return api.StateBatch{}, invalidState("mutation count", nil)
	}
	// Every operation needs at least key-length, mutation-kind and condition.
	if count > uint64(d.Remaining()/3) {
		return api.StateBatch{}, invalidState("mutations", codec.ErrTruncated)
	}
	batch.Operations = make([]api.StateMutation, int(count))
	for i := range batch.Operations {
		op := &batch.Operations[i]
		key, err := d.Bytes(api.MaxStateKeyBytes)
		if err != nil {
			return api.StateBatch{}, invalidState("mutation key", err)
		}
		op.Key = bytes.Clone(key)
		if i > 0 && bytes.Compare(batch.Operations[i-1].Key, op.Key) >= 0 {
			return api.StateBatch{}, invalidState("mutation order", nil)
		}
		kind, err := d.Uint()
		if err != nil {
			return api.StateBatch{}, invalidState("mutation kind", err)
		}
		switch kind {
		case 1:
			op.Kind = api.MutationPut
			op.Value, err = decodeValue(d)
		case 2:
			op.Kind = api.MutationDelete
		default:
			return api.StateBatch{}, invalidState("mutation kind", nil)
		}
		if err != nil {
			return api.StateBatch{}, err
		}
		condition, err := d.Uint()
		if err != nil {
			return api.StateBatch{}, invalidState("condition kind", err)
		}
		switch condition {
		case 0:
			op.Condition.Kind = api.ConditionAny
		case 1:
			op.Condition.Kind = api.ConditionAbsent
		case 2:
			op.Condition.Kind = api.ConditionValue
			op.Condition.Value, err = decodeValue(d)
		default:
			return api.StateBatch{}, invalidState("condition kind", nil)
		}
		if err != nil {
			return api.StateBatch{}, err
		}
	}
	if err := d.Done(); err != nil {
		return api.StateBatch{}, invalidState("batch", err)
	}
	canonical, err := EncodeStateBatch(batch)
	if err != nil || !bytes.Equal(canonical, b) {
		return api.StateBatch{}, invalidState("noncanonical batch", err)
	}
	return batch, nil
}

func makeValue(space model.SpaceID, value api.Value) (codec.Object, error) {
	if err := validSpace(space); err != nil {
		return codec.Object{}, err
	}
	payload, err := appendValue(corePrefix(), value)
	if err != nil {
		return codec.Object{}, err
	}
	return coreObject(valueType, space, nil, payload)
}

func readValue(o codec.Object) (api.Value, error) {
	if err := checkCoreObject(o, valueType, false); err != nil {
		return api.Value{}, err
	}
	d, err := coreDecoder(o.Payload)
	if err != nil {
		return api.Value{}, err
	}
	v, err := decodeValue(d)
	if err != nil {
		return api.Value{}, err
	}
	if err := d.Done(); err != nil {
		return api.Value{}, invalidState("value", err)
	}
	if err := checkRefs(o, nil); err != nil {
		return api.Value{}, err
	}
	return v, nil
}

func makeCommand(req api.CommitRequest) (codec.Object, error) {
	if err := validSpace(req.Space); err != nil {
		return codec.Object{}, err
	}
	payload, err := CanonicalCommand(req)
	if err != nil {
		return codec.Object{}, invalidState("command", err)
	}
	return coreObject(commandType, req.Space, nil, payload)
}

func readCommand(o codec.Object) (api.CommitRequest, error) {
	if err := checkCoreObject(o, commandType, false); err != nil {
		return api.CommitRequest{}, err
	}
	req, err := decodeCanonicalCommand(o.Payload)
	if err != nil {
		return api.CommitRequest{}, err
	}
	if req.Space != o.Space {
		return api.CommitRequest{}, invalidState("command scope", nil)
	}
	if err := checkRefs(o, nil); err != nil {
		return api.CommitRequest{}, err
	}
	return req, nil
}

func appendValue(b []byte, v api.Value) ([]byte, error) {
	switch v.Kind {
	case api.ValueNull:
		if len(v.Data) != 0 {
			return nil, invalidState("null data", nil)
		}
		return codec.AppendUint(b, 0), nil
	case api.ValueBytes:
		if len(v.Data) > api.MaxStateValueBytes {
			return nil, invalidState("value size", codec.ErrLengthOverflow)
		}
		b = codec.AppendUint(b, 1)
		b, _ = codec.AppendBytes(b, v.Data)
		return b, nil
	default:
		return nil, invalidState("value kind", nil)
	}
}

func decodeValue(d *codec.Decoder) (api.Value, error) {
	kind, err := d.Uint()
	if err != nil {
		return api.Value{}, invalidState("value kind", err)
	}
	switch kind {
	case 0:
		return api.Value{Kind: api.ValueNull}, nil
	case 1:
		b, err := d.Bytes(api.MaxStateValueBytes)
		if err != nil {
			return api.Value{}, invalidState("value data", err)
		}
		return api.Value{Kind: api.ValueBytes, Data: bytes.Clone(b)}, nil
	default:
		return api.Value{}, invalidState("value kind", nil)
	}
}

func zeroValue(v api.Value) bool { return v.Kind == "" && len(v.Data) == 0 }

func validSpace(space model.SpaceID) error {
	if space == "" || len(space) > model.MaxIdentityBytes || !utf8.ValidString(string(space)) || strings.HasPrefix(string(space), "ate:") {
		return invalidState("space", nil)
	}
	return nil
}

func corePrefix() []byte { return []byte{1, 0} }

func coreDecoder(b []byte) (*codec.Decoder, error) {
	d := codec.NewDecoder(b)
	schema, err := d.Uint()
	if err != nil {
		return nil, invalidState("schema", err)
	}
	if schema != 1 {
		return nil, fmt.Errorf("%w: schema %d", ErrUnsupportedSchema, schema)
	}
	flags, err := d.Uint()
	if err != nil {
		return nil, invalidState("flags", err)
	}
	if flags != 0 {
		return nil, fmt.Errorf("%w: flags %d", ErrUnsupportedSchema, flags)
	}
	return d, nil
}

func invalidState(field string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrInvalidStateData, field)
	}
	return fmt.Errorf("%w: %s: %w", ErrInvalidStateData, field, cause)
}

func coreObject(typ string, space model.SpaceID, refs []model.ObjectID, payload []byte) (codec.Object, error) {
	o := codec.Object{Type: typ, Space: space, Version: 1, Refs: refs, Payload: payload}
	if _, err := codec.EncodeObject(o); err != nil {
		return codec.Object{}, invalidState("object envelope", err)
	}
	return o, nil
}

func checkCoreObject(o codec.Object, typ string, system bool) error {
	if o.Type != typ || o.Version != 1 {
		return fmt.Errorf("%w: %s version %d", ErrUnsupportedSchema, o.Type, o.Version)
	}
	if system {
		if o.Space != systemSpace {
			return invalidState("system scope", nil)
		}
	} else if err := validSpace(o.Space); err != nil {
		return err
	}
	if _, err := codec.EncodeObject(o); err != nil {
		return invalidState("object envelope", err)
	}
	return nil
}

func canonicalRefs(ids ...model.ObjectID) []model.ObjectID {
	refs := make([]model.ObjectID, 0, len(ids))
	for _, id := range ids {
		if id != "" {
			refs = append(refs, id)
		}
	}
	slices.Sort(refs)
	return slices.Compact(refs)
}

func checkRefs(o codec.Object, expected []model.ObjectID) error {
	if !slices.Equal(o.Refs, expected) {
		return invalidState("object references", nil)
	}
	return nil
}

func validObjectID(id model.ObjectID) bool {
	return model.ValidDigest(string(id)) && id != "0000000000000000000000000000000000000000000000000000000000000000"
}

func appendID(b []byte, id model.ObjectID) ([]byte, error) {
	if !validObjectID(id) {
		return nil, invalidState("object identity", nil)
	}
	raw, _ := hex.DecodeString(string(id))
	b, _ = codec.AppendBytes(b, raw)
	return b, nil
}

func readID(d *codec.Decoder) (model.ObjectID, error) {
	raw, err := d.Bytes(32)
	if err != nil {
		return "", invalidState("object identity", err)
	}
	if len(raw) != 32 {
		return "", invalidState("object identity length", nil)
	}
	id := model.ObjectID(hex.EncodeToString(raw))
	if !validObjectID(id) {
		return "", invalidState("zero object identity", nil)
	}
	return id, nil
}

func appendOptionalID(b []byte, id model.ObjectID) ([]byte, error) {
	if id == "" {
		return codec.AppendUint(b, 0), nil
	}
	return appendID(codec.AppendUint(b, 1), id)
}

func readOptionalID(d *codec.Decoder) (model.ObjectID, error) {
	presence, err := d.Uint()
	if err != nil {
		return "", invalidState("optional identity", err)
	}
	switch presence {
	case 0:
		return "", nil
	case 1:
		return readID(d)
	default:
		return "", invalidState("optional identity presence", nil)
	}
}
