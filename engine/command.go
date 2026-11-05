// Package engine implements experimental core state commands, durable receipts,
// and deterministic replay over the physical journal. This is a trusted-host
// capability, not the permission-filtered agent profile or a model executor.
package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// CanonicalCommand encodes the experimental command envelope. Body is an opaque
// canonical domain command supplied by its schema, not arbitrary wire JSON.
// Deadlines, connections, correlation IDs and server-assigned IDs are excluded.
func CanonicalCommand(r api.CommitRequest) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	b := []byte("ATE-COMMAND\x00")
	b = codec.AppendUint(b, 1)
	for _, s := range []string{string(r.Space), string(r.Principal), string(r.RequestID), r.Method} {
		b, _ = codec.AppendString(b, s)
	}
	b, _ = codec.AppendBytes(b, r.Body)
	if r.ExpectedFence == nil {
		return codec.AppendUint(b, 0), nil
	}
	b = codec.AppendUint(b, 1)
	b, _ = codec.AppendString(b, string(r.ExpectedFence.Store))
	b = codec.AppendUint(b, r.ExpectedFence.LSN)
	b, _ = codec.AppendString(b, string(r.ExpectedFence.CommitDigest))
	return b, nil
}

// CommandDigest streams the exact canonical envelope into SHA-256. Admission
// can reject an alias without allocating a second copy of its possibly large
// body; only admitted commands acquire owned request bytes.
func CommandDigest(r api.CommitRequest) (model.ObjectID, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("ATE-COMMAND\x00"))
	integer := func(v uint64) { var b [10]byte; n := binary.PutUvarint(b[:], v); h.Write(b[:n]) }
	field := func(b []byte) { integer(uint64(len(b))); h.Write(b) }
	integer(1)
	for _, s := range []string{string(r.Space), string(r.Principal), string(r.RequestID), r.Method} {
		field([]byte(s))
	}
	field(r.Body)
	if r.ExpectedFence == nil {
		integer(0)
	} else {
		integer(1)
		field([]byte(r.ExpectedFence.Store))
		integer(r.ExpectedFence.LSN)
		field([]byte(r.ExpectedFence.CommitDigest))
	}
	return model.ObjectID(hex.EncodeToString(h.Sum(nil))), nil
}

func decodeCanonicalCommand(encoded []byte) (api.CommitRequest, error) {
	const prefix = "ATE-COMMAND\x00"
	var r api.CommitRequest
	if len(encoded) > api.MaxCommandBody+6*model.MaxIdentityBytes+128 {
		return r, invalidState("command size", codec.ErrLengthOverflow)
	}
	if len(encoded) < len(prefix) {
		return r, invalidState("command prefix", codec.ErrTruncated)
	}
	if string(encoded[:len(prefix)]) != prefix {
		return r, invalidState("command prefix", nil)
	}
	d := codec.NewDecoder(encoded[len(prefix):])
	schema, err := d.Uint()
	if err != nil {
		return r, invalidState("command schema", err)
	}
	if schema != 1 {
		return r, fmt.Errorf("%w: command schema %d", ErrUnsupportedSchema, schema)
	}
	fields := make([]string, 4)
	for i := range fields {
		if fields[i], err = d.String(model.MaxIdentityBytes); err != nil {
			return api.CommitRequest{}, invalidState("command identity", err)
		}
	}
	r.Space, r.Principal, r.RequestID, r.Method = model.SpaceID(fields[0]), model.PrincipalID(fields[1]), model.RequestID(fields[2]), fields[3]
	body, err := d.Bytes(api.MaxCommandBody)
	if err != nil {
		return api.CommitRequest{}, invalidState("command body", err)
	}
	r.Body = bytes.Clone(body)
	presence, err := d.Uint()
	if err != nil {
		return api.CommitRequest{}, invalidState("command fence presence", err)
	}
	switch presence {
	case 0:
	case 1:
		store, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return api.CommitRequest{}, invalidState("fence store", err)
		}
		lsn, err := d.Uint()
		if err != nil {
			return api.CommitRequest{}, invalidState("fence LSN", err)
		}
		digest, err := d.String(64)
		if err != nil {
			return api.CommitRequest{}, invalidState("fence digest", err)
		}
		r.ExpectedFence = &model.CommitFence{Store: model.StoreID(store), LSN: lsn, CommitDigest: model.CommitDigest(digest)}
	default:
		return api.CommitRequest{}, invalidState("command fence presence", nil)
	}
	if err := d.Done(); err != nil {
		return api.CommitRequest{}, invalidState("command", err)
	}
	if err := r.Validate(); err != nil {
		return api.CommitRequest{}, invalidState("command", err)
	}
	if err := validSpace(r.Space); err != nil {
		return api.CommitRequest{}, err
	}
	canonical, err := CanonicalCommand(r)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return api.CommitRequest{}, invalidState("noncanonical command", err)
	}
	return r, nil
}
