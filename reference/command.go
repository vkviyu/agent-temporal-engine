package reference

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// CommandDigest independently implements the envelope in ATE-CANON-1.
// Deliberately do not call engine.CommandDigest, CanonicalCommand, or codec:
// sharing the implementation under test would conceal framing regressions.
func CommandDigest(r api.CommitRequest) (model.ObjectID, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	b := []byte("ATE-COMMAND\x00\x01")
	field := func(v []byte) { b = binary.AppendUvarint(b, uint64(len(v))); b = append(b, v...) }
	for _, value := range []string{string(r.Space), string(r.Principal), string(r.RequestID), r.Method} {
		field([]byte(value))
	}
	field(r.Body)
	if r.ExpectedFence == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		field([]byte(r.ExpectedFence.Store))
		b = binary.AppendUvarint(b, r.ExpectedFence.LSN)
		field([]byte(r.ExpectedFence.CommitDigest))
	}
	sum := sha256.Sum256(b)
	return model.ObjectID(hex.EncodeToString(sum[:])), nil
}
