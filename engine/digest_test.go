package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestStreamingCommandDigestMatchesCanonicalEnvelope(t *testing.T) {
	base := api.CommitRequest{Space: "space\x00中", Principal: "actor", RequestID: "id", Method: "method\x00", Body: []byte("\x00payload")}
	for _, length := range []int{0, 127, 128, 16384, 1 << 20} {
		for _, expected := range []bool{false, true} {
			req := base
			req.Body = []byte(strings.Repeat("x", length))
			if expected {
				req.ExpectedFence = &model.CommitFence{Store: "store", LSN: ^uint64(0), CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
			}
			encoded, err := CanonicalCommand(req)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(encoded)
			got, err := CommandDigest(req)
			if err != nil || string(got) != hex.EncodeToString(hash[:]) {
				t.Fatalf("length=%d expected=%t digest changed: %s %v", length, expected, got, err)
			}
		}
	}
}
