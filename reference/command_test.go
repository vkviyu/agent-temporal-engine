package reference_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/reference"
)

func TestIndependentCommandDigestGoldenAndDifferentialFraming(t *testing.T) {
	r := command("same")
	// Fixed ATE-CANON-1 envelope, not produced by either implementation.
	golden := []byte("ATE-COMMAND\x00\x01\x01s\x01p\x04same\x06record\x04body\x00")
	want := fmt.Sprintf("%x", sha256.Sum256(golden))
	got, err := reference.CommandDigest(r)
	if err != nil || string(got) != want {
		t.Fatalf("independent golden digest %s != %s: %v", got, want, err)
	}
	for i := range 160 {
		r := command(fmt.Sprintf("id-%d", i))
		r.Body = []byte(strings.Repeat("\x00中", i))
		r.Space = model.SpaceID(fmt.Sprintf("s\x00%d", i))
		if i%2 == 0 {
			r.ExpectedFence = &model.CommitFence{Store: "store", LSN: uint64(i * 129), CommitDigest: model.CommitDigest(strings.Repeat("b", 64))}
		}
		want, err := reference.CommandDigest(r)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := engine.CommandDigest(r)
		if err != nil || want != actual {
			t.Fatalf("case %d framing differs: %s/%s %v", i, want, actual, err)
		}
		if _, err := hex.DecodeString(string(want)); err != nil {
			t.Fatal(err)
		}
	}
}
