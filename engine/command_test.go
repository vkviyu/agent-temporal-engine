package engine_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func commandFixture() api.CommitRequest {
	return api.CommitRequest{Space: "s", Principal: "p", RequestID: "r", Method: "m", Body: []byte{0, 0xff}}
}

func TestCommandExperimentalGolden(t *testing.T) {
	r := commandFixture()
	// Domain, version, four one-byte strings, two body bytes, absent fence.
	want := []byte{'A', 'T', 'E', '-', 'C', 'O', 'M', 'M', 'A', 'N', 'D', 0, 1, 1, 's', 1, 'p', 1, 'r', 1, 'm', 2, 0, 0xff, 0}
	got, err := engine.CanonicalCommand(r)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got %x %v, want %x", got, err, want)
	}
	sum := sha256.Sum256(want)
	digest, err := engine.CommandDigest(r)
	if err != nil || string(digest) != hex.EncodeToString(sum[:]) {
		t.Fatal("digest does not hash golden envelope", digest, err)
	}
	r.ExpectedFence = &model.CommitFence{Store: "t", LSN: 128, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
	want = append(want[:len(want)-1], 1, 1, 't', 0x80, 1, 64)
	want = append(want, []byte(strings.Repeat("a", 64))...)
	got, err = engine.CanonicalCommand(r)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("fence golden %x %v", got, err)
	}
	again, err := engine.CanonicalCommand(r)
	if err != nil || !bytes.Equal(got, again) {
		t.Fatal("encoding is unstable")
	}
}

func TestCommandDigestBindsEveryField(t *testing.T) {
	base := commandFixture()
	base.ExpectedFence = &model.CommitFence{Store: "t", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
	original, err := engine.CommandDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*api.CommitRequest)
	}{
		{"space", func(r *api.CommitRequest) { r.Space = "other" }},
		{"principal", func(r *api.CommitRequest) { r.Principal = "other" }},
		{"request ID", func(r *api.CommitRequest) { r.RequestID = "other" }},
		{"method", func(r *api.CommitRequest) { r.Method = "other" }},
		{"body", func(r *api.CommitRequest) { r.Body = []byte{0, 0xfe} }},
		{"fence presence", func(r *api.CommitRequest) { r.ExpectedFence = nil }},
		{"store", func(r *api.CommitRequest) { r.ExpectedFence.Store = "other" }},
		{"LSN", func(r *api.CommitRequest) { r.ExpectedFence.LSN = 2 }},
		{"commit digest", func(r *api.CommitRequest) { r.ExpectedFence.CommitDigest = model.CommitDigest(strings.Repeat("b", 64)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			fence := *base.ExpectedFence
			r.ExpectedFence = &fence
			tc.change(&r)
			got, err := engine.CommandDigest(r)
			if err != nil || got == original {
				t.Fatalf("field not bound: %s %v", got, err)
			}
		})
	}
}

func TestCommandNULTupleBoundaries(t *testing.T) {
	a := commandFixture()
	a.Space = "a\x00b"
	a.Principal = "c"
	b := commandFixture()
	b.Space = "a"
	b.Principal = "b\x00c"
	// These collide under NUL joining; length-prefixed fields must distinguish them.
	if strings.Join([]string{string(a.Space), string(a.Principal)}, "\x00") != strings.Join([]string{string(b.Space), string(b.Principal)}, "\x00") {
		t.Fatal("bad collision fixture")
	}
	da, ea := engine.CommandDigest(a)
	db, eb := engine.CommandDigest(b)
	if ea != nil || eb != nil || da == db {
		t.Fatalf("tuple collision: %v %v", ea, eb)
	}
	a = commandFixture()
	b = a
	a.Method = "x\x00"
	a.Body = []byte("y")
	b.Method = "x"
	b.Body = []byte("\x00y")
	da, ea = engine.CommandDigest(a)
	db, eb = engine.CommandDigest(b)
	if ea != nil || eb != nil || da == db {
		t.Fatal("method/body boundary collision")
	}
}

func TestInvalidCommandsProduceNoEncodingOrDigest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*api.CommitRequest)
	}{
		{"space empty", func(r *api.CommitRequest) { r.Space = "" }},
		{"principal empty", func(r *api.CommitRequest) { r.Principal = "" }},
		{"request ID empty", func(r *api.CommitRequest) { r.RequestID = "" }},
		{"key UTF8", func(r *api.CommitRequest) { r.Space = model.SpaceID(string([]byte{0xff})) }},
		{"key too long", func(r *api.CommitRequest) {
			r.RequestID = model.RequestID(strings.Repeat("x", model.MaxIdentityBytes+1))
		}},
		{"method empty", func(r *api.CommitRequest) { r.Method = "" }},
		{"method UTF8", func(r *api.CommitRequest) { r.Method = string([]byte{0xff}) }},
		{"method too long", func(r *api.CommitRequest) { r.Method = strings.Repeat("x", api.MaxIdentityBytes+1) }},
		{"body too long", func(r *api.CommitRequest) { r.Body = make([]byte, api.MaxCommandBody+1) }},
		{"fence invalid", func(r *api.CommitRequest) { r.ExpectedFence = &model.CommitFence{Store: "s", CommitDigest: "bad"} }},
		{"fence store UTF8", func(r *api.CommitRequest) {
			r.ExpectedFence = &model.CommitFence{Store: model.StoreID(string([]byte{0xff})), CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
		}},
		{"fence store too long", func(r *api.CommitRequest) {
			r.ExpectedFence = &model.CommitFence{Store: model.StoreID(strings.Repeat("x", api.MaxIdentityBytes+1)), CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := commandFixture()
			tc.change(&r)
			if b, err := engine.CanonicalCommand(r); err == nil || len(b) != 0 {
				t.Fatalf("invalid command encoded: %x %v", b, err)
			}
			if digest, err := engine.CommandDigest(r); err == nil || digest != "" {
				t.Fatalf("invalid command digested: %s %v", digest, err)
			}
		})
	}
}
