package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestFramingShortIOAndBounds(t *testing.T) {
	var b bytes.Buffer
	payload := []byte(`{"version":1,"correlation_id":"1","method":"hello","body":{"token":"x"}}`)
	if err := WriteFrame(&b, payload); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&b)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("%v %s", err, got)
	}
	if _, err := ReadFrame(strings.NewReader("\x00\x00\x00\x00")); err == nil {
		t.Fatal("zero frame accepted")
	}
	if err := WriteFrame(&b, make([]byte, MaxFrameBytes+1)); err != ErrFrameLimit {
		t.Fatal(err)
	}
}
func TestStrictRejectsDuplicateUnknownTrailingDepthAndFloatRoundTrip(t *testing.T) {
	var out Request
	for _, raw := range []string{`{"version":1,"version":1,"correlation_id":"1","method":"x","body":{}}`, `{"version":1,"correlation_id":"1","method":"x","body":{},"extra":1}`, `{"version":1,"correlation_id":"1","method":"x","body":{}} true`} {
		if err := DecodeStrict([]byte(raw), &out); err == nil {
			t.Fatalf("accepted invalid %s", raw)
		}
	}
	var v struct {
		N uint64 `json:"n"`
	}
	if err := DecodeStrict([]byte(`{"n":18446744073709551615}`), &v); err != nil || v.N != ^uint64(0) {
		t.Fatal(err, v.N)
	}
	if err := DecodeStrict([]byte(`{"n":1.5}`), &v); err == nil {
		t.Fatal("float accepted as uint")
	}
	deep := `{"a":` + strings.Repeat("[", MaxDepth+2) + `0` + strings.Repeat("]", MaxDepth+2) + `}`
	if err := DecodeStrict([]byte(deep), &out); err == nil {
		t.Fatal("deep JSON accepted")
	}
}

func TestReceiverFrameLimitRejectsBeforeReadingBody(t *testing.T) {
	header := []byte{0, 0, 4, 0}
	if _, err := ReadFrameLimit(bytes.NewReader(header), 128); !errors.Is(err, ErrFrameLimit) {
		t.Fatalf("lower frame ceiling ignored: %v", err)
	}
}
