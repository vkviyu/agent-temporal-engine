package engine

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestDerivationIndependentCanonicalVector(t *testing.T) {
	object := model.ObjectID(strings.Repeat("a", 64))
	sources := []model.ObjectID{model.ObjectID(strings.Repeat("b", 64)), model.ObjectID(strings.Repeat("c", 64))}
	// Independently framed: domain, schema=1, length=64, object, count=2,
	// followed by two length=64 source strings. No production encoder is used.
	vector := []byte("ATE-DERIVATION\x00\x01\x40" + string(object) + "\x02\x40" + string(sources[0]) + "\x40" + string(sources[1]))
	raw, err := encodeDerivation(object, sources)
	if err != nil || !bytes.Equal(raw, vector) {
		t.Fatalf("canonical derivation changed: %x %v", raw, err)
	}
	got, err := decodeDerivation(vector, object)
	if err != nil || !reflect.DeepEqual(got, sources) {
		t.Fatalf("independent vector decode: %v %v", got, err)
	}
	for n := 0; n < len(vector); n++ {
		if _, err := decodeDerivation(vector[:n], object); !errors.Is(err, ErrInvalidStateData) {
			t.Fatalf("truncation at %d accepted: %v", n, err)
		}
	}
	for name, bad := range map[string][]byte{
		"trailing":  append(bytes.Clone(vector), 0),
		"schema":    append(append([]byte("ATE-DERIVATION\x00"), 2), vector[len(derivationDomain)+1:]...),
		"reordered": []byte("ATE-DERIVATION\x00\x01\x40" + string(object) + "\x02\x40" + string(sources[1]) + "\x40" + string(sources[0])),
		"duplicate": []byte("ATE-DERIVATION\x00\x01\x40" + string(object) + "\x02\x40" + string(sources[0]) + "\x40" + string(sources[0])),
	} {
		if _, err := decodeDerivation(bad, object); !errors.Is(err, ErrInvalidStateData) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	if _, err := decodeDerivation(vector, sources[0]); !errors.Is(err, ErrInvalidStateData) {
		t.Fatalf("identity substitution accepted: %v", err)
	}
	for _, bad := range [][]model.ObjectID{nil, {object}, {sources[1], sources[0]}, {sources[0], sources[0]}, {"invalid"}} {
		if _, err := encodeDerivation(object, bad); !errors.Is(err, ErrInvalidStateData) {
			t.Fatalf("invalid source set accepted: %v %v", bad, err)
		}
	}
}
