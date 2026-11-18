package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

type closureObjectReader struct {
	objects map[model.ObjectID][]byte
	reads   int
	readIDs []model.ObjectID
}

func (r *closureObjectReader) ReadObject(_ context.Context, id model.ObjectID) ([]byte, error) {
	r.reads++
	r.readIDs = append(r.readIDs, id)
	raw, ok := r.objects[id]
	if !ok {
		return nil, ErrMissingObject
	}
	return raw, nil
}

func TestSchemaContentClosureAuthenticatesTransitiveSharedObjects(t *testing.T) {
	reader := &closureObjectReader{objects: map[model.ObjectID][]byte{}}
	add := func(object codec.Object) model.ObjectID {
		t.Helper()
		id, err := codec.ObjectID(object)
		if err != nil {
			t.Fatal(err)
		}
		reader.objects[id], err = codec.EncodeObject(object)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	leaf := add(codec.Object{Type: "test.content", Space: "space", Version: 1, Payload: []byte("original")})
	catalog := add(codec.Object{Type: "test.catalog", Space: systemSpace, Version: 1, Refs: []model.ObjectID{leaf}})
	if err := verifyContentClosure(context.Background(), reader, physicalReferences([]model.ObjectID{catalog, catalog, leaf, state.EmptyRoot})); err != nil || reader.reads != 2 {
		t.Fatalf("shared physical content was not verified once: %d %v", reader.reads, err)
	}
	raw := reader.objects[leaf]
	delete(reader.objects, leaf)
	if err := verifyContentClosure(context.Background(), reader, physicalReferences([]model.ObjectID{catalog})); !errors.Is(err, ErrMissingObject) {
		t.Fatalf("missing transitive reference ignored: %v", err)
	}
	reader.objects[leaf] = append([]byte(nil), raw...)
	reader.objects[leaf][len(raw)-1] ^= 1
	if err := verifyContentClosure(context.Background(), reader, physicalReferences([]model.ObjectID{catalog})); !errors.Is(err, codec.ErrObjectDigest) {
		t.Fatalf("transitive object identity not authenticated: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := reader.reads
	if err := verifyContentClosure(ctx, reader, physicalReferences([]model.ObjectID{catalog})); !errors.Is(err, context.Canceled) || reader.reads != before {
		t.Fatal("canceled closure performed object IO")
	}
}
