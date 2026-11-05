package graphstore

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

type radixObjects map[model.ObjectID][]byte

func (r radixObjects) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, ok := r[id]
	if !ok {
		return nil, errors.New("missing value")
	}
	return v, nil
}
func (r radixObjects) put(t *testing.T, o codec.Object) model.ObjectID {
	t.Helper()
	id, err := codec.ObjectID(o)
	if err != nil {
		t.Fatal(err)
	}
	r[id], err = codec.EncodeObject(o)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func TestRadixStateReaderAuthenticatesRootPathsAndReferencedValues(t *testing.T) {
	objects := radixObjects{}
	value := objects.put(t, codec.Object{Type: "test.value", Space: "s", Version: 1, Payload: []byte("value")})
	delta, err := state.Apply(context.Background(), objects, "s", state.EmptyRoot, []state.Mutation{{Key: []byte("k"), Value: value}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range delta.Objects {
		objects.put(t, o)
	}
	revision, err := graph.BuildRevision("s", "", nil, delta.Root, nil, []graph.Entry{{Key: []byte("k"), Value: value}})
	if err != nil {
		t.Fatal(err)
	}
	reader := RadixStateReader{Reader: objects}
	if err = graph.VerifyState(revision, reader); err != nil {
		t.Fatal(err)
	}
	fake, err := graph.BuildRevision("s", "", nil, delta.Root, nil, []graph.Entry{{Key: []byte("different"), Value: value}})
	if err != nil {
		t.Fatal(err)
	}
	if err = graph.VerifyState(fake, reader); !errors.Is(err, graph.ErrStateMismatch) {
		t.Fatal("accepted claimed entries", err)
	}
	saved := objects[value]
	delete(objects, value)
	if err = graph.VerifyState(revision, reader); err == nil {
		t.Fatal("missing value authenticated")
	}
	objects[value] = append([]byte(nil), saved...)
	objects[value][len(saved)-1] ^= 1
	if err = graph.VerifyState(revision, reader); err == nil {
		t.Fatal("tampered value authenticated")
	}
	objects[value] = saved
	if err = graph.VerifyState(revision, RadixStateReader{Reader: objects, Limits: state.ScanLimits{MaxBytes: 1}}); !errors.Is(err, state.ErrLimit) {
		t.Fatal("truncated snapshot treated complete", err)
	}
}
