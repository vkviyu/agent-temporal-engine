package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var (
	ErrMissingObject = errors.New("referenced immutable object is missing")
	ErrStoredObject  = errors.New("stored object integrity verification failed")
	ErrObjectIO      = errors.New("stored object read failed")
)

type objectIndex struct {
	mu      sync.RWMutex
	source  io.ReaderAt
	entries map[model.ObjectID]storage.RecordLocation
	parent  *objectIndex
	staged  map[model.ObjectID][]byte
}

func newObjectIndex() *objectIndex {
	return &objectIndex{entries: make(map[model.ObjectID]storage.RecordLocation)}
}

// A batch owns only its additions. Published entries and their reader stay in
// the parent, so starting a batch does not copy the journal's object history.
func newStagingIndex(parent *objectIndex) *objectIndex {
	return &objectIndex{entries: make(map[model.ObjectID]storage.RecordLocation), parent: parent,
		staged: make(map[model.ObjectID][]byte)}
}

func (r *objectIndex) contains(id model.ObjectID) bool {
	r.mu.RLock()
	_, ok := r.entries[id]
	r.mu.RUnlock()
	if ok {
		return true
	}
	return r.parent != nil && r.parent.contains(id)
}

func (r *objectIndex) count() uint64 {
	r.mu.RLock()
	n := uint64(len(r.entries))
	r.mu.RUnlock()
	if r.parent != nil {
		n += r.parent.count()
	}
	return n
}

// The map is a rebuildable physical catalog, never the domain state. Every
// load rechecks the content identity against the canonical bytes on disk.
func (r *objectIndex) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b, ok := r.staged[id]; ok {
		if _, err := codec.VerifyObject(id, b); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return bytes.Clone(b), nil
	}
	r.mu.RLock()
	loc, ok := r.entries[id]
	r.mu.RUnlock()
	if !ok {
		if r.parent != nil {
			return r.parent.ReadObject(ctx, id)
		}
		return nil, fmt.Errorf("%w: %w: %s", ErrStoredObject, ErrMissingObject, id)
	}
	if r.source == nil {
		return nil, fmt.Errorf("%w: no reader", ErrObjectIO)
	}
	if loc.PayloadLength == 0 || loc.PayloadLength > codec.MaxEncodedObject || loc.PayloadOffset < 0 {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, ErrInvalidStateData)
	}
	b := make([]byte, int(loc.PayloadLength))
	n, err := r.source.ReadAt(b, loc.PayloadOffset)
	if n == len(b) && err == io.EOF {
		err = nil
	}
	if n < len(b) && (err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, io.ErrUnexpectedEOF)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrObjectIO, err)
	}
	if n != len(b) {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, io.ErrUnexpectedEOF)
	}
	if _, err = codec.VerifyObject(id, b); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return b, nil
}

func readObject(ctx context.Context, reader interface {
	ReadObject(context.Context, model.ObjectID) ([]byte, error)
}, id model.ObjectID) (codec.Object, error) {
	b, err := reader.ReadObject(ctx, id)
	if err != nil {
		return codec.Object{}, err
	}
	return codec.VerifyObject(id, b)
}

type stagedObject struct {
	id      model.ObjectID
	encoded []byte
	kind    storage.Kind
}

type objectBuilder struct {
	base       *objectIndex
	objects    []stagedObject
	staged     map[model.ObjectID][]byte
	ctx        context.Context
	maxObjects uint64
}

func newBuilder(ctx context.Context, base *objectIndex, maxObjects uint64) *objectBuilder {
	return &objectBuilder{base: base, staged: make(map[model.ObjectID][]byte), ctx: ctx, maxObjects: maxObjects}
}

func (b *objectBuilder) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if data, ok := b.staged[id]; ok {
		return bytes.Clone(data), nil
	}
	return b.base.ReadObject(ctx, id)
}

func (b *objectBuilder) add(obj codec.Object, kind storage.Kind) (model.ObjectID, error) {
	if err := b.ctx.Err(); err != nil {
		return "", err
	}
	encoded, err := codec.EncodeObject(obj)
	if err != nil {
		return "", err
	}
	id, err := codec.ObjectID(obj)
	if err != nil {
		return "", err
	}
	if previous, ok := b.staged[id]; ok {
		if !bytes.Equal(previous, encoded) {
			return "", ErrInvalidStateData
		}
		return id, nil
	}
	if b.base.contains(id) {
		previous, err := b.base.ReadObject(b.ctx, id)
		if err != nil {
			return "", err
		}
		if !bytes.Equal(previous, encoded) {
			return "", ErrInvalidStateData
		}
		return id, nil
	}
	if b.base.count()+uint64(len(b.objects)) >= b.maxObjects {
		return "", storage.ErrLimit
	}
	for _, ref := range obj.Refs {
		old := b.base.contains(ref)
		_, staged := b.staged[ref]
		if !old && !staged {
			return "", fmt.Errorf("%w: non-topological reference", ErrMissingObject)
		}
	}
	b.staged[id] = encoded
	b.objects = append(b.objects, stagedObject{id: id, encoded: encoded, kind: kind})
	return id, nil
}

// put publishes only an immutable object location; no borrowed data is stored.
func (r *objectIndex) put(id model.ObjectID, loc storage.RecordLocation) {
	r.mu.Lock()
	r.entries[id] = loc
	r.mu.Unlock()
}
