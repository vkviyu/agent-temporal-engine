package engine

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// A fixed reader borrows only the index's location metadata. Its physical
// source remains the pinned immutable layout selected by the readCut.
type fixedObjectReader struct {
	index     *objectIndex
	source    io.ReaderAt
	canonical io.ReaderAt
}

func (r *objectIndex) location(id model.ObjectID) (storage.RecordLocation, bool) {
	r.mu.RLock()
	location, found := r.entries[id]
	r.mu.RUnlock()
	if !found && r.parent != nil {
		return r.parent.location(id)
	}
	return location, found
}

func (r *fixedObjectReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loc, found := r.index.location(id)
	if !found {
		return nil, fmt.Errorf("%w: %w: %s", ErrStoredObject, ErrMissingObject, id)
	}
	if loc.PayloadLength == 0 || loc.PayloadLength > codec.MaxEncodedObject || loc.PayloadOffset < 0 {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, ErrInvalidStateData)
	}
	data := make([]byte, int(loc.PayloadLength))
	source := r.source
	// Internal fault tests replace the historical index source after a cut is
	// captured. Honor that explicit injection while ordinary production reads
	// remain pinned to the immutable layout selected by the cut.
	if r.index.source != nil && r.index.source != r.canonical {
		source = r.index.source
	}
	n, err := source.ReadAt(data, loc.PayloadOffset)
	if n == len(data) && errors.Is(err, io.EOF) {
		err = nil
	}
	if n != len(data) && (err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, io.ErrUnexpectedEOF)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrObjectIO, err)
	}
	if _, err := codec.VerifyObject(id, data); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}
