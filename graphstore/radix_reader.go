package graphstore

import (
	"context"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

// RadixStateReader bridges an immutable, retained object view to graph's state
// proof contract. Reader must remain pinned and authorized for this operation;
// this adapter authenticates bytes, not ACLs or a caller-selected publication.
// ValidateValue may impose the registered domain's value schema after hashing.
type RadixStateReader struct {
	Context       context.Context
	Reader        state.ObjectReader
	Limits        state.ScanLimits
	MaxValueBytes int
	ValidateValue func(codec.Object) error
}

var _ graph.StateReader = RadixStateReader{}

func (r RadixStateReader) ReadEntries(space model.SpaceID, root model.ObjectID) ([]graph.Entry, error) {
	ctx := r.Context
	if ctx == nil {
		ctx = context.Background()
	}
	maxValueBytes := r.MaxValueBytes
	if maxValueBytes == 0 {
		maxValueBytes = 64 << 20
	}
	if maxValueBytes < 1 || maxValueBytes > 256<<20 {
		return nil, state.ErrLimit
	}
	usedValueBytes := 0
	entries, err := state.Entries(ctx, r.Reader, space, root, r.Limits)
	if err != nil {
		return nil, err
	}
	out := make([]graph.Entry, len(entries))
	verified := map[model.ObjectID]bool{}
	for i, e := range entries {
		if !verified[e.Value] {
			if r.Reader == nil {
				return nil, graph.ErrStateUnverified
			}
			raw, err := r.Reader.ReadObject(ctx, e.Value)
			if err != nil {
				return nil, err
			}
			if len(raw) > maxValueBytes-usedValueBytes {
				return nil, state.ErrLimit
			}
			usedValueBytes += len(raw)
			obj, err := codec.VerifyObject(e.Value, raw)
			if err != nil {
				return nil, err
			}
			if obj.Space != space {
				return nil, fmt.Errorf("%w: value Space", graph.ErrStateMismatch)
			}
			if r.ValidateValue != nil {
				if err = r.ValidateValue(obj); err != nil {
					return nil, err
				}
			}
			verified[e.Value] = true
		}
		out[i] = graph.Entry{Key: e.Key, Value: e.Value}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
