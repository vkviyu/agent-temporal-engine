package engine

import (
	"bytes"
	"context"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

const cursorMaxReads = 65536
const cursorMaxReadBytes = 64 << 20

// cursorReader accounts for the entire operation, including value objects,
// rather than bounding radix nodes while permitting unbounded value IO.
type cursorReader struct {
	source       state.ObjectReader
	reads, bytes int
}

func (r *cursorReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.reads >= cursorMaxReads {
		return nil, state.ErrLimit
	}
	r.reads++
	b, err := r.source.ReadObject(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(b) > cursorMaxReadBytes-r.bytes {
		return nil, state.ErrLimit
	}
	r.bytes += len(b)
	return b, nil
}

// resolveDomainCursor resolves only against the already-authenticated RootSet
// captured for anchor.Fence. Membership in that BranchRefs tree establishes
// that the Revision existed at this cut; consulting today's graph would leak
// future revisions into an old View. The caller must hold the View's read pin
// and run the normal output gate before returning content.
func (e *Engine) resolveDomainCursor(ctx context.Context, roots model.RootSet, anchor model.SnapshotAnchor) (model.ObjectID, error) {
	if err := anchor.Validate(); err != nil {
		return "", err
	}
	if anchor.Knowledge != nil {
		return "", ErrUnsupportedMethod
	}
	reader := &cursorReader{source: e.index}
	if anchor.Cursor == nil {
		return domainRoot(ctx, reader, roots, anchor.Space)
	}
	branchRoot, err := slotSpaceRoot(ctx, reader, roots.BranchRefs, anchor.Space)
	if err != nil {
		return "", err
	}
	id, found, err := state.Get(ctx, reader, anchor.Space, branchRoot, graphRevisionKey(anchor.Cursor.Revision))
	if err != nil {
		return "", err
	}
	if !found {
		return "", model.ErrAnchorMismatch
	}
	obj, err := readObject(ctx, reader, id)
	if err != nil {
		return "", err
	}
	if obj.Space != anchor.Space {
		return "", ErrInvalidStateData
	}
	v, err := readValue(obj)
	if err != nil {
		return "", err
	}
	if v.Kind != api.ValueBytes {
		return "", ErrInvalidStateData
	}
	r, err := graph.DecodeRevision(v.Data)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if r.Space != anchor.Space || r.ID != anchor.Cursor.Revision {
		return "", ErrInvalidStateData
	}
	entries, err := state.Entries(ctx, reader, anchor.Space, r.StateRoot, state.ScanLimits{MaxNodes: cursorMaxReads, MaxEntries: graph.MaxRevisionEntries, MaxBytes: 16 << 20, MaxReadBytes: cursorMaxReadBytes})
	if err != nil {
		return "", err
	}
	if len(entries) != len(r.Entries) {
		return "", fmt.Errorf("%w: cursor snapshot entries", ErrInvalidStateData)
	}
	for i, item := range entries {
		if !bytes.Equal(item.Key, r.Entries[i].Key) || item.Value != r.Entries[i].Value {
			return "", fmt.Errorf("%w: cursor snapshot entry", ErrInvalidStateData)
		}
		value, err := readObject(ctx, reader, item.Value)
		if err != nil {
			return "", err
		}
		if value.Space != anchor.Space {
			return "", ErrInvalidStateData
		}
		if _, err = readValue(value); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return r.StateRoot, nil
}
