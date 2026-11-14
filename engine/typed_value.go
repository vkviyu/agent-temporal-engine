package engine

import (
	"bytes"
	"context"
	"slices"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// prepareTypedValue is the publication half of a registered domain reducer.
// It never decodes caller-supplied StateOps; key/raw must be produced by the
// reducer while holding the coordinator's private candidate.
func (e *Engine) prepareTypedValue(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, key, raw []byte) (*commitPlan, error) {
	return e.prepareTypedValues(ctx, req, digest, txID, lsn, recorded, []typedValueWrite{{key: key, raw: raw}})
}

type typedValueWrite struct {
	key       []byte
	raw       []byte
	radixRoot model.ObjectID
}

// A reducer may atomically publish its primary record and the bounded derived
// indexes that are part of its invariant. Keys and values are engine-owned.
func (e *Engine) prepareTypedValues(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, writes []typedValueWrite) (*commitPlan, error) {
	return e.prepareTypedValuesWithBuilder(ctx, req, digest, txID, lsn, recorded, newBuilder(ctx, e.index, e.options.MaxObjects), writes)
}

func (e *Engine) prepareTypedValuesWithBuilder(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, b *objectBuilder, writes []typedValueWrite) (*commitPlan, error) {
	if len(writes) == 0 || len(writes) > api.MaxStateMutations {
		return nil, storage.ErrLimit
	}
	writes = slices.Clone(writes)
	slices.SortFunc(writes, func(a, b typedValueWrite) int { return bytes.Compare(a.key, b.key) })
	beforeRoot, err := slotSpaceRoot(ctx, e.index, methodRoot(e.roots, req.Method), req.Space)
	if err != nil {
		return nil, err
	}
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := b.add(command, storage.KindObject)
	if err != nil {
		return nil, err
	}
	mutations := make([]state.Mutation, len(writes))
	changes := make([]stateChange, len(writes))
	for i, write := range writes {
		if len(write.raw) > api.MaxStateValueBytes {
			return nil, storage.ErrLimit
		}
		if i > 0 && bytes.Equal(writes[i-1].key, write.key) {
			return nil, ErrInvalidStateData
		}
		before, found, err := state.Get(ctx, e.index, req.Space, beforeRoot, write.key)
		if err != nil {
			return nil, err
		}
		if found {
			previous, err := readObject(ctx, e.index, before)
			if err != nil {
				return nil, err
			}
			if previous.Space != req.Space {
				return nil, ErrInvalidStateData
			}
			if write.radixRoot != "" {
				if previous.Type != state.NodeType || previous.Version != state.NodeVersion {
					return nil, ErrInvalidStateData
				}
			} else if _, err = readValue(previous); err != nil {
				return nil, err
			}
		}
		after := write.radixRoot
		if after != "" {
			object, err := readObject(ctx, b, after)
			if err != nil {
				return nil, err
			}
			if object.Space != req.Space || object.Type != state.NodeType || object.Version != state.NodeVersion || len(write.raw) != 0 {
				return nil, ErrInvalidStateData
			}
		} else {
			object, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: write.raw})
			if err != nil {
				return nil, err
			}
			after, err = b.add(object, storage.KindObject)
			if err != nil {
				return nil, err
			}
		}
		mutations[i] = state.Mutation{Key: write.key, Value: after}
		changes[i] = stateChange{Key: bytes.Clone(write.key), Before: before, After: after}
	}
	delta, err := state.Apply(ctx, b, req.Space, beforeRoot, mutations)
	if err != nil {
		return nil, err
	}
	for _, obj := range delta.Objects {
		if _, err = b.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	ops, err := makeOperations(operationsRecord{Space: req.Space, BeforeRoot: beforeRoot, AfterRoot: delta.Root, Changes: changes})
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		expected, e := makeOperations(*recorded)
		if e != nil || expected.Space != ops.Space || !bytes.Equal(expected.Payload, ops.Payload) {
			return nil, ErrInvalidStateData
		}
	}
	opsID, err := b.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	catalog, err := state.Apply(ctx, b, systemSpace, methodRoot(e.roots, req.Method), []state.Mutation{{Key: []byte(req.Space), Value: delta.Root}})
	if err != nil {
		return nil, err
	}
	for _, obj := range catalog.Objects {
		if _, err = b.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	roots := e.roots
	setMethodRoot(&roots, req.Method, catalog.Root)
	return e.finishPlan(ctx, req, digest, txID, lsn, b, roots, delta.Root, commandID, opsID)
}
