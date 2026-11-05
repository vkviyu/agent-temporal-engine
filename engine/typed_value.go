package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// prepareTypedValue is the publication half of a registered domain reducer.
// It never decodes caller-supplied StateOps; key/raw must be produced by the
// reducer while holding the coordinator's private candidate.
func (e *Engine) prepareTypedValue(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, key, raw []byte) (*commitPlan, error) {
	if len(raw) > api.MaxStateValueBytes {
		return nil, storage.ErrLimit
	}
	beforeRoot, err := slotSpaceRoot(ctx, e.index, methodRoot(e.roots, req.Method), req.Space)
	if err != nil {
		return nil, err
	}
	before, found, err := state.Get(ctx, e.index, req.Space, beforeRoot, key)
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
		if _, err = readValue(previous); err != nil {
			return nil, err
		}
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := b.add(command, storage.KindObject)
	if err != nil {
		return nil, err
	}
	obj, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		return nil, err
	}
	after, err := b.add(obj, storage.KindObject)
	if err != nil {
		return nil, err
	}
	delta, err := state.Apply(ctx, b, req.Space, beforeRoot, []state.Mutation{{Key: key, Value: after}})
	if err != nil {
		return nil, err
	}
	for _, obj := range delta.Objects {
		if _, err = b.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	ops, err := makeOperations(operationsRecord{Space: req.Space, BeforeRoot: beforeRoot, AfterRoot: delta.Root, Changes: []stateChange{{Key: bytes.Clone(key), Before: before, After: after}}})
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
