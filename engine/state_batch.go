package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func applyStateBatch(ctx context.Context, builder *objectBuilder, space model.SpaceID, beforeRoot model.ObjectID, batch api.StateBatch) (state.TreeDelta, []stateChange, error) {
	if batch.ExpectedRoot != nil && *batch.ExpectedRoot != beforeRoot {
		return state.TreeDelta{}, nil, errCAS
	}
	changes := make([]stateChange, len(batch.Operations))
	mutations := make([]state.Mutation, len(changes))
	for i, op := range batch.Operations {
		before, found, err := state.Get(ctx, builder, space, beforeRoot, op.Key)
		if err != nil {
			return state.TreeDelta{}, nil, err
		}
		if found {
			previous, err := readObject(ctx, builder, before)
			if err != nil {
				return state.TreeDelta{}, nil, err
			}
			if previous.Space != space {
				return state.TreeDelta{}, nil, ErrInvalidStateData
			}
			if _, err := readValue(previous); err != nil {
				return state.TreeDelta{}, nil, err
			}
		}
		switch op.Condition.Kind {
		case api.ConditionAbsent:
			if found {
				return state.TreeDelta{}, nil, errCAS
			}
		case api.ConditionValue:
			value, err := makeValue(space, op.Condition.Value)
			if err != nil {
				return state.TreeDelta{}, nil, err
			}
			expected, err := codec.ObjectID(value)
			if err != nil {
				return state.TreeDelta{}, nil, err
			}
			if !found || before != expected {
				return state.TreeDelta{}, nil, errCAS
			}
		}
		after := state.EmptyRoot
		if op.Kind == api.MutationPut {
			value, err := makeValue(space, op.Value)
			if err != nil {
				return state.TreeDelta{}, nil, err
			}
			after, err = builder.add(value, storage.KindObject)
			if err != nil {
				return state.TreeDelta{}, nil, err
			}
		}
		changes[i] = stateChange{Key: bytes.Clone(op.Key), Before: before, After: after}
		mutations[i] = state.Mutation{Key: changes[i].Key, Value: changes[i].After, Delete: changes[i].After == state.EmptyRoot}
	}
	// beforeRoot is either the current domain root or an explicitly pinned branch root.
	delta, err := state.Apply(ctx, builder, space, beforeRoot, mutations)
	if err != nil {
		return state.TreeDelta{}, nil, err
	}
	for _, obj := range delta.Objects {
		if _, err = builder.add(obj, storage.KindObject); err != nil {
			return state.TreeDelta{}, nil, err
		}
	}
	return delta, changes, nil
}
