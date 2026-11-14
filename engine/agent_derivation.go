package engine

import (
	"context"
	"slices"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func controlDerivationKey(space model.SpaceID, object model.ObjectID) []byte {
	b, _ := codec.AppendString(nil, "ate/derivation/v1")
	b, _ = codec.AppendString(b, string(space))
	b, _ = codec.AppendString(b, string(object))
	return b
}

// A final-answer renderer has Query scope and cannot import completed workers'
// private knowledge into its Packet. Its attestation supplies explicit outcome
// provenance instead. Commit these links in the SAME LiveControl transition as
// Activity creation, so raw request/attestation reads cannot bypass withdrawal.
func (e *Engine) agentDerivationMutations(ctx context.Context, req api.CommitRequest, command agentprofile.Command, builder *objectBuilder) ([]state.Mutation, []stateChange, error) {
	if command.Control == nil || command.Control.CreateActivity == nil || command.Control.CreateActivity.Spec.FinalAnswerInput == "" {
		return nil, nil, nil
	}
	spec := command.Control.CreateActivity.Spec
	sources, err := e.finalAnswerInputDisclosureIDs(ctx, spec.FinalAnswerInput)
	if err != nil {
		return nil, nil, err
	}
	links := map[model.ObjectID][]model.ObjectID{
		spec.FinalAnswerInput: sources,
		spec.Request:          {spec.FinalAnswerInput},
		spec.ContextPacket:    {spec.FinalAnswerInput},
	}
	objects := make([]model.ObjectID, 0, len(links))
	for id := range links {
		objects = append(objects, id)
	}
	slices.Sort(objects)
	reader := &cursorReader{source: e.index}
	var mutations []state.Mutation
	var changes []stateChange
	for _, id := range objects {
		key := controlDerivationKey(req.Space, id)
		previous, err := loadDerivationAt(ctx, reader, e.roots.LiveControl, systemSpace, req.Space, key, id)
		if err != nil {
			return nil, nil, err
		}
		dependencies := append(slices.Clone(links[id]), previous...)
		slices.Sort(dependencies)
		dependencies = slices.Compact(dependencies)
		dependencies = slices.DeleteFunc(dependencies, func(source model.ObjectID) bool { return source == "" || source == id })
		if len(dependencies) > maxDerivationNodes {
			return nil, nil, storage.ErrLimit
		}
		raw, err := encodeDerivation(id, dependencies)
		if err != nil {
			return nil, nil, err
		}
		before, _, err := state.Get(ctx, reader, systemSpace, e.roots.LiveControl, key)
		if err != nil {
			return nil, nil, err
		}
		object, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
		if err != nil {
			return nil, nil, err
		}
		after, err := builder.add(object, storage.KindObject)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, state.Mutation{Key: key, Value: after})
		changes = append(changes, stateChange{Key: key, Before: before, After: after})
	}
	return mutations, changes, nil
}
