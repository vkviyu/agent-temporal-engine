package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func (e *Engine) agentPublishedAnchor(space model.SpaceID) model.SnapshotAnchor {
	fence := e.publication.Fence()
	if e.baseHistory != nil {
		fence = model.CommitFence{}
		for _, p := range e.baseHistory {
			if fence.Store == "" || p.fence.LSN > fence.LSN {
				fence = p.fence
			}
		}
	}
	return model.SnapshotAnchor{Space: space, Fence: fence}
}

func (e *Engine) agentInputContent(ctx context.Context, req api.CommitRequest, anchor model.SnapshotAnchor, ids ...model.ObjectID) error {
	roots, _, err := e.rootsAtFence(ctx, anchor.Fence, true)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	root, err := e.resolveDomainCursor(ctx, roots, anchor)
	if err != nil {
		return err
	}
	// Authenticate domain membership once per input set, with one cumulative
	// read budget. Re-scanning the whole radix for each InputObject would turn
	// a bounded command into quadratic IO and allocation.
	reader := &cursorReader{source: e.index}
	entries, err := state.Entries(ctx, reader, anchor.Space, root, state.ScanLimits{})
	if err != nil {
		return err
	}
	present := make(map[model.ObjectID]bool, len(entries))
	for _, item := range entries {
		present[item.Value] = true
	}
	seen := make(map[model.ObjectID]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if !present[id] {
			entry, ok := e.events[model.EventID(id)]
			if !ok {
				entry, ok = e.baseEvents[model.EventID(id)]
			}
			if !e.options.EventProfile || !ok || entry.Space != anchor.Space || entry.Principal != req.Principal || entry.LSN > anchor.Fence.LSN {
				return agentprofile.ErrUnknown
			}
			object, err := readObject(ctx, reader, id)
			if err != nil {
				return err
			}
			event, err := eventlog.Decode(object)
			if err != nil || event.ID != model.EventID(id) || event.Transaction != entry.Transaction || event.RecordedBy != req.Principal {
				return ErrStoredObject
			}
			continue
		}
		object, err := readObject(ctx, reader, id)
		if err != nil {
			return err
		}
		if object.Space != anchor.Space {
			return ErrInvalidStateData
		}
		if _, err = readValue(object); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) agentInputRequest(ctx context.Context, req api.CommitRequest, anchor model.SnapshotAnchor, id, digest model.ObjectID) error {
	if err := e.agentInputContent(ctx, req, anchor, id); err != nil {
		return err
	}
	object, err := readObject(ctx, e.index, id)
	if err != nil {
		return err
	}
	if object.Type != valueType {
		return agentprofile.ErrCommandShape
	}
	value, err := readValue(object)
	if err != nil {
		return err
	}
	if value.Kind != api.ValueBytes || agentprofile.ExactRequestDigest(value.Data) != digest {
		return agentprofile.ErrConflict
	}
	return nil
}

// validateCurrentTypedPacket prevents a model dispatch from reusing a packet
// whose promoted memory or experience-path provenance has since been
// superseded or retracted. Historical packet reads remain available; current
// execution must make the revocation visible before invoking the model.
func (e *Engine) validateCurrentTypedPacket(ctx context.Context, packet knowledge.ContextPacket, snapshot knowledge.Snapshot, space model.SpaceID, principal model.PrincipalID) error {
	if packet.Key == nil {
		return nil
	}
	observations := make(map[model.ObjectID]knowledge.Observation, len(snapshot.Observations))
	for _, observation := range snapshot.Observations {
		observations[observation.ID] = observation
	}
	for _, item := range packet.Items {
		observation, ok := observations[item.Observation]
		if !ok || observation.PayloadKind == "" {
			continue
		}
		store, _, _, err := e.loadMemory(ctx, e.roots, space)
		if err != nil {
			return err
		}
		at := model.SnapshotAnchor{Space: space, Fence: e.publication.Fence()}
		if observation.PayloadKind == "memory" {
			_, err = store.ReusableMemory(observation.Payload, at)
		} else if observation.PayloadKind == "experience_path" {
			_, err = store.ReusablePath(observation.Payload, at)
		} else if observation.PayloadKind == "summary" {
			summary, _, found, loadErr := e.loadSummary(ctx, e.roots, space, principal, observation.Payload)
			if loadErr != nil {
				return loadErr
			}
			if !found || summary.Validation.Status != "validated" {
				return agentprofile.ErrConflict
			}
			err = store.AccessPolicy().Check(summaryPolicyIDs(summary), memory.AccessReuse)
		} else {
			err = agentprofile.ErrConflict
		}
		if err != nil {
			return agentprofile.ErrConflict
		}
	}
	return nil
}
