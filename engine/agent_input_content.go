package engine

import (
	"context"
	"errors"

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
	reader := &cursorReader{source: e.reader(ctx)}
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
	object, err := readObject(ctx, e.reader(ctx), id)
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

func isModelActivityRole(role string) bool {
	return role == "routing" || role == "query_decision" || role == "primary_decision" || role == "aux_summary"
}

// validateCurrentTypedPacket prevents a model dispatch from reusing a packet
// whose promoted memory or experience-path provenance has since been
// superseded or retracted. Historical packet reads remain available; current
// execution must make the revocation visible before invoking the model.
func (e *Engine) validateCurrentTypedPacket(ctx context.Context, packet knowledge.ContextPacket, snapshot knowledge.Snapshot, space model.SpaceID, principal model.PrincipalID) error {
	if len(snapshot.Observations) != len(packet.Items) {
		return ErrInvalidStateData
	}
	return e.validateCurrentTypedObservations(ctx, snapshot.Observations, space, principal)
}

func (e *Engine) validateCurrentTypedObservations(ctx context.Context, selected []knowledge.Observation, space model.SpaceID, principal model.PrincipalID) error {
	var pending []knowledge.SummarySource
	for _, observation := range selected {
		if observation.PayloadKind != "" {
			pending = append(pending, knowledge.SummarySource{Content: observation.Payload, PayloadKind: observation.PayloadKind})
		}
	}
	seen := make(map[knowledge.SummarySource]bool, len(pending))
	var store *memory.Store
	at := model.SnapshotAnchor{Space: space, Fence: e.publication.Fence()}
	query := memory.Query{Space: space, At: at}
	edges := len(pending)
	for len(pending) > 0 {
		source := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[source] {
			continue
		}
		if len(seen) >= maxDerivationNodes || edges > maxDerivationEdges {
			return state.ErrLimit
		}
		seen[source] = true
		switch source.PayloadKind {
		case "memory", "experience_path":
			if store == nil {
				var err error
				store, _, _, err = e.loadMemory(ctx, e.roots, space)
				if err != nil {
					return err
				}
			}
			var err error
			if source.PayloadKind == "memory" {
				_, err = store.ReusableMemoryFor(source.Content, query)
			} else {
				_, err = store.ReusablePathFor(source.Content, query)
			}
			if err != nil {
				return errors.Join(agentprofile.ErrConflict, err)
			}
		case "summary":
			summary, _, found, err := e.loadSummary(ctx, e.roots, space, principal, source.Content)
			if err != nil {
				return err
			}
			if !found || summary.Validation.Status != "validated" {
				return agentprofile.ErrConflict
			}
			if err := e.checkPolicyIDs(ctx, space, summaryPolicyIDs(summary), memory.AccessReuse); err != nil {
				return errors.Join(agentprofile.ErrConflict, err)
			}
			for _, dependency := range summary.Sources {
				if dependency.PayloadKind != "" {
					pending = append(pending, knowledge.SummarySource{Content: dependency.Content, PayloadKind: dependency.PayloadKind})
					edges++
				}
			}
		default:
			return ErrInvalidStateData
		}
	}
	return nil
}
