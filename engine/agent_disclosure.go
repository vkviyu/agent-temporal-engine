package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

// activityInputDisclosureIDs follows recorded input provenance, not today's
// knowledge. Inline result/argument bytes may derive from this exact packet;
// changing their projection must not sever that dependency at an output gate.
func (e *Engine) activityInputDisclosureIDs(ctx context.Context, space model.SpaceID, principal model.PrincipalID, spec agentprofile.ActivitySpec) ([]model.ObjectID, error) {
	ids := []model.ObjectID{spec.Request, spec.ContextPacket, spec.Contract.Definition}
	answerIDs, err := e.finalAnswerInputDisclosureIDs(ctx, spec.FinalAnswerInput)
	if err != nil {
		return nil, err
	}
	ids = append(ids, answerIDs...)
	if spec.ContextPacket == "" {
		return ids, nil
	}
	record, err := e.activityPacketAt(ctx, space, principal, spec.InputFence, spec.ContextPacket)
	if err != nil {
		return nil, err
	}
	dependencies, err := e.packetDependencyIDs(ctx, record.roots, space, principal, record.packet, record.selected)
	if err != nil {
		return nil, err
	}
	return append(ids, dependencies...), nil
}

// The attestation is immutable and validated at Activity creation. Its declared
// evidence remains part of every later disclosure/dispatch dependency closure.
// Checking only the attestation's own ID would miss withdrawal of an outcome.
func (e *Engine) finalAnswerInputDisclosureIDs(ctx context.Context, id model.ObjectID) ([]model.ObjectID, error) {
	if id == "" {
		return nil, nil
	}
	object, err := readObject(ctx, e.reader(ctx), id)
	if err != nil {
		return nil, err
	}
	value, err := readValue(object)
	if err != nil || value.Kind != api.ValueBytes {
		return nil, ErrStoredObject
	}
	input, err := agentprofile.DecodeFinalAnswerInput(value.Data)
	if err != nil {
		return nil, ErrStoredObject
	}
	ids := []model.ObjectID{id, input.Request, input.ContextPacket}
	for _, outcome := range input.Outcomes {
		ids = append(ids, outcome.Result, outcome.Validation)
	}
	return ids, nil
}

// checkAgentSnapshotDisclosure gates the complete control projection. Like
// GetQueryProgress and Node reads it refuses a withdrawn historical record;
// it never silently redacts bytes and then claims an exact snapshot. The core
// lock must protect current policy until the caller has finished this check.
func (e *Engine) checkAgentSnapshotDisclosure(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, snapshot agentprofile.Snapshot, collect bool) ([]model.ObjectID, error) {
	policy, err := e.readPolicy(ctx, space)
	if err != nil {
		return nil, err
	}
	if !collect && !policy.HasDisclosureDenials() {
		return nil, nil
	}
	dependencies := agentprofile.ContentReferences(snapshot)
	if err := e.checkPolicyIDs(ctx, space, dependencies, memory.AccessAudit); err != nil {
		return nil, err
	}
	if recovery := snapshot.Lifecycle.Recovery; recovery != nil {
		for _, spec := range recovery.ActivitySpecs {
			ids, err := e.activityInputDisclosureIDs(ctx, space, principal, spec)
			if err != nil {
				return nil, err
			}
			if err := e.checkPolicyIDs(ctx, space, ids, memory.AccessAudit); err != nil {
				return nil, err
			}
			if collect {
				dependencies = append(dependencies, ids...)
			}
		}
	}
	// Query-only tool definitions live in QueryProgress, outside the execution
	// Invocation collection. They remain provenance for inline request bytes.
	for _, query := range snapshot.Lifecycle.Queries {
		id, present, err := state.Get(ctx, e.reader(ctx), systemSpace, roots.LiveControl, queryProgressKey(space, principal, query.ID))
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		object, err := readObject(ctx, e.reader(ctx), id)
		if err != nil {
			return nil, err
		}
		progress, err := decodeQueryProgressValue(object)
		if err != nil {
			return nil, err
		}
		var ids []model.ObjectID
		for _, decision := range progress.Decisions {
			for _, invocation := range decision.Invocations {
				ids = append(ids, invocation.Definition)
			}
		}
		if err := e.checkPolicyIDs(ctx, space, ids, memory.AccessAudit); err != nil {
			return nil, err
		}
		if collect {
			dependencies = append(dependencies, ids...)
		}
	}
	return dependencies, nil
}

// A pending intake already carries declared sources in its exact next Work,
// even before that Work creates any Task or Activity. Reading the reservation
// must not disclose the same input through an alternate recovery endpoint.
func (e *Engine) checkQueryIntakeDisclosure(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, intake api.QueryIntake) ([]model.ObjectID, error) {
	unit, err := api.DecodeWorkUnit(intake.Next.Body)
	if err != nil {
		return nil, ErrInvalidStateData
	}
	var ids []model.ObjectID
	for _, step := range unit.Steps {
		switch step.Method {
		case api.MethodApplyKnowledge:
			command, err := knowledge.DecodeCommand(step.Body)
			if err != nil {
				return nil, ErrInvalidStateData
			}
			if o := command.Observation; o != nil {
				ids = append(ids, o.ID, o.Payload, o.ImportedFrom)
			}
			if packet := command.Packet; packet != nil {
				ids = append(ids, packetPolicyIDs(*packet, knowledge.Snapshot{})...)
			}
		case api.MethodApplyAgent:
			command, err := agentprofile.DecodeCommand(step.Body)
			if err != nil || command.Control == nil {
				return nil, ErrInvalidStateData
			}
			if create := command.Control.CreateActivity; create != nil {
				ids = append(ids, create.Spec.Request, create.Spec.ContextPacket)
			}
		}
	}
	if err := e.checkDisclosureIDs(ctx, space, ids); err != nil {
		return nil, err
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.reader(ctx), roots.LiveControl, space, principal)
	if err != nil {
		return nil, err
	}
	provenance, err := e.queryProgressDisclosureIDs(ctx, space, principal, ledger, intake.Query.ID)
	if err != nil {
		return nil, err
	}
	if err := e.checkDisclosureIDs(ctx, space, provenance); err != nil {
		return nil, err
	}
	return append(ids, provenance...), nil
}
