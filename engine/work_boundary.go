package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
)

// validateWorkBoundary checks the semantic work unit as a whole before any
// private substep is applied. The existing reducers still check every local
// authority, epoch, uniqueness and historical-source precondition.
func (e *Engine) validateWorkBoundary(ctx context.Context, req api.CommitRequest, u api.WorkUnit, seed *initialWorkSeed) error {
	if u.Kind == "emission_dispatch" {
		return e.validateEmissionDispatch(ctx, req, u)
	}
	if u.QueryTurn != nil {
		return e.validateQueryTurn(ctx, req, u)
	}
	inputs, err := e.collectWorkBoundaryInputs(ctx, req, u)
	if err != nil {
		return err
	}
	ledger := inputs.ledger
	accepts := inputs.accepts
	finishes := inputs.finishes
	continuations := inputs.continuations
	if (u.Kind == "query_start") != (len(inputs.starts) > 0) || (u.Kind == "child_spawn") != (len(inputs.spawns) > 0) || (u.Kind == "message_consume") != (len(inputs.consumes) > 0) {
		return ErrWorkBoundary
	}
	if u.Kind != "model_decision" && len(inputs.invocations) > 0 {
		return ErrWorkBoundary
	}
	for execution, finish := range finishes {
		continuation := continuations[execution]
		if (u.Kind != "checkpoint" && u.Kind != "model_decision") || continuation == nil || continuation.Checkpoint == nil ||
			continuation.Checkpoint.Continuation.Position.Step != finish.Sequence || continuation.Checkpoint.Authority != finish.Authority {
			return ErrWorkBoundary
		}
	}
	seed.finishedSteps = finishes
	// A primary model result cannot use the generic checkpoint escape hatch.
	for _, accept := range accepts {
		recovery := ledger.Snapshot().Lifecycle.Recovery
		if recovery == nil {
			return ErrWorkBoundary
		}
		for _, spec := range recovery.ActivitySpecs {
			if spec.Activity == accept.Activity && spec.Role == "primary_decision" && u.Kind != "model_decision" {
				return ErrWorkBoundary
			}
		}
	}
	return e.validateWorkBoundaryKind(ctx, req, u, seed, inputs)
}
