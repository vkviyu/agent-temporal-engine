package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

var ErrActionReservation = errors.New("action reservation rejected")

// checkActionReservation enforces the durable action-intent boundary for a
// new host request. A request that is already published is handled by the
// receipt-first coordinator path and must never be rejected by a newer
// reservation. For a new request, the exact request key, method and bytes
// must match one current pending/rebound intent and its current authority.
func (e *Engine) checkActionReservation(ctx context.Context, req api.CommitRequest, recorded *operationsRecord) error {
	if recorded != nil || !e.options.AgentProfile {
		return nil
	}
	if err := e.checkQueryReservation(ctx, req); err != nil {
		return err
	}
	if err := e.checkQueryTurnReservation(ctx, req); err != nil {
		return err
	}
	ledger, _, found, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil || !found {
		return err
	}
	return validateActionReservation(ledger, req)
}

func validateActionReservation(ledger *agentprofile.Ledger, req api.CommitRequest) error {
	snapshot := ledger.Snapshot()
	var selected *agentprofile.ActionIntent
	for _, intent := range recoveryActions(snapshot) {
		if intent.Action.Request != req.Key() {
			continue
		}
		// Check the entire history before accepting one slot. Legacy data may
		// contain more than one reservation for a RequestKey; selecting whichever
		// slice member happens to come first would make authority ambiguous.
		if selected != nil {
			return fmt.Errorf("%w: %w", ErrActionReservation, agentprofile.ErrConflict)
		}
		copy := intent
		selected = &copy
	}
	if selected == nil {
		return nil
	}
	if selected.Method != req.Method || !bytes.Equal(selected.ExactCommand, req.Body) || !sameExpectedFence(selected.ExpectedFence, req.ExpectedFence) {
		return fmt.Errorf("%w: %w", ErrActionReservation, agentprofile.ErrConflict)
	}
	if err := ledger.ValidateActionDispatch(selected.Action.Key, selected.Action.Generation, req.Key()); err != nil {
		return fmt.Errorf("%w: %w", ErrActionReservation, err)
	}
	return nil
}

func sameExpectedFence(a, b *model.CommitFence) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// recoveryActions returns intents from an owned ledger snapshot.
func recoveryActions(snapshot agentprofile.Snapshot) []agentprofile.ActionIntent {
	if snapshot.Lifecycle.Recovery == nil {
		return nil
	}
	return snapshot.Lifecycle.Recovery.Actions
}
