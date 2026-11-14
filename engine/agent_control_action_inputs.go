package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) checkAgentActionInputs(
	ctx context.Context,
	req api.CommitRequest,
	ledger *agentprofile.Ledger,
	x *agentprofile.ControlCommand,
	latest model.SnapshotAnchor,
) error {
	checkFence := func(f model.CommitFence) error {
		_, _, err := e.rootsAtFence(ctx, f, true)
		return err
	}
	checkIntent := func(intent agentprofile.ActionIntent) error {
		// The reservation transaction must not consume the namespace needed
		// by its future command. A receipt for a different command under that
		// key would otherwise leave the action permanently unresolvable.
		if intent.Action.Request == req.Key() {
			return agentprofile.ErrUniqueSlot
		}
		if err := checkGraphActionIntent(intent); err != nil {
			return err
		}
		if _, _, err := e.agentActionReceiptAtRoots(ctx, e.roots, intent); err != nil {
			return err
		}
		anchor := model.SnapshotAnchor{Space: req.Space, Fence: intent.Action.InputFence}
		if seed, ok := ctx.Value(initialWorkKey{}).(*initialWorkSeed); ok && seed.targets[intent.Action.Owner] != (model.KnowledgeKey{}) {
			// Creation Work may reserve the first action before the target
			// execution has a published knowledge partition. verifyWorkContinuation
			// checks the complete goal, seed and continuation proof after all
			// private steps have reduced against one candidate root.
			if _, _, err := e.rootsAtFence(ctx, intent.Action.InputFence, true); err != nil {
				return err
			}
		} else if err := e.checkGoalInputCut(ctx, req, ledger, intent.Action.Owner, intent.Action.InputFence); err != nil {
			return err
		}
		return e.agentInputContent(ctx, req, anchor, intent.InputObjects...)
	}

	switch {
	case x.PutAction != nil:
		return checkIntent(x.PutAction.Intent)
	case x.Checkpoint != nil:
		if err := e.checkContinuationKnowledge(ctx, req, x.Checkpoint.Continuation); err != nil {
			return err
		}
		return checkIntent(x.Checkpoint.Intent)
	case x.SaveContinuation != nil:
		if err := e.checkContinuationKnowledge(ctx, req, x.SaveContinuation.Value); err != nil {
			return err
		}
		if err := e.checkGoalInputCut(ctx, req, ledger, x.SaveContinuation.Value.Execution, x.SaveContinuation.Value.InputFence); err != nil {
			return err
		}
		return checkFence(x.SaveContinuation.Value.InputFence)
	case x.CompleteTask != nil:
		return e.agentInputContent(ctx, req, latest, x.CompleteTask.Result, x.CompleteTask.Validation)
	case x.ResolveAction != nil:
		v := x.ResolveAction.Resolution
		intent, err := agentActionIntent(ledger.Snapshot(), v.Key, v.Generation)
		if err != nil {
			return err
		}
		if v.ObservedFence.Store != intent.Action.InputFence.Store || v.ObservedFence.LSN < intent.Action.InputFence.LSN {
			return model.ErrAnchorMismatch
		}
		id, found, err := e.agentActionReceipt(ctx, v.ObservedFence, intent)
		if err != nil {
			return err
		}
		if (v.Outcome == "applied" && (!found || id != v.Receipt)) || (v.Outcome != "applied" && found) {
			return agentprofile.ErrConflict
		}
		// Absence cannot override a newer original receipt, including one from
		// an earlier private group member. A rejection based on that candidate
		// remains deferred until the predecessor is durably published.
		if v.Outcome == "absent" {
			_, nowFound, err := e.agentActionReceiptAtRoots(ctx, e.roots, intent)
			if err != nil {
				return err
			}
			if nowFound {
				return agentprofile.ErrConflict
			}
		}
	case x.RebindAction != nil:
		intent, err := agentActionIntent(ledger.Snapshot(), x.RebindAction.Key, x.RebindAction.ExpectedGeneration)
		if err != nil {
			return err
		}
		_, found, err := e.agentActionReceiptAtRoots(ctx, e.roots, intent)
		if err != nil {
			return err
		}
		if found {
			return agentprofile.ErrConflict
		}
		next := intent
		next.Action.Request = x.RebindAction.NewRequest
		next.Method, next.ExactCommand, next.ExpectedFence = x.RebindAction.Method, x.RebindAction.ExactCommand, x.RebindAction.ExpectedFence
		next.Authority = x.RebindAction.Authority
		if err := checkGraphActionIntent(next); err != nil {
			return err
		}
		if _, _, err := e.agentActionReceiptAtRoots(ctx, e.roots, next); err != nil {
			return err
		}
	}
	return nil
}
