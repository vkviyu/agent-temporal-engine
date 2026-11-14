package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) checkAgentControlInputs(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, c agentprofile.Command) error {
	if c.CompleteStep != nil {
		return ErrWorkBoundary
	}
	latest := e.agentPublishedAnchor(req.Space)
	if c.Lifecycle != nil && c.Lifecycle.PutQuery != nil && c.Lifecycle.PutQuery.InputFence != (model.CommitFence{}) {
		if _, _, err := e.rootsAtFence(ctx, c.Lifecycle.PutQuery.InputFence, true); err != nil {
			return err
		}
	}
	if c.Lifecycle != nil && c.Lifecycle.PutContinuation != nil {
		if err := e.checkContinuationKnowledge(ctx, req, *c.Lifecycle.PutContinuation); err != nil {
			return err
		}
	}
	if c.Control == nil {
		return nil
	}
	x := c.Control
	// These transitions publish only a fragment of a long-running Agent work
	// unit (task/child, message consumption, or a model decision's accepted
	// invocation set). Public ApplyAgent must not expose them as independently
	// receipted transactions: the Work reducer is the only place that can
	// atomically bind seed knowledge, observations and the next continuation.
	// A private reduction is created only by prepareWork after validateWorkBoundary
	// has checked the complete protected group.
	if !e.privateReduction {
		switch {
		case x.StartWork != nil, x.SpawnChild != nil, x.ConsumeMessage != nil, x.RecordInvocation != nil, x.DispatchAttempt != nil, x.FinishStep != nil:
			return ErrWorkBoundary
		case x.AcceptActivity != nil:
			if spec, err := queryActivitySpec(ledger, x.AcceptActivity.Activity); err == nil && spec.Role == "primary_decision" {
				return ErrWorkBoundary
			}
		case x.ImportActivityResult != nil:
			if spec, err := queryActivitySpec(ledger, x.ImportActivityResult.Activity); err == nil && spec.Role == "primary_decision" {
				return ErrWorkBoundary
			}
		}
	}
	switch {
	case x.AnswerQuery != nil, x.ObserveGoal != nil, x.RebaseInherited != nil, x.Resume != nil:
		return e.checkAgentQueryGoalInputs(ctx, req, ledger, x, latest)
	case x.AcceptActivity != nil, x.ClaimAttempt != nil, x.DispatchAttempt != nil, x.BindValidation != nil, x.BeginStep != nil, x.RecordInvocation != nil, x.SpawnChild != nil, x.CreateActivity != nil:
		return e.checkAgentActivityInputs(ctx, req, ledger, x)
	case x.PutAction != nil, x.Checkpoint != nil, x.SaveContinuation != nil, x.CompleteTask != nil, x.ResolveAction != nil, x.RebindAction != nil:
		return e.checkAgentActionInputs(ctx, req, ledger, x, latest)
	}
	return nil
}

func agentActionIntent(s agentprofile.Snapshot, key string, generation uint64) (agentprofile.ActionIntent, error) {
	if s.Lifecycle.Recovery != nil {
		for _, intent := range s.Lifecycle.Recovery.Actions {
			if intent.Action.Key == key && intent.Action.Generation == generation {
				return intent, nil
			}
		}
	}
	return agentprofile.ActionIntent{}, agentprofile.ErrUnknown
}
