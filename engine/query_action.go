package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
)

// A managed Query can create activities inside its validated Work, or by
// executing the exact action that its controller already reserved. An
// alternate key cannot allocate the same activity ahead of recovery.
func (e *Engine) checkQueryActivityCreate(ctx context.Context, req api.CommitRequest, spec agentprofile.ActivitySpec) error {
	p, found, err := e.loadQueryProgress(ctx, req, spec.Query)
	if err != nil {
		return err
	}
	if !found {
		if spec.Role == "external_tool" {
			return ErrWorkBoundary
		}
		return nil
	}
	if p.Next == nil || !sameQueryRequest(*p.Next, req) {
		return ErrActionReservation
	}
	if spec.Role == "external_tool" {
		inv, found := queryInvocation(p, spec.Activity)
		if !found || !matchesQueryToolEffect(inv, spec) {
			return ErrWorkBoundary
		}
	}
	return nil
}

func matchesQueryToolEffect(inv api.QueryInvocation, spec agentprofile.ActivitySpec) bool {
	return spec.Role == "external_tool" && inv.DispatchClass == spec.Effect &&
		spec.Contract.Definition == inv.Definition
}

// A Query continuation may reserve only its own registered controller action.
// An arbitrary valid CommitRequest is not sufficient: it could otherwise point
// at another Query or silently downgrade atomic decision/answer processing.
func (e *Engine) validateQueryNext(ctx context.Context, req api.CommitRequest, p api.QueryProgress, l *agentprofile.Ledger) error {
	if p.Next == nil {
		return nil
	}
	if p.Next.Method == api.MethodApplyWork {
		u, err := api.DecodeWorkUnit(p.Next.Body)
		if err != nil || u.Query != p.Query {
			return ErrWorkBoundary
		}
		if u.Kind == "emission_dispatch" {
			c, err := agentprofile.DecodeCommand(u.Steps[1].Body)
			if err != nil || c.Control == nil || c.Control.DispatchAttempt == nil {
				return ErrWorkBoundary
			}
			recovery := l.Snapshot().Lifecycle.Recovery
			if recovery == nil {
				return ErrWorkBoundary
			}
			for _, attempt := range recovery.AttemptControls {
				if attempt.Attempt == c.Control.DispatchAttempt.Attempt {
					spec, err := queryActivitySpec(l, attempt.Activity)
					if err == nil && spec.Query == p.Query && spec.Execution == "" {
						return nil
					}
				}
			}
			return ErrWorkBoundary
		}
		if u.QueryTurn == nil || u.QueryTurn.ExpectedVersion != p.Version || u.QueryTurn.AdapterVersion != p.AdapterVersion {
			return ErrWorkBoundary
		}
		return nil
	}
	c, err := agentprofile.DecodeCommand(p.Next.Body)
	if err != nil || c.Control == nil {
		return ErrWorkBoundary
	}
	x := c.Control
	activity, attempt := "", ""
	switch {
	case x.CreateActivity != nil:
		s := x.CreateActivity.Spec
		inv, found := queryInvocation(p, s.Activity)
		if !found || s.Query != p.Query || s.Execution != "" || s.Role != "external_tool" || s.Invocation != "" || s.Step != 0 || !matchesQueryToolEffect(inv, s) {
			return ErrWorkBoundary
		}
		return e.checkToolActivityDefinition(ctx, req, s)
	case x.ClaimAttempt != nil:
		activity = x.ClaimAttempt.Activity
	case x.DispatchAttempt != nil:
		return ErrWorkBoundary
	case x.CaptureResult != nil:
		activity = x.CaptureResult.Result.Activity
	case x.SettleActivity != nil:
		activity = x.SettleActivity.Activity
	case x.ReconcileAttempt != nil:
		attempt = x.ReconcileAttempt.Attempt
	case x.RetryActivity != nil:
		activity = x.RetryActivity.Activity
	default:
		return ErrWorkBoundary
	}
	if attempt != "" {
		for _, a := range l.Snapshot().Lifecycle.Attempts {
			if a.ID == attempt {
				activity = a.Activity
			}
		}
	}
	s, err := queryActivitySpec(l, activity)
	if err != nil || s.Query != p.Query || s.Execution != "" {
		return ErrWorkBoundary
	}
	return nil
}
