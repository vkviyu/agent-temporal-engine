package engine

import (
	"context"
	"encoding/json"
	"math"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) validateQueryTurn(ctx context.Context, req api.CommitRequest, u api.WorkUnit) error {
	t := u.QueryTurn
	l, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	q, ok := queryDefinition(l, u.Query)
	if !ok {
		if intake, found, intakeErr := e.loadQueryIntake(ctx, req.Space, req.Principal, u.Query); intakeErr != nil {
			return intakeErr
		} else if found {
			q, ok = intake.Intake.Query, true
		}
	}
	if !ok || q.Space != req.Space || q.Owner != req.Principal || q.Status != "open" {
		return ErrWorkBoundary
	}
	if t.Source.Space != req.Space {
		return ErrWorkBoundary
	}
	if _, _, err := e.rootsAtFence(ctx, t.Source.Fence, true); err != nil {
		return err
	}
	p, found, err := e.loadQueryProgress(ctx, req, u.Query)
	if err != nil {
		return err
	}
	if p.Version != t.ExpectedVersion || p.Completed || p.Version == math.MaxUint64 || (found && p.AdapterVersion != t.AdapterVersion) {
		return ErrWorkBoundary
	}
	if u.Kind == "query_open" {
		if found {
			return agentprofile.ErrUniqueSlot
		}
		intake, exists, err := e.loadQueryIntake(ctx, req.Space, req.Principal, u.Query)
		if err != nil {
			return err
		}
		if !exists || intake.Started || !sameQueryRequest(intake.Intake.Next, req) {
			return ErrWorkBoundary
		}
	} else {
		if !found || p.Next == nil {
			return ErrWorkBoundary
		}
		if !sameQueryRequest(*p.Next, req) {
			// The previous exact action must be receipted before moving the
			// controller forward. An interrupted waiter is never an absence proof.
			if _, applied, err := e.lookup(ctx, p.Next.Key()); err != nil {
				return err
			} else if !applied {
				return ErrWorkBoundary
			}
		}
	}
	if u.Kind == "query_decision" {
		if p.Sequence == math.MaxUint64 || t.Sequence != p.Sequence+1 || !queryToolsDone(p) || u.Decision.Activity != t.Activity || u.Decision.Result == "" || len(t.Invocations) != len(u.Decision.Invocations) {
			return ErrWorkBoundary
		}
		for _, old := range p.Decisions {
			if old.Activity == t.Activity {
				return agentprofile.ErrUniqueSlot
			}
		}
	} else if t.Sequence != p.Sequence {
		return ErrWorkBoundary
	}
	if t.Next != nil {
		if t.Next.Space != req.Space || t.Next.Principal != req.Principal || t.Next.Key() == req.Key() || (t.Next.Method != api.MethodApplyAgent && t.Next.Method != api.MethodApplyWork) {
			return ErrWorkBoundary
		}
		if _, exists, err := e.lookup(ctx, t.Next.Key()); err != nil {
			return err
		} else if exists {
			return agentprofile.ErrUniqueSlot
		}
		if _, exists, err := e.readQueryProgress(ctx, queryTurnReservationKey(t.Next.Key())); err != nil {
			return err
		} else if exists {
			return agentprofile.ErrUniqueSlot
		}
		if _, exists, err := e.readIntakeValue(ctx, intakeReservationKey(t.Next.Key())); err != nil {
			return err
		} else if exists {
			return agentprofile.ErrUniqueSlot
		}
		if r := l.Snapshot().Lifecycle.Recovery; r != nil {
			for _, intent := range r.Actions {
				if intent.Action.Request == t.Next.Key() {
					return agentprofile.ErrUniqueSlot
				}
			}
		}
	}
	creates, accepts, answers := 0, 0, 0
	for _, step := range u.Steps {
		if step.Method != api.MethodApplyAgent {
			return ErrWorkBoundary
		}
		c, err := agentprofile.DecodeCommand(step.Body)
		if err != nil {
			return err
		}
		x := c.Control
		activity := ""
		switch {
		case x.CreateActivity != nil:
			s := x.CreateActivity.Spec
			if s.Query != u.Query || s.Execution != "" || s.Step != 0 || s.Invocation != "" {
				return ErrWorkBoundary
			}
			if s.Role == "query_decision" || s.Role == "routing" {
				if (u.Kind != "query_open" && u.Kind != "query_checkpoint") || !queryToolsDone(p) || s.Activity != t.Activity {
					return ErrWorkBoundary
				}
			} else {
				inv, exists := queryInvocation(p, s.Activity)
				if u.Kind != "query_tool" || !exists || inv.AcceptedResult != "" || s.Role != "external_tool" || !matchesQueryToolEffect(inv, s) || s.Activity != t.Activity {
					return ErrWorkBoundary
				}
			}
			creates++
		case x.ClaimAttempt != nil:
			activity = x.ClaimAttempt.Activity
		case x.DispatchAttempt != nil:
			return ErrWorkBoundary
		case x.CaptureResult != nil:
			activity = x.CaptureResult.Result.Activity
		case x.AcceptActivity != nil:
			activity = x.AcceptActivity.Activity
			accepts++
			if u.Kind == "query_decision" {
				if activity != t.Activity || x.AcceptActivity.Result != u.Decision.Result {
					return ErrWorkBoundary
				}
			} else if u.Kind != "query_tool" || activity != t.Activity {
				return ErrWorkBoundary
			}
		case x.SettleActivity != nil:
			activity = x.SettleActivity.Activity
		case x.RetryActivity != nil:
			activity = x.RetryActivity.Activity
		case x.ReconcileAttempt != nil:
			for _, a := range l.Snapshot().Lifecycle.Attempts {
				if a.ID == x.ReconcileAttempt.Attempt {
					activity = a.Activity
				}
			}
		case x.AnswerQuery != nil:
			answers++
			if u.Kind != "query_answer" || x.AnswerQuery.Query != u.Query || !x.AnswerQuery.Final || !queryToolsDone(p) || len(p.Decisions) == 0 {
				return ErrWorkBoundary
			}
			if r := l.Snapshot().Lifecycle.Recovery; r != nil {
				for _, spec := range r.ActivitySpecs {
					if spec.Query == u.Query {
						ctl, err := queryActivityControl(l, spec.Activity)
						if err != nil || ctl.AcceptedResult == "" {
							return ErrWorkBoundary
						}
					}
				}
			}
		default:
			return ErrWorkBoundary
		}
		if activity != "" {
			s, err := queryActivitySpec(l, activity)
			if err != nil || s.Query != u.Query || s.Execution != "" || activity != t.Activity {
				return ErrWorkBoundary
			}
			if _, exists := queryInvocation(p, activity); exists {
				if u.Kind != "query_tool" && u.Kind != "query_checkpoint" {
					return ErrWorkBoundary
				}
			} else if u.Kind == "query_tool" || (s.Role != "query_decision" && s.Role != "routing") {
				return ErrWorkBoundary
			}
		} else if x.CreateActivity == nil && x.AnswerQuery == nil {
			return ErrWorkBoundary
		}
	}
	if u.Kind == "query_open" && creates != 1 || u.Kind == "query_decision" && accepts != 1 || u.Kind == "query_answer" && answers != 1 || u.Kind != "query_answer" && answers != 0 {
		return ErrWorkBoundary
	}
	if u.Kind == "query_decision" {
		spec, err := queryActivitySpec(l, t.Activity)
		if err != nil || spec.Query != u.Query || spec.Execution != "" || spec.Role != "query_decision" {
			return ErrWorkBoundary
		}
		definitions := make([]model.ObjectID, len(t.Invocations))
		for i, inv := range t.Invocations {
			definitions[i] = inv.Definition
		}
		if err := e.agentInputContent(ctx, req, model.SnapshotAnchor{Space: req.Space, Fence: spec.InputFence}, definitions...); err != nil {
			return err
		}
		for i, inv := range t.Invocations {
			binding := u.Decision.Invocations[i]
			if inv.ID != binding.Invocation || inv.Definition != binding.Definition || inv.ToolVersion != binding.Version || inv.DispatchClass != binding.DispatchClass {
				return ErrWorkBoundary
			}
			for _, old := range p.Decisions {
				for _, v := range old.Invocations {
					if v.ID == inv.ID || v.Activity == inv.Activity {
						return agentprofile.ErrUniqueSlot
					}
				}
			}
			if _, err := queryActivitySpec(l, inv.Activity); err == nil {
				return agentprofile.ErrUniqueSlot
			}
			if err := e.checkInvocationDefinition(ctx, req.Space, inv.Definition, inv.Tool, inv.ToolVersion, inv.DispatchClass, "external"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) finishQueryTurn(ctx context.Context, req api.CommitRequest, u api.WorkUnit, b *objectBuilder) error {
	t := u.QueryTurn
	p, _, err := e.loadQueryProgress(ctx, req, u.Query)
	if err != nil {
		return err
	}
	p.Query, p.Version, p.Sequence, p.AdapterVersion, p.Phase, p.Source, p.Next = u.Query, t.ExpectedVersion+1, t.Sequence, t.AdapterVersion, t.Phase, t.Source, t.Next
	l, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	if u.Kind == "query_decision" {
		// Acceptance has already run in the private candidate. Bind the entire
		// immutable tool set only if this is a successful, uniquely accepted result.
		r, err := l.GetActivityResult(u.Decision.Result)
		ctl, err2 := queryActivityControl(l, t.Activity)
		if err != nil || err2 != nil || r.Outcome != "success" || ctl.AcceptedResult != r.ID {
			return ErrWorkBoundary
		}
		p.Decisions = append(p.Decisions, api.QueryDecision{Sequence: t.Sequence, Activity: t.Activity, AcceptedResult: r.ID, Invocations: t.Invocations})
	}
	for di := range p.Decisions {
		for ii := range p.Decisions[di].Invocations {
			inv := &p.Decisions[di].Invocations[ii]
			ctl, err := queryActivityControl(l, inv.Activity)
			if err == nil && ctl.AcceptedResult != "" {
				inv.AcceptedResult = ctl.AcceptedResult
			}
		}
	}
	if u.Kind == "query_answer" {
		p.Completed = true
	}
	if err := e.validateQueryNext(ctx, req, p, l); err != nil {
		return err
	}
	if len(p.Decisions) > agentprofile.MaxLifecycleRecords {
		return api.ErrInvalidRequest
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if err := e.writeWorkRecord(ctx, req.Space, queryProgressKey(req.Space, req.Principal, u.Query), raw, b); err != nil {
		return err
	}
	if p.Next != nil {
		if err := e.writeWorkRecord(ctx, req.Space, queryTurnReservationKey(p.Next.Key()), raw, b); err != nil {
			return err
		}
	}
	if u.Kind == "query_open" {
		intake, _, err := e.loadQueryIntake(ctx, req.Space, req.Principal, u.Query)
		if err != nil {
			return err
		}
		intake.Started = true
		return e.writeQueryIntake(ctx, req, intake, b)
	}
	return nil
}
