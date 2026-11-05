package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) checkAgentControlInputs(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, c agentprofile.Command) error {
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
	case x.AnswerQuery != nil:
		if !e.privateReduction {
			if _, found, err := e.loadQueryProgress(ctx, req, x.AnswerQuery.Query); err != nil {
				return err
			} else if found {
				return agentprofile.ErrConflict
			}
		}
	case x.AcceptActivity != nil:
		spec, err := queryActivitySpec(ledger, x.AcceptActivity.Activity)
		if err != nil {
			return err
		}
		if spec.Query != "" && spec.Role == "query_decision" && !e.privateReduction {
			if _, found, err := e.loadQueryProgress(ctx, req, spec.Query); err != nil {
				return err
			} else if found {
				return agentprofile.ErrConflict
			}
		}
	case x.ClaimAttempt != nil:
		return e.checkCurrentAttemptUse(ctx, req, ledger, x.ClaimAttempt.Activity, "")
	case x.DispatchAttempt != nil:
		return e.checkCurrentAttemptUse(ctx, req, ledger, "", x.DispatchAttempt.Attempt)
	case x.BindValidation != nil:
		r := x.BindValidation.Requirement
		if r.Spec.Space != req.Space {
			return agentprofile.ErrCommandScope
		}
		return e.agentInputContent(ctx, req, model.SnapshotAnchor{Space: req.Space, Fence: r.Source}, r.Spec.Result, r.Spec.Command, r.Spec.Environment)
	case x.ObserveGoal != nil:
		v, err := ledger.GetExecution(x.ObserveGoal.Execution)
		if err != nil {
			return err
		}
		return e.checkGoalAdoption(ctx, req, ledger, v.ID, v.Task, x.ObserveGoal.Epoch, x.ObserveGoal.Observation)
	case x.RebaseInherited != nil:
		return e.checkGoalAdoption(ctx, req, ledger, x.RebaseInherited.Execution, x.RebaseInherited.SourceTask, x.RebaseInherited.Epoch, x.RebaseInherited.Observation)
	case x.BeginStep != nil:
		if err := e.checkGoalInputCut(ctx, req, ledger, x.BeginStep.Step.Execution, x.BeginStep.Step.InputAnchor); err != nil {
			return err
		}
		return checkFence(x.BeginStep.Step.InputAnchor)
	case x.RecordInvocation != nil:
		return e.checkExecutionInvocationDefinition(ctx, req, ledger, x.RecordInvocation.Invocation, "")
	case x.SpawnChild != nil:
		inv, err := ledger.GetInvocation(x.SpawnChild.Invocation)
		if err != nil {
			return err
		}
		if err := e.checkExecutionInvocationDefinition(ctx, req, ledger, inv, "delegate"); err != nil {
			return err
		}
		current, _, _, err := e.loadMemory(ctx, e.roots, req.Space)
		if err != nil {
			return err
		}
		return current.AccessPolicy().Check([]model.ObjectID{inv.ToolDefinition}, memory.AccessReuse)
	case x.CreateActivity != nil:
		a := x.CreateActivity.Spec
		if a.Role == "external_tool" {
			if err := e.checkToolActivityDefinition(ctx, req, a); err != nil {
				return err
			}
		} else if a.Contract.Definition != "" {
			if err := e.agentInputContent(ctx, req, model.SnapshotAnchor{Space: req.Space, Fence: a.InputFence}, a.Contract.Definition); err != nil {
				return err
			}
		}
		if err := e.checkCurrentActivityUse(ctx, req, ledger, a); err != nil {
			return err
		}
		if a.Query != "" && !e.privateReduction {
			if err := e.checkQueryActivityCreate(ctx, req, a); err != nil {
				return err
			}
		}
		anchor := model.SnapshotAnchor{Space: req.Space, Fence: a.InputFence}
		if a.Execution != "" {
			if err := e.checkGoalInputCut(ctx, req, ledger, a.Execution, a.InputFence); err != nil {
				return err
			}
		}
		modelActivity := a.Role == "routing" || a.Role == "query_decision" || a.Role == "primary_decision" || a.Role == "aux_summary"
		if modelActivity {
			if err := checkFence(a.InputFence); err != nil {
				return err
			}
		} else {
			if err := e.agentInputRequest(ctx, req, anchor, a.Request, a.RequestDigest); err != nil {
				return err
			}
		}
		if a.ContextPacket != "" {
			roots, _, err := e.rootsAtFence(ctx, a.InputFence, true)
			if err != nil {
				return err
			}
			snap, _, _, err := e.loadKnowledge(ctx, roots, req.Space, req.Principal)
			if err != nil {
				return err
			}
			for _, packet := range snap.Packets {
				if packet.ID != a.ContextPacket {
					continue
				}
				if modelActivity {
					if err := e.validateCurrentTypedPacket(ctx, packet, snap, req.Space, req.Principal); err != nil {
						return err
					}
					input := packet.Input
					if input == nil || input.Object != a.Request || input.Anchor.Fence.Store != a.InputFence.Store || input.Anchor.Fence.LSN > a.InputFence.LSN {
						return agentprofile.ErrConflict
					}
					if err := e.agentInputRequest(ctx, req, input.Anchor, input.Object, a.RequestDigest); err != nil {
						return err
					}
				}
				if a.Execution == "" {
					if packet.Key != nil {
						return agentprofile.ErrCommandScope
					}
					return nil
				} else {
					execution, err := ledger.GetExecution(a.Execution)
					if err != nil || packet.Key == nil || packet.Key.Execution != execution.ID || packet.Key.Actor != execution.Actor {
						return agentprofile.ErrCommandScope
					}
					if modelActivity {
						if err := e.checkModelGoalPacket(ctx, req, ledger, a.Execution, packet, a.Role); err != nil {
							return err
						}
					}
					return nil
				}
			}
			return agentprofile.ErrUnknown
		}
	case x.Resume != nil:
		return e.agentInputContent(ctx, req, latest, x.Resume.Checkpoint)
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
		if _, _, err := e.agentActionReceiptAtRoots(ctx, e.roots, next); err != nil {
			return err
		}
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
