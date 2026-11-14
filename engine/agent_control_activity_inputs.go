package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) checkAgentActivityInputs(
	ctx context.Context,
	req api.CommitRequest,
	ledger *agentprofile.Ledger,
	x *agentprofile.ControlCommand,
) error {
	checkFence := func(f model.CommitFence) error {
		_, _, err := e.rootsAtFence(ctx, f, true)
		return err
	}

	switch {
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
		return e.checkPolicyIDs(ctx, req.Space, []model.ObjectID{inv.ToolDefinition}, memory.AccessReuse)
	case x.CreateActivity != nil:
		a := x.CreateActivity.Spec
		if a.FinalAnswerInput != "" {
			if _, err := e.finalAnswerInput(ctx, req, a); err != nil {
				return err
			}
		}
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
		modelActivity := isModelActivityRole(a.Role)
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
			record, err := e.activityPacketAt(ctx, req.Space, req.Principal, a.InputFence, a.ContextPacket)
			if err != nil {
				return err
			}
			packet := record.packet
			if modelActivity {
				// Old activities already checked eligibility at creation.
				if e.replayingEvents && !record.indexed {
					if err := e.validateCurrentTypedPacket(ctx, packet, record.selected, req.Space, req.Principal, true); err != nil {
						return err
					}
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
			}
			execution, err := ledger.GetExecution(a.Execution)
			if err != nil || packet.Key == nil || packet.Key.Execution != execution.ID || packet.Key.Actor != execution.Actor {
				return agentprofile.ErrCommandScope
			}
			if modelActivity {
				return e.checkModelGoalPacket(ctx, req, ledger, a.Execution, packet, a.Role)
			}
			return nil
		}
	}
	return nil
}
