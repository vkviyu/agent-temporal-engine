package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) checkAgentQueryGoalInputs(
	ctx context.Context,
	req api.CommitRequest,
	ledger *agentprofile.Ledger,
	x *agentprofile.ControlCommand,
	latest model.SnapshotAnchor,
) error {
	switch {
	case x.AnswerQuery != nil:
		if !e.privateReduction {
			if _, found, err := e.loadQueryProgress(ctx, req, x.AnswerQuery.Query); err != nil {
				return err
			} else if found {
				return agentprofile.ErrConflict
			}
		}
		if x.AnswerQuery.Final {
			return e.checkFinalAnswerInputs(ctx, req, ledger, *x.AnswerQuery)
		}
	case x.ObserveGoal != nil:
		v, err := ledger.GetExecution(x.ObserveGoal.Execution)
		if err != nil {
			return err
		}
		return e.checkGoalAdoption(ctx, req, ledger, v.ID, v.Task, x.ObserveGoal.Epoch, x.ObserveGoal.Observation)
	case x.RebaseInherited != nil:
		return e.checkGoalAdoption(ctx, req, ledger, x.RebaseInherited.Execution, x.RebaseInherited.SourceTask, x.RebaseInherited.Epoch, x.RebaseInherited.Observation)
	case x.Resume != nil:
		return e.agentInputContent(ctx, req, latest, x.Resume.Checkpoint)
	}
	return nil
}
