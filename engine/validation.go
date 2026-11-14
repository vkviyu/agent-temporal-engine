package engine

import (
	"context"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// checkTaskValidation enforces typed completion evidence for the frozen V1
// submission and replay contract.
func (e *Engine) checkTaskValidation(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, c agentprofile.Command, _ bool) error {
	if c.Control == nil || c.Control.CompleteTask == nil {
		return nil
	}
	complete := c.Control.CompleteTask
	latest := e.agentPublishedAnchor(req.Space)
	if err := e.agentInputContent(ctx, req, latest, complete.Result, complete.Validation); err != nil {
		return err
	}
	object, err := readObject(ctx, e.index, complete.Validation)
	if err != nil {
		return err
	}
	value, err := readValue(object)
	if err != nil {
		return err
	}
	if value.Kind != api.ValueBytes || !agentprofile.IsValidationEnvelope(value.Data) {
		return validationRejection("typed validation envelope required")
	}
	envelope, err := agentprofile.DecodeValidationEnvelope(value.Data)
	if err != nil {
		return validationRejection("malformed validation envelope")
	}
	required, err := agentprofile.LatestValidationRequirement(ledger.Snapshot(), complete.Task, complete.Execution, complete.Epoch)
	if err != nil || envelope.Satisfies(required) != nil {
		return validationRejection("envelope does not satisfy the current requirement")
	}
	task, err := ledger.GetTask(complete.Task)
	if err != nil {
		return err
	}
	execution, err := ledger.GetExecution(complete.Execution)
	if err != nil {
		return err
	}
	goal, err := agentprofile.GoalKnowledgeDigest(task)
	if err != nil || task.Owner != req.Principal || task.Space != req.Space || execution.Task != task.ID || required.Spec.Goal != goal || required.Spec.GoalEpoch != task.GoalEpoch || required.Spec.Result != complete.Result {
		return validationRejection("goal, owner, execution or artifact binding changed")
	}
	ids := append([]model.ObjectID{required.Spec.Result, required.Spec.Command, required.Spec.Environment}, envelope.Evidence...)
	// Every proof member must already belong to the published input cut; a
	// candidate-local object or a guessed hash is not acceptance evidence.
	if err := e.agentInputContent(ctx, req, latest, ids...); err != nil {
		return err
	}
	return e.checkPolicyIDs(ctx, req.Space, ids, memory.AccessReuse)
}

func validationRejection(reason string) error {
	// Keep validation mismatch in the established profile business-rejection
	// class: malformed caller evidence must not fence the store.
	return fmt.Errorf("%w: %w: %s", agentprofile.ErrConflict, agentprofile.ErrValidation, reason)
}
