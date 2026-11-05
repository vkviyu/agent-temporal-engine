package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type initialWorkKey struct{}
type initialWorkSeed struct {
	tx               model.TxID
	targets          map[model.ExecutionID]model.KnowledgeKey
	messageRecipient model.KnowledgeKey
	finishedSteps    map[model.ExecutionID]*agentprofile.FinishStepCommand
}

func initialWorkContext(ctx context.Context, u api.WorkUnit, seed *initialWorkSeed) context.Context {
	ctx = context.WithValue(ctx, initialWorkKey{}, seed)
	if u.Kind != "query_start" && u.Kind != "child_spawn" {
		return ctx
	}
	seed.targets = make(map[model.ExecutionID]model.KnowledgeKey)
	for _, step := range u.Steps {
		if step.Method != api.MethodApplyAgent {
			continue
		}
		c, err := agentprofile.DecodeCommand(step.Body)
		if err != nil || c.Control == nil {
			continue
		}
		var x *agentprofile.Execution
		if c.Control.StartWork != nil {
			x = &c.Control.StartWork.Execution
		}
		if c.Control.SpawnChild != nil {
			x = &c.Control.SpawnChild.Execution
		}
		if x != nil {
			seed.targets[x.ID] = model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
		}
	}
	return ctx
}

// A creation Work may adopt a published observation source before the receiving
// execution exists in published history.
func initialWorkObservation(ctx context.Context, req api.CommitRequest, o *knowledge.Observation) bool {
	seed, ok := ctx.Value(initialWorkKey{}).(*initialWorkSeed)
	return ok && o != nil && seed.targets[o.Key.Execution] == o.Key && o.Source.Anchor.Space == req.Space && o.Source.Anchor.Knowledge == nil && o.PayloadKind == "" && o.ImportedFrom == ""
}

func (e *Engine) bindWorkContinuation(ctx context.Context, req api.CommitRequest, step api.WorkStep) (api.WorkStep, error) {
	seed, ok := ctx.Value(initialWorkKey{}).(*initialWorkSeed)
	if !ok || step.Method != api.MethodApplyAgent {
		return step, nil
	}
	c, err := agentprofile.DecodeCommand(step.Body)
	if err != nil {
		return step, err
	}
	if c.Control == nil {
		return step, ErrWorkBoundary
	}
	var continuation *agentprofile.Continuation
	if x := c.Control.Checkpoint; x != nil {
		continuation = &x.Continuation
	}
	if x := c.Control.SaveContinuation; x != nil {
		continuation = &x.Value
	}
	if continuation == nil {
		return step, nil
	}
	key, initial := seed.targets[continuation.Execution]
	message := seed.messageRecipient.Execution == continuation.Execution
	if message {
		key = seed.messageRecipient
	}
	requiresWorkSource := initial || message
	p := continuation.Position
	if p == nil {
		return step, ErrWorkBoundary
	}
	if p.KnowledgeSource != nil || p.KnowledgeFence != (model.CommitFence{}) || p.KnowledgeCursor != (model.Cursor{}) || p.KnowledgeKey != (model.KnowledgeKey{}) {
		if requiresWorkSource {
			return step, ErrWorkBoundary
		}
		return step, nil
	}
	if !requiresWorkSource {
		ledger, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
		if err != nil {
			return step, err
		}
		x, err := ledger.GetExecution(continuation.Execution)
		if err != nil {
			return step, err
		}
		key = model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	}
	snap, _, _, err := e.loadKnowledge(ctx, e.roots, req.Space, req.Principal)
	if err != nil {
		return step, err
	}
	_, revision, err := knowledge.SelectRevision(snap, req.Space, key, "")
	if err != nil {
		return step, err
	}
	if revision.Sequence == 0 {
		if requiresWorkSource {
			return step, ErrWorkBoundary
		}
		return step, nil
	}
	p.KnowledgeSource = &model.SourcePublicationRef{Store: e.publication.Store, Space: req.Space, ProducerTx: seed.tx, Cursor: &model.Cursor{Store: e.publication.Store, Space: req.Space, Revision: revision.ID}, Knowledge: &key}
	return api.AgentWorkStep(c)
}

// Check the recovery position against the final private knowledge after all
// steps. Creation also requires explicit goal and query observations.
func (e *Engine) verifyWorkContinuation(ctx context.Context, req api.CommitRequest, u api.WorkUnit) error {
	seed, ok := ctx.Value(initialWorkKey{}).(*initialWorkSeed)
	if !ok || (len(seed.targets) == 0 && seed.messageRecipient.Execution == "" && len(seed.finishedSteps) == 0) {
		return nil
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	snap, _, _, err := e.loadKnowledge(ctx, e.roots, req.Space, req.Principal)
	if err != nil {
		return err
	}
	for execution := range seed.finishedSteps {
		x, err := ledger.GetExecution(execution)
		if err != nil {
			return err
		}
		key := model.KnowledgeKey{Actor: x.Actor, Execution: execution}
		_, revision, err := knowledge.SelectRevision(snap, req.Space, key, "")
		if err != nil {
			return err
		}
		continuation, err := ledger.GetContinuation(execution)
		if err != nil {
			return err
		}
		action, err := ledger.GetPending(continuation.PendingKey)
		if err != nil || action.Status != agentprofile.ActionPending {
			return ErrWorkBoundary
		}
		// Knowledge adopted in this Work must be present in the closing
		// continuation's cut. A published cut may still name the same revision
		// when closing the step does not change its execution knowledge.
		position := continuation.Position
		cursor := position.KnowledgeCursor
		if position.KnowledgeSource != nil {
			cursor = *position.KnowledgeSource.Cursor
		}
		if cursor.Revision != revision.ID {
			return ErrWorkBoundary
		}
	}
	if key := seed.messageRecipient; key.Execution != "" {
		_, revision, err := knowledge.SelectRevision(snap, req.Space, key, "")
		if err != nil {
			return err
		}
		return verifyWorkContinuationPosition(ledger, key, seed.tx, revision)
	}
	for execution, key := range seed.targets {
		x, err := ledger.GetExecution(execution)
		if err != nil {
			return err
		}
		task, err := ledger.GetTask(x.Task)
		if err != nil {
			return err
		}
		if _, err := goalKnowledgeObservation(ctx, e.index, snap, key, "", task); err != nil {
			return ErrWorkBoundary
		}
		selected, revision, err := knowledge.SelectRevision(snap, req.Space, key, "")
		if err != nil {
			return err
		}
		if err := verifyWorkContinuationPosition(ledger, key, seed.tx, revision); err != nil {
			return err
		}
		if u.Kind == "query_start" {
			intake, found, err := e.loadQueryIntake(ctx, req.Space, req.Principal, u.Query)
			if err != nil {
				return err
			}
			if !found {
				return ErrWorkBoundary
			}
			queryKnown := false
			for _, observation := range selected.Observations {
				if observation.PayloadKind != "" {
					continue
				}
				object, err := readObject(ctx, e.index, observation.Payload)
				if err != nil {
					return err
				}
				value, err := readValue(object)
				if err != nil {
					return err
				}
				queryKnown = queryKnown || value.Kind == api.ValueBytes && bytes.Equal(value.Data, intake.Intake.Query.Input)
			}
			if !queryKnown {
				return ErrWorkBoundary
			}
		}
	}
	return nil
}

func verifyWorkContinuationPosition(ledger *agentprofile.Ledger, key model.KnowledgeKey, tx model.TxID, revision knowledge.Revision) error {
	continuation, err := ledger.GetContinuation(key.Execution)
	if err != nil || continuation.Position == nil || revision.Sequence == 0 {
		return ErrWorkBoundary
	}
	source := continuation.Position.KnowledgeSource
	if source == nil || source.ProducerTx != tx || source.Knowledge == nil || *source.Knowledge != key || source.Cursor == nil || source.Cursor.Revision != revision.ID {
		return ErrWorkBoundary
	}
	return nil
}
