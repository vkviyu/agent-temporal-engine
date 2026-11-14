package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) prepareWorkInput(ctx context.Context, req api.CommitRequest) (api.WorkUnit, *Engine, *objectBuilder, model.ObjectID, error) {
	if !e.options.AgentProfile {
		return api.WorkUnit{}, nil, nil, "", ErrUnsupportedMethod
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return api.WorkUnit{}, nil, nil, "", err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return api.WorkUnit{}, nil, nil, "", errCAS
	}
	unit, err := api.DecodeWorkUnit(req.Body)
	if err != nil {
		return api.WorkUnit{}, nil, nil, "", err
	}
	view := e.workView()
	builder := newBuilder(ctx, e.index, e.options.MaxObjects)
	command, err := makeCommand(req)
	if err != nil {
		return api.WorkUnit{}, nil, nil, "", err
	}
	commandID, err := builder.add(command, storage.KindObject)
	if err != nil {
		return api.WorkUnit{}, nil, nil, "", err
	}
	return unit, view, builder, commandID, nil
}

func (e *Engine) prepareWorkSteps(ctx context.Context, req api.CommitRequest, unit api.WorkUnit, view *Engine, builder *objectBuilder, digest model.ObjectID, txID model.TxID, lsn uint64) error {
	seed := initialWorkSeed{tx: txID}
	if err := view.validateWorkBoundary(ctx, req, unit, &seed); err != nil {
		return err
	}
	workCtx := initialWorkContext(ctx, unit, &seed)
	for _, step := range unit.Steps {
		step, err := view.bindWorkContinuation(workCtx, req, step)
		if err != nil {
			return err
		}
		sub := api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID, Method: step.Method, Body: step.Body}
		var plan *commitPlan
		switch step.Method {
		case api.MethodApplyAgent:
			plan, err = view.prepareAgent(workCtx, sub, digest, txID, lsn, nil)
		case api.MethodApplyEmission:
			plan, err = view.prepareEmission(workCtx, sub, digest, txID, lsn, nil)
		case api.MethodApplyKnowledge:
			plan, err = view.prepareKnowledge(workCtx, sub, digest, txID, lsn, nil)
		}
		if err != nil {
			return err
		}
		if err = absorbWorkPlan(ctx, view, builder, plan); err != nil {
			return err
		}
	}
	if err := view.verifyWorkContinuation(workCtx, req, unit); err != nil {
		return err
	}
	if unit.Kind == "query_start" {
		intake, _, err := view.loadQueryIntake(ctx, req.Space, req.Principal, unit.Query)
		if err != nil {
			return err
		}
		intake.Started = true
		if err = view.writeQueryIntake(ctx, req, intake, builder); err != nil {
			return err
		}
	}
	if unit.Kind == "model_decision" {
		if err := view.writeWorkRecord(ctx, req.Space, decisionKey(req.Space, req.Principal, unit.Decision.Activity), req.Body, builder); err != nil {
			return err
		}
	}
	if unit.QueryTurn != nil {
		if err := view.finishQueryTurn(ctx, req, unit, builder); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) finishWorkPlan(ctx context.Context, req api.CommitRequest, unit api.WorkUnit, view *Engine, builder *objectBuilder, commandID model.ObjectID, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	// A RootSet with the previous receipt root is the joint transition's
	// result object. The final receipt is added afterwards by finishPlan.
	post, err := makeRootSet(view.roots)
	if err != nil {
		return nil, err
	}
	postID, err := builder.add(post, storage.KindObject)
	if err != nil {
		return nil, err
	}
	changes := []stateChange{
		{Key: []byte("execution_knowledge"), Before: e.roots.ExecutionKnowledge, After: view.roots.ExecutionKnowledge},
		{Key: []byte("live_control"), Before: e.roots.LiveControl, After: view.roots.LiveControl},
	}
	ops, err := makeOperations(operationsRecord{Space: req.Space, BeforeRoot: e.rootSet, AfterRoot: postID, Changes: changes})
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		expected, er := makeOperations(*recorded)
		if er != nil || expected.Space != ops.Space || !bytes.Equal(expected.Payload, ops.Payload) {
			return nil, ErrInvalidStateData
		}
	}
	opsID, err := builder.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	return e.finishPlan(ctx, req, digest, txID, lsn, builder, view.roots, postID, commandID, opsID)
}
