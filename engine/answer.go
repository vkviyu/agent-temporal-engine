package engine

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) finalAnswerInput(ctx context.Context, req api.CommitRequest, spec agentprofile.ActivitySpec) (agentprofile.FinalAnswerInput, error) {
	var input agentprofile.FinalAnswerInput
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: spec.InputFence}
	if err := e.agentInputContent(ctx, req, anchor, spec.FinalAnswerInput); err != nil {
		return input, err
	}
	object, err := readObject(ctx, e.index, spec.FinalAnswerInput)
	if err != nil {
		return input, err
	}
	value, err := readValue(object)
	if err != nil || value.Kind != api.ValueBytes {
		return input, agentprofile.ErrCommandShape
	}
	input, err = agentprofile.DecodeFinalAnswerInput(value.Data)
	if err != nil {
		return input, err
	}
	roots, _, err := e.rootsAtFence(ctx, spec.InputFence, true)
	if err != nil {
		return input, err
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.index, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return input, err
	}
	if err := agentprofile.ValidateFinalAnswerInput(ledger.Snapshot(), spec, input); err != nil {
		return input, err
	}
	record, err := e.packetAt(ctx, roots, req.Space, req.Principal, spec.ContextPacket)
	if err != nil {
		return input, err
	}
	packet := record.packet
	if packet.Renderer != input.HostPolicyVersion || packet.Input == nil || packet.Input.Object != input.Request {
		return input, agentprofile.ErrConflict
	}
	ids := []model.ObjectID{spec.FinalAnswerInput, input.Request, input.ContextPacket}
	for _, outcome := range input.Outcomes {
		ids = append(ids, outcome.Result, outcome.Validation)
	}
	return input, e.checkPolicyIDs(ctx, req.Space, ids, memory.AccessReuse)
}

func (e *Engine) checkFinalAnswerInputs(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, answer agentprofile.AnswerQueryCommand) error {
	result, err := ledger.GetActivityResult(answer.GeneratedFrom)
	if err != nil {
		return err
	}
	spec, err := queryActivitySpec(ledger, result.Activity)
	if err != nil {
		return err
	}
	if spec.FinalAnswerInput != answer.InputEvidence {
		return agentprofile.ErrConflict
	}
	if answer.InputEvidence != "" {
		input, err := e.finalAnswerInput(ctx, req, spec)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(input.Outcomes, answer.Outcomes) {
			return agentprofile.ErrConflict
		}
	}
	ids, err := e.activityInputDisclosureIDs(ctx, req.Space, req.Principal, spec)
	if err != nil {
		return err
	}
	return e.checkPolicyIDs(ctx, req.Space, ids, memory.AccessReuse)
}
