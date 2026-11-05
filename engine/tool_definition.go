package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (e *Engine) readToolDefinition(ctx context.Context, space model.SpaceID, id model.ObjectID) (agentprofile.ToolDefinition, error) {
	object, err := readObject(ctx, e.index, id)
	if err != nil {
		return agentprofile.ToolDefinition{}, err
	}
	if object.Space != space || object.Type != valueType {
		return agentprofile.ToolDefinition{}, agentprofile.ErrConflict
	}
	value, err := readValue(object)
	if err != nil {
		return agentprofile.ToolDefinition{}, err
	}
	if value.Kind != api.ValueBytes {
		return agentprofile.ToolDefinition{}, agentprofile.ErrConflict
	}
	return agentprofile.DecodeToolDefinition(value.Data)
}

func (e *Engine) checkInvocationDefinition(ctx context.Context, space model.SpaceID, id model.ObjectID, name, version, dispatchClass, mode string) error {
	definition, err := e.readToolDefinition(ctx, space, id)
	if err != nil {
		return err
	}
	if definition.Name != name || definition.Version != version || definition.DispatchClass != dispatchClass || mode != "" && definition.DispatchMode != mode {
		return agentprofile.ErrConflict
	}
	return nil
}

func (e *Engine) checkExecutionInvocationDefinition(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, inv agentprofile.Invocation, mode string) error {
	recovery := ledger.Snapshot().Lifecycle.Recovery
	if recovery == nil {
		return agentprofile.ErrConflict
	}
	for _, spec := range recovery.ActivitySpecs {
		if spec.Execution == inv.Execution && spec.Step == inv.Step && spec.Role == "primary_decision" {
			if err := e.agentInputContent(ctx, req, model.SnapshotAnchor{Space: req.Space, Fence: spec.InputFence}, inv.ToolDefinition); err != nil {
				return err
			}
			return e.checkInvocationDefinition(ctx, req.Space, inv.ToolDefinition, inv.Tool, inv.ToolVersion, inv.DispatchClass, mode)
		}
	}
	return agentprofile.ErrConflict
}

func (e *Engine) checkToolActivityDefinition(ctx context.Context, req api.CommitRequest, spec agentprofile.ActivitySpec) error {
	id := spec.Contract.Definition
	if err := e.agentInputContent(ctx, req, model.SnapshotAnchor{Space: req.Space, Fence: spec.InputFence}, id); err != nil {
		return err
	}
	definition, err := e.readToolDefinition(ctx, req.Space, id)
	if err != nil {
		return err
	}
	if definition.DispatchMode != "external" || spec.Effect != definition.DispatchClass || spec.Contract != definition.BindEffect(id) {
		return agentprofile.ErrConflict
	}
	return nil
}
