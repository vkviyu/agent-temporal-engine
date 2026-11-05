package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// checkExecutionContextRequirements authenticates completeness against the
// packet's historical control cut. Current dispatch checks remain separate:
// they may block an old packet, but never inject future facts into that input.
func (e *Engine) checkExecutionContextRequirements(ctx context.Context, req api.CommitRequest, execution model.ExecutionID, packet knowledge.ContextPacket) error {
	manifest := packet.Requirements
	if manifest == nil || manifest.RulesVersion != agentprofile.ContextRequirementsVersion || manifest.HostPolicyVersion == "" {
		return knowledge.ErrContextSelectionInvalid
	}
	roots, _, err := e.rootsAtFence(ctx, packet.Source.Anchor.Fence, true)
	if err != nil {
		return err
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.index, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	x, err := ledger.GetExecution(execution)
	if err != nil {
		return err
	}
	key := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	if packet.Key == nil || *packet.Key != key {
		return agentprofile.ErrCommandScope
	}
	expected, err := agentprofile.ContextRequirements(ledger.Snapshot(), execution)
	if err != nil {
		return err
	}
	if len(expected) != len(manifest.Facts) {
		return knowledge.ErrContextSelectionInvalid
	}
	snapshot, _, _, _, err := e.selectedKnowledge(ctx, roots, packet.Source.Anchor, req.Principal)
	if err != nil {
		return err
	}
	observations := make(map[model.ObjectID]knowledge.Observation, len(snapshot.Observations))
	for _, o := range snapshot.Observations {
		observations[o.ID] = o
	}
	for i, image := range expected {
		fact := manifest.Facts[i]
		if fact.Kind != image.Kind || fact.ID != image.ID {
			return knowledge.ErrContextSelectionInvalid
		}
		o, ok := observations[fact.Observation]
		if !ok || o.Key != key || o.PayloadKind != "" || o.Payload != fact.Content {
			return knowledge.ErrContextSelectionInvalid
		}
		object, err := readObject(ctx, e.index, o.Payload)
		if err != nil {
			return err
		}
		value, err := readValue(object)
		if err != nil {
			return err
		}
		if value.Kind != api.ValueBytes || !bytes.Equal(value.Data, image.Bytes) {
			return knowledge.ErrContextSelectionInvalid
		}
		if image.Kind == "goal" && image.ID == string(x.Task) && packet.GoalRef != o.Payload {
			return knowledge.ErrContextSelectionInvalid
		}
	}
	return nil
}
