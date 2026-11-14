package engine

import (
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
)

func validateQueued(req api.CommitRequest) error {
	if err := validSpace(req.Space); err != nil {
		return err
	}
	switch req.Method {
	case api.MethodImportTransfer:
		_, err := api.DecodeImportTransfer(req.Body)
		return err
	case api.MethodApplyAnnotation:
		_, err := annotation.DecodeRecord(req.Body)
		return err
	case api.MethodUpdateInvestigation:
		if req.ExpectedFence != nil || validAgentScope(req.Space, req.Principal) != nil {
			return api.ErrInvalidRequest
		}
		_, err := investigation.DecodeUpdate(req.Body)
		return err
	case api.MethodApplyTaskTrace:
		_, err := api.DecodeTaskTraceEdit(req.Body)
		return err
	case api.MethodApplySummary:
		_, err := knowledge.DecodeSummary(req.Body)
		return err
	case api.MethodApplyEmission:
		_, err := api.DecodeEmissionCommand(req.Body)
		return err
	case api.MethodApplyWork:
		_, err := api.DecodeWorkUnit(req.Body)
		return err
	case api.MethodMutateState:
		_, err := DecodeStateBatch(req.Body)
		return err
	case api.MethodApplyAgent:
		_, err := agentprofile.DecodeCommand(req.Body)
		return err
	case api.MethodApplyGraph:
		space, _, err := graph.DecodeCommand(req.Body)
		if err == nil && space != req.Space {
			return graph.ErrSpaceMismatch
		}
		return err
	case api.MethodApplyKnowledge:
		_, err := knowledge.DecodeCommand(req.Body)
		if err != nil {
			return fmt.Errorf("knowledge admission decode: %w", err)
		}
		return nil
	case api.MethodApplyMemory:
		_, err := memory.DecodeCommand(req.Body)
		return err
	default:
		return ErrUnsupportedMethod
	}
}
