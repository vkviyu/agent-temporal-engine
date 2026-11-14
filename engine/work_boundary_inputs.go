package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type workBoundaryInputs struct {
	ledger        *agentprofile.Ledger
	controls      []*agentprofile.ControlCommand
	knowledge     []knowledge.Command
	starts        []*agentprofile.StartWorkCommand
	spawns        []*agentprofile.SpawnChildCommand
	consumes      []*agentprofile.ConsumeMessageCommand
	accepts       []*agentprofile.AcceptActivityCommand
	invocations   []*agentprofile.RecordInvocationCommand
	finishes      map[model.ExecutionID]*agentprofile.FinishStepCommand
	continuations map[model.ExecutionID]*agentprofile.ControlCommand
	authorities   map[model.ExecutionID]bool
}

func (e *Engine) collectWorkBoundaryInputs(
	ctx context.Context,
	req api.CommitRequest,
	u api.WorkUnit,
) (workBoundaryInputs, error) {
	ledger, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return workBoundaryInputs{}, err
	}
	inputs := workBoundaryInputs{
		ledger:        ledger,
		finishes:      map[model.ExecutionID]*agentprofile.FinishStepCommand{},
		continuations: map[model.ExecutionID]*agentprofile.ControlCommand{},
		authorities:   map[model.ExecutionID]bool{},
	}
	for _, step := range u.Steps {
		if step.Method == api.MethodApplyAgent {
			c, err := agentprofile.DecodeCommand(step.Body)
			if err != nil {
				return workBoundaryInputs{}, err
			}
			if c.Control.DispatchAttempt != nil {
				return workBoundaryInputs{}, ErrWorkBoundary
			}
			if create := c.Control.CreateActivity; create != nil && create.Spec.Query != "" && create.Spec.Role == "external_tool" {
				return workBoundaryInputs{}, ErrWorkBoundary
			}
			inputs.controls = append(inputs.controls, c.Control)
		} else {
			c, err := knowledge.DecodeCommand(step.Body)
			if err != nil {
				return workBoundaryInputs{}, err
			}
			inputs.knowledge = append(inputs.knowledge, c)
		}
	}
	for _, c := range inputs.controls {
		if c.StartWork != nil {
			inputs.starts = append(inputs.starts, c.StartWork)
		}
		if c.SpawnChild != nil {
			inputs.spawns = append(inputs.spawns, c.SpawnChild)
		}
		if c.ConsumeMessage != nil {
			inputs.consumes = append(inputs.consumes, c.ConsumeMessage)
		}
		if c.AcceptActivity != nil {
			inputs.accepts = append(inputs.accepts, c.AcceptActivity)
		}
		if c.ImportActivityResult != nil {
			inputs.accepts = append(inputs.accepts, c.ImportActivityResult)
		}
		if c.RecordInvocation != nil {
			inputs.invocations = append(inputs.invocations, c.RecordInvocation)
		}
		if x := c.FinishStep; x != nil {
			if inputs.finishes[x.Execution] != nil {
				return workBoundaryInputs{}, ErrWorkBoundary
			}
			inputs.finishes[x.Execution] = x
		}
		if x := c.SaveContinuation; x != nil {
			inputs.continuations[x.Value.Execution] = c
		}
		if x := c.Checkpoint; x != nil {
			inputs.continuations[x.Continuation.Execution] = c
		}
		if c.GrantAuthority != nil {
			inputs.authorities[c.GrantAuthority.Authority.Execution] = true
		}
	}
	return inputs, nil
}
