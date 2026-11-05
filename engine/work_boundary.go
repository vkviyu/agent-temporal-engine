package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

// validateWorkBoundary checks the semantic work unit as a whole before any
// private substep is applied. The existing reducers still check every local
// authority, epoch, uniqueness and historical-source precondition.
func (e *Engine) validateWorkBoundary(ctx context.Context, req api.CommitRequest, u api.WorkUnit, seed *initialWorkSeed) error {
	if u.Kind == "emission_dispatch" {
		return e.validateEmissionDispatch(ctx, req, u)
	}
	if u.QueryTurn != nil {
		return e.validateQueryTurn(ctx, req, u)
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	var controls []*agentprofile.ControlCommand
	var know []knowledge.Command
	for _, step := range u.Steps {
		if step.Method == api.MethodApplyAgent {
			c, err := agentprofile.DecodeCommand(step.Body)
			if err != nil {
				return err
			}
			if c.Control.DispatchAttempt != nil {
				return ErrWorkBoundary
			}
			if create := c.Control.CreateActivity; create != nil && create.Spec.Query != "" && create.Spec.Role == "external_tool" {
				return ErrWorkBoundary
			}
			controls = append(controls, c.Control)
		} else {
			c, err := knowledge.DecodeCommand(step.Body)
			if err != nil {
				return err
			}
			know = append(know, c)
		}
	}
	var starts []*agentprofile.StartWorkCommand
	var spawns []*agentprofile.SpawnChildCommand
	var consumes []*agentprofile.ConsumeMessageCommand
	var accepts []*agentprofile.AcceptActivityCommand
	var invocations []*agentprofile.RecordInvocationCommand
	finishes := map[model.ExecutionID]*agentprofile.FinishStepCommand{}
	continuations := map[model.ExecutionID]*agentprofile.ControlCommand{}
	authorities := map[model.ExecutionID]bool{}
	for _, c := range controls {
		if c.StartWork != nil {
			starts = append(starts, c.StartWork)
		}
		if c.SpawnChild != nil {
			spawns = append(spawns, c.SpawnChild)
		}
		if c.ConsumeMessage != nil {
			consumes = append(consumes, c.ConsumeMessage)
		}
		if c.AcceptActivity != nil {
			accepts = append(accepts, c.AcceptActivity)
		}
		if c.ImportActivityResult != nil {
			accepts = append(accepts, c.ImportActivityResult)
		}
		if c.RecordInvocation != nil {
			invocations = append(invocations, c.RecordInvocation)
		}
		if x := c.FinishStep; x != nil {
			if finishes[x.Execution] != nil {
				return ErrWorkBoundary
			}
			finishes[x.Execution] = x
		}
		if x := c.SaveContinuation; x != nil {
			continuations[x.Value.Execution] = c
		}
		if x := c.Checkpoint; x != nil {
			continuations[x.Continuation.Execution] = c
		}
		if c.GrantAuthority != nil {
			authorities[c.GrantAuthority.Authority.Execution] = true
		}
	}
	if (u.Kind == "query_start") != (len(starts) > 0) || (u.Kind == "child_spawn") != (len(spawns) > 0) || (u.Kind == "message_consume") != (len(consumes) > 0) {
		return ErrWorkBoundary
	}
	if u.Kind != "model_decision" && len(invocations) > 0 {
		return ErrWorkBoundary
	}
	for execution, finish := range finishes {
		continuation := continuations[execution]
		if (u.Kind != "checkpoint" && u.Kind != "model_decision") || continuation == nil || continuation.Checkpoint == nil ||
			continuation.Checkpoint.Continuation.Position.Step != finish.Sequence || continuation.Checkpoint.Authority != finish.Authority {
			return ErrWorkBoundary
		}
	}
	seed.finishedSteps = finishes
	// A primary model result cannot use the generic checkpoint escape hatch.
	for _, accept := range accepts {
		recovery := ledger.Snapshot().Lifecycle.Recovery
		if recovery == nil {
			return ErrWorkBoundary
		}
		for _, spec := range recovery.ActivitySpecs {
			if spec.Activity == accept.Activity && spec.Role == "primary_decision" && u.Kind != "model_decision" {
				return ErrWorkBoundary
			}
		}
	}
	switch u.Kind {
	case "query_start":
		if len(starts) != 1 || len(know) == 0 {
			return ErrWorkBoundary
		}
		intake, found, err := e.loadQueryIntake(ctx, req.Space, req.Principal, u.Query)
		if err != nil {
			return err
		}
		if !found || intake.Started {
			return ErrWorkBoundary
		}
		next := intake.Intake.Next
		if next.Key() != req.Key() || next.Method != req.Method || !bytes.Equal(next.Body, req.Body) || !sameExpectedFence(next.ExpectedFence, req.ExpectedFence) {
			return ErrWorkBoundary
		}
		x := starts[0]
		for _, command := range know {
			if command.Observation == nil || command.Observation.Key != (model.KnowledgeKey{Actor: x.Execution.Actor, Execution: x.Execution.ID}) || command.Observation.Source.Anchor.Knowledge != nil || command.Observation.PayloadKind != "" {
				return ErrWorkBoundary
			}
		}
		if x.Query != u.Query || x.ExpectedQueryVersion != intake.Intake.Query.Version || !authorities[x.Execution.ID] || continuations[x.Execution.ID] == nil {
			return ErrWorkBoundary
		}
	case "child_spawn":
		if len(spawns) != 1 {
			return ErrWorkBoundary
		}
		x := spawns[0]
		if _, err := ledger.GetExecution(x.Execution.ID); err == nil {
			return agentprofile.ErrUniqueSlot
		}
		if !authorities[x.Execution.ID] || continuations[x.Execution.ID] == nil {
			return ErrWorkBoundary
		}
		seeded := false
		for _, c := range know {
			key := model.KnowledgeKey{Actor: x.Execution.Actor, Execution: x.Execution.ID}
			if c.Import != nil && c.Import.Target == key && c.Import.Source.Anchor.Knowledge != nil && c.Import.Source.Anchor.Knowledge.Execution == x.Parent && len(c.Import.Observations) > 0 {
				seeded = true
				continue
			}
			if c.Observation == nil || c.Observation.Key != key || c.Observation.Source.Anchor.Knowledge != nil || c.Observation.PayloadKind != "" {
				return ErrWorkBoundary
			}
		}
		if !seeded {
			return ErrWorkBoundary
		}
	case "message_consume":
		if len(consumes) != 1 {
			return ErrWorkBoundary
		}
		x := consumes[0].Consumption
		for _, old := range ledger.Snapshot().Lifecycle.Consumptions {
			if old.Message == x.Message && old.Recipient == x.Recipient {
				return agentprofile.ErrUniqueSlot
			}
		}
		if continuations[x.Recipient] == nil {
			return ErrWorkBoundary
		}
		message, err := ledger.GetMessage(x.Message)
		if err != nil {
			return err
		}
		execution, err := ledger.GetExecution(x.Recipient)
		if err != nil {
			return err
		}
		key := model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID}
		current, _, _, err := e.loadKnowledge(ctx, e.roots, req.Space, req.Principal)
		if err != nil {
			return err
		}
		known := make(map[model.ObjectID]bool)
		for _, o := range current.Observations {
			if o.Key == key {
				known[o.ID] = true
			}
		}
		observed := false
		for _, c := range know {
			if c.Observation == nil || c.Observation.Key != key || c.Observation.PayloadKind != "" || known[c.Observation.ID] {
				continue
			}
			roots, _, err := e.rootsAtFence(ctx, c.Observation.Source.Anchor.Fence, true)
			if err != nil {
				return err
			}
			if err = e.visibleSourceObject(ctx, roots, c.Observation.Source.Anchor, req.Principal, c.Observation.Payload); err != nil {
				return err
			}
			o, err := readObject(ctx, e.index, c.Observation.Payload)
			if err != nil {
				return err
			}
			value, err := readValue(o)
			if err == nil && value.Kind == api.ValueBytes && bytes.Equal(value.Data, message.Payload) {
				observed = true
			}
		}
		if !observed {
			return ErrWorkBoundary
		}
		seed.messageRecipient = key
	case "model_decision":
		if _, found, err := state.Get(ctx, e.index, systemSpace, e.roots.LiveControl, decisionKey(req.Space, req.Principal, u.Decision.Activity)); err != nil {
			return err
		} else if found {
			return agentprofile.ErrUniqueSlot
		}
		if len(accepts) != 1 || u.Decision.Activity != accepts[0].Activity || u.Decision.Result != accepts[0].Result || len(invocations) != len(u.Decision.Invocations) {
			return ErrWorkBoundary
		}
		var spec *agentprofile.ActivitySpec
		if ledger.Snapshot().Lifecycle.Recovery != nil {
			for _, s := range ledger.Snapshot().Lifecycle.Recovery.ActivitySpecs {
				if s.Activity == u.Decision.Activity {
					v := s
					spec = &v
				}
			}
		}
		if spec == nil || spec.Role != "primary_decision" || continuations[spec.Execution] == nil {
			return ErrWorkBoundary
		}
		finish := finishes[spec.Execution]
		// Decisions with pending tools publish their complete dispatch set and
		// recovery position first. FinishStep is only legal after those tools
		// have accepted outcomes; requiring it here made tool-bearing decisions
		// impossible to commit.
		if len(invocations) == 0 && finish == nil ||
			len(finishes) > 0 && (len(finishes) != 1 || finish == nil || finish.Sequence != spec.Step) {
			return ErrWorkBoundary
		}
		seen := map[string]bool{}
		for i, binding := range u.Decision.Invocations {
			if _, err := ledger.GetInvocation(binding.Invocation); err == nil {
				return agentprofile.ErrUniqueSlot
			}
			if binding.Invocation == "" || seen[binding.Invocation] || (i > 0 && u.Decision.Invocations[i-1].Invocation >= binding.Invocation) || binding.Version == "" {
				return ErrWorkBoundary
			}
			seen[binding.Invocation] = true
			if binding.DispatchClass != "pure" && binding.DispatchClass != "idempotent" && binding.DispatchClass != "non_idempotent" {
				return ErrWorkBoundary
			}
			matched := false
			for _, invocation := range invocations {
				v := invocation.Invocation
				contractMatches := v.AcceptedResult == u.Decision.Result && v.ToolDefinition == binding.Definition && v.ToolVersion == binding.Version && v.DispatchClass == binding.DispatchClass
				if invocation.Invocation.ID == binding.Invocation &&
					invocation.Invocation.Execution == spec.Execution &&
					invocation.Invocation.Step == spec.Step &&
					contractMatches {
					matched = true
				}
			}
			if !matched {
				return ErrWorkBoundary
			}
		}
	case "checkpoint":
		if len(continuations) == 0 {
			return ErrWorkBoundary
		}
	}
	return nil
}
