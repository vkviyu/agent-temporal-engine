package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func (e *Engine) validateWorkBoundaryKind(
	ctx context.Context,
	req api.CommitRequest,
	u api.WorkUnit,
	seed *initialWorkSeed,
	inputs workBoundaryInputs,
) error {
	switch u.Kind {
	case "query_start":
		if len(inputs.starts) != 1 || len(inputs.knowledge) == 0 {
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
		x := inputs.starts[0]
		for _, command := range inputs.knowledge {
			if command.Observation == nil || command.Observation.Key != (model.KnowledgeKey{Actor: x.Execution.Actor, Execution: x.Execution.ID}) || command.Observation.Source.Anchor.Knowledge != nil || command.Observation.PayloadKind != "" {
				return ErrWorkBoundary
			}
		}
		if x.Query != u.Query || x.ExpectedQueryVersion != intake.Intake.Query.Version || !inputs.authorities[x.Execution.ID] || inputs.continuations[x.Execution.ID] == nil {
			return ErrWorkBoundary
		}
	case "child_spawn":
		if len(inputs.spawns) != 1 {
			return ErrWorkBoundary
		}
		x := inputs.spawns[0]
		if _, err := inputs.ledger.GetExecution(x.Execution.ID); err == nil {
			return agentprofile.ErrUniqueSlot
		}
		if !inputs.authorities[x.Execution.ID] || inputs.continuations[x.Execution.ID] == nil {
			return ErrWorkBoundary
		}
		seeded := false
		for _, c := range inputs.knowledge {
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
		if len(inputs.consumes) != 1 {
			return ErrWorkBoundary
		}
		x := inputs.consumes[0].Consumption
		for _, old := range inputs.ledger.Snapshot().Lifecycle.Consumptions {
			if old.Message == x.Message && old.Recipient == x.Recipient {
				return agentprofile.ErrUniqueSlot
			}
		}
		if inputs.continuations[x.Recipient] == nil {
			return ErrWorkBoundary
		}
		message, err := inputs.ledger.GetMessage(x.Message)
		if err != nil {
			return err
		}
		execution, err := inputs.ledger.GetExecution(x.Recipient)
		if err != nil {
			return err
		}
		key := model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID}
		index, _, err := e.knowledgeIndexAt(ctx, e.roots, req.Space, req.Principal)
		if err != nil {
			return err
		}
		observed := false
		for _, c := range inputs.knowledge {
			if c.Observation == nil || c.Observation.Key != key || c.Observation.PayloadKind != "" {
				continue
			}
			if _, exists, err := index.observation(ctx, c.Observation.ID); err != nil {
				return err
			} else if exists {
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
		if len(inputs.accepts) != 1 || u.Decision.Activity != inputs.accepts[0].Activity || u.Decision.Result != inputs.accepts[0].Result || len(inputs.invocations) != len(u.Decision.Invocations) {
			return ErrWorkBoundary
		}
		var spec *agentprofile.ActivitySpec
		if inputs.ledger.Snapshot().Lifecycle.Recovery != nil {
			for _, s := range inputs.ledger.Snapshot().Lifecycle.Recovery.ActivitySpecs {
				if s.Activity == u.Decision.Activity {
					v := s
					spec = &v
				}
			}
		}
		if spec == nil || spec.Role != "primary_decision" || inputs.continuations[spec.Execution] == nil {
			return ErrWorkBoundary
		}
		finish := inputs.finishes[spec.Execution]
		// Decisions with pending tools publish their complete dispatch set and
		// recovery position first. FinishStep is only legal after those tools
		// have accepted outcomes; requiring it here made tool-bearing decisions
		// impossible to commit.
		if len(inputs.invocations) == 0 && finish == nil ||
			len(inputs.finishes) > 0 && (len(inputs.finishes) != 1 || finish == nil || finish.Sequence != spec.Step) {
			return ErrWorkBoundary
		}
		seen := map[string]bool{}
		for i, binding := range u.Decision.Invocations {
			if _, err := inputs.ledger.GetInvocation(binding.Invocation); err == nil {
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
			for _, invocation := range inputs.invocations {
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
		if len(inputs.continuations) == 0 {
			return ErrWorkBoundary
		}
	}
	return nil
}
