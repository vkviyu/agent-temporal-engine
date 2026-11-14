package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func (e *Engine) profileEventDrafts(ctx context.Context, req api.CommitRequest, builder state.ObjectReader, roots model.RootSet, registry uint64) ([]profileEventDraft, error) {
	linked := registry >= eventlog.ProfileRegistryVersion
	if req.Method != api.MethodApplyAgent && req.Method != api.MethodApplyWork {
		return e.domainEventDrafts(ctx, req, builder, roots, linked)
	}
	ledger, _, _, err := loadAgentLedger(ctx, builder, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	snapshot := ledger.Snapshot()
	if req.Method == api.MethodApplyAgent {
		c, err := agentprofile.DecodeCommand(req.Body)
		if err != nil {
			return nil, err
		}
		d, err := agentEventDraft(c, snapshot, linked)
		if err == nil {
			err = bindQueryEventDecision(ctx, builder, roots, req, &d)
		}
		return []profileEventDraft{d}, err
	}
	u, err := api.DecodeWorkUnit(req.Body)
	if err != nil {
		return nil, err
	}
	a := model.EventAttribution{Subject: eventSubject("WorkUnit", string(req.RequestID)), Operation: "Work." + u.Kind, Query: u.Query}
	if a.Query != "" {
		a.Subject = eventSubject("Query", a.Query)
	}
	d := profileEventDraft{attribution: a}
	if u.QueryTurn != nil {
		d.attribution.DecisionSequence = u.QueryTurn.Sequence
	}
	bindProfileOwner(&d, snapshot)
	if !eventlog.RegisteredAgentOperation(a.Operation) {
		return nil, ErrUnsupportedSchema
	}
	drafts := []profileEventDraft{d}
	for _, step := range u.Steps {
		switch step.Method {
		case api.MethodApplyAgent:
			c, err := agentprofile.DecodeCommand(step.Body)
			if err != nil {
				return nil, err
			}
			d, err := agentEventDraft(c, snapshot, linked)
			if err != nil {
				return nil, err
			}
			if err := bindQueryEventDecision(ctx, builder, roots, req, &d); err != nil {
				return nil, err
			}
			drafts = append(drafts, d)
		case api.MethodApplyEmission:
			sub := req
			sub.Method, sub.Body = step.Method, step.Body
			more, err := emissionEventDrafts(ctx, sub, builder, roots)
			if err != nil {
				return nil, err
			}
			drafts = append(drafts, more...)
		case api.MethodApplyKnowledge:
			c, err := knowledge.DecodeCommand(step.Body)
			if err != nil {
				return nil, err
			}
			d := knowledgeWorkEventDraft(c, snapshot, linked)
			if linked && c.Import != nil {
				if err := linkImportedObservationFacts(ctx, builder, roots, req, *c.Import, &d); err != nil {
					return nil, err
				}
			}
			if !eventlog.RegisteredAgentOperation(d.attribution.Operation) || !d.attribution.Subject.Valid() {
				return nil, ErrUnsupportedSchema
			}
			drafts = append(drafts, d)
		default:
			return nil, ErrUnsupportedSchema
		}
	}
	if u.Kind == "query_decision" && u.QueryTurn != nil {
		for _, invocation := range u.QueryTurn.Invocations {
			d := profileEventDraft{attribution: model.EventAttribution{Subject: eventSubject("QueryInvocation", recoveryNodeID(u.Query, invocation.ID)), Operation: "Query.InvocationRecorded", Query: u.Query, DecisionSequence: u.QueryTurn.Sequence, Invocation: invocation.ID}}
			bindProfileOwner(&d, snapshot)
			if u.Decision != nil {
				d.causes = []profileEventCause{{"Control.AcceptActivity", u.Decision.Activity}}
			}
			drafts = append(drafts, d)
		}
	}
	if u.Query == "" && len(drafts) > 1 {
		first := drafts[1]
		drafts[0].attribution.Subject = first.attribution.Subject
		drafts[0].attribution.Execution = first.attribution.Execution
		drafts[0].attribution.Query = first.attribution.Query
		drafts[0].attribution.Session = first.attribution.Session
		drafts[0].attribution.Step = first.attribution.Step
		drafts[0].attribution.InitiatingActor = first.attribution.InitiatingActor
		drafts[0].attribution.Task = first.attribution.Task
		drafts[0].stream = first.stream
	}
	if linked && u.Kind == "message_consume" {
		if err := linkConsumedObservations(ctx, builder, u, snapshot, drafts); err != nil {
			return nil, err
		}
	}
	return drafts, nil
}

func linkImportedObservationFacts(ctx context.Context, reader state.ObjectReader, roots model.RootSet, req api.CommitRequest, command knowledge.ImportCommand, draft *profileEventDraft) error {
	root, err := slotSpaceRoot(ctx, reader, roots.ExecutionKnowledge, req.Space)
	if err != nil {
		return err
	}
	index, err := openKnowledgeIndex(ctx, reader, root, req.Space, req.Principal)
	if err != nil {
		return err
	}
	origins := make(map[model.ObjectID]knowledge.Observation, len(command.Observations))
	for _, id := range command.Observations {
		o, found, err := index.observation(ctx, id)
		if err != nil {
			return err
		}
		if !found {
			return ErrInvalidStateData
		}
		origins[id] = o
	}
	imported, err := knowledge.ImportedObservations(command, origins)
	if err != nil {
		return err
	}
	for _, o := range imported {
		draft.factAliases = append(draft.factAliases, profileEventCause{"Knowledge.Observation", string(o.ID)})
	}
	return nil
}

func linkConsumedObservations(ctx context.Context, reader state.ObjectReader, unit api.WorkUnit, snapshot agentprofile.Snapshot, drafts []profileEventDraft) error {
	var messageID string
	var recipient model.ExecutionID
	for _, step := range unit.Steps {
		if step.Method != api.MethodApplyAgent {
			continue
		}
		command, err := agentprofile.DecodeCommand(step.Body)
		if err != nil {
			return err
		}
		if c := command.Control.ConsumeMessage; c != nil {
			messageID, recipient = c.Consumption.Message, c.Consumption.Recipient
			break
		}
	}
	var message *agentprofile.Message
	for i := range snapshot.Lifecycle.Messages {
		if snapshot.Lifecycle.Messages[i].ID == messageID {
			message = &snapshot.Lifecycle.Messages[i]
			break
		}
	}
	if message == nil {
		return ErrInvalidStateData
	}
	for i, step := range unit.Steps {
		if step.Method != api.MethodApplyKnowledge {
			continue
		}
		command, err := knowledge.DecodeCommand(step.Body)
		if err != nil {
			return err
		}
		observation := command.Observation
		if observation == nil || observation.Key.Execution != recipient || observation.PayloadKind != "" {
			continue
		}
		object, err := readObject(ctx, reader, observation.Payload)
		if err != nil {
			return err
		}
		value, err := readValue(object)
		if err != nil {
			return err
		}
		if value.Kind == api.ValueBytes && bytes.Equal(value.Data, message.Payload) {
			// The adoption's producer is the earlier Send, even when the
			// host orders Observation before Consume in their shared Work.
			drafts[i+1].causedBy("Control.SendMessage", messageID)
			drafts[i+1].causedBy("Lifecycle.PutMessage", messageID)
		}
	}
	return nil
}

// Query-owned Activities share no Execution/Step. Their DecisionRef is resolved
// from the same protected QueryProgress cut used to authorize that activity,
// including direct Claim/Dispatch/Capture transitions outside a Work envelope.
func bindQueryEventDecision(ctx context.Context, reader state.ObjectReader, roots model.RootSet, req api.CommitRequest, d *profileEventDraft) error {
	a := &d.attribution
	if a.Query == "" || d.activity == "" {
		return nil
	}
	id, found, err := state.Get(ctx, reader, systemSpace, roots.LiveControl, queryProgressKey(req.Space, req.Principal, a.Query))
	if err != nil || !found {
		return err
	}
	object, err := readObject(ctx, reader, id)
	if err != nil {
		return err
	}
	progress, err := decodeQueryProgressValue(object)
	if err != nil {
		return err
	}
	if progress.Query != a.Query {
		return ErrInvalidStateData
	}
	for _, decision := range progress.Decisions {
		if decision.Activity == d.activity {
			a.DecisionSequence = decision.Sequence
			return nil
		}
		for _, invocation := range decision.Invocations {
			if invocation.Activity == d.activity {
				a.DecisionSequence = decision.Sequence
				a.Invocation = invocation.ID
				if a.Operation == "Control.CreateActivity" {
					d.causes = append(d.causes, profileEventCause{"Query.InvocationRecorded", recoveryNodeID(a.Query, invocation.ID)})
				}
				return nil
			}
		}
	}
	// Only a pending model decision may use the next sequence. Other kinds
	// remain unbound until an explicit decision record says what they belong to.
	ledger, _, _, err := loadAgentLedger(ctx, reader, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	if recovery := ledger.Snapshot().Lifecycle.Recovery; recovery != nil {
		for _, spec := range recovery.ActivitySpecs {
			if spec.Activity == d.activity && spec.Query == a.Query && spec.Role == "query_decision" {
				if progress.Sequence == ^uint64(0) {
					return ErrInvalidStateData
				}
				a.DecisionSequence = progress.Sequence + 1
				break
			}
		}
	}
	return nil
}
