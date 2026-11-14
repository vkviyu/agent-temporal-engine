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

// goalKnowledgeObservation checks an actual adopted fact, never an opaque host
// attestation. An empty observation ID is used only when discovering an inherited
// ancestor's exact canonical goal image in the receiving execution's corpus.
func goalKnowledgeObservation(ctx context.Context, reader state.ObjectReader, snapshot knowledge.Snapshot, key model.KnowledgeKey, id model.ObjectID, task agentprofile.Task) (knowledge.Observation, error) {
	expected, err := agentprofile.GoalKnowledge(task)
	if err != nil {
		return knowledge.Observation{}, err
	}
	for _, observation := range snapshot.Observations {
		if observation.Key != key || (id != "" && observation.ID != id) {
			continue
		}
		if observation.PayloadKind != "" {
			if id != "" {
				return knowledge.Observation{}, agentprofile.ErrGoalEpoch
			}
			continue
		}
		object, err := readObject(ctx, reader, observation.Payload)
		if err != nil {
			return knowledge.Observation{}, err
		}
		if object.Space != task.Space {
			return knowledge.Observation{}, ErrInvalidStateData
		}
		if object.Type != valueType {
			if id != "" {
				return knowledge.Observation{}, agentprofile.ErrGoalEpoch
			}
			continue
		}
		value, err := readValue(object)
		if err != nil {
			return knowledge.Observation{}, err
		}
		if value.Kind == api.ValueBytes && bytes.Equal(value.Data, expected) {
			return observation, nil
		}
		if id != "" {
			return knowledge.Observation{}, agentprofile.ErrGoalEpoch
		}
	}
	return knowledge.Observation{}, agentprofile.ErrGoalEpoch
}

func (e *Engine) checkGoalAdoption(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, execution model.ExecutionID, taskID model.EntityID, epoch uint64, observation model.ObjectID) error {
	x, err := ledger.GetExecution(execution)
	if err != nil {
		return err
	}
	task, err := ledger.GetTask(taskID)
	if err != nil {
		return err
	}
	if task.Space != req.Space || task.Owner != req.Principal {
		return agentprofile.ErrCommandScope
	}
	if task.GoalEpoch != epoch {
		return agentprofile.ErrGoalEpoch
	}
	anchor := e.agentPublishedAnchor(req.Space)
	roots, _, err := e.rootsAtFence(ctx, anchor.Fence, true)
	if err != nil {
		return err
	}
	key := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	anchor.Knowledge = &key
	snapshot, _, _, _, err := e.selectedKnowledgeObservations(ctx, roots, anchor, req.Principal)
	if err != nil {
		return err
	}
	_, err = goalKnowledgeObservation(ctx, &cursorReader{source: e.index}, snapshot, key, observation, task)
	return err
}

type goalInputRequirement struct {
	task        agentprofile.Task
	observation model.ObjectID
	own         bool
}

func requiredExecutionGoals(ledger *agentprofile.Ledger, execution model.ExecutionID) (model.KnowledgeKey, []goalInputRequirement, error) {
	x, err := ledger.GetExecution(execution)
	if err != nil {
		return model.KnowledgeKey{}, nil, err
	}
	key := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	task, err := ledger.GetTask(x.Task)
	if err != nil {
		return key, nil, err
	}
	recovery := ledger.Snapshot().Lifecycle.Recovery
	if recovery == nil {
		if task.GoalEpoch > 1 {
			return key, nil, agentprofile.ErrGoalEpoch
		}
		return key, nil, nil
	}
	requirements := []goalInputRequirement{}
	if task.GoalEpoch > 1 {
		var observation model.ObjectID
		for _, goal := range recovery.Goals {
			if goal.Execution == x.ID && goal.Epoch == task.GoalEpoch {
				observation = goal.Observation
				break
			}
		}
		if observation == "" {
			return key, nil, agentprofile.ErrGoalEpoch
		}
		requirements = append(requirements, goalInputRequirement{task: task, observation: observation, own: true})
	}
	links := make(map[model.EntityID]agentprofile.TaskLink, len(recovery.TaskLinks))
	for _, link := range recovery.TaskLinks {
		links[link.Task] = link
	}
	seenTasks := map[model.EntityID]bool{}
	seenExecutions := map[model.ExecutionID]bool{}
	for current := x; ; {
		if seenExecutions[current.ID] {
			return key, nil, agentprofile.ErrConflict
		}
		seenExecutions[current.ID] = true
		if link, linked := links[current.Task]; linked && !seenTasks[link.InheritedTask] {
			source, err := ledger.GetTask(link.InheritedTask)
			if err != nil {
				return key, nil, err
			}
			if source.GoalEpoch != link.InheritedEpoch {
				return key, nil, agentprofile.ErrGoalEpoch
			}
			seenTasks[source.ID] = true
			if source.GoalEpoch > 1 {
				// A direct child rebase names the adopted child observation. An
				// inherited ancestor must independently be present in this child's
				// knowledge; the parent's private ObservationID is not transferable.
				var observation model.ObjectID
				if current.ID == x.ID {
					observation = link.GoalObservation
					if observation == "" {
						return key, nil, agentprofile.ErrGoalEpoch
					}
				}
				requirements = append(requirements, goalInputRequirement{task: source, observation: observation})
			}
		}
		if current.Parent == "" {
			break
		}
		current, err = ledger.GetExecution(current.Parent)
		if err != nil {
			return key, nil, err
		}
	}
	return key, requirements, nil
}

// checkGoalInputCut makes the declared input fence carry the actual new goal
// adoption. A current control epoch cannot bless a cut preceding that fact.
func (e *Engine) checkGoalInputCut(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, execution model.ExecutionID, fence model.CommitFence) error {
	key, requirements, err := requiredExecutionGoals(ledger, execution)
	if err != nil {
		return err
	}
	if len(requirements) == 0 {
		return nil
	}
	roots, _, err := e.rootsAtFence(ctx, fence, true)
	if err != nil {
		return err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: fence, Knowledge: &key}
	snapshot, _, _, _, err := e.selectedKnowledgeObservations(ctx, roots, anchor, req.Principal)
	if err != nil {
		return err
	}
	reader := &cursorReader{source: e.index}
	for _, required := range requirements {
		if _, err = goalKnowledgeObservation(ctx, reader, snapshot, key, required.observation, required.task); err != nil {
			return err
		}
	}
	return nil
}

// checkModelGoalPacket binds changed own and inherited goals to the exact
// packet source and declared input members. Accepting a new control epoch alone
// cannot make an older ContextPacket suitable for the next model dispatch.
func (e *Engine) checkModelGoalPacket(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, execution model.ExecutionID, packet knowledge.ContextPacket, role string) error {
	if role == "primary_decision" {
		if err := e.checkExecutionContextRequirements(ctx, req, execution, packet); err != nil {
			return err
		}
	}
	key, requirements, err := requiredExecutionGoals(ledger, execution)
	if err != nil {
		return err
	}
	if len(requirements) == 0 {
		return nil
	}
	if packet.Key == nil || *packet.Key != key || packet.Source.Anchor.Knowledge == nil || *packet.Source.Anchor.Knowledge != key {
		return agentprofile.ErrGoalEpoch
	}
	roots, _, err := e.rootsAtFence(ctx, packet.Source.Anchor.Fence, true)
	if err != nil {
		return err
	}
	snapshot, _, _, _, err := e.selectedKnowledge(ctx, roots, packet.Source.Anchor, req.Principal)
	if err != nil {
		return err
	}
	reader := &cursorReader{source: e.index}
	for _, required := range requirements {
		observation, err := goalKnowledgeObservation(ctx, reader, snapshot, key, required.observation, required.task)
		if err != nil {
			return err
		}
		if required.own && packet.GoalRef != observation.Payload {
			return agentprofile.ErrGoalEpoch
		}
		included := false
		for _, item := range packet.Items {
			if item.Observation == observation.ID && item.Content == observation.Payload {
				included = true
				break
			}
		}
		if !included {
			return agentprofile.ErrGoalEpoch
		}
	}
	return nil
}
