package engine

import (
	"encoding/base64"
	"encoding/json"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func agentEntity(kind model.EntityKind, id string) model.NodeRef {
	return model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "agent", Kind: kind, ID: model.EntityID(id)}}
}

// Composite local identities use length-prefixed fields, not concatenated
// delimiters. A model can retain the NodeRef returned by ListNodes verbatim.
func agentStepID(exec model.ExecutionID, n uint64) string {
	b, _ := codec.AppendString(nil, string(exec))
	b = codec.AppendUint(b, n)
	return base64.RawURLEncoding.EncodeToString(b)
}
func consumptionID(message string, recipient model.ExecutionID) string {
	b, _ := codec.AppendString(nil, message)
	b, _ = codec.AppendString(b, string(recipient))
	return base64.RawURLEncoding.EncodeToString(b)
}

func addProfileNodes(c *nodeCatalog, s agentprofile.Snapshot, snapshot model.ObjectID, anchor model.SnapshotAnchor) error {
	count := len(s.Tasks) + len(s.Executions) + len(s.Steps) + len(s.Invocations) + len(s.Actions) + len(s.Lifecycle.Queries) + len(s.Lifecycle.Activities) + len(s.Lifecycle.Attempts) + len(s.Lifecycle.Results) + len(s.Lifecycle.Messages) + len(s.Lifecycle.Consumptions) + len(s.Lifecycle.Continuations)
	if count > maxNodeQueryRecords {
		return storage.ErrLimit
	}
	add := func(kind model.EntityKind, id, label string, data any, parents ...model.NodeRef) error {
		ref := agentEntity(kind, id)
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Label: label, Kind: kind}, Payload: snapshot, Data: raw, Encoding: "agent/" + string(kind) + "/v1+json", Anchor: anchor.Clone()}, parents: parents}
		return nil
	}
	for _, x := range s.Tasks {
		if err := add("Task", string(x.ID), x.Goal, x); err != nil {
			return err
		}
	}
	for _, x := range s.Executions {
		p := []model.NodeRef{agentEntity("Task", string(x.Task))}
		if x.Parent != "" {
			p = append(p, agentEntity("Execution", string(x.Parent)))
		}
		if err := add("Execution", string(x.ID), string(x.Status), x, p...); err != nil {
			return err
		}
	}
	for _, x := range s.Steps {
		if err := add("Step", agentStepID(x.Execution, x.Sequence), string(x.Status), x, agentEntity("Execution", string(x.Execution))); err != nil {
			return err
		}
	}
	for _, x := range s.Invocations {
		if err := add("Invocation", x.ID, x.Tool, x, agentEntity("Step", agentStepID(x.Execution, x.Step))); err != nil {
			return err
		}
	}
	for _, x := range s.Actions {
		p := []model.NodeRef{agentEntity("Execution", string(x.Owner))}
		if x.Supersedes != "" {
			p = append(p, agentEntity("PendingAction", x.Supersedes))
		}
		if err := add("PendingAction", x.Key, string(x.Status), x, p...); err != nil {
			return err
		}
	}
	for _, x := range s.Lifecycle.Queries {
		if err := add("Query", x.ID, x.Status, x); err != nil {
			return err
		}
	}
	queryActivities := map[string]string{}
	if s.Lifecycle.Recovery != nil {
		for _, spec := range s.Lifecycle.Recovery.ActivitySpecs {
			if spec.Query != "" {
				queryActivities[spec.Activity] = spec.Query
			}
		}
	}
	for _, x := range s.Lifecycle.Activities {
		p := []model.NodeRef{}
		if x.Execution != "" {
			p = append(p, agentEntity("Execution", string(x.Execution)))
		} else if query := queryActivities[x.ID]; query != "" {
			p = append(p, agentEntity("Query", query))
		}
		if x.Step != 0 {
			p = append(p, agentEntity("Step", agentStepID(x.Execution, x.Step)))
		}
		if x.Invocation != "" {
			p = append(p, agentEntity("Invocation", x.Invocation))
		}
		if err := add("Activity", x.ID, x.Kind, x, p...); err != nil {
			return err
		}
	}
	for _, x := range s.Lifecycle.Attempts {
		if err := add("Attempt", x.ID, x.Status, x, agentEntity("Activity", x.Activity)); err != nil {
			return err
		}
	}
	for _, x := range s.Lifecycle.Results {
		parents := []model.NodeRef{agentEntity("Activity", x.Activity), agentEntity("Attempt", x.Attempt)}
		for _, id := range x.OutputChunks {
			parents = append(parents, agentEntity("OutputChunk", string(id)))
		}
		if err := add("ActivityResult", x.ID, x.Outcome, x, parents...); err != nil {
			return err
		}
	}
	for _, x := range s.Lifecycle.Messages {
		if err := add("Message", x.ID, "message", x, agentEntity("Execution", string(x.Sender)), agentEntity("Execution", string(x.Recipient))); err != nil {
			return err
		}
	}
	for _, x := range s.Lifecycle.Consumptions {
		if err := add("MessageConsumption", consumptionID(x.Message, x.Recipient), "consumed", x, agentEntity("Message", x.Message), agentEntity("Execution", string(x.Recipient))); err != nil {
			return err
		}
	}
	for _, x := range s.Lifecycle.Continuations {
		if err := add("Continuation", string(x.Execution), x.Phase, x, agentEntity("Execution", string(x.Execution)), agentEntity("PendingAction", x.PendingKey)); err != nil {
			return err
		}
	}
	if err := addRecoveryNodes(c, s.Lifecycle.Recovery, snapshot, anchor); err != nil {
		return err
	}
	for ref, p := range c.nodes {
		parents := []model.NodeRef{}
		seen := map[model.NodeRef]bool{}
		for _, parent := range p.parents {
			if _, ok := c.nodes[parent]; ok && !seen[parent] {
				seen[parent] = true
				parents = append(parents, parent)
			}
		}
		sort.Slice(parents, func(i, j int) bool { return nodeSortKey(parents[i]) < nodeSortKey(parents[j]) })
		p.parents = parents
		sources := append([]model.NodeRef{{Kind: model.NodeObject, Object: snapshot}}, parents...)
		for _, source := range p.detail.Sources {
			if source.Validate() != nil {
				return ErrInvalidStateData
			}
			sources = append(sources, source)
		}
		sourceSet := map[model.NodeRef]bool{}
		p.detail.Sources = nil
		for _, source := range sources {
			if !sourceSet[source] {
				sourceSet[source] = true
				p.detail.Sources = append(p.detail.Sources, source)
			}
		}
		c.nodes[ref] = p
	}
	return nil
}
