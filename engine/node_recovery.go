package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Composite keys identify logical control records without depending on slice
// order or delimiters inside host IDs. Occurrence identities remain Events;
// repeating one identical decision does not invent a second logical entity.
func recoveryNodeID(fields ...string) string {
	var b []byte
	for _, field := range fields {
		b, _ = codec.AppendString(b, field)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
func recoveryVersion(v uint64) string { return strconv.FormatUint(v, 10) }

func addRecoveryNodes(c *nodeCatalog, s *agentprofile.RecoverySnapshot, snapshot model.ObjectID, anchor model.SnapshotAnchor) error {
	if s == nil {
		return nil
	}
	count := len(s.Authorities) + len(s.ActivitySpecs) + len(s.ActivityControls) + len(s.AttemptControls) + len(s.Captures) + len(s.Acceptances) + len(s.Settlements) + len(s.Reconciliations) + len(s.Answers) + len(s.Starts) + len(s.TaskLinks) + len(s.Goals) + len(s.Outcomes) + len(s.Actions) + len(s.Resolutions) + len(s.Resumes) + len(s.Joins) + len(s.Imports) + len(s.ValidationRequirements)
	if count > maxNodeQueryRecords-len(c.nodes) {
		return storage.ErrLimit
	}
	add := func(kind model.EntityKind, id, label string, value any, parents []model.NodeRef, objects ...model.ObjectID) error {
		ref := agentEntity(kind, id)
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if old, exists := c.nodes[ref]; exists {
			if !bytes.Equal(old.detail.Data, raw) {
				return ErrInvalidStateData
			}
			return nil
		}
		sources := []model.NodeRef{}
		for _, id := range objects {
			if id != "" {
				sources = append(sources, model.NodeRef{Kind: model.NodeObject, Object: id})
			}
		}
		c.nodes[ref] = nodeProjection{detail: api.NodeDetail{Ref: ref, Summary: api.NodeSummary{Ref: ref, Label: label, Kind: kind}, Payload: snapshot, Data: raw, Encoding: "agent/" + string(kind) + "/v1+json", Anchor: anchor.Clone(), Sources: sources}, parents: parents}
		return nil
	}
	entity := func(kind model.EntityKind, id string) model.NodeRef { return agentEntity(kind, id) }
	for _, v := range s.Authorities {
		if err := add("ExecutionAuthority", string(v.Execution), v.Status, v, []model.NodeRef{entity("Execution", string(v.Execution))}); err != nil {
			return err
		}
	}
	for _, v := range s.ActivitySpecs {
		parents := []model.NodeRef{entity("Activity", v.Activity)}
		if v.Execution != "" {
			parents = append(parents, entity("ExecutionAuthority", string(v.Execution)))
		} else {
			parents = append(parents, entity("Query", v.Query))
		}
		if err := add("ActivitySpec", v.Activity, v.Role, v, parents, v.Request, v.ContextPacket); err != nil {
			return err
		}
	}
	for _, v := range s.ActivityControls {
		parents := []model.NodeRef{entity("Activity", v.Activity), entity("ActivitySpec", v.Activity)}
		if v.AcceptedResult != "" {
			parents = append(parents, entity("ActivityResult", v.AcceptedResult))
		}
		if err := add("ActivityControl", v.Activity, "activity control", v, parents); err != nil {
			return err
		}
	}
	for _, v := range s.AttemptControls {
		if err := add("AttemptControl", v.Attempt, v.Status, v, []model.NodeRef{entity("Attempt", v.Attempt), entity("Activity", v.Activity)}); err != nil {
			return err
		}
	}
	for _, v := range s.Captures {
		label := "captured"
		if v.Quarantined {
			label = "quarantined"
		}
		if err := add("ResultCapture", v.Result, label, v, []model.NodeRef{entity("ActivityResult", v.Result)}); err != nil {
			return err
		}
	}
	for _, v := range s.Acceptances {
		if err := add("ActivityAcceptance", v.Activity, "accepted", v, []model.NodeRef{entity("Activity", v.Activity), entity("ActivityResult", v.Result)}); err != nil {
			return err
		}
	}
	for _, v := range s.Imports {
		if err := add("ActivityResultImport", recoveryNodeID(v.Activity, recoveryVersion(v.Version), v.Result), "imported", v, []model.NodeRef{entity("Activity", v.Activity), entity("ActivityResult", v.Result)}); err != nil {
			return err
		}
	}
	for _, v := range s.Settlements {
		if err := add("ActivitySettlement", recoveryNodeID(v.Activity, recoveryVersion(v.Version)), v.Status, v, []model.NodeRef{entity("Activity", v.Activity)}); err != nil {
			return err
		}
	}
	for _, v := range s.Reconciliations {
		if err := add("AttemptReconciliation", recoveryNodeID(v.Attempt, recoveryVersion(v.Version)), v.Outcome, v, []model.NodeRef{entity("Attempt", v.Attempt), entity("AttemptControl", v.Attempt)}); err != nil {
			return err
		}
	}
	for _, v := range s.Answers {
		if err := add("QueryAnswer", recoveryNodeID(v.Query, v.Slot), v.Kind, v, []model.NodeRef{entity("Query", v.Query)}); err != nil {
			return err
		}
	}
	for _, v := range s.Starts {
		if err := add("WorkStart", recoveryNodeID(v.Query, v.Slot), "work started", v, []model.NodeRef{entity("Query", v.Query), entity("Task", string(v.Task)), entity("Execution", string(v.Execution))}); err != nil {
			return err
		}
	}
	for _, v := range s.TaskLinks {
		if err := add("TaskLink", string(v.Task), "spawned task", v, []model.NodeRef{entity("Task", string(v.Task)), entity("Execution", string(v.ParentExecution)), entity("Invocation", v.Invocation), entity("Task", string(v.InheritedTask))}); err != nil {
			return err
		}
	}
	for _, v := range s.Goals {
		if err := add("GoalObservation", recoveryNodeID(string(v.Execution), recoveryVersion(v.Epoch)), "goal observed", v, []model.NodeRef{entity("Execution", string(v.Execution))}); err != nil {
			return err
		}
	}
	for _, v := range s.Outcomes {
		parents := []model.NodeRef{entity("Task", string(v.Task)), entity("Execution", string(v.Execution))}
		if v.ValidationRequirement != "" {
			parents = append(parents, entity("ValidationRequirement", string(v.ValidationRequirement)))
		}
		if err := add("TaskOutcome", string(v.Task), "completed", v, parents, v.Result, v.Validation); err != nil {
			return err
		}
	}
	for _, v := range s.ValidationRequirements {
		if err := add("ValidationRequirement", string(v.ID), v.Spec.PolicyVersion, v, []model.NodeRef{entity("Task", string(v.Spec.Task)), entity("Execution", string(v.Spec.Execution))}, v.Spec.Result, v.Spec.Command, v.Spec.Environment); err != nil {
			return err
		}
	}
	for _, v := range s.Actions {
		if err := add("ActionIntent", recoveryNodeID(v.Action.Key, recoveryVersion(v.Action.Generation)), v.Method, v, []model.NodeRef{entity("PendingAction", v.Action.Key), entity("Execution", string(v.Action.Owner))}, v.InputObjects...); err != nil {
			return err
		}
	}
	for _, v := range s.Resolutions {
		id := recoveryNodeID(v.Key, recoveryVersion(v.Generation), v.Outcome, string(v.ObservedFence.Store), recoveryVersion(v.ObservedFence.LSN), string(v.ObservedFence.CommitDigest), string(v.Receipt))
		if err := add("ActionResolution", id, v.Outcome, v, []model.NodeRef{entity("PendingAction", v.Key), entity("ActionIntent", recoveryNodeID(v.Key, recoveryVersion(v.Generation)))}, v.Receipt); err != nil {
			return err
		}
	}
	for _, v := range s.Resumes {
		if err := add("ResumePlan", recoveryNodeID(string(v.Execution), recoveryVersion(v.Next.Incarnation)), v.AdapterVersion, v, []model.NodeRef{entity("Execution", string(v.Execution)), entity("ExecutionAuthority", string(v.Execution))}, v.Checkpoint); err != nil {
			return err
		}
	}
	for _, v := range s.Joins {
		parents := []model.NodeRef{entity("Execution", string(v.Parent))}
		for _, id := range v.Members {
			parents = append(parents, entity("Execution", string(id)))
		}
		if err := add("ExecutionJoin", v.ID, v.Status, v, parents); err != nil {
			return err
		}
	}
	return nil
}
