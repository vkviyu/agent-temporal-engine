package eventlog

import (
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const KindAgentTransition = "agent.transition_applied"
const (
	LegacyProfileRegistryVersion uint64 = 1
	ProfileRegistryVersion       uint64 = 2
)
const MaxProfileEvents = 256

func SupportsProfileRegistry(version uint64) bool {
	return version == LegacyProfileRegistryVersion || version == ProfileRegistryVersion
}

// SubjectRef is the registry-owned navigation mapping. Content-addressed
// Knowledge records use NodeObject; Agent subjects use logical NodeEntity.
// This mapping identifies a node but never grants visibility to its payload.
func SubjectRef(a model.EventAttribution) (model.NodeRef, error) {
	if !a.Subject.Valid() || !registeredSubject(a.Subject) {
		return model.NodeRef{}, ErrInvalid
	}
	if a.Subject.Profile == "knowledge" && (a.Subject.Kind == "Observation" || a.Subject.Kind == "ContextPacket") {
		ref := model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(a.Subject.ID)}
		if ref.Validate() != nil {
			return model.NodeRef{}, ErrInvalid
		}
		return ref, nil
	}
	if a.Subject.Profile == "graph" && a.Subject.Kind == "Revision" {
		ref := model.NodeRef{Kind: model.NodeRevision, Revision: model.RevisionID(a.Subject.ID)}
		return ref, ref.Validate()
	}
	if a.Subject.Profile == "memory" || a.Subject.Profile == "knowledge" && a.Subject.Kind == "Summary" {
		kind := a.Subject.Kind
		switch kind {
		case "Memory":
			kind = "memory"
		case "ExperiencePath":
			kind = "experience_path"
		case "Summary":
			kind = "summary"
		}
		k := a.Subject
		k.Kind = kind
		return model.NodeRef{Kind: model.NodeEntity, Entity: k}, nil
	}
	return model.NodeRef{Kind: model.NodeEntity, Entity: a.Subject}, nil
}

// The registry is a closed set of transition names. Renaming a command
// field does not silently rename its historical event contract.
func RegisteredAgentOperation(operation string) bool {
	switch operation {
	case "Core.annotation.apply/v1", "Core.graph.apply/v1", "Core.memory.apply/v1", "Core.knowledge.apply/v1", "Core.knowledge.summary.apply/v1", "Core.agent.emission.apply/v1", "Core.transfer.import/v1":
		return true
	case "Graph.AddRevision", "Graph.Fork", "Graph.Advance", "Graph.Join", "Graph.JoinRevision", "Graph.Prune",
		"Graph.ForkLocal", "Graph.WriteRevision", "Graph.WriteBranch", "Graph.JoinLocalRevision", "Graph.JoinLocal", "Graph.PublishBranch",
		"Memory.PutCandidate", "Memory.Validate", "Memory.Promote", "Memory.Transition", "Memory.RetractSource", "Memory.BuildPath", "Memory.RetractPath",
		"Annotation.Apply", "Annotation.Retract", "Summary.Apply", "Emission.Issue", "Emission.Claim", "Emission.Revoke", "Transfer.Import":
		return true
	case "CreateTask", "Spawn", "StartStep", "RecordInvocation", "CompleteStep", "CancelTask", "RebaseGoal", "PutPending", "RebindPending", "SettleInvocation",
		"Lifecycle.PutQuery", "Lifecycle.PutActivity", "Lifecycle.PutAttempt", "Lifecycle.PutResult", "Lifecycle.PutMessage", "Lifecycle.ConsumeMessage", "Lifecycle.PutContinuation",
		"Control.AnswerQuery", "Control.StartWork", "Control.GrantAuthority", "Control.FenceAuthority", "Control.Resume", "Control.CreateActivity", "Control.ClaimAttempt", "Control.DispatchAttempt", "Control.CaptureResult", "Control.AcceptActivity", "Control.SettleActivity", "Control.ReconcileAttempt", "Control.RetryActivity", "Control.SaveContinuation", "Control.ObserveGoal", "Control.CompleteTask", "Control.CancelExecution", "Control.SpawnChild", "Control.PutAction", "Control.ResolveAction", "Control.RebindAction", "Control.BeginStep", "Control.RecordInvocation", "Control.FinishStep", "Control.JoinExecutions", "Control.RebaseInherited", "Control.Checkpoint", "Control.SendMessage", "Control.ConsumeMessage", "Control.ImportActivityResult", "Control.BindValidation",
		"Work.query_intake", "Work.query_start", "Work.model_decision", "Work.message_consume", "Work.child_spawn", "Work.checkpoint", "Work.emission_dispatch", "Work.query_open", "Work.query_checkpoint", "Work.query_decision", "Work.query_tool", "Work.query_answer",
		"Query.InvocationRecorded", "Control.CaptureOutputChunk",
		"Knowledge.Observation", "Knowledge.Packet", "Knowledge.Import":
		return true
	}
	return false
}

func registeredSubject(k model.EntityKey) bool {
	if k.Profile == "core" {
		return k.Kind == "Method"
	}
	if k.Profile == "knowledge" {
		return k.Kind == "Observation" || k.Kind == "ContextPacket" || k.Kind == "Knowledge" || k.Kind == "Summary"
	}
	if k.Profile == "annotation" {
		return k.Kind == "Annotation"
	}
	if k.Profile == "memory" {
		return k.Kind == "Memory" || k.Kind == "ExperiencePath" || k.Kind == "Source"
	}
	if k.Profile == "graph" {
		return k.Kind == "Revision" || k.Kind == "Branch"
	}
	if k.Profile == "transfer" {
		return k.Kind == "Manifest" || k.Kind == "ImportRecord" || k.Kind == "ArchiveEntry"
	}
	if k.Profile != "agent" {
		return false
	}
	switch k.Kind {
	case "Task", "Execution", "Step", "Invocation", "PendingAction", "Query", "Activity", "Attempt", "ActivityResult", "OutputChunk", "Message", "MessageConsumption", "Continuation", "QueryAnswer", "WorkStart", "ExecutionAuthority", "ActivityAcceptance", "ActivitySettlement", "AttemptReconciliation", "GoalObservation", "TaskOutcome", "ExecutionJoin", "TaskLink", "ValidationRequirement", "WorkUnit", "QueryInvocation", "Emission":
		return true
	}
	return false
}

// Domain operation names also fix the subject namespace. A valid entity key
// alone must not allow a Graph fact to masquerade as an Agent or Memory fact.
func registeredOperationSubject(operation string, subject model.EntityKey) bool {
	match := func(profile, kind string) bool {
		return string(subject.Profile) == profile && string(subject.Kind) == kind
	}
	switch operation {
	case "Graph.AddRevision", "Graph.JoinRevision", "Graph.WriteRevision", "Graph.JoinLocalRevision":
		return match("graph", "Revision") && model.ValidDigest(string(subject.ID))
	case "Graph.Fork", "Graph.Advance", "Graph.Join", "Graph.Prune", "Graph.ForkLocal", "Graph.WriteBranch", "Graph.JoinLocal", "Graph.PublishBranch":
		return match("graph", "Branch")
	case "Memory.PutCandidate", "Memory.Validate", "Memory.Promote", "Memory.Transition":
		return match("memory", "Memory") && model.ValidDigest(string(subject.ID))
	case "Memory.BuildPath", "Memory.RetractPath":
		return match("memory", "ExperiencePath") && model.ValidDigest(string(subject.ID))
	case "Memory.RetractSource":
		return match("memory", "Source") && model.ValidDigest(string(subject.ID))
	case "Annotation.Apply", "Annotation.Retract":
		return match("annotation", "Annotation") && model.ValidDigest(string(subject.ID))
	case "Summary.Apply":
		return match("knowledge", "Summary") && model.ValidDigest(string(subject.ID))
	case "Emission.Issue", "Emission.Claim", "Emission.Revoke":
		return match("agent", "Emission")
	case "Transfer.Import":
		return match("transfer", "ImportRecord") && model.ValidDigest(string(subject.ID))
	case "Core.annotation.apply/v1", "Core.graph.apply/v1", "Core.memory.apply/v1", "Core.knowledge.apply/v1", "Core.knowledge.summary.apply/v1", "Core.agent.emission.apply/v1", "Core.transfer.import/v1":
		return match("core", "Method")
	default:
		return true
	}
}

func validateRegisteredEvent(e model.Event) error {
	if e.PreviousInStream != "" && !model.ValidDigest(string(e.PreviousInStream)) {
		return ErrInvalid
	}
	if e.Attribution != nil {
		a := e.Attribution
		if e.Kind != KindAgentTransition || e.SchemaVersion != 1 || e.ProfileRegistryVersion != 0 || len(e.ProfileEvents) != 0 ||
			!a.Subject.Valid() || !registeredSubject(a.Subject) || !registeredOperationSubject(a.Operation, a.Subject) || !text(string(a.Subject.Profile)) || !text(string(a.Subject.Kind)) || len(a.Subject.ID) > 4096 || !utf8.ValidString(string(a.Subject.ID)) || !RegisteredAgentOperation(a.Operation) || a.Ordinal == 0 ||
			(a.InitiatingActor != "" && !text(string(a.InitiatingActor))) || (a.Execution != "" && !text(string(a.Execution))) || (a.Query != "" && !text(a.Query)) || (a.Session != "" && !text(a.Session)) || (a.Invocation != "" && !text(a.Invocation)) || (a.Task != "" && !text(string(a.Task))) {
			return ErrInvalid
		}
		if a.Operation == "Query.InvocationRecorded" && (a.Query == "" || a.DecisionSequence == 0 || a.Invocation == "" || a.Execution != "") {
			return ErrInvalid
		}
		switch e.Stream.Kind {
		case "execution":
			if a.Execution == "" || e.Stream.Owner != string(a.Execution) || e.Stream.Incarnation == 0 {
				return ErrInvalid
			}
		case "query":
			if a.Execution != "" || a.Query == "" || e.Stream.Owner != a.Query || e.Stream.Incarnation != 0 {
				return ErrInvalid
			}
		case "principal":
			if e.Stream.Owner != string(e.RecordedBy) || e.Stream.Incarnation != 0 {
				return ErrInvalid
			}
		case "task":
			if !text(e.Stream.Owner) || e.Stream.Incarnation != 0 {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
		return nil
	}
	if e.Kind != KindCommandApplied || e.Stream.Kind != "principal" || e.Stream.Owner != string(e.RecordedBy) || e.Stream.Incarnation != 0 {
		return ErrInvalid
	}
	if e.SchemaVersion == 1 {
		if e.PreviousInStream != "" || e.ProfileRegistryVersion != 0 || len(e.ProfileEvents) != 0 {
			return ErrInvalid
		}
		return nil
	}
	if e.SchemaVersion != 2 || !SupportsProfileRegistry(e.ProfileRegistryVersion) || len(e.ProfileEvents) == 0 || len(e.ProfileEvents) > MaxProfileEvents || len(e.CausalParents) != 0 {
		return ErrInvalid
	}
	seen := map[model.EventID]bool{}
	for _, id := range e.ProfileEvents {
		if !model.ValidDigest(string(id)) || seen[id] {
			return ErrInvalid
		}
		seen[id] = true
	}
	return nil
}
