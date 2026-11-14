package agentprofile

import (
	"math"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func validBinding(b AuthorityBinding) bool {
	return commandText(b.Deployment) && commandText(b.Holder) && b.OwnerEpoch > 0 && b.Incarnation > 0
}
func validDigest(v model.ObjectID) bool   { return model.ValidDigest(string(v)) }
func zeroBinding(b AuthorityBinding) bool { return b == (AuthorityBinding{}) }
func validEffect(s string) bool           { return s == "pure" || s == "idempotent" || s == "non_idempotent" }
func validRole(s string) bool {
	switch s {
	case "routing", "query_decision", "primary_decision", "aux_summary", "external_tool", "resource_prepare", "validation":
		return true
	}
	return false
}
func modelRole(s string) bool {
	return s == "routing" || s == "query_decision" || s == "primary_decision" || s == "aux_summary"
}
func validActivityToolLink(s ActivitySpec) bool {
	if s.Role != "external_tool" {
		return s.Invocation == ""
	}
	if !validDigest(s.Contract.Definition) {
		return false
	}
	if s.Execution == "" {
		return s.Invocation == "" && s.Step == 0
	}
	return s.Step > 0 && commandText(s.Invocation)
}
func validSettlement(s string) bool {
	return s == "pending" || s == "partial" || s == "settled" || s == "unknown"
}
func validReconciliation(s string) bool {
	return s == "occurred" || s == "no_effect" || s == "compensated" || s == "unresolved"
}

func validateControlShape(c ControlCommand) error {
	if c.SchemaVersion() != currentControlSchema {
		return ErrCommandVersion
	}
	if _, _, err := controlOperation(c); err != nil {
		return err
	}
	bad := false
	switch {
	case c.AnswerQuery != nil:
		x := c.AnswerQuery
		bad = !commandText(x.Query) || !commandText(x.Slot) || x.ExpectedVersion == 0 || len(x.Body) > MaxCommandBytes || (x.Kind != "progress" && x.Kind != "clarification" && x.Kind != "final") || x.Final != (x.Kind == "final")
		bad = bad || (x.Final && !commandText(x.GeneratedFrom)) || (!x.Final && (x.GeneratedFrom != "" || len(x.Outcomes) > 0 || x.InputEvidence != "")) || len(x.Outcomes) > MaxLifecycleRecords || (x.InputEvidence != "" && !validDigest(x.InputEvidence))
	case c.StartWork != nil:
		x := c.StartWork
		bad = !commandText(x.Query) || !commandText(x.Slot) || x.ExpectedQueryVersion == 0 || Command{CreateTask: &x.Task}.Validate() != nil || Command{Spawn: &x.Execution}.Validate() != nil || x.Execution.Incarnation != 1 || x.Execution.Task != x.Task.ID || x.Execution.Parent != ""
	case c.GrantAuthority != nil:
		x := c.GrantAuthority
		bad = !commandText(string(x.Authority.Execution)) || x.Authority.Version != 0 || x.Authority.Status != "active" || !validBinding(x.Authority.Binding) || !validDigest(x.Authority.Evidence)
	case c.FenceAuthority != nil:
		x := c.FenceAuthority
		bad = !commandText(string(x.Execution)) || x.ExpectedVersion == 0 || x.ExpectedVersion == math.MaxUint64 || !validDigest(x.Evidence)
	case c.Resume != nil:
		x := c.Resume
		bad = !commandText(string(x.Execution)) || x.ExpectedIncarnation == 0 || x.ExpectedIncarnation == math.MaxUint64 || x.ExpectedAuthorityVersion == 0 || !validBinding(x.Next) || !commandText(x.AdapterVersion) || !validDigest(x.Checkpoint) || !validDigest(x.CompatibilityEvidence) || !validDigest(x.ResourceFenceEvidence)
	case c.PutIdempotencyGuarantee != nil:
		bad = !validGuaranteeShape(c.PutIdempotencyGuarantee.Guarantee)
	case c.AuthorizeRetry != nil:
		bad = !validRetryAuthorizationShape(c.AuthorizeRetry.Authorization)
	case c.CreateActivity != nil:
		x := c.CreateActivity
		s := x.Spec
		if s.FinalAnswerInput != "" && (!validDigest(s.FinalAnswerInput) || s.Query == "" || s.Role != "query_decision") {
			return ErrCommandShape
		}
		bad = !commandText(s.Activity) || (s.Execution == "") == (s.Query == "") || !validRole(s.Role) || s.InputFence.Validate() != nil || !validDigest(s.RequestDigest) || !validDigest(s.Request) || !commandText(s.Recipient) || !validEffect(s.Effect) || !validEffectContract(s) || !commandText(s.ResourceScope) || (modelRole(s.Role) && !validDigest(s.ContextPacket)) || (s.Effect == "idempotent" && !commandText(s.IdempotencyKey))
		if s.Execution != "" {
			bad = bad || !validBinding(x.Authority) || s.GoalEpoch == 0
		} else {
			bad = bad || !zeroBinding(x.Authority) || s.GoalEpoch == 0 || s.Step != 0 || s.Invocation != ""
		}
		bad = bad || !validActivityToolLink(s) || (s.ContextPacket != "" && !validDigest(s.ContextPacket)) || (s.Role == "primary_decision" && (s.Execution == "" || s.Step == 0))
	case c.ClaimAttempt != nil:
		x := c.ClaimAttempt
		bad = !commandText(x.Attempt) || !commandText(x.Activity) || x.Sequence == 0 || !validRetryUse(x.Use)
	case c.DispatchAttempt != nil:
		x := c.DispatchAttempt
		bad = !commandText(x.Attempt) || !validDigest(x.Evidence) || !validRetryUse(x.Use)
	case c.CaptureResult != nil:
		x := c.CaptureResult.Result
		bad = !commandText(x.ID) || !commandText(x.Activity) || !commandText(x.Attempt) || x.Incarnation == 0 || !validDigest(x.RequestDigest) || (x.Outcome != "success" && x.Outcome != "failure" && x.Outcome != "unknown") || len(x.Payload) > MaxCommandBytes
		bad = bad || !validResultCapture(x)
	case c.CaptureOutputChunk != nil:
		bad = !validOutputChunk(c.CaptureOutputChunk.Chunk)
	case c.AcceptActivity != nil:
		x := c.AcceptActivity
		bad = !commandText(x.Activity) || !commandText(x.Result) || x.ExpectedVersion == 0 || !validDigest(x.Evidence)
	case c.SettleActivity != nil:
		x := c.SettleActivity
		bad = !commandText(x.Activity) || !validSettlement(x.Status) || !validDigest(x.Evidence)
	case c.ReconcileAttempt != nil:
		x := c.ReconcileAttempt
		bad = !commandText(x.Attempt) || !validReconciliation(x.Outcome) || !validDigest(x.Evidence) || (x.Outcome == "no_effect" && !x.FutureEffectFenced)
	case c.RetryActivity != nil:
		x := c.RetryActivity
		bad = !commandText(x.Activity) || x.ExpectedVersion == 0 || !validRetryUse(x.Use)
	case c.SaveContinuation != nil:
		x := c.SaveContinuation
		v := x.Value
		bad = !commandText(string(v.Execution)) || v.Version != x.ExpectedVersion+1 || x.ExpectedVersion == math.MaxUint64 || v.Incarnation == 0 || !commandText(v.PendingKey) || v.PendingGeneration == 0 || v.InputFence.Validate() != nil || !commandText(v.Phase) || !validBinding(x.Authority)
		bad = bad || validateContinuationPosition(v) != nil
	case c.ObserveGoal != nil:
		x := c.ObserveGoal
		bad = !commandText(string(x.Execution)) || x.Epoch == 0 || !validDigest(x.Evidence) || !validBinding(x.Authority)
		bad = bad || !validDigest(x.Observation)
	case c.CompleteTask != nil:
		x := c.CompleteTask
		bad = !commandText(string(x.Task)) || !commandText(string(x.Execution)) || x.Epoch == 0 || !validDigest(x.Result) || !validDigest(x.Validation) || !validBinding(x.Authority)
	case c.CancelExecution != nil:
		x := c.CancelExecution
		bad = !commandText(string(x.Execution)) || x.ExpectedIncarnation == 0
	case c.SpawnChild != nil:
		x := c.SpawnChild
		bad = !commandText(string(x.Parent)) || !commandText(x.Invocation) || Command{CreateTask: &x.Task}.Validate() != nil || Command{Spawn: &x.Execution}.Validate() != nil || x.Execution.Incarnation != 1 || x.Execution.Task != x.Task.ID || x.Execution.Parent != x.Parent || x.InheritedEpoch == 0 || !validDigest(x.Compatibility) || !validBinding(x.Authority)
	case c.PutAction != nil:
		x := c.PutAction
		v := x.Intent
		bad = !commandText(v.Method) || len(v.ExactCommand) == 0 || len(v.ExactCommand) > MaxCommandBytes || v.Action.Key == "" || v.Action.Generation != 1 || v.Action.Status != ActionPending || v.Action.Request.Validate() != nil || !validDigest(v.Action.CommandDigest) || v.Action.InputFence.Validate() != nil || !validBinding(x.Authority) || v.Authority != x.Authority || len(v.InputObjects) > MaxLifecycleRecords
		bad = bad || (v.ExpectedFence != nil && v.ExpectedFence.Validate() != nil)
	case c.ResolveAction != nil:
		x := c.ResolveAction.Resolution
		bad = !commandText(x.Key) || x.Generation == 0 || x.ObservedFence.Validate() != nil || (x.Outcome != "applied" && x.Outcome != "absent" && x.Outcome != "unknown") || (x.Outcome == "applied" && !validDigest(x.Receipt)) || (x.Outcome != "applied" && x.Receipt != "")
	case c.RebindAction != nil:
		x := c.RebindAction
		bad = !commandText(x.Key) || x.ExpectedGeneration == 0 || x.ExpectedGeneration == math.MaxUint64 || x.NewRequest.Validate() != nil || !commandText(x.Method) || len(x.ExactCommand) == 0 || len(x.ExactCommand) > MaxCommandBytes || !validDigest(x.CommandDigest) || !validBinding(x.Authority) || !validDigest(x.FenceEvidence)
		bad = bad || (x.ExpectedFence != nil && x.ExpectedFence.Validate() != nil)
	case c.BeginStep != nil:
		x := c.BeginStep
		bad = Command{StartStep: &x.Step}.Validate() != nil || !validBinding(x.Authority)
	case c.RecordInvocation != nil:
		x := c.RecordInvocation
		bad = Command{RecordInvocation: &x.Invocation}.Validate() != nil || !validBinding(x.Authority) || x.Invocation.Status != InvocationPending
		bad = bad || !commandInvocationContract(x.Invocation)
	case c.FinishStep != nil:
		x := c.FinishStep
		bad = !commandText(string(x.Execution)) || x.Sequence == 0 || x.Epoch == 0 || !validBinding(x.Authority)
	case c.JoinExecutions != nil:
		x := c.JoinExecutions
		j := x.Join
		bad = !commandText(j.ID) || !commandText(string(j.Parent)) || len(j.Members) == 0 || len(j.Members) > MaxLifecycleRecords || j.Version != 0 || j.Status != "" || len(j.Winners) != 0 || (j.Mode != "all" && j.Mode != "any" && j.Mode != "quorum") || !validBinding(x.Authority) || (j.Mode == "quorum" && (j.Quorum == 0 || j.Quorum > uint64(len(j.Members))))
	case c.RebaseInherited != nil:
		x := c.RebaseInherited
		bad = !commandText(string(x.Execution)) || !commandText(string(x.SourceTask)) || x.ExpectedEpoch == 0 || x.Epoch <= x.ExpectedEpoch || !validDigest(x.Compatibility) || !validBinding(x.Authority) || !validDigest(x.Observation)
	case c.Checkpoint != nil:
		x := c.Checkpoint
		bad = validateControlShape(ControlCommand{PutAction: &PutActionCommand{Intent: x.Intent, Authority: x.Authority}}) != nil || validateControlShape(ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: x.Continuation, ExpectedVersion: x.ExpectedVersion, Authority: x.Authority}}) != nil || x.Intent.Action.Owner != x.Continuation.Execution || x.Intent.Action.Key != x.Continuation.PendingKey || x.Intent.Action.Generation != x.Continuation.PendingGeneration || x.Intent.Action.InputFence != x.Continuation.InputFence
	case c.SendMessage != nil:
		x := c.SendMessage
		bad = validateLifecycleCommand(LifecycleCommand{PutMessage: &x.Message}) != nil || !validBinding(x.Authority)
	case c.ConsumeMessage != nil:
		x := c.ConsumeMessage
		bad = validateLifecycleCommand(LifecycleCommand{ConsumeMessage: &x.Consumption}) != nil || !validBinding(x.Authority)
	case c.ImportActivityResult != nil:
		bad = validateControlShape(ControlCommand{AcceptActivity: c.ImportActivityResult}) != nil
	case c.BindValidation != nil:
		x := c.BindValidation
		_, err := x.Requirement.Canonical()
		bad = err != nil || x.ExpectedVersion == math.MaxUint64 || x.Requirement.Version != x.ExpectedVersion+1 || !validBinding(x.Authority)
	}
	if bad {
		return ErrCommandShape
	}
	return nil
}
