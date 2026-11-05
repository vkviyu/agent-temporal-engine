package agentprofile

import (
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestInvocationContractCodecRejectsLegacyAndPartialContract(t *testing.T) {
	legacy := commandInvocation()
	legacy.Status, legacy.Result = InvocationPending, []byte{}
	golden, _ := hex.DecodeString("0103696e7604657865630463616c6c04726561640770656e64696e670104617267730001")
	if _, err := DecodeInvocation(golden); err == nil {
		t.Fatal("schema-1 invocation admitted by current decoder")
	}
	bound := legacy
	bound.AcceptedResult, bound.ToolDefinition, bound.ToolVersion, bound.DispatchClass = "accepted-result", digest('a'), "read/v1", "pure"
	raw, err := EncodeInvocation(bound)
	if err != nil || raw[0] != 2 {
		t.Fatal(err)
	}
	got, err := DecodeInvocation(raw)
	if err != nil || !reflect.DeepEqual(got, bound) {
		t.Fatalf("contract roundtrip: %+v %v", got, err)
	}
	for end := 0; end < len(raw); end++ {
		if _, err := DecodeInvocation(raw[:end]); err == nil {
			t.Fatalf("accepted truncated contract at %d", end)
		}
	}
	for _, mutate := range []func(*Invocation){
		func(i *Invocation) { i.AcceptedResult = "" },
		func(i *Invocation) { i.ToolDefinition = "" },
		func(i *Invocation) { i.ToolVersion = "" },
		func(i *Invocation) { i.DispatchClass = "unsafe" },
	} {
		bad := bound
		mutate(&bad)
		if _, err := EncodeInvocation(bad); err == nil {
			t.Fatal("accepted incomplete/invalid contract")
		}
	}
	control := ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: bound, Authority: AuthorityBinding{Deployment: "host", Holder: "worker", OwnerEpoch: 1, Incarnation: 1}}}
	if control.SchemaVersion() != 5 {
		t.Fatal("contract did not use V1 control schema 5")
	}
	if _, err := EncodeCommand(Command{Control: &control}); err != nil {
		t.Fatalf("complete invocation control rejected: %v", err)
	}
}

func TestInvocationContractRequiresAcceptedPrimaryAndKeepsDispatchClass(t *testing.T) {
	l, authority := recoveryFixture(t)
	recoveryControl(t, l, "step", ControlCommand{BeginStep: &BeginStepCommand{Step: commandStep(), Authority: authority}})
	spec := ActivitySpec{Activity: "decision", Execution: "exec", Step: 1, Role: "primary_decision", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('b'), RequestDigest: digest('c'), ContextPacket: digest('d'), Recipient: "model", Effect: "pure", ResourceScope: "session"}
	recoveryControl(t, l, "create", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: spec, Authority: authority}})
	recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Activity: spec.Activity, Attempt: "attempt", Sequence: 1, Authority: authority}})
	recoveryControl(t, l, "dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "attempt", Authority: authority, Evidence: digest('a')}})
	result := ActivityResult{ID: "model-result", Activity: spec.Activity, Attempt: "attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("call read")}
	missing := commandInvocation()
	missing.AcceptedResult = "missing-result"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: missing, Authority: authority}}}, ErrConflict)
	recoveryControl(t, l, "capture", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
	inv := commandInvocation()
	inv.Status, inv.Result = InvocationPending, nil
	inv.AcceptedResult, inv.ToolDefinition, inv.ToolVersion, inv.DispatchClass = result.ID, digest('b'), "read/v1", "pure"
	command := func(i Invocation) Command {
		return Command{Control: &ControlCommand{RecordInvocation: &RecordInvocationCommand{Invocation: i, Authority: authority}}}
	}
	assertRecoveryRejected(t, l, command(inv), ErrConflict)
	recoveryControl(t, l, "accept", ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: authority, Evidence: digest('a')}})
	recoveryApply(t, l, "record", command(inv))
	for _, mutate := range []func(*Invocation){
		func(i *Invocation) { i.AcceptedResult = "other-result" },
		func(i *Invocation) { i.ToolVersion = "read/v2" },
		func(i *Invocation) { i.Args = []byte("changed args") },
		func(i *Invocation) { i.DispatchClass = "non_idempotent" },
	} {
		bad := inv
		mutate(&bad)
		assertRecoveryRejected(t, l, command(bad), ErrConflict)
	}
	tool := spec
	tool.Activity, tool.Role, tool.Invocation, tool.ContextPacket = "read-activity", "external_tool", inv.ID, ""
	tool.Contract = EffectContract{Definition: inv.ToolDefinition, Version: "effect/v1", Recipient: tool.Recipient, VerifiedPure: true, DifferentRequest: "unsupported", ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "none"}
	tool.Effect = "non_idempotent"
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CreateActivity: &CreateActivityCommand{Spec: tool, Authority: authority}}}, ErrConflict)
	tool.Effect = "pure"
	recoveryControl(t, l, "tool", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: tool, Authority: authority}})
	if _, err := DecodeSnapshot(mustInvocationSnapshot(t, l)); err != nil {
		t.Fatal(err)
	}
	snapshot := l.Snapshot()
	snapshot.Invocations[0].AcceptedResult = "uncaptured-result"
	snapshot.Invocations[0].Status = InvocationPending
	if err := snapshot.Validate(); !errors.Is(err, ErrConflict) {
		t.Fatalf("restore accepted pending invocation with a missing decision: %v", err)
	}
	snapshot.Invocations[0].Status = InvocationAccepted
	if err := snapshot.Validate(); !errors.Is(err, ErrConflict) {
		t.Fatalf("restore accepted forged decision: %v", err)
	}
}

func mustInvocationSnapshot(t *testing.T, l *Ledger) []byte {
	t.Helper()
	raw, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// acceptPrimaryResult records the real model lifecycle used by tool contracts.
func acceptPrimaryResult(t *testing.T, l *Ledger, authority AuthorityBinding, execution model.ExecutionID, step uint64, resultID string) {
	t.Helper()
	spec := ActivitySpec{Activity: "primary-" + resultID, Execution: execution, Step: step, Role: "primary_decision", GoalEpoch: 1, InputFence: recoveryFence(), Request: digest('b'), RequestDigest: digest('c'), ContextPacket: digest('d'), Recipient: "model", Effect: "pure", ResourceScope: "session"}
	recoveryControl(t, l, "create-"+resultID, ControlCommand{CreateActivity: &CreateActivityCommand{Spec: spec, Authority: authority}})
	attempt := "attempt-" + resultID
	recoveryControl(t, l, "claim-"+resultID, ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Activity: spec.Activity, Attempt: attempt, Sequence: 1, Authority: authority}})
	recoveryControl(t, l, "dispatch-"+resultID, ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: attempt, Authority: authority, Evidence: digest('a')}})
	result := ActivityResult{ID: resultID, Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest, Incarnation: authority.Incarnation, Outcome: "success", Payload: []byte("accepted tool decision")}
	recoveryControl(t, l, "capture-"+resultID, ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
	recoveryControl(t, l, "accept-"+resultID, ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: authority, Evidence: digest('a')}})
}

func grantPrimaryAuthority(t *testing.T, l *Ledger, execution model.ExecutionID) AuthorityBinding {
	t.Helper()
	authority := AuthorityBinding{Deployment: "deployment", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryControl(t, l, "primary-authority", ControlCommand{GrantAuthority: &GrantAuthorityCommand{Authority: ExecutionAuthority{Execution: execution, Binding: authority, Status: "active", Evidence: digest('a')}}})
	return authority
}
