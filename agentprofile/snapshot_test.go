package agentprofile

import (
	"bytes"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestSnapshotRoundTripIsDeterministicAndOwned(t *testing.T) {
	l := New()
	task, err := l.CreateTask(CreateTask{Task: Task{ID: "task", Space: "space", Owner: "owner", Goal: "goal", GoalEpoch: 1, Status: TaskOpen}})
	if err != nil {
		t.Fatal(err)
	}
	exec, err := l.Spawn(SpawnRequest{Execution: Execution{ID: "exec", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.StartStep(StepRequest{Step: Step{Execution: exec.ID, Sequence: 1, GoalEpoch: 1, Status: StepOpen}}); err != nil {
		t.Fatal(err)
	}
	authority := grantPrimaryAuthority(t, l, exec.ID)
	acceptPrimaryResult(t, l, authority, exec.ID, 1, "result")
	if _, err = l.RecordInvocation(InvocationRequest{Invocation: Invocation{ID: "inv", Execution: exec.ID, Step: 1, ProviderCallKey: "call", Tool: "tool", Args: []byte("args"), Status: InvocationPending, Attempt: 1, AcceptedResult: "result", ToolDefinition: digest('d'), ToolVersion: "tool/v1", DispatchClass: "pure"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = l.PutPending(PendingAction{Key: "action", Generation: 1, Request: model.RequestKey{Space: "space", Principal: "owner", RequestID: "r"}, CommandDigest: digest('a'), Owner: exec.ID, Status: ActionPending}); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EncodeSnapshot(decoded)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("noncanonical roundtrip: %v", err)
	}
	restored, err := RestoreSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restored.GetInvocation("inv")
	if err != nil || string(got.Args) != "args" {
		t.Fatalf("restored invocation=%+v err=%v", got, err)
	}
	got.Args[0] = 'X'
	againInvocation, _ := restored.GetInvocation("inv")
	if string(againInvocation.Args) != "args" {
		t.Fatal("restored invocation aliases caller bytes")
	}
}

func TestSnapshotRejectsDuplicateAndTrailingRecords(t *testing.T) {
	l := New()
	if _, err := l.CreateTask(CreateTask{Task: Task{ID: "task", Space: "space", Owner: "owner", Goal: "goal", GoalEpoch: 1, Status: TaskOpen}}); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	// Append a byte after the final action section; canonical decoding must not
	// accept hidden bytes as an alternate snapshot.
	if _, err := DecodeSnapshot(append(append([]byte(nil), raw...), 0)); err == nil {
		t.Fatal("trailing snapshot data accepted")
	}
	// Duplicate the sole task record by changing the task count to two is not
	// accepted because the second record is absent/truncated.
	mutated := append([]byte(nil), raw...)
	for i := range mutated {
		if mutated[i] == 1 {
			mutated[i] = 2
			break
		}
	}
	if _, err := DecodeSnapshot(mutated); err == nil {
		t.Fatal("malformed record count accepted")
	}
}
