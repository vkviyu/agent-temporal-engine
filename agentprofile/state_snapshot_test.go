package agentprofile

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestStateSnapshotUsesRecordLimits(t *testing.T) {
	many := Snapshot{}
	for i := 0; i < MaxStateRecords+1; i++ {
		many.Tasks = append(many.Tasks, Task{
			ID: model.EntityID(fmt.Sprint(i)), Space: "s", Owner: "p",
			Goal: "g", GoalEpoch: 1, Status: TaskOpen,
		})
	}
	if err := ValidateStateSnapshot(many); err == nil {
		t.Fatal("state validation ignored record count limit")
	}
	if ledger, err := RestoreStateSnapshot(many); err == nil || ledger != nil {
		t.Fatal("over-budget state was restored")
	}

	many.Executions = []Execution{{
		ID: "x", Task: "absent", Actor: "a", Incarnation: 1,
		AcceptedGoalEpoch: 1, Status: ExecutionReady,
	}}
	if err := ValidateStateSnapshot(many); err == nil {
		t.Fatal("record validation skipped relationship checks")
	}
}

func TestStateSnapshotRestoreOwnsState(t *testing.T) {
	snapshot := Snapshot{}
	payload := bytes.Repeat([]byte("x"), 128)
	for i := 0; i < 30; i++ {
		snapshot.Lifecycle.Queries = append(snapshot.Lifecycle.Queries, Query{
			ID: fmt.Sprint(i), Space: "s", Owner: "p", Version: 1,
			Input: bytes.Clone(payload),
		})
	}
	if err := ValidateStateSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	first, err := RestoreStateSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RestoreStateSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'z'
	first.lifecycle.Queries[0].Input[0] = 'y'
	if second.lifecycle.Queries[0].Input[0] != 'x' {
		t.Fatal("restore aliases caller or another ledger")
	}

	bad := Snapshot{Tasks: []Task{{
		ID: "t", Space: "s", Owner: "p",
		Goal:      string(bytes.Repeat([]byte("g"), 1025)),
		GoalEpoch: 1, Status: TaskOpen,
	}}}
	if err := ValidateStateSnapshot(bad); err == nil {
		t.Fatal("record validation skipped child decoder limits")
	}
}
