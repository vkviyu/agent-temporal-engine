package agentprofile

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func snapshotImageFixture(t testing.TB, groups int) ([]byte, Snapshot) {
	t.Helper()
	ledger := New()
	for i := 0; i < groups; i++ {
		task := Task{
			ID: model.EntityID(fmt.Sprintf("task-%04d", i)), Space: "space",
			Owner: "owner", Goal: "goal", GoalEpoch: 1, Status: TaskOpen,
		}
		execution := Execution{
			ID:   model.ExecutionID(fmt.Sprintf("execution-%04d", i)),
			Task: task.ID, Actor: "worker", Incarnation: 1,
			AcceptedGoalEpoch: 1, Status: ExecutionReady,
		}
		recoveryApply(t, ledger, fmt.Sprintf("task-%04d", i), Command{CreateTask: &task})
		recoveryApply(t, ledger, fmt.Sprintf("execution-%04d", i), Command{Spawn: &execution})

		authority := AuthorityBinding{
			Deployment:  fmt.Sprintf("deployment-%04d", i),
			OwnerEpoch:  1,
			Incarnation: 1,
			Holder:      "worker",
		}
		recoveryControl(t, ledger, fmt.Sprintf("authority-%04d", i), ControlCommand{
			GrantAuthority: &GrantAuthorityCommand{
				Authority: ExecutionAuthority{
					Execution: execution.ID,
					Binding:   authority,
					Status:    "active",
					Evidence:  digest(byte('a' + byte(i%26))),
				},
			},
		})
		recoveryControl(t, ledger, fmt.Sprintf("step-%04d", i), ControlCommand{
			BeginStep: &BeginStepCommand{
				Step: Step{
					Execution: execution.ID,
					Sequence:  1,
					GoalEpoch: 1,
					Status:    StepOpen,
				},
				Authority: authority,
			},
		})

		resultID := fmt.Sprintf("result-%04d", i)
		acceptPrimaryResult(t, ledger, authority, execution.ID, 1, resultID)
		recoveryControl(t, ledger, fmt.Sprintf("invocation-%04d", i), ControlCommand{
			RecordInvocation: &RecordInvocationCommand{
				Invocation: Invocation{
					ID:              fmt.Sprintf("invocation-%04d", i),
					Execution:       execution.ID,
					Step:            1,
					ProviderCallKey: fmt.Sprintf("call-%04d", i),
					Tool:            "read",
					Attempt:         1,
					Status:          InvocationPending,
					AcceptedResult:  resultID,
					ToolDefinition:  digest('d'),
					ToolVersion:     "read/v1",
					DispatchClass:   "pure",
				},
				Authority: authority,
			},
		})
	}
	snapshot := ledger.Snapshot()
	for i := 0; i < groups; i++ {
		snapshot.Lifecycle.Queries = append(snapshot.Lifecycle.Queries, Query{
			ID: fmt.Sprintf("query-%04d", i), Space: "space", Owner: "owner",
			Version: 1, Status: "open", Input: []byte("input"),
		})
	}
	return recordCanonical(t, snapshot), snapshot
}

func mutateImageOwnedValue(v reflect.Value) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			mutateImageOwnedValue(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			mutateImageOwnedValue(v.Field(i))
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 && v.Len() > 0 && v.CanSet() {
			v.Index(0).SetUint(v.Index(0).Uint() ^ 1)
			return
		}
		for i := 0; i < v.Len(); i++ {
			mutateImageOwnedValue(v.Index(i))
		}
	case reflect.String:
		if v.CanSet() && v.Len() > 0 {
			v.SetString("mutated")
		}
	}
}

func recordCanonical(t testing.TB, snapshot Snapshot) []byte {
	t.Helper()
	image, err := EncodeRecordImage(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := image.Records()
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	for _, record := range records {
		out = append(out, record.Key...)
		out = append(out, record.Raw...)
	}
	return out
}

func restoreRecord(t testing.TB, snapshot Snapshot) *Ledger {
	t.Helper()
	image, err := EncodeRecordImage(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := image.Restore()
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}
