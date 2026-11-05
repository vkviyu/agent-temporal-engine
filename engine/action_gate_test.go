package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestActionGateRejectsAmbiguousLegacyReservations(t *testing.T) {
	ledger := agentprofile.New()
	apply := func(command agentprofile.Command) {
		t.Helper()
		if _, err := ledger.ApplyCommand(model.RequestKey{Space: "coordinator-space", Principal: "host", RequestID: "setup"}, command); err != nil {
			t.Fatal(err)
		}
	}
	task := profileCoordinatorTask("task")
	apply(agentprofile.Command{CreateTask: &task})
	x := agentprofile.Execution{ID: "execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	apply(agentprofile.Command{Spawn: &x})
	binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	apply(agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(integrationHex('a'))}}}})
	request := coordinatorRequest(t, "reserved", "effect", "once", api.ValueCondition{})
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "first", Generation: 1, Request: request.Key(), CommandDigest: agentprofile.ExactRequestDigest(request.Body), InputFence: model.CommitFence{Store: "store", CommitDigest: model.CommitDigest(integrationHex('b'))}, Owner: x.ID, Status: agentprofile.ActionPending}, Method: request.Method, ExactCommand: request.Body, Authority: binding}
	apply(agentprofile.Command{Control: &agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}})
	// Historical schema-1 snapshots could contain distinct slots sharing a
	// request. Restore such data through its canonical snapshot validation;
	// newer admission rules must not silently choose one historical slot.
	snapshot := ledger.Snapshot()
	intent.Action.Key = "second"
	snapshot.Actions = append(snapshot.Actions, intent.Action)
	snapshot.Lifecycle.Recovery.Actions = append(snapshot.Lifecycle.Recovery.Actions, intent)
	raw, err := agentprofile.EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err = agentprofile.RestoreSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateActionReservation(ledger, request); !errors.Is(err, ErrActionReservation) || !errors.Is(err, agentprofile.ErrConflict) {
		t.Fatalf("selected first of two legacy reservations: %v", err)
	}
}

func TestActionAbsenceCannotIgnoreEarlierPrivateGroupReceipt(t *testing.T) {
	e, _ := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	ctx := context.Background()
	apply := func(id string, command agentprofile.Command) api.CommitResult {
		t.Helper()
		return coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, id, command))))
	}
	task := profileCoordinatorTask("task")
	apply("task", agentprofile.Command{CreateTask: &task})
	x := agentprofile.Execution{ID: "execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	apply("execution", agentprofile.Command{Spawn: &x})
	binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	authority := agentprofile.ExecutionAuthority{Execution: x.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(integrationHex('a'))}
	apply("authority", agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: authority}}})
	original := coordinatorRequest(t, "original", "effect", "once", api.ValueCondition{})
	before, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "next", Generation: 1, Request: original.Key(), CommandDigest: agentprofile.ExactRequestDigest(original.Body), InputFence: before.Fence, Owner: x.ID, Status: agentprofile.ActionPending}, Method: original.Method, ExactCommand: original.Body, Authority: binding}
	// A reservation cannot occupy the RequestKey its future command needs.
	self := profileCoordinatorRequest(t, string(original.RequestID), agentprofile.Command{Control: &agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}})
	rejected := coordinatorWait(t, coordinatorCommit(e, ctx, self))
	if rejected.result.Code != api.CodeRejected || !errors.Is(rejected.err, agentprofile.ErrUniqueSlot) {
		t.Fatalf("self-consuming reservation was admitted: %+v %v", rejected.result, rejected.err)
	}
	apply("intent", agentprofile.Command{Control: &agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}})
	e.mu.Lock()
	defer e.mu.Unlock()
	view := e.stagingView()
	plan, prepared := stagingPrepare(t, view, original, "4")
	if err := view.acceptStaged(plan, prepared); err != nil {
		t.Fatal(err)
	}
	resolution := agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "next", Generation: 1, Outcome: "absent", ObservedFence: e.publication.Fence()}}}
	request := profileCoordinatorRequest(t, "incorrect-absence", agentprofile.Command{Control: &resolution})
	digest, err := CommandDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = view.prepare(ctx, request, digest, "55555555555555555555555555555555", view.publication.Position.LSN+1, nil); !errors.Is(err, agentprofile.ErrConflict) {
		t.Fatalf("private original receipt was ignored by absence resolution: %v", err)
	}
}

func integrationHex(ch byte) string {
	const size = 64
	var out [size]byte
	for i := range out {
		out[i] = ch
	}
	return string(out[:])
}
