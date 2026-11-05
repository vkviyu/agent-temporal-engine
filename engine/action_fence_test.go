package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestSchema2NewAdmissionRejectedWithoutHistoricalCompatibility(t *testing.T) {
	e, _ := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	t.Cleanup(func() { _ = e.Close() })
	ctx := context.Background()
	task := profileCoordinatorTask("strict-task")
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "strict-task", agentprofile.Command{CreateTask: &task}))))
	x := agentprofile.Execution{ID: "strict-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "strict-execution", agentprofile.Command{Spawn: &x}))))
	binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	modern := profileCoordinatorRequest(t, "strict-grant", agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(integrationHex('a'))}}}})
	legacy := modern
	legacy.Body = bytes.Clone(modern.Body)
	version := bytes.Index(legacy.Body, []byte("ATE-AGENT-CONTROL\x00")) + len("ATE-AGENT-CONTROL\x00")
	legacy.Body[version] = 2
	before, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out := coordinatorWait(t, coordinatorCommit(e, ctx, legacy)); out.result.Code != api.CodeRejected || !errors.Is(out.err, agentprofile.ErrCommandVersion) {
		t.Fatalf("schema2 request was admitted: %+v %v", out.result, out.err)
	}
	if after, err := e.Snapshot(ctx); err != nil || after != before {
		t.Fatalf("legacy rejection changed/fenced store: %+v %v", after, err)
	}
}

func actionFenceFixture(t *testing.T) (*Engine, *coordinatorIO, agentprofile.AuthorityBinding, model.CommitFence) {
	t.Helper()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	ctx := context.Background()
	apply := func(id string, c agentprofile.Command) api.CommitResult {
		return coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, id, c))))
	}
	task := profileCoordinatorTask("fence-task")
	apply("fence-task", agentprofile.Command{CreateTask: &task})
	x := agentprofile.Execution{ID: "fence-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	apply("fence-execution", agentprofile.Command{Spawn: &x})
	binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	ack := apply("fence-authority", agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(integrationHex('a'))}}}})
	return e, disk, binding, ack.Fence
}

func TestReservedExpectedFenceCannotBeDroppedOrRefreshedAfterRestart(t *testing.T) {
	e, disk, binding, source := actionFenceFixture(t)
	ctx := context.Background()
	original := coordinatorRequest(t, "reserved-with-fence", "effect", "once", api.ValueCondition{})
	original.ExpectedFence = &source
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "fenced-next", Generation: 1, Request: original.Key(), CommandDigest: agentprofile.ExactRequestDigest(original.Body), InputFence: source, Owner: "fence-execution", Status: agentprofile.ActionPending}, Method: original.Method, ExactCommand: original.Body, Authority: binding, ExpectedFence: &source}
	reserve := profileCoordinatorRequest(t, "fence-reservation", agentprofile.Command{Control: &agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}})
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, reserve)))
	for _, reopen := range []bool{false, true} {
		if reopen {
			options := e.options
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			e, _, err = OpenExisting(disk, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
		}
		for _, changed := range []*model.CommitFence{nil, &ack.Fence} {
			request := original
			request.ExpectedFence = changed
			out := coordinatorWait(t, coordinatorCommit(e, ctx, request))
			if out.result.Code != api.CodeRejected || !errors.Is(out.err, ErrActionReservation) {
				t.Fatalf("changed ExpectedFence admitted (reopen=%v): %+v %v", reopen, out.result, out.err)
			}
		}
		// An exact but stale fence is a normal CAS conflict. The engine never
		// silently clears it or advances it to the reservation's own receipt.
		out := coordinatorWait(t, coordinatorCommit(e, ctx, original))
		if out.err != nil || out.result.Code != api.CodeConflict || out.result.Conflict != api.ConflictPrecondition {
			t.Fatalf("stale exact ExpectedFence changed semantics: %+v %v", out.result, out.err)
		}
		current, err := e.Snapshot(ctx)
		if err != nil || current.Fence != ack.Fence {
			t.Fatalf("caller rejection changed/fenced store: %+v %v", current, err)
		}
	}
}

func TestExpectedFenceReceiptReconciliationAndRetryRemainExact(t *testing.T) {
	e, disk, binding, source := actionFenceFixture(t)
	ctx := context.Background()
	original := coordinatorRequest(t, "already-published", "effect", "once", api.ValueCondition{})
	original.ExpectedFence = &source
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, original)))
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "published-next", Generation: 1, Request: original.Key(), CommandDigest: agentprofile.ExactRequestDigest(original.Body), InputFence: source, Owner: "fence-execution", Status: agentprofile.ActionPending}, Method: original.Method, ExactCommand: original.Body, Authority: binding}
	// Same request key/method/body with the wrong optional outer precondition
	// cannot acquire a new reservation over an existing receipt.
	bad := profileCoordinatorRequest(t, "wrong-outer-condition", agentprofile.Command{Control: &agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}})
	if out := coordinatorWait(t, coordinatorCommit(e, ctx, bad)); out.result.Code != api.CodeRejected || !errors.Is(out.err, agentprofile.ErrConflict) {
		t.Fatalf("mismatched receipt precondition admitted: %+v %v", out.result, out.err)
	}
	e.mu.Lock()
	if _, _, err := e.agentActionReceiptAtRoots(ctx, e.roots, intent); !errors.Is(err, agentprofile.ErrConflict) {
		e.mu.Unlock()
		t.Fatalf("receipt lookup ignored the exact outer precondition: %v", err)
	}
	e.mu.Unlock()
	intent.ExpectedFence = &source
	reserve := profileCoordinatorRequest(t, "correct-outer-condition", agentprofile.Command{Control: &agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, reserve)))
	resolution := profileCoordinatorRequest(t, "applied-with-fence", agentprofile.Command{Control: &agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: intent.Action.Key, Generation: 1, Outcome: "applied", ObservedFence: ack.Fence, Receipt: ack.ResultDigest}}}})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, resolution)))
	options := e.options
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, _, err := OpenExisting(disk, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if retry := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, original))); retry != ack {
		t.Fatal("receipt-first retry reevaluated stale ExpectedFence or applied status")
	}
}
