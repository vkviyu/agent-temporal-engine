package engine

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func assertAuditIndexesAbsent(t testing.TB, e *Engine) {
	t.Helper()
	if e.traceEvents != nil || e.traceEventMeta != nil || e.traceEventKeys != nil || e.traceCausalChildren != nil {
		t.Fatalf("private projection allocated TaskTrace indexes: events=%d meta=%d keys=%d children=%d",
			len(e.traceEvents), len(e.traceEventMeta), len(e.traceEventKeys), len(e.traceCausalChildren))
	}
}

func assertAuditIndexesPopulated(t testing.TB, e *Engine) {
	t.Helper()
	if len(e.traceEvents) == 0 || len(e.traceEventMeta) == 0 || len(e.traceEventKeys) == 0 || e.traceCausalChildren == nil {
		t.Fatalf("fixture must build all four TaskTrace indexes: events=%d meta=%d keys=%d children=%d",
			len(e.traceEvents), len(e.traceEventMeta), len(e.traceEventKeys), len(e.traceCausalChildren))
	}
}

func assertAuditSemanticState(t *testing.T, got, want *Engine) {
	t.Helper()
	if got.roots != want.roots || got.rootSet != want.rootSet ||
		got.publication.Fence() != want.publication.Fence() || got.index.count() != want.index.count() {
		t.Fatal("roots, fence or object count changed without TaskTrace indexes")
	}
	for _, field := range []struct {
		name      string
		got, want any
	}{
		{"events", got.events, want.events},
		{"eventHeads", got.eventHeads, want.eventHeads},
		{"eventBlocks", got.eventBlocks, want.eventBlocks},
		{"history", got.history, want.history},
		{"envelopes", got.envelopes, want.envelopes},
		{"receiptHints", got.receiptHints, want.receiptHints},
	} {
		if !reflect.DeepEqual(field.got, field.want) {
			t.Fatalf("%s changed without TaskTrace indexes", field.name)
		}
	}
}

func TestAuditVerifierVerifyPublishedPreservesSemanticState(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, EventProfile: true})
	var requests []api.CommitRequest
	var receipts []api.CommitResult
	commit := func(req api.CommitRequest) api.CommitResult {
		t.Helper()
		ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, req)))
		requests, receipts = append(requests, req), append(receipts, ack)
		return ack
	}
	// Two execution streams with profile facts and historical step input fences.
	for i := 0; i < 2; i++ {
		task := profileCoordinatorTask(model.EntityID(fmt.Sprintf("audit-task-%d", i)))
		ack := commit(profileCoordinatorRequest(t, fmt.Sprintf("audit-create-%d", i), agentprofile.Command{CreateTask: &task}))
		execution := agentprofile.Execution{ID: model.ExecutionID(fmt.Sprintf("audit-execution-%d", i)),
			Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
		commit(profileCoordinatorRequest(t, fmt.Sprintf("audit-spawn-%d", i), agentprofile.Command{Spawn: &execution}))
		step := agentprofile.Step{Execution: execution.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: ack.Fence}
		commit(profileCoordinatorRequest(t, fmt.Sprintf("audit-step-%d", i), agentprofile.Command{StartStep: &step}))
	}
	assertAuditIndexesPopulated(t, e)

	verify := func(t *testing.T) {
		t.Helper()
		indexed := newEngine(e.options)
		verifier := newAuditVerifier(e.options)
		assertAuditIndexesAbsent(t, verifier)
		p, count, err := e.journal.VerifyPublished(ctx, indexed.replay)
		if err != nil || count != uint64(len(requests)) {
			t.Fatalf("indexed VerifyPublished: count=%d err=%v", count, err)
		}
		seen := 0
		q, verified, err := e.journal.VerifyPublished(ctx, func(tx storage.ScannedTransaction, source io.ReaderAt) error {
			if err := verifier.replay(tx, source); err != nil {
				return err
			}
			seen++
			assertAuditIndexesAbsent(t, verifier)
			return nil
		})
		if err != nil || verified != count || seen != len(requests) || q != p {
			t.Fatalf("audit VerifyPublished: count=%d seen=%d err=%v", verified, seen, err)
		}
		assertAuditIndexesPopulated(t, indexed)
		assertAuditSemanticState(t, verifier, indexed)
		assertAuditSemanticState(t, verifier, e)
	}
	t.Run("published", verify)
	if _, err := e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	t.Run("compacted", verify)
	assertAuditIndexesPopulated(t, e)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenExisting(disk, e.options)
	if err != nil || !report.Verified {
		t.Fatalf("reopen: report=%+v err=%v", report, err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertAuditIndexesPopulated(t, reopened)
	assertAuditSemanticState(t, reopened, e)
	for _, pair := range [][2]any{
		{reopened.traceEvents, e.traceEvents},
		{reopened.traceEventMeta, e.traceEventMeta},
		{reopened.traceEventKeys, e.traceEventKeys},
		{reopened.traceCausalChildren, e.traceCausalChildren},
	} {
		if !reflect.DeepEqual(pair[0], pair[1]) {
			t.Fatal("reopen did not rebuild the live TaskTrace index")
		}
	}
	for i, req := range requests {
		if got, err := reopened.Commit(ctx, req); err != nil || got != receipts[i] {
			t.Fatalf("receipt %d changed after compaction/reopen: %+v %v", i, got, err)
		}
	}
}

func TestAuditStagingSkipsIndexesAndRetainsPredecessors(t *testing.T) {
	actual := stagingEngine(t)
	actual.options.AgentProfile, actual.options.EventProfile = true, true
	staged, indexed := actual.stagingView(), actual.stagingView()
	// Reference projection of the same batch with query indexing enabled.
	indexed.skipTaskTraceIndex = false
	zero := &Engine{}
	task := profileCoordinatorTask("audit-staged-task")
	execution := agentprofile.Execution{ID: "audit-staged-execution", Task: task.ID,
		Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	requests := []api.CommitRequest{
		profileCoordinatorRequest(t, "audit-staged-create", agentprofile.Command{CreateTask: &task}),
		profileCoordinatorRequest(t, "audit-staged-spawn", agentprofile.Command{Spawn: &execution}),
	}
	for i := 0; i < 3; i++ {
		if i == 2 {
			step := agentprofile.Step{Execution: execution.ID, Sequence: 1, GoalEpoch: 1,
				Status: agentprofile.StepOpen, InputAnchor: staged.publication.Fence()}
			requests = append(requests, profileCoordinatorRequest(t, "audit-staged-step", agentprofile.Command{StartStep: &step}))
		}
		for _, view := range []*Engine{staged, indexed} {
			plan, prepared := stagingPrepare(t, view, requests[i], fmt.Sprint(i+1))
			if err := view.acceptStaged(plan, prepared); err != nil {
				t.Fatal(err)
			}
			if view == staged {
				zero.installEvent(plan)
			}
		}
		assertAuditIndexesAbsent(t, staged)
		assertAuditSemanticState(t, staged, indexed)
		if !staged.knownAgentFence(staged.publication.Fence()) {
			t.Fatal("staging lost predecessor fence")
		}
	}
	assertAuditIndexesPopulated(t, indexed)
	assertAuditIndexesPopulated(t, zero)
	if !reflect.DeepEqual(zero.traceEventMeta, indexed.traceEventMeta) ||
		!reflect.DeepEqual(zero.traceEvents, indexed.traceEvents) ||
		!reflect.DeepEqual(zero.traceEventKeys, indexed.traceEventKeys) ||
		!reflect.DeepEqual(zero.traceCausalChildren, indexed.traceCausalChildren) {
		t.Fatal("zero-value Engine no longer installs the same TaskTrace indexes")
	}
	if len(actual.events) != 0 || actual.eventBlocks != nil || len(actual.history) != 0 {
		t.Fatal("staging installed unpublished semantic state in its parent")
	}
	assertAuditIndexesAbsent(t, actual)
}
