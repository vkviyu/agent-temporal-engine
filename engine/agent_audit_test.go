package engine

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Corrupt only the bytes returned for the immutable profile snapshot. The
// underlying durable prefix is unchanged, so reopening can independently prove
// that rejection did not publish a replacement snapshot or damage receipts.
type agentAuditSnapshotReader struct {
	reader io.ReaderAt
	offset int64
	length uint32
}

func (r agentAuditSnapshotReader) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.reader.ReadAt(p, offset)
	if offset == r.offset && n == int(r.length) && n > 0 {
		p[n-1] ^= 1
	}
	return n, err
}

func TestAgentStoredSnapshotCorruptionFencesRatherThanRejectingCommand(t *testing.T) {
	e, disk := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	task := profileCoordinatorTask("audit-corruption-task")
	baseRequest := profileCoordinatorRequest(t, "audit-corruption-base", agentprofile.Command{CreateTask: &task})
	baseACK := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), baseRequest)))
	before, err := e.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writesBefore, _, currentsBefore, _ := disk.counts()
	e.mu.Lock()
	id, found, err := state.Get(context.Background(), e.index, systemSpace, e.roots.LiveControl, agentPartitionKey(baseRequest.Space, baseRequest.Principal))
	if err != nil || !found {
		e.mu.Unlock()
		t.Fatalf("profile object missing: %v %v", found, err)
	}
	location := e.index.entries[id]
	if location.PayloadLength == 0 {
		e.mu.Unlock()
		t.Fatal("profile object has no published location")
	}
	e.index.source = agentAuditSnapshotReader{reader: e.index.source, offset: location.PayloadOffset, length: location.PayloadLength}
	objectsBefore := e.index.count()
	e.mu.Unlock()
	x := agentprofile.Execution{ID: "audit-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	request := profileCoordinatorRequest(t, "audit-corruption-spawn", agentprofile.Command{Spawn: &x})
	result := coordinatorWait(t, coordinatorCommit(e, context.Background(), request))
	if result.result.Code != api.CodeStoreFenced || !errors.Is(result.err, storage.ErrStoreFenced) || !errors.Is(result.err, ErrStoredObject) || !errors.Is(result.err, storage.ErrCorruption) {
		t.Fatalf("stored-object corruption became business rejection: %+v %v", result.result, result.err)
	}
	if agentCommandRejection(result.err) {
		t.Fatal("stored-object corruption classified as a command rejection")
	}
	if ClassifyError(result.err) != api.ErrorStoreFenced {
		t.Fatalf("fenced error classification=%s", ClassifyError(result.err))
	}
	e.mu.Lock()
	unchanged := e.roots == before.Roots && e.rootSet == before.RootSet && e.index.count() == objectsBefore
	e.mu.Unlock()
	if !unchanged {
		t.Fatal("stored corruption installed a candidate")
	}
	writesAfter, _, currentsAfter, _ := disk.counts()
	if writesAfter != writesBefore || currentsAfter != currentsBefore {
		t.Fatal("stored corruption reached a durable write")
	}
	got, err := e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: baseRequest.Space, Principal: baseRequest.Principal})
	if !errors.Is(err, storage.ErrStoreFenced) || !reflect.DeepEqual(got, api.AgentStateResult{}) {
		t.Fatalf("fenced read leaked profile: %+v %v", got, err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenExisting(disk, e.options)
	if err != nil || !report.Verified {
		t.Fatalf("unchanged durable prefix could not reopen: %+v %v", report, err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if restored := coordinatorLookup(t, reopened, baseRequest); restored.Code != api.CodeDurableAck || restored.Result != baseACK {
		t.Fatalf("original receipt changed: %+v", restored)
	}
	if lookup := coordinatorLookup(t, reopened, request); lookup.Code != api.CodeNotFoundAtFence || lookup.ObservedFence != baseACK.Fence {
		t.Fatalf("failed command got a receipt: %+v", lookup)
	}
	state, err := reopened.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: baseRequest.Space, Principal: baseRequest.Principal})
	if err != nil || !state.Found || state.Fence != baseACK.Fence || len(state.Snapshot.Tasks) != 1 || len(state.Snapshot.Executions) != 0 {
		t.Fatalf("reopened prefix changed: %+v %v", state, err)
	}
}

func TestAgentStagingAuthenticatesEarlierGroupMemberFence(t *testing.T) {
	actual := stagingEngine(t)
	actual.options.AgentProfile = true
	view := actual.stagingView()
	task := profileCoordinatorTask("audit-group-task")
	create := profileCoordinatorRequest(t, "audit-group-task", agentprofile.Command{CreateTask: &task})
	first, prepared := stagingPrepare(t, view, create, "1")
	if err := view.acceptStaged(first, prepared); err != nil {
		t.Fatal(err)
	}
	input := view.publication.Fence()
	x := agentprofile.Execution{ID: "audit-group-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	spawn := profileCoordinatorRequest(t, "audit-group-spawn", agentprofile.Command{Spawn: &x})
	second, prepared := stagingPrepare(t, view, spawn, "2")
	if err := view.acceptStaged(second, prepared); err != nil {
		t.Fatal(err)
	}
	step := agentprofile.Step{Execution: x.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: input}
	start := profileCoordinatorRequest(t, "audit-group-step", agentprofile.Command{StartStep: &step})
	third, prepared := stagingPrepare(t, view, start, "3")
	if err := view.acceptStaged(third, prepared); err != nil {
		t.Fatal(err)
	}
	if actual.knownAgentFence(input) {
		t.Fatal("unpublished group member fence became public")
	}
	if !view.knownAgentFence(input) {
		t.Fatal("valid earlier group member fence lost from private history")
	}
	ledger, _, found, err := loadAgentLedger(context.Background(), view.index, view.roots.LiveControl, create.Space, create.Principal)
	if err != nil || !found {
		t.Fatalf("private profile absent: %v %v", found, err)
	}
	got, err := ledger.GetStep(x.ID, 1)
	if err != nil || got.InputAnchor != input {
		t.Fatalf("staged input anchor changed: %+v %v", got, err)
	}
	if actual.roots.LiveControl != "" || actual.rootSet != "" || actual.index.count() != 0 {
		t.Fatal("private group installed public control state")
	}
}
