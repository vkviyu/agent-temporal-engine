package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func profileCoordinatorRequest(t *testing.T, id string, command agentprofile.Command) api.CommitRequest {
	t.Helper()
	body, err := agentprofile.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	return api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: model.RequestID(id), Method: api.MethodApplyAgent, Body: body}
}

func profileCoordinatorTask(id model.EntityID) agentprofile.Task {
	return agentprofile.Task{ID: id, Space: "coordinator-space", Owner: "host", Goal: "recover exactly", GoalEpoch: 1, Status: agentprofile.TaskOpen}
}

func TestAgentPublicationUnknownNeverInstallsCandidate(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after-current=%t", after), func(t *testing.T) {
			options := Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}}
			e, io := coordinatorStore(t, options)
			task := profileCoordinatorTask("unknown-task")
			baseReq := profileCoordinatorRequest(t, "unknown-base", agentprofile.Command{CreateTask: &task})
			baseACK := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), baseReq)))
			before, err := e.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			objectsBefore := e.index.count()
			execution := agentprofile.Execution{ID: "unknown-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
			request := profileCoordinatorRequest(t, "unknown-spawn", agentprofile.Command{Spawn: &execution})
			gate := newCoordinatorGate()
			gate.err = errors.New("test: Agent CURRENT publication response unavailable")
			gate.after = after
			io.arm("current", gate)
			pending := coordinatorCommit(e, context.Background(), request)
			coordinatorWait(t, gate.entered)
			if lookup := coordinatorLookup(t, e, request); lookup.Code != api.CodeInFlight || lookup.Result != (api.CommitResult{}) {
				t.Fatalf("candidate escaped its admission flight: %+v", lookup)
			}
			gate.unblock()
			unknown := coordinatorWait(t, pending)
			if unknown.result.Code != api.CodeCommitUnknown || !errors.Is(unknown.err, storage.ErrCommitUnknown) || !errors.Is(unknown.err, gate.err) {
				t.Fatalf("Agent publication failure was not unknown: %+v %v", unknown.result, unknown.err)
			}
			if unknown.result.Fence != (model.CommitFence{}) || unknown.result.RootSet != "" || unknown.result.StateRoot != "" || unknown.result.ResultDigest != "" {
				t.Fatalf("unknown Agent result disclosed candidate proof: %+v", unknown.result)
			}
			e.mu.Lock()
			rootsAfter, rootSetAfter, objectsAfter := e.roots, e.rootSet, e.index.count()
			e.mu.Unlock()
			if rootsAfter != before.Roots || rootSetAfter != before.RootSet || objectsAfter != objectsBefore {
				t.Fatalf("unknown publication installed private control candidate: roots=%+v rootset=%s objects=%d", rootsAfter, rootSetAfter, objectsAfter)
			}
			if got, err := e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "coordinator-space", Principal: "host"}); !errors.Is(err, storage.ErrStoreFenced) || !reflect.DeepEqual(got, api.AgentStateResult{}) {
				t.Fatalf("fenced Agent read returned old/candidate content: %+v %v", got, err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			options.Journal.TailPolicy = storage.TailQuarantine
			reopened, report, err := OpenExisting(io, options)
			if err != nil {
				t.Fatalf("unknown Agent recovery failed: report=%+v err=%v", report, err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			wantLSN := baseACK.Fence.LSN
			wantExecutions := 0
			if after {
				wantLSN++
				wantExecutions = 1
			}
			state, err := reopened.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "coordinator-space", Principal: "host"})
			if err != nil || !state.Found || state.Fence.LSN != wantLSN || len(state.Snapshot.Executions) != wantExecutions || len(state.Snapshot.Tasks) != 1 || state.Snapshot.Tasks[0] != task {
				t.Fatalf("recovery selected a different control prefix: state=%+v report=%+v err=%v", state, report, err)
			}
			if got := coordinatorLookup(t, reopened, baseReq); got.Code != api.CodeDurableAck || got.Result != baseACK {
				t.Fatalf("recovery changed base receipt: %+v", got)
			}
			lookup := coordinatorLookup(t, reopened, request)
			if after {
				if lookup.Code != api.CodeDurableAck || lookup.Result.Fence != state.Fence || lookup.Result.RootSet != state.RootSet || lookup.Result.StateRoot != state.LiveControlRoot {
					t.Fatalf("published unknown lost its exact control receipt: %+v state=%+v", lookup, state)
				}
				if retried := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(reopened, context.Background(), request))); retried != lookup.Result {
					t.Fatalf("unknown retry changed original receipt: %+v", retried)
				}
			} else if lookup.Code != api.CodeNotFoundAtFence || lookup.ObservedFence != baseACK.Fence || lookup.Result != (api.CommitResult{}) {
				t.Fatalf("unpublished control transition was promoted: %+v", lookup)
			}
		})
	}
}

func TestAgentWaiterCancellationDoesNotCancelAdmittedTransition(t *testing.T) {
	e, io := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	task := profileCoordinatorTask("cancel-wait-task")
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), profileCoordinatorRequest(t, "cancel-wait-base", agentprofile.Command{CreateTask: &task}))))
	execution := agentprofile.Execution{ID: "cancel-wait-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	request := profileCoordinatorRequest(t, "cancel-wait-spawn", agentprofile.Command{Spawn: &execution})
	gate := newCoordinatorGate()
	io.arm("current", gate)
	ctx, cancel := context.WithCancel(context.Background())
	pending := coordinatorCommit(e, ctx, request)
	coordinatorWait(t, gate.entered)
	cancel()
	coordinatorInterrupted(t, coordinatorWait(t, pending), context.Canceled)
	if lookup := coordinatorLookup(t, e, request); lookup.Code != api.CodeInFlight {
		t.Fatalf("canceled waiter erased owned command: %+v", lookup)
	}
	retry := coordinatorCommit(e, context.Background(), request)
	coordinatorFlightWaiters(t, e, request, 1)
	gate.unblock()
	ack := coordinatorAck(t, coordinatorWait(t, retry))
	state, err := e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil || len(state.Snapshot.Executions) != 1 || state.Snapshot.Executions[0].ID != execution.ID || state.Fence != ack.Fence || ack.Fence.LSN != 2 {
		t.Fatalf("admitted transition duplicated/lost after wait cancellation: state=%+v ack=%+v err=%v", state, ack, err)
	}
}

func TestAgentAndDomainCandidatesUseOneGroupPublication(t *testing.T) {
	e, io := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 2, MaxDelay: time.Second}})
	host := coordinatorRequest(t, "mixed-host", "host-key", "host-value", api.ValueCondition{})
	task := profileCoordinatorTask("mixed-task")
	agent := profileCoordinatorRequest(t, "mixed-agent", agentprofile.Command{CreateTask: &task})
	gate := newCoordinatorGate()
	io.arm("current", gate)
	_, _, currentsBefore, _ := io.counts()
	hostWait, agentWait, hostFlight, agentFlight := speculativePair(t, e, host, agent)
	coordinatorWait(t, gate.entered)
	speculativePending(t, hostFlight)
	speculativePending(t, agentFlight)
	gate.unblock()
	hostACK := coordinatorAck(t, coordinatorWait(t, hostWait))
	agentACK := coordinatorAck(t, coordinatorWait(t, agentWait))
	_, _, currentsAfter, _ := io.counts()
	if currentsAfter-currentsBefore != 1 || hostACK.Fence.LSN != 1 || agentACK.Fence.LSN != 2 {
		t.Fatalf("domain/profile candidates used separate publication owners: CURRENT delta=%d host=%+v agent=%+v", currentsAfter-currentsBefore, hostACK, agentACK)
	}
	current, err := e.Snapshot(context.Background())
	if err != nil || current.Roots.DomainStates == "" || current.Roots.LiveControl == "" || current.Roots.CommandReceipts == "" || current.RootSet != agentACK.RootSet {
		t.Fatalf("mixed group lost a root authority: %+v %v", current, err)
	}
	if lookup := coordinatorLookup(t, e, host); lookup.Code != api.CodeDurableAck || lookup.Result != hostACK || lookup.ObservedFence != agentACK.Fence {
		t.Fatalf("group member proof was replaced by batch-tip proof: %+v", lookup)
	}
}

func TestAgentCandidateRejectionWaitsForItsPredecessorPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("publication-failure=%t", fail), func(t *testing.T) {
			e, io := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 2, MaxDelay: time.Second}})
			task := profileCoordinatorTask("candidate-reject")
			firstRequest := profileCoordinatorRequest(t, "candidate-first", agentprofile.Command{CreateTask: &task})
			secondRequest := profileCoordinatorRequest(t, "candidate-duplicate", agentprofile.Command{CreateTask: &task})
			gate := newCoordinatorGate()
			if fail {
				gate.err = errors.New("test: candidate control predecessor was not published")
			}
			io.arm("current", gate)
			first, second, firstFlight, secondFlight := speculativePair(t, e, firstRequest, secondRequest)
			coordinatorWait(t, gate.entered)
			speculativePending(t, firstFlight)
			speculativePending(t, secondFlight)
			gate.unblock()
			one, two := coordinatorWait(t, first), coordinatorWait(t, second)
			if fail {
				if one.result.Code != api.CodeCommitUnknown || two.result.Code != api.CodeStoreFenced || !errors.Is(two.err, storage.ErrStoreFenced) {
					t.Fatalf("unpublished candidate resolved a dependent decision: first=%+v %v second=%+v %v", one.result, one.err, two.result, two.err)
				}
				return
			}
			coordinatorAck(t, one)
			if two.result.Code != api.CodeRejected || two.err == nil || !errors.Is(two.err, agentprofile.ErrConflict) {
				t.Fatalf("semantic rejection lost its dependent outcome: %+v %v", two.result, two.err)
			}
			if got := coordinatorLookup(t, e, secondRequest); got.Code != api.CodeNotFoundAtFence || got.ObservedFence != one.result.Fence {
				t.Fatalf("rejected command retained a durable receipt: %+v", got)
			}
		})
	}
}

func TestAgentResourceRejectionPreservesPublishedStateAndReceipt(t *testing.T) {
	e, _ := coordinatorStore(t, Options{AgentProfile: true, Journal: storage.JournalOptions{MaxPublishedTransactions: 1}})
	task := profileCoordinatorTask("quota-task")
	firstRequest := profileCoordinatorRequest(t, "quota-first", agentprofile.Command{CreateTask: &task})
	firstACK := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), firstRequest)))
	before, err := e.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	execution := agentprofile.Execution{ID: "quota-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	secondRequest := profileCoordinatorRequest(t, "quota-second", agentprofile.Command{Spawn: &execution})
	rejected := coordinatorWait(t, coordinatorCommit(e, context.Background(), secondRequest))
	if rejected.result.Code != api.CodeResourceLimit || !errors.Is(rejected.err, storage.ErrLimit) {
		t.Fatalf("quota failure was not a resource rejection: %+v %v", rejected.result, rejected.err)
	}
	if after, err := e.Snapshot(context.Background()); err != nil || after != before {
		t.Fatalf("quota rejection fenced or changed publication: %+v %v", after, err)
	}
	state, err := e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil || !state.Found || len(state.Snapshot.Tasks) != 1 || len(state.Snapshot.Executions) != 0 {
		t.Fatalf("quota rejection exposed candidate execution: %+v %v", state, err)
	}
	if retry := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), firstRequest))); retry != firstACK {
		t.Fatalf("quota prevented reconciliation of original receipt: %+v", retry)
	}
}
