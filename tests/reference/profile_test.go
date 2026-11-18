package reference_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

type harness struct {
	t         *testing.T
	expected  *oracle.Profile
	actual    *agentprofile.Ledger
	sequence  int
	decisions map[string]string
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, expected: oracle.NewProfile(), actual: agentprofile.New(), decisions: map[string]string{}}
}

func agentStateBytes(t testing.TB, snapshot agentprofile.Snapshot) []byte {
	t.Helper()
	image, err := agentprofile.EncodeRecordImage(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	records, err := image.Records()
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	for _, record := range records {
		raw = append(raw, record.Key...)
		raw = append(raw, record.Raw...)
	}
	return raw
}

func restoreAgentState(t testing.TB, ledger *agentprofile.Ledger) *agentprofile.Ledger {
	t.Helper()
	image, err := agentprofile.EncodeRecordImage(ledger.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := image.Restore()
	if err != nil {
		t.Fatal(err)
	}
	return restored
}

func digest(c string) model.ObjectID { return model.ObjectID(strings.Repeat(c, 64)) }
func key(id string) model.RequestKey {
	return model.RequestKey{Space: "space", Principal: "owner", RequestID: model.RequestID(id)}
}
func binding(a oracle.Authority) agentprofile.AuthorityBinding {
	return agentprofile.AuthorityBinding{Deployment: "fixture", OwnerEpoch: a.OwnerEpoch, Incarnation: a.Incarnation, Holder: a.Holder}
}
func (h *harness) control(c agentprofile.ControlCommand) agentprofile.Command {
	return agentprofile.Command{Control: &c}
}
func (h *harness) raw(c agentprofile.Command) {
	h.t.Helper()
	h.sequence++
	if _, err := h.actual.ApplyCommand(key(fmt.Sprintf("fixture-%d", h.sequence)), c); err != nil {
		h.t.Fatal(err)
	}
}
func (h *harness) apply(name string, expected func(*oracle.Profile) error, actual agentprofile.Command) {
	h.t.Helper()
	before := agentStateBytes(h.t, h.actual.Snapshot())
	wantErr := h.expected.Batch(expected)
	h.sequence++
	_, gotErr := h.actual.ApplyCommand(key(fmt.Sprintf("%s-%d", name, h.sequence)), actual)
	if (wantErr == nil) != (gotErr == nil) {
		h.t.Fatalf("%s acceptance mismatch: oracle=%v actual=%v", name, wantErr, gotErr)
	}
	if gotErr != nil {
		after := agentStateBytes(h.t, h.actual.Snapshot())
		if !bytes.Equal(before, after) {
			h.t.Fatalf("%s rejected transition mutated state", name)
		}
	}
	h.compare(name)
	// Restore uses the production codec/reconstruction path, but never changes
	// the independent map expectation, catching lost or invented projections.
	if h.sequence%3 == 0 {
		h.actual = restoreAgentState(h.t, h.actual)
		h.compare(name + " restore")
	}
}
func (h *harness) compare(name string) {
	h.t.Helper()
	snapshot := h.actual.Snapshot()
	tasks := map[string]oracle.Task{}
	executions := map[string]oracle.Execution{}
	authorities := map[string]oracle.Authority{}
	joins := map[string]oracle.Join{}
	for _, t := range snapshot.Tasks {
		tasks[string(t.ID)] = oracle.Task{Epoch: t.GoalEpoch, Status: string(t.Status)}
	}
	for _, e := range snapshot.Executions {
		executions[string(e.ID)] = oracle.Execution{Task: string(e.Task), Parent: string(e.Parent), Status: string(e.Status), Incarnation: e.Incarnation, Goal: e.AcceptedGoalEpoch}
	}
	if r := snapshot.Lifecycle.Recovery; r != nil {
		for _, a := range r.Authorities {
			authorities[string(a.Execution)] = oracle.Authority{OwnerEpoch: a.Binding.OwnerEpoch, Incarnation: a.Binding.Incarnation, Version: a.Version, Holder: a.Binding.Holder, Status: a.Status}
		}
		for _, j := range r.Joins {
			var members, winners []string
			for _, id := range j.Members {
				members = append(members, string(id))
			}
			for _, id := range j.Winners {
				winners = append(winners, string(id))
			}
			joins[j.ID] = oracle.Join{Parent: string(j.Parent), Mode: j.Mode, Status: j.Status, Members: members, Winners: winners, Quorum: j.Quorum, Version: j.Version}
		}
	}
	if !reflect.DeepEqual(tasks, h.expected.Tasks) || !reflect.DeepEqual(executions, h.expected.Executions) || !reflect.DeepEqual(authorities, h.expected.Authorities) || !reflect.DeepEqual(joins, h.expected.Joins) {
		h.t.Fatalf("%s state mismatch\n tasks=%+v expected=%+v\n executions=%+v expected=%+v\n authorities=%+v expected=%+v\n joins=%+v expected=%+v", name, tasks, h.expected.Tasks, executions, h.expected.Executions, authorities, h.expected.Authorities, joins, h.expected.Joins)
	}
}
func (h *harness) root() {
	task := agentprofile.Task{ID: "root-task", Space: "space", Owner: "owner", Goal: "root", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	h.apply("task", func(p *oracle.Profile) error { return p.CreateTask("root-task") }, agentprofile.Command{CreateTask: &task})
	e := agentprofile.Execution{ID: "root", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	h.apply("root", func(p *oracle.Profile) error { return p.Spawn("root", "root-task", "") }, agentprofile.Command{Spawn: &e})
	h.grant("root")
}
func (h *harness) grant(id string) {
	a := agentprofile.ExecutionAuthority{Execution: model.ExecutionID(id), Binding: agentprofile.AuthorityBinding{Deployment: "fixture", OwnerEpoch: 1, Incarnation: 1, Holder: id}, Status: "active", Evidence: digest("a")}
	h.apply("grant-"+id, func(p *oracle.Profile) error { return p.Grant(id, id) }, h.control(agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: a}}))
}
func (h *harness) step(id string, seq uint64) {
	a := h.expected.Authorities[id]
	x := h.expected.Executions[id]
	s := agentprofile.Step{Execution: model.ExecutionID(id), Sequence: seq, GoalEpoch: h.expected.Tasks[x.Task].Epoch, Status: agentprofile.StepOpen}
	h.apply("step-"+id, func(p *oracle.Profile) error { return p.BeginStep(id, a) }, h.control(agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: s, Authority: binding(a)}}))
}
func (h *harness) primaryResult(execution string) string {
	h.t.Helper()
	if id := h.decisions[execution]; id != "" {
		return id
	}
	auth := binding(h.expected.Authorities[execution])
	resultID := "model-result-" + execution
	spec := agentprofile.ActivitySpec{Activity: "primary-" + execution, Execution: model.ExecutionID(execution), Step: 1, Role: "primary_decision", GoalEpoch: 1, InputFence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digest("f"))}, Request: digest("a"), RequestDigest: digest("b"), ContextPacket: digest("c"), Recipient: "model", Effect: "pure", ResourceScope: "session"}
	h.raw(h.control(agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: auth}}))
	attempt := "primary-attempt-" + execution
	h.raw(h.control(agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: attempt, Sequence: 1, Authority: auth}}))
	h.raw(h.control(agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: attempt, Authority: auth, Evidence: digest("d")}}))
	result := agentprofile.ActivityResult{Capture: "full", ID: resultID, Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest, Incarnation: auth.Incarnation, Outcome: "success", Payload: []byte("delegate child")}
	h.raw(h.control(agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}}))
	h.raw(h.control(agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: resultID, ExpectedVersion: 1, Authority: auth, Evidence: digest("e")}}))
	h.decisions[execution] = resultID
	return resultID
}
func (h *harness) child(parent, id string) {
	a := h.expected.Authorities[parent]
	inv := agentprofile.Invocation{ID: "spawn-" + id, Execution: model.ExecutionID(parent), Step: 1, ProviderCallKey: id, Tool: "delegate", Status: agentprofile.InvocationPending, Attempt: 1, AcceptedResult: h.primaryResult(parent), ToolDefinition: digest("d"), ToolVersion: "delegate/v1", DispatchClass: "pure"}
	h.raw(h.control(agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: inv, Authority: binding(a)}}))
	task := agentprofile.Task{ID: model.EntityID("task-" + id), Space: "space", Owner: "owner", Goal: id, GoalEpoch: 1, Status: agentprofile.TaskOpen}
	e := agentprofile.Execution{ID: model.ExecutionID(id), Task: task.ID, Parent: model.ExecutionID(parent), Actor: model.ActorID(id), Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	h.apply("child-"+id, func(p *oracle.Profile) error {
		if !p.CanRun(parent, a, true) {
			return oracle.ErrRejected
		}
		if err := p.CreateTask(string(task.ID)); err != nil {
			return err
		}
		return p.Spawn(id, string(task.ID), parent)
	}, h.control(agentprofile.ControlCommand{SpawnChild: &agentprofile.SpawnChildCommand{Parent: model.ExecutionID(parent), Invocation: inv.ID, Task: task, Execution: e, InheritedEpoch: 1, Compatibility: digest("a"), Authority: binding(a)}}))
	h.grant(id)
}
func (h *harness) complete(id string) {
	x, a := h.expected.Executions[id], h.expected.Authorities[id]
	h.apply("complete-"+id, func(p *oracle.Profile) error { return p.Complete(id, a) }, h.control(agentprofile.ControlCommand{CompleteTask: &agentprofile.CompleteTaskCommand{Task: model.EntityID(x.Task), Execution: model.ExecutionID(id), Epoch: h.expected.Tasks[x.Task].Epoch, Result: digest("b"), Validation: digest("c"), Authority: binding(a)}}))
}

func TestIndependentProfileGoalAndAuthorityTransitions(t *testing.T) {
	h := newHarness(t)
	h.root()
	h.apply("rebase", func(p *oracle.Profile) error { return p.Rebase("root-task", 1) }, agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: "root-task", ExpectedEpoch: 1, NewGoal: "revised"}})
	h.step("root", 1) // stale accepted goal blocks the operation.
	a := h.expected.Authorities["root"]
	h.apply("observe", func(p *oracle.Profile) error { return p.Observe("root", 2, a) }, h.control(agentprofile.ControlCommand{ObserveGoal: &agentprofile.ObserveGoalCommand{Execution: "root", Epoch: 2, Observation: digest("d"), Evidence: digest("a"), Authority: binding(a)}}))
	h.step("root", 1)
	h.apply("wrong-fence", func(p *oracle.Profile) error { return p.Fence("root", 9) }, h.control(agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "root", ExpectedVersion: 9, Evidence: digest("a")}}))
	h.apply("fence", func(p *oracle.Profile) error { return p.Fence("root", 1) }, h.control(agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "root", ExpectedVersion: 1, Evidence: digest("a")}}))
	h.step("root", 2)
	next := oracle.Authority{OwnerEpoch: 2, Incarnation: 2, Holder: "replacement", Status: "active"}
	h.apply("resume", func(p *oracle.Profile) error { return p.Resume("root", 2, next) }, h.control(agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: "root", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: binding(next), AdapterVersion: "fixture/v1", Checkpoint: digest("b"), CompatibilityEvidence: digest("c"), ResourceFenceEvidence: digest("d")}}))
}

func TestIndependentProfileFenceAndCancelTrees(t *testing.T) {
	h := newHarness(t)
	h.root()
	h.step("root", 1)
	h.child("root", "left")
	h.child("root", "right")
	h.step("left", 1)
	h.child("left", "grandchild")
	h.apply("fence-tree", func(p *oracle.Profile) error { return p.Fence("root", 1) }, h.control(agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "root", ExpectedVersion: 1, Evidence: digest("a")}}))
	h.apply("already-fenced-child", func(p *oracle.Profile) error { return p.Fence("grandchild", 1) }, h.control(agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "grandchild", ExpectedVersion: 1, Evidence: digest("a")}}))
	h.step("grandchild", 1)
	next := oracle.Authority{OwnerEpoch: 2, Incarnation: 2, Holder: "root-next", Status: "active"}
	h.apply("resume-parent-only", func(p *oracle.Profile) error { return p.Resume("root", 2, next) }, h.control(agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: "root", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: binding(next), AdapterVersion: "fixture/v1", Checkpoint: digest("b"), CompatibilityEvidence: digest("c"), ResourceFenceEvidence: digest("d")}}))
	h.step("grandchild", 1)
	h.apply("cancel-wrong-incarnation", func(p *oracle.Profile) error { return p.CancelExecution("root", 3) }, h.control(agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "root", ExpectedIncarnation: 3}}))
	h.apply("cancel-tree", func(p *oracle.Profile) error { return p.CancelExecution("root", 2) }, h.control(agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "root", ExpectedIncarnation: 2}}))
}

func TestIndependentProfileJoinModesTimeoutAndStableWinners(t *testing.T) {
	for _, mode := range []string{"all", "any", "quorum"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t)
			h.root()
			h.step("root", 1)
			for _, id := range []string{"a", "b", "c"} {
				h.child("root", id)
			}
			members := []string{"a", "b", "c"}
			a := h.expected.Authorities["root"]
			q := uint64(0)
			if mode == "quorum" {
				q = 2
			}
			join := func(id string, version uint64, timeout bool) {
				h.apply(id, func(p *oracle.Profile) error { return p.Join(id, "root", mode, members, q, version, timeout, a) }, h.control(agentprofile.ControlCommand{JoinExecutions: &agentprofile.JoinExecutionsCommand{Join: agentprofile.ExecutionJoin{ID: id, Parent: "root", Mode: mode, Members: []model.ExecutionID{"a", "b", "c"}, Quorum: q}, ExpectedVersion: version, TimedOut: timeout, Authority: binding(a)}}))
			}
			join("timed-out", 0, true)
			join("join", 0, false)
			h.complete("b")
			join("join", 1, false)
			h.complete("a")
			join("join", 2, false)
			h.complete("c")
			join("join", h.expected.Joins["join"].Version, false)
			// A terminal timeout remains terminal after children finish, as does an
			// already selected any/quorum winner set after later completions.
			join("timed-out", 1, false)
		})
	}
}
