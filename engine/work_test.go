package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func workControl(t *testing.T, c agentprofile.ControlCommand) api.WorkStep {
	t.Helper()
	s, err := api.AgentWorkStep(agentprofile.Command{Control: &c})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func workCheckpoint(t *testing.T, e model.ExecutionID, a agentprofile.AuthorityBinding, f model.CommitFence, key string, version uint64, position ...any) api.WorkStep {
	t.Helper()
	body, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte(key), integrationBytes('r'))}})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: key, Owner: e, Generation: 1, Status: agentprofile.ActionPending, Request: agentKey("next-" + key), InputFence: f, CommandDigest: model.ObjectID(hex.EncodeToString(sum[:]))}, Method: api.MethodMutateState, ExactCommand: body, Authority: a}
	step := uint64(0)
	if len(position) > 0 {
		_ = position[0]
	}
	if len(position) > 1 {
		if v, ok := position[1].(uint64); ok {
			step = v
		}
	}
	continuation := agentprofile.Continuation{Execution: e, Version: version + 1, Incarnation: a.Incarnation, PendingKey: key, PendingGeneration: 1, InputFence: f, Phase: "pending-render", Position: &agentprofile.ContinuationPosition{Space: "host-space", Principal: "host-principal", Step: step, AdapterVersion: "work/v1", NextActions: []agentprofile.PendingActionRef{{Key: key, Generation: 1}}}}
	return workControl(t, agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: continuation, ExpectedVersion: version, Authority: a}})
}
func workApply(t *testing.T, s *integrationStore, r api.ApplyWorkRequest) api.CommitResult {
	t.Helper()
	out, err := s.e.ApplyWork(context.Background(), r)
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("work %+v: %+v %v", r.Unit.Kind, out, err)
	}
	return out
}
func workIntake(t *testing.T, s *integrationStore) (api.ApplyWorkRequest, api.ApplyWorkRequest) {
	t.Helper()
	a := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	task := agentTask("intake-task")
	x := agentprofile.Execution{ID: "intake-root", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	input := []byte("solve the long task exactly")
	goal, err := agentprofile.GoalKnowledge(task)
	if err != nil {
		t.Fatal(err)
	}
	published := integrationMutate(t, s.e, integrationRequest("initial-source-images", integrationPut([]byte("initial-query-input"), api.Value{Kind: api.ValueBytes, Data: input}), integrationPut([]byte("initial-goal-image"), api.Value{Kind: api.ValueBytes, Data: goal})))
	f := published.Fence
	queryPayload, _ := recoveryValueAt(t, s, f, "initial-query-input")
	goalPayload, _ := recoveryValueAt(t, s, f, "initial-goal-image")
	key := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	querySeed := workSeedObservation(t, key, queryPayload, f, 1, "initial Query input")
	goalSeed := workSeedObservation(t, key, goalPayload, f, 2, "initial Goal")
	start := api.ApplyWorkRequest{Key: agentKey("exact-start"), Unit: api.WorkUnit{Kind: "query_start", Query: "query", Steps: []api.WorkStep{
		workControl(t, agentprofile.ControlCommand{StartWork: &agentprofile.StartWorkCommand{Query: "query", Slot: "root", ExpectedQueryVersion: 1, Task: task, Execution: x}}),
		workControl(t, agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Status: "active", Binding: a, Evidence: model.ObjectID(integrationID('a'))}}}),
		querySeed, goalSeed, workCheckpoint(t, x.ID, a, f, "first-root-action", 0),
	}}}
	b, err := api.EncodeWorkUnit(start.Unit)
	if err != nil {
		t.Fatal(err)
	}
	query := agentprofile.Query{ID: "query", Session: "session", Space: "host-space", Owner: "host-principal", Input: input, InputFence: f, Status: "open", Version: 1}
	intake := api.ApplyWorkRequest{Key: agentKey("intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: "query", Intake: &api.QueryIntake{Query: query, Route: "root-driver/v1", Configuration: []byte(`{"model":"deterministic","resource":"workspace-v1"}`), Next: api.CommitRequest{Space: start.Key.Space, Principal: start.Key.Principal, RequestID: start.Key.RequestID, Method: api.MethodApplyWork, Body: b}}}}
	return intake, start
}

func TestWorkQueryIntakeRecoversBeforeTaskAndEnforcesExactUniqueStart(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	intake, start := workIntake(t, s)
	first := workApply(t, s, intake)
	read := api.ReadQueryIntakeRequest{Space: "host-space", Principal: "host-principal", Query: "query"}
	before, err := s.e.GetQueryIntake(ctx, read)
	if err != nil || !before.Found || before.State.Started || !reflect.DeepEqual(before.State.Intake, *intake.Unit.Intake) {
		t.Fatalf("intake %+v %v", before, err)
	}
	control, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: read.Space, Principal: read.Principal})
	if err != nil || len(control.Snapshot.Tasks) != 0 || len(control.Snapshot.Executions) != 0 || len(control.Snapshot.Lifecycle.Queries) != 1 {
		t.Fatalf("task created before start: %+v %v", control, err)
	}
	s.reopen()
	before2, err := s.e.GetQueryIntake(ctx, read)
	if err != nil || !reflect.DeepEqual(before, before2) {
		t.Fatalf("lost pending intake: %+v %v", before2, err)
	}
	// The reservation applies even to another method, before its reducer runs.
	bad, err := s.e.Commit(ctx, api.CommitRequest{Space: start.Key.Space, Principal: start.Key.Principal, RequestID: start.Key.RequestID, Method: api.MethodMutateState, Body: []byte{1}})
	if err == nil || bad.Code != api.CodeRejected {
		t.Fatalf("reserved start hijacked: %+v %v", bad, err)
	}
	wrong := start
	wrong.Key.RequestID = "replacement-key"
	if out, err := s.e.ApplyWork(ctx, wrong); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected {
		t.Fatalf("new key start %+v %v", out, err)
	}
	ack := workApply(t, s, start)
	if ack.Fence.LSN != first.Fence.LSN+1 {
		t.Fatal("private steps published independent transactions")
	}
	got, err := s.e.GetQueryIntake(ctx, read)
	if err != nil || !got.State.Started {
		t.Fatalf("start missing %+v %v", got, err)
	}
	old := read
	old.Anchor = &before.Anchor
	past, err := s.e.GetQueryIntake(ctx, old)
	if err != nil || past.State.Started {
		t.Fatal("historical intake changed")
	}
	if _, err = s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	if retry := workApply(t, s, start); retry != ack {
		t.Fatal("start original receipt changed")
	}
	if retry := workApply(t, s, intake); retry != first {
		t.Fatal("intake original receipt changed")
	}
	wrong.Key.RequestID = "another-key-after-start"
	if out, err := s.e.ApplyWork(ctx, wrong); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected {
		t.Fatalf("duplicated root %+v %v", out, err)
	}
	control, err = s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: read.Space, Principal: read.Principal})
	if err != nil || len(control.Snapshot.Tasks) != 1 || len(control.Snapshot.Executions) != 1 || len(control.Snapshot.Lifecycle.Continuations) != 1 {
		t.Fatalf("wrong root set %+v %v", control, err)
	}
}

func TestWorkFailureRollsBackEveryPrivateStep(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	intake, start := workIntake(t, s)
	// Make the saved request itself contain a failing last step. It remains a
	// legitimate durable intent, but its earlier StartWork/Grant/Checkpoint must
	// never become visible when that final local check rejects.
	start.Unit.Steps = append(start.Unit.Steps, workControl(t, agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "intake-root", ExpectedIncarnation: 99}}))
	b, err := api.EncodeWorkUnit(start.Unit)
	if err != nil {
		t.Fatal(err)
	}
	intake.Unit.Intake.Next.Body = b
	workApply(t, s, intake)
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyWork(context.Background(), start)
	if !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("partial publication: %+v %v", out, err)
	}
	s.reopen()
	control, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(control.Snapshot.Tasks) != 0 || len(control.Snapshot.Executions) != 0 {
		t.Fatalf("partial recovered: %+v %v", control, err)
	}
}

type messageWorkFixture struct {
	store           *integrationStore
	request         api.ApplyWorkRequest
	prior, observed knowledge.Observation
	priorAck        api.CommitResult
}

func newMessageWorkFixture(t *testing.T) messageWorkFixture {
	t.Helper()
	s, payload, source := knowledgeFixture(t)
	a := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	for _, x := range []model.ExecutionID{"one", "two"} {
		recoveryCoreControl(t, s, string(x)+"-grant-work", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x, Status: "active", Binding: a, Evidence: model.ObjectID(integrationID('a'))}}})
	}
	prior := knowledgeObservation(payload, source, "one", 1)
	_, priorAck := applyObservation(t, s.e, "before-message-knowledge", prior)
	message := agentprofile.Message{ID: "child-result", Sender: "two", Recipient: "one", GoalEpoch: 1, Payload: integrationBytes('a').Data}
	recoveryCoreControl(t, s, "send-work-message", agentprofile.ControlCommand{SendMessage: &agentprofile.SendMessageCommand{Message: message, Authority: a}})
	o := knowledgeObservation(payload, source, "one", 2)
	observe, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &o})
	if err != nil {
		t.Fatal(err)
	}
	consume := workControl(t, agentprofile.ControlCommand{ConsumeMessage: &agentprofile.ConsumeMessageCommand{Consumption: agentprofile.MessageConsumption{Message: message.ID, Recipient: "one", Incarnation: 1, GoalEpoch: 1}, Authority: a}})
	checkpoint := workCheckpoint(t, "one", a, source, "after-message", 0, model.ActorID("same-actor"))
	unit := api.WorkUnit{Kind: "message_consume", Steps: []api.WorkStep{consume, observe, checkpoint}}
	req := api.ApplyWorkRequest{Key: agentKey("consume-work"), Unit: unit}
	return messageWorkFixture{store: s, request: req, prior: prior, observed: o, priorAck: priorAck}
}

func TestWorkMessageConsumptionKnowledgeAndContinuationAreOnePublication(t *testing.T) {
	ctx := context.Background()
	fixture := newMessageWorkFixture(t)
	s, req, prior, o, priorAck := fixture.store, fixture.request, fixture.prior, fixture.observed, fixture.priorAck
	priorAnchor := model.SnapshotAnchor{Space: prior.Source.Anchor.Space, Fence: priorAck.Fence, Knowledge: &prior.Key}
	priorState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: req.Key.Space, Principal: req.Key.Principal, Anchor: &priorAnchor})
	if err != nil || priorState.Revision == nil {
		t.Fatalf("prior knowledge %+v %v", priorState, err)
	}
	unit := req.Unit
	consume, observe, checkpoint := unit.Steps[0], unit.Steps[1], unit.Steps[2]
	before := integrationSnapshot(t, s.e)
	staleCommand, err := agentprofile.DecodeCommand(checkpoint.Body)
	if err != nil {
		t.Fatal(err)
	}
	staleCommand.Control.Checkpoint.Continuation.Position.KnowledgeSource = &model.SourcePublicationRef{
		Store: priorAck.Fence.Store, Space: prior.Source.Anchor.Space, ProducerTx: priorAck.TxID, Knowledge: &prior.Key,
		Cursor: &model.Cursor{Store: priorAck.Fence.Store, Space: prior.Source.Anchor.Space, Revision: priorState.Revision.ID},
	}
	staleCheckpoint := workControl(t, *staleCommand.Control)
	priorObservation, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &prior})
	if err != nil {
		t.Fatal(err)
	}
	laterObservation := workSeedObservation(t, o.Key, o.Payload, o.Source.Anchor.Fence, 3, "later observation in same Work")
	for _, tc := range []struct {
		id    model.RequestID
		steps []api.WorkStep
	}{
		{"consume-stale-position", []api.WorkStep{consume, observe, staleCheckpoint}},
		{"consume-before-observation", []api.WorkStep{consume, checkpoint, observe}},
		{"consume-before-final-observation", []api.WorkStep{consume, observe, checkpoint, laterObservation}},
		{"consume-existing-observation", []api.WorkStep{consume, priorObservation, checkpoint}},
	} {
		bad := req
		bad.Key.RequestID = tc.id
		bad.Unit.Steps = tc.steps
		if out, err := s.e.ApplyWork(ctx, bad); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
			t.Fatalf("%s: %+v %v", tc.id, out, err)
		}
	}
	bad := req
	bad.Key.RequestID = "consume-bad"
	bad.Unit.Steps = append(append([]api.WorkStep(nil), unit.Steps...), workControl(t, agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "one", ExpectedIncarnation: 99}}))
	if out, err := s.e.ApplyWork(ctx, bad); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("bad work %+v %v", out, err)
	}
	ack := workApply(t, s, req)
	after := integrationSnapshot(t, s.e)
	if ack.Fence.LSN != before.Fence.LSN+1 || after.Roots.LiveControl == before.Roots.LiveControl || after.Roots.ExecutionKnowledge == before.Roots.ExecutionKnowledge {
		t.Fatal("roots not jointly published")
	}
	check := func() {
		control, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
		if err != nil || len(control.Snapshot.Lifecycle.Consumptions) != 1 || len(control.Snapshot.Lifecycle.Continuations) != 1 {
			t.Fatalf("lost consumption/continuation %+v %v", control, err)
		}
		assertWorkSeedCut(t, s, ack, o.Key.Execution, 2)
		known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &o.Key})
		if err != nil || known.Revision == nil || known.Revision.Observation != o.ID {
			t.Fatalf("lost knowledge %+v %v", known, err)
		}
	}
	check()
	s.reopen()
	check()
	if _, err = s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
	if retry := workApply(t, s, req); retry != ack {
		t.Fatal("work retry changed proof")
	}
}

func TestWorkMessageConsumptionRecoversWholePublicationAfterCurrentFault(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published bool
	}{{"before-current", false}, {"after-current", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newMessageWorkFixture(t)
			s, req := fixture.store, fixture.request
			before := integrationSnapshot(t, s.e)
			if err := s.e.Close(); err != nil {
				t.Fatal(err)
			}
			fault := &workCurrentFaultIO{DurableIO: s.d, after: tc.published}
			var err error
			s.e, _, err = engine.OpenExisting(fault, s.options)
			if err != nil {
				t.Fatal(err)
			}
			fault.armed = true
			if out, err := s.e.ApplyWork(ctx, req); out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkCurrent) {
				t.Fatalf("message Work should have unknown outcome: %+v %v", out, err)
			}
			s.options.Journal.TailPolicy = storage.TailQuarantine
			s.reopen()
			state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: req.Key.Space, Principal: req.Key.Principal})
			if err != nil {
				t.Fatal(err)
			}
			known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: req.Key.Space, Principal: req.Key.Principal, Knowledge: &fixture.observed.Key})
			if err != nil || known.Revision == nil {
				t.Fatalf("recovered Knowledge %+v %v", known, err)
			}
			lookup := api.LookupRequestRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID}
			receipt, err := s.e.LookupRequest(ctx, lookup)
			if err != nil {
				t.Fatal(err)
			}
			count, observation, wantCode := 0, fixture.prior.ID, api.CodeNotFoundAtFence
			if tc.published {
				count, observation, wantCode = 1, fixture.observed.ID, api.CodeDurableAck
			} else if recovered := integrationSnapshot(t, s.e); recovered != before {
				t.Fatalf("unpublished Work changed root or fence: %+v", recovered)
			}
			if len(state.Snapshot.Lifecycle.Consumptions) != count || len(state.Snapshot.Lifecycle.Continuations) != count || known.Revision.Observation != observation || receipt.Code != wantCode {
				t.Fatalf("mixed recovery: control=%+v knowledge=%+v receipt=%+v", state.Snapshot.Lifecycle, known.Revision, receipt)
			}
			ack := workApply(t, s, req)
			if tc.published && receipt.Result != ack {
				t.Fatalf("original receipt changed after retry: %+v -> %+v", receipt.Result, ack)
			}
			assertWorkSeedCut(t, s, ack, fixture.observed.Key.Execution, 2)
		})
	}
}

func workChildFixture(t *testing.T) (*integrationStore, api.ApplyWorkRequest, agentprofile.Execution, knowledge.Observation) {
	t.Helper()
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	a := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "spawn-work-grant", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Status: "active", Binding: a, Evidence: model.ObjectID(integrationID('a'))}}})
	o := knowledgeObservation(payload, source, "one", 1)
	_, adopt := applyObservation(t, s.e, "spawn-work-source", o)
	step := agentprofile.Step{Execution: "one", Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: adopt.Fence}
	recoveryCoreControl(t, s, "spawn-work-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: step, Authority: a}})
	inv := agentprofile.Invocation{ID: "spawn-call", Execution: "one", Step: 1, ProviderCallKey: "call1", Tool: "delegate", Attempt: 1, Status: agentprofile.InvocationPending, AcceptedResult: "model-result", ToolDefinition: model.ObjectID(integrationID('d')), ToolVersion: "delegate/v1", DispatchClass: "pure"}
	bindTestInvocation(t, s, &inv, a, "delegate")
	commitTestInvocation(t, s, "spawn-work-invocation", inv, a)
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &o.Key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	seedSource := knowledge.SourceAnchor{Anchor: anchor, Frontier: known.Frontier}
	childTask := agentTask("atomic-child-task")
	child := agentprofile.Execution{ID: "atomic-child", Task: childTask.ID, Actor: "child", Parent: "one", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	childAuthority := a
	childAuthority.Holder = "child"
	goal, err := agentprofile.GoalKnowledge(childTask)
	if err != nil {
		t.Fatal(err)
	}
	goalAck := integrationMutate(t, s.e, integrationRequest("publish-child-goal", integrationPut([]byte("child-goal"), api.Value{Kind: api.ValueBytes, Data: goal})))
	goalPayload, _ := recoveryValueAt(t, s, goalAck.Fence, "child-goal")
	goalSeed := workSeedObservation(t, model.KnowledgeKey{Actor: child.Actor, Execution: child.ID}, goalPayload, goalAck.Fence, 2, "initial child Goal")
	seed, err := api.KnowledgeWorkStep(knowledge.Command{Import: &knowledge.ImportCommand{Target: model.KnowledgeKey{Actor: child.Actor, Execution: child.ID}, Source: seedSource, Observations: []model.ObjectID{o.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	unit := api.WorkUnit{Kind: "child_spawn", Steps: []api.WorkStep{
		workControl(t, agentprofile.ControlCommand{SpawnChild: &agentprofile.SpawnChildCommand{Parent: "one", Invocation: inv.ID, Task: childTask, Execution: child, InheritedEpoch: 1, Compatibility: model.ObjectID(integrationID('a')), Authority: a}}),
		workControl(t, agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: child.ID, Binding: childAuthority, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}}),
		seed, goalSeed, workCheckpoint(t, child.ID, childAuthority, source, "child-first-action", 0, model.ActorID("same-actor")),
	}}
	req := api.ApplyWorkRequest{Key: agentKey("atomic-child"), Unit: unit}
	return s, req, child, o
}

func TestWorkChildSpawnSeedAuthorityAndInitialContinuationAreAtomic(t *testing.T) {
	ctx := context.Background()
	s, req, child, o := workChildFixture(t)
	unit := req.Unit
	before := integrationSnapshot(t, s.e)
	parent, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(parent.Snapshot.Invocations) != 1 {
		t.Fatalf("parent decision fixture: %+v %v", parent, err)
	}
	spawnCommand, err := agentprofile.DecodeCommand(unit.Steps[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	spawn := *spawnCommand.Control.SpawnChild
	invocation := parent.Snapshot.Invocations[0]
	definition := testPureToolDefinition(invocation.Tool, "test-tool/v1")
	definition.DispatchMode = "delegate"
	input, request := recoveryValueAt(t, s, before.Fence, "invocation-source-one-input")
	wrongMode := agentprofile.ActivitySpec{Activity: "delegate-cannot-dispatch", Execution: invocation.Execution, Step: invocation.Step, Invocation: invocation.ID, Role: "external_tool", GoalEpoch: 1, InputFence: before.Fence, Request: input, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: definition.Contract.Recipient, Effect: definition.DispatchClass, ResourceScope: "workspace", Contract: definition.BindEffect(invocation.ToolDefinition)}
	wrongModeCommand := agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: wrongMode, Authority: spawn.Authority}}}
	if out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("delegate-as-external"), Command: wrongModeCommand}); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("delegation also dispatched as an external tool: %+v %v", out, err)
	}
	undeclared := parent.Snapshot.Invocations[0]
	undeclared.ID, undeclared.ProviderCallKey = "undeclared-child", "undeclared-call"
	spawn.Invocation = undeclared.ID
	newInvocation := workControl(t, agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: undeclared, Authority: spawn.Authority}})
	undeclaredSpawn := req
	undeclaredSpawn.Key.RequestID = "undeclared-spawn"
	undeclaredSpawn.Unit.Steps = append([]api.WorkStep{newInvocation, workControl(t, agentprofile.ControlCommand{SpawnChild: &spawn})}, unit.Steps[1:]...)
	if out, err := s.e.ApplyWork(ctx, undeclaredSpawn); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("child spawn extended an already published decision: %+v %v", out, err)
	}
	bad := req
	bad.Key.RequestID = "bad-child"
	bad.Unit.Steps = append(append([]api.WorkStep(nil), unit.Steps...), workControl(t, agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: child.ID, ExpectedIncarnation: 88}}))
	if out, err := s.e.ApplyWork(ctx, bad); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("partial spawn %+v %v", out, err)
	}
	ack := workApply(t, s, req)
	if ack.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("spawn not one transaction")
	}
	check := func() {
		control, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
		if err != nil || len(control.Snapshot.Tasks) != 2 || len(control.Snapshot.Executions) != 3 || len(control.Snapshot.Lifecycle.Continuations) != 2 {
			t.Fatalf("child facts %+v %v", control, err)
		}
		key := model.KnowledgeKey{Actor: child.Actor, Execution: child.ID}
		k, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
		imported := false
		for _, observation := range k.Snapshot.Observations {
			imported = imported || observation.ImportedFrom == o.ID
		}
		if err != nil || len(k.Snapshot.Observations) != 2 || !imported {
			t.Fatalf("child seed %+v %v", k, err)
		}
		assertWorkSeedCut(t, s, ack, child.ID, 2)
	}
	check()
	s.reopen()
	check()
	if retry := workApply(t, s, req); retry != ack {
		t.Fatal("spawn ACK changed")
	}
	req.Key.RequestID = "same-spawn-new-key"
	if out, err := s.e.ApplyWork(ctx, req); !errors.Is(err, agentprofile.ErrUniqueSlot) || out.Code != api.CodeRejected {
		t.Fatalf("duplicate seed %+v %v", out, err)
	}
}

type workCurrentFaultIO struct {
	storage.DurableIO
	armed, after bool
}

var errWorkCurrent = errors.New("test: work CURRENT response lost")

func (d *workCurrentFaultIO) ReplaceAtomic(from, to string) error {
	if !d.armed || to != "CURRENT" {
		return d.DurableIO.ReplaceAtomic(from, to)
	}
	d.armed = false
	if d.after {
		if err := d.DurableIO.ReplaceAtomic(from, to); err != nil {
			return err
		}
	}
	return errWorkCurrent
}

func TestWorkPhysicalPublicationUnknownRecoversWholeQueryStart(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[after], func(t *testing.T) {
			s := newIntegrationStore(t, engine.Options{AgentProfile: true})
			intake, start := workIntake(t, s)
			workApply(t, s, intake)
			if err := s.e.Close(); err != nil {
				t.Fatal(err)
			}
			fault := &workCurrentFaultIO{DurableIO: s.d, after: after}
			var err error
			s.e, _, err = engine.OpenExisting(fault, s.options)
			if err != nil {
				t.Fatal(err)
			}
			fault.armed = true
			out, err := s.e.ApplyWork(context.Background(), start)
			if out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkCurrent) || out.RootSet != "" {
				t.Fatalf("unknown %+v %v", out, err)
			}
			s.options.Journal.TailPolicy = storage.TailQuarantine
			s.reopen()
			got, err := s.e.GetQueryIntake(context.Background(), api.ReadQueryIntakeRequest{Space: "host-space", Principal: "host-principal", Query: "query"})
			if err != nil || got.State.Started != after {
				t.Fatalf("intake recovered %+v %v", got, err)
			}
			control, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
			count := 0
			if after {
				count = 1
			}
			if err != nil || len(control.Snapshot.Tasks) != count || len(control.Snapshot.Executions) != count || len(control.Snapshot.Lifecycle.Continuations) != count {
				t.Fatalf("mixed state %+v %v", control, err)
			}
			known, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal"})
			if err != nil || len(known.Snapshot.Observations) != count*2 {
				t.Fatalf("partial initial knowledge: %+v %v", known, err)
			}
			ack := workApply(t, s, start)
			assertWorkSeedCut(t, s, ack, "intake-root", 2)
		})
	}
}
