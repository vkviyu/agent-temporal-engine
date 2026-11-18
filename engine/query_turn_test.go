package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestQueryOnlyOpenPersistsProgressWithoutTaskOrExecution(t *testing.T) {
	for _, tools := range []bool{false, true} {
		name := "direct-answer"
		if tools {
			name = "tool-answer"
		}
		t.Run(name, func(t *testing.T) { runQueryOnlyRecovery(t, tools) })
	}
}

func TestQueryDecisionAcceptancePublicationUnknownRecoversWholeTurn(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[after], func(t *testing.T) {
			runQueryOnlyRecovery(t, false, after)
		})
	}
}

func TestUnmanagedQueryCannotCreateExternalTool(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	definition := testPureToolDefinition("read", "executor")
	definitionID, _ := publishTestToolDefinition(t, s, "unmanaged-tool-definition", definition)
	request := []byte("read a file")
	published := integrationMutate(t, s.e, integrationRequest("unmanaged-tool-request", integrationPut([]byte("tool-request"), api.Value{Kind: api.ValueBytes, Data: request})))
	requestID, _ := recoveryValueAt(t, s, published.Fence, "tool-request")
	query := agentprofile.Query{ID: "unmanaged", Session: "session", Space: "host-space", Owner: "host-principal", Input: []byte("question"), InputFence: published.Fence, Status: "open", Version: 1}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("unmanaged-query"), Command: agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &query}}})
	spec := agentprofile.ActivitySpec{Activity: "unbound-query-tool", Query: query.ID, Role: "external_tool", GoalEpoch: 1, InputFence: published.Fence, Request: requestID, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: definition.Contract.Recipient, Effect: definition.DispatchClass, ResourceScope: "workspace", Contract: definition.BindEffect(definitionID)}
	create := agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}}
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("unbound-query-tool"), Command: agentprofile.Command{Control: &create}})
	if !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("unmanaged Query created an external tool activity: %+v %v", out, err)
	}
	task := agentTask("unmanaged-query-other-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("unmanaged-query-other-task"), Command: agentprofile.Command{CreateTask: &task}})
	execution := agentprofile.Execution{ID: "unmanaged-query-other-execution", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("unmanaged-query-other-execution"), Command: agentprofile.Command{Spawn: &execution}})
	authority := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	recoveryCoreControl(t, s, "unmanaged-query-other-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: execution.ID, Status: "active", Binding: authority, Evidence: definitionID}}})
	before = integrationSnapshot(t, s.e)
	checkpoint := workCheckpoint(t, execution.ID, authority, before.Fence, "unmanaged-query-checkpoint-next", 0, 0)
	unit := api.WorkUnit{Kind: "checkpoint", Steps: []api.WorkStep{workControl(t, create), checkpoint}}
	out, err = s.e.ApplyWork(ctx, api.ApplyWorkRequest{Key: agentKey("unbound-query-tool-work"), Unit: unit})
	if !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("generic Work created a Query external tool activity: %+v %v", out, err)
	}
	unit.Steps = []api.WorkStep{checkpoint}
	workApply(t, s, api.ApplyWorkRequest{Key: agentKey("standalone-checkpoint-without-query-tool"), Unit: unit})
}

func runQueryOnlyRecovery(t *testing.T, withTools bool, publicationFault ...bool) {
	if len(publicationFault) > 1 {
		t.Fatal("at most one publication fault configuration is supported")
	}
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	definition := testPureToolDefinition("read", "executor")
	definitionID, _ := publishTestToolDefinition(t, s, "query-read-definition", definition)
	inputAck := integrationMutate(t, s.e, integrationRequest("query-input", integrationPut([]byte("evidence"), api.Value{Kind: api.ValueBytes, Data: []byte("exact query request")})))
	source := inputAck.Fence
	inputID, input := recoveryValueAt(t, s, source, "evidence")
	packet := knowledge.ContextPacket{Space: "host-space", Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}}, Renderer: "query/v1", Tokenizer: "test/v1", Input: &knowledge.RecordedInput{Object: inputID, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Encoding: "query/v1"}}
	packet.ID = knowledge.PacketID(packet)
	packetAck, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("query-packet"), Command: knowledge.Command{Packet: &packet}})
	if err != nil || packetAck.Code != api.CodeDurableAck {
		t.Fatal(packetAck, err)
	}
	query := agentprofile.Query{ID: "short-query", Session: "session", Space: "host-space", Owner: "host-principal", Input: []byte("answer this"), InputFence: source, Status: "open", Version: 1}
	modelSpec := agentprofile.ActivitySpec{Activity: "query-model", Query: query.ID, Role: "query_decision", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: inputID, RequestDigest: agentprofile.ExactRequestDigest(input), Recipient: "model/v1", Effect: "pure", ResourceScope: "query"}
	step, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: modelSpec}}})
	if err != nil {
		t.Fatal(err)
	}
	claimBody, err := agentprofile.EncodeCommand(agentprofile.Command{Control: &agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: modelSpec.Activity, Attempt: "query-attempt", Sequence: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	claim := api.CommitRequest{Space: query.Space, Principal: query.Owner, RequestID: "query-claim", Method: api.MethodApplyAgent, Body: claimBody}
	open := api.ApplyWorkRequest{Key: agentKey("query-open"), Unit: api.WorkUnit{Kind: "query_open", Query: query.ID, QueryTurn: &api.QueryTurn{Activity: modelSpec.Activity, AdapterVersion: "query/v1", Phase: "model", Source: model.SnapshotAnchor{Space: query.Space, Fence: packetAck.Fence}, Next: &claim}, Steps: []api.WorkStep{step}}}
	body, err := api.EncodeWorkUnit(open.Unit)
	if err != nil {
		t.Fatal(err)
	}
	intake := api.ApplyWorkRequest{Key: agentKey("query-intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: query.ID, Intake: &api.QueryIntake{Query: query, Route: "query/v1", Configuration: []byte(`{"model":"fixture"}`), Next: api.CommitRequest{Space: open.Key.Space, Principal: open.Key.Principal, RequestID: open.Key.RequestID, Method: api.MethodApplyWork, Body: body}}}}
	workApply(t, s, intake)
	ack, err := s.e.ApplyWork(ctx, open)
	if err != nil {
		qstate, qerr := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: query.Space, Principal: query.Owner})
		t.Fatalf("open err=%v ack=%+v state=%+v stateErr=%v", err, ack, qstate, qerr)
	}
	read := api.ReadQueryProgressRequest{Space: query.Space, Principal: query.Owner, Query: query.ID}
	first, err := s.e.GetQueryProgress(ctx, read)
	if err != nil || !first.Found || first.Progress.Query != query.ID || first.Progress.Version != 1 || first.Progress.Next == nil || first.Progress.AdapterVersion != "query/v1" {
		t.Fatalf("progress=%+v err=%v", first, err)
	}
	if _, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: query.Space, Principal: query.Owner}); err != nil {
		t.Fatal(err)
	}
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: query.Space, Principal: query.Owner})
	if err != nil || len(state.Snapshot.Tasks) != 0 || len(state.Snapshot.Executions) != 0 || len(state.Snapshot.Lifecycle.Queries) != 1 {
		t.Fatalf("query-only created execution: %+v %v", state, err)
	}
	s.reopen()
	second, err := s.e.GetQueryProgress(ctx, read)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("progress changed after restart: %+v %v", second, err)
	}
	if retry := workApply(t, s, open); retry != ack {
		t.Fatal("query open receipt changed")
	}
	proof := model.ObjectID(integrationID('a'))
	commitExact := func(r api.CommitRequest) {
		t.Helper()
		out, err := s.e.Commit(ctx, r)
		if err != nil || out.Code != api.CodeDurableAck {
			t.Fatal(out, err)
		}
	}
	exact := func(id string, c agentprofile.ControlCommand) api.CommitRequest {
		t.Helper()
		raw, err := agentprofile.EncodeCommand(agentprofile.Command{Control: &c})
		if err != nil {
			t.Fatal(err)
		}
		return api.CommitRequest{Space: query.Space, Principal: query.Owner, RequestID: model.RequestID(id), Method: api.MethodApplyAgent, Body: raw}
	}
	wrong := claim
	wrong.Body = []byte("changed reserved request")
	if out, err := s.e.Commit(ctx, wrong); err == nil || out.Code != api.CodeRejected {
		t.Fatal("reserved action replaced", out, err)
	}
	commitExact(*second.Progress.Next)
	recoveryCoreControl(t, s, "query-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "query-attempt", Evidence: proof}})
	result := agentprofile.ActivityResult{Capture: "full", ID: "query-result", Activity: modelSpec.Activity, Attempt: "query-attempt", RequestDigest: modelSpec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("direct answer")}
	recoveryCoreControl(t, s, "query-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	for name, c := range map[string]agentprofile.ControlCommand{
		"raw-primary-accept": {AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: modelSpec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: proof}},
		"raw-query-answer":   {AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "bypass", Kind: "final", ExpectedVersion: 1, Body: []byte("bypass"), Final: true, GeneratedFrom: result.ID}},
	} {
		tip := integrationSnapshot(t, s.e)
		if out, err := s.e.Commit(ctx, exact(name, c)); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
			t.Fatal("managed Query bypassed Work", name, out, err)
		}
	}
	toolSpec := agentprofile.ActivitySpec{Activity: "query-tool", Query: query.ID, Role: "external_tool", GoalEpoch: 1, InputFence: source, Request: inputID, RequestDigest: agentprofile.ExactRequestDigest(input), Recipient: definition.Contract.Recipient, Effect: "pure", ResourceScope: "query", Contract: definition.BindEffect(definitionID)}
	createTool := exact("query-create-tool", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: toolSpec}})
	inv := api.QueryInvocation{ID: "read-file", Activity: toolSpec.Activity, ProviderCallKey: "call-1", Tool: "read", Definition: definitionID, ToolVersion: "read/v1", Arguments: []byte(`{"path":"README.md"}`), DispatchClass: "pure"}
	decision := api.ApplyWorkRequest{Key: agentKey("query-decision"), Unit: api.WorkUnit{Kind: "query_decision", Query: query.ID, Decision: &api.DecisionBinding{Activity: modelSpec.Activity, Result: result.ID, Invocations: []api.DecisionInvocation{{Invocation: inv.ID, Definition: inv.Definition, Version: inv.ToolVersion, DispatchClass: inv.DispatchClass}}}, QueryTurn: &api.QueryTurn{ExpectedVersion: 1, Sequence: 1, Activity: modelSpec.Activity, Invocations: []api.QueryInvocation{inv}, AdapterVersion: "query/v1", Phase: "create-tool", Source: packet.Source.Anchor, Next: &createTool}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: modelSpec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: proof}})}}}
	bad := decision
	bad.Key.RequestID = "changed-query-contract"
	turn := *decision.Unit.QueryTurn
	bad.Unit.QueryTurn = &turn
	turn.Invocations = append([]api.QueryInvocation(nil), turn.Invocations...)
	turn.Invocations[0].ToolVersion = "read/v2"
	tip := integrationSnapshot(t, s.e)
	if out, err := s.e.ApplyWork(ctx, bad); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("invalid contract partly published", out, err)
	}
	bad.Key.RequestID = "query-unverified-tool-version"
	binding := *decision.Unit.Decision
	binding.Invocations = append([]api.DecisionInvocation(nil), binding.Invocations...)
	binding.Invocations[0].Version = turn.Invocations[0].ToolVersion
	bad.Unit.Decision = &binding
	if out, err := s.e.ApplyWork(ctx, bad); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("Query accepted a version different from the published definition", out, err)
	}
	bad = decision
	bad.Key.RequestID = "query-unverified-tool-capabilities"
	turn = *decision.Unit.QueryTurn
	bad.Unit.QueryTurn = &turn
	badTool := toolSpec
	badTool.Contract.ResultQuery = "strong"
	badNext := exact("query-bad-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: badTool}})
	turn.Next = &badNext
	if out, err := s.e.ApplyWork(ctx, bad); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("Query reserved tool capabilities different from the published definition", out, err)
	}
	decisionNext := createTool
	if !withTools {
		decisionNext = exact("query-settle-model", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: modelSpec.Activity, ExpectedVersion: 0, Status: "settled", Evidence: proof}})
		decision.Unit.Decision.Invocations = nil
		decision.Unit.QueryTurn.Invocations = nil
		decision.Unit.QueryTurn.Next = &decisionNext
	}
	var decisionAck api.CommitResult
	if len(publicationFault) == 0 {
		decisionAck = workApply(t, s, decision)
	} else {
		if err := s.e.Close(); err != nil {
			t.Fatal(err)
		}
		fault := &workCurrentFaultIO{DurableIO: s.d, after: publicationFault[0]}
		var err error
		s.e, _, err = engine.OpenExisting(fault, s.options)
		if err != nil {
			t.Fatal(err)
		}
		fault.armed = true
		out, err := s.e.ApplyWork(ctx, decision)
		if out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkCurrent) {
			t.Fatalf("query decision publication should be unknown: %+v %v", out, err)
		}
		s.options.Journal.TailPolicy = storage.TailQuarantine
		s.reopen()
		decisionAck = workApply(t, s, decision)
	}
	s.reopen()
	progress, err := s.e.GetQueryProgress(ctx, read)
	if err != nil || progress.Progress.Sequence != 1 || progress.Progress.Decisions[0].AcceptedResult != result.ID || !reflect.DeepEqual(progress.Progress.Next, &decisionNext) {
		t.Fatal("decision lost after restart", progress, err)
	}
	if retry := workApply(t, s, decision); retry != decisionAck {
		t.Fatal("decision retry changed receipt")
	}
	if !withTools {
		commitExact(decisionNext)
		// An old accepted model result is not a second logical decision, even
		// when the original pending action is already receipted.
		reused := decision
		reused.Key.RequestID = "reused-primary-result"
		turn := *decision.Unit.QueryTurn
		reused.Unit.QueryTurn = &turn
		turn.ExpectedVersion = 2
		turn.Sequence = 2
		fresh := exact("new-model-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: modelSpec.Activity, Attempt: "new-attempt", Sequence: 2}})
		turn.Next = &fresh
		tip := integrationSnapshot(t, s.e)
		if out, err := s.e.ApplyWork(ctx, reused); !errors.Is(err, agentprofile.ErrUniqueSlot) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
			t.Fatal("old result became fresh decision", out, err)
		}
		answer := api.ApplyWorkRequest{Key: agentKey("direct-answer"), Unit: api.WorkUnit{Kind: "query_answer", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 2, Sequence: 1, AdapterVersion: "query/v1", Phase: "answered", Source: packet.Source.Anchor}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "final", Kind: "final", ExpectedVersion: 1, Body: []byte("direct answer"), Final: true, GeneratedFrom: result.ID}})}}}
		changed := answer
		changed.Key.RequestID = "changed-final-answer-body"
		changed.Unit.Steps = []api.WorkStep{workControl(t, agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "final", Kind: "final", ExpectedVersion: 1, Body: []byte("invented answer"), Final: true, GeneratedFrom: result.ID}})}
		if out, err := s.e.ApplyWork(ctx, changed); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
			t.Fatal("final body escaped accepted model result", out, err)
		}
		ack := workApply(t, s, answer)
		if _, err := s.e.CompactAuditFull(ctx); err != nil {
			t.Fatal(err)
		}
		s.reopen()
		final, err := s.e.GetQueryProgress(ctx, read)
		if err != nil || !final.Progress.Completed || final.Progress.Next != nil || len(final.Progress.Decisions) != 1 || len(final.Progress.Decisions[0].Invocations) != 0 {
			t.Fatal(final, err)
		}
		if retry := workApply(t, s, answer); retry != ack {
			t.Fatal("direct answer receipt changed")
		}
		return
	}
	premature := api.ApplyWorkRequest{Key: agentKey("premature-answer"), Unit: api.WorkUnit{Kind: "query_answer", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 2, Sequence: 1, AdapterVersion: "query/v1", Phase: "answered", Source: packet.Source.Anchor}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "premature", Kind: "final", ExpectedVersion: 1, Body: []byte("incorrectly done"), Final: true, GeneratedFrom: result.ID}})}}}
	tip = integrationSnapshot(t, s.e)
	if out, err := s.e.ApplyWork(ctx, premature); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("pending action or tool was bypassed", out, err)
	}
	wrongTool := toolSpec
	wrongTool.Effect = "non_idempotent"
	wrongCreate := exact("changed-tool-effect", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: wrongTool}})
	if out, err := s.e.Commit(ctx, wrongCreate); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("tool effect changed outside Work", out, err)
	}
	wrongTool = toolSpec
	wrongTool.Role = "resource_prepare"
	wrongCreate = exact("changed-tool-role", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: wrongTool}})
	if out, err := s.e.Commit(ctx, wrongCreate); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("Query tool slot accepted an unrelated Activity role", out, err)
	}
	wrongTool = toolSpec
	alternative := definition
	alternative.Schema = []byte(`{"type":"object","additionalProperties":false}`)
	otherDefinition, otherFence := publishTestToolDefinition(t, s, "other-query-tool-definition", alternative)
	wrongTool.InputFence = otherFence
	wrongTool.Contract = alternative.BindEffect(otherDefinition)
	wrongCreate = exact("changed-tool-effect-definition", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: wrongTool}})
	tip = integrationSnapshot(t, s.e)
	if out, err := s.e.Commit(ctx, wrongCreate); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("Query tool used an effect contract from another definition", out, err)
	}
	alternateKey := createTool
	alternateKey.RequestID = "query-create-tool-alternate-key"
	if out, err := s.e.Commit(ctx, alternateKey); !errors.Is(err, engine.ErrActionReservation) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != tip {
		t.Fatal("managed Query tool creation replaced the reserved request key", out, err)
	}
	commitExact(createTool)
	recoveryCoreControl(t, s, "tool-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: toolSpec.Activity, Attempt: "tool-attempt", Sequence: 1}})
	recoveryCoreControl(t, s, "tool-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "tool-attempt", Evidence: proof}})
	toolResult := agentprofile.ActivityResult{Capture: "full", ID: "tool-result", Activity: toolSpec.Activity, Attempt: "tool-attempt", RequestDigest: toolSpec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("file bytes")}
	recoveryCoreControl(t, s, "tool-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: toolResult}})
	finalSpec := modelSpec
	finalSpec.Activity = "final-query-model"
	finalClaim := exact("final-query-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: finalSpec.Activity, Attempt: "final-query-attempt", Sequence: 1}})
	finalOpen := api.ApplyWorkRequest{Key: agentKey("final-query-open"), Unit: api.WorkUnit{Kind: "query_checkpoint", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 3, Sequence: 1, Activity: finalSpec.Activity, AdapterVersion: "query/v1", Phase: "final-model", Source: packet.Source.Anchor, Next: &finalClaim}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: finalSpec}})}}}
	finalOpenBody, err := api.EncodeWorkUnit(finalOpen.Unit)
	if err != nil {
		t.Fatal(err)
	}
	nextFinalModel := api.CommitRequest{Space: query.Space, Principal: query.Owner, RequestID: finalOpen.Key.RequestID, Method: api.MethodApplyWork, Body: finalOpenBody}
	finalResult := agentprofile.ActivityResult{Capture: "full", ID: "final-query-result", Activity: finalSpec.Activity, Attempt: "final-query-attempt", RequestDigest: finalSpec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("answer from file")}
	answer := api.ApplyWorkRequest{Key: agentKey("query-answer"), Unit: api.WorkUnit{Kind: "query_answer", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 5, Sequence: 2, AdapterVersion: "query/v1", Phase: "answered", Source: packet.Source.Anchor}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "final", Kind: "final", Final: true, Body: finalResult.Payload, ExpectedVersion: 1, GeneratedFrom: finalResult.ID}})}}}
	answerBody, err := api.EncodeWorkUnit(answer.Unit)
	if err != nil {
		t.Fatal(err)
	}
	nextAnswer := api.CommitRequest{Space: query.Space, Principal: query.Owner, RequestID: answer.Key.RequestID, Method: api.MethodApplyWork, Body: answerBody}
	tool := api.ApplyWorkRequest{Key: agentKey("query-tool-result"), Unit: api.WorkUnit{Kind: "query_tool", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 2, Sequence: 1, Activity: toolSpec.Activity, AdapterVersion: "query/v1", Phase: "final-model", Source: packet.Source.Anchor, Next: &nextFinalModel}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: toolSpec.Activity, Result: toolResult.ID, ExpectedVersion: 1, Evidence: proof}})}}}
	workApply(t, s, tool)
	workApply(t, s, finalOpen)
	commitExact(finalClaim)
	recoveryCoreControl(t, s, "final-query-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: finalResult.Attempt, Evidence: proof}})
	recoveryCoreControl(t, s, "final-query-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: finalResult}})
	workApply(t, s, api.ApplyWorkRequest{Key: agentKey("final-query-decision"), Unit: api.WorkUnit{Kind: "query_decision", Query: query.ID, Decision: &api.DecisionBinding{Activity: finalSpec.Activity, Result: finalResult.ID}, QueryTurn: &api.QueryTurn{ExpectedVersion: 4, Sequence: 2, Activity: finalSpec.Activity, AdapterVersion: "query/v1", Phase: "answer", Source: packet.Source.Anchor, Next: &nextAnswer}, Steps: []api.WorkStep{workControl(t, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: finalSpec.Activity, Result: finalResult.ID, ExpectedVersion: 1, Evidence: proof}})}}})
	answerAck := workApply(t, s, answer)
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	final, err := s.e.GetQueryProgress(ctx, read)
	if err != nil || !final.Progress.Completed || final.Progress.Next != nil || final.Progress.Decisions[0].Invocations[0].AcceptedResult != toolResult.ID {
		t.Fatal("completed query lost", final, err)
	}
	if retry := workApply(t, s, answer); retry != answerAck {
		t.Fatal("answer receipt changed")
	}
	state, err = s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: query.Space, Principal: query.Owner})
	if err != nil || len(state.Snapshot.Tasks) != 0 || len(state.Snapshot.Executions) != 0 || len(state.Snapshot.Steps) != 0 || string(state.Snapshot.Lifecycle.Queries[0].Answer) != "answer from file" {
		t.Fatal("query manufactured task", state, err)
	}
	oldRead := read
	oldRead.Anchor = &first.Anchor
	old, err := s.e.GetQueryProgress(ctx, oldRead)
	if err != nil || old.Progress.Sequence != 0 || old.Progress.Completed {
		t.Fatal("historical query changed", old, err)
	}
	assertQueryProgressNodeIsolation(t, s, query.ID, first.Anchor)
	applyMemory(t, s.e, "query-input-privacy", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: inputID, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "withdraw model input"}}})
	if _, err := s.e.GetQueryProgress(ctx, read); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatal("query arguments disclosed after input withdrawal", err)
	}
	if _, err := s.e.GetQueryProgress(ctx, oldRead); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatal("historical Query progress bypassed current privacy", err)
	}
}
