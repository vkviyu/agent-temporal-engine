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
)

func TestWorkModelDecisionPublishesPendingToolsAndContractAtomically(t *testing.T) {
	t.Run("current-result", func(t *testing.T) { testWorkModelDecision(t, false) })
	t.Run("reconciled-result", func(t *testing.T) { testWorkModelDecision(t, true) })
}

func testWorkModelDecision(t *testing.T, reconcile bool) {
	t.Helper()
	ctx := context.Background()
	s, content, source := knowledgeFixture(t)
	o := knowledgeObservation(content, source, "one", 1)
	applyObservation(t, s.e, "decision-observe", o)
	goal, goalFence, _ := goalCoreImage(t, s, "decision-goal", agentTask("kt"))
	goalObservation := knowledgeObservation(goal, goalFence, "one", 2)
	_, adopt := applyObservation(t, s.e, "decision-adopt-goal", goalObservation)
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &o.Key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	packet := knowledge.ContextPacket{
		Space: "host-space", Key: &o.Key, GoalRef: goal,
		Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: view.ViewDigest, Frontier: known.Frontier},
		Items: []knowledge.ContextItem{
			{Observation: o.ID, Content: content, Label: "input"},
			{Ordinal: 1, Observation: goalObservation.ID, Content: goal, Label: "goal"},
		},
		Renderer: "decision/v1", Tokenizer: "test/v1",
		Input: &knowledge.RecordedInput{Object: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Encoding: "test/v1"},
	}
	publishPacket := func(id string, p knowledge.ContextPacket) (model.ObjectID, model.CommitFence) {
		t.Helper()
		p.ID = knowledge.PacketID(p)
		ack, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey(id), Command: knowledge.Command{Packet: &p}})
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatal(ack, err)
		}
		return p.ID, ack.Fence
	}
	a := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	proof := model.ObjectID(integrationID('a'))
	recoveryCoreControl(t, s, "decision-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Status: "active", Binding: a, Evidence: proof}}})
	recoveryCoreControl(t, s, "decision-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: agentprofile.Step{Execution: "one", Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: adopt.Fence}, Authority: a}})
	_, request := recoveryValueAt(t, s, source, "evidence")
	spec := agentprofile.ActivitySpec{Activity: "primary", Execution: "one", Step: 1, Role: "primary_decision", GoalEpoch: 1, Request: content, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: "model/v1", Effect: "pure", ResourceScope: "session"}
	checkRejectedPacket := func(id string, p knowledge.ContextPacket) {
		t.Helper()
		bad := spec
		bad.ContextPacket, bad.InputFence = publishPacket(id+"-packet", p)
		before := integrationSnapshot(t, s.e)
		out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey(id), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: bad, Authority: a}}}})
		if !errors.Is(err, knowledge.ErrContextSelectionInvalid) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
			t.Fatalf("invalid primary packet changed or fenced store: %+v %v", out, err)
		}
	}
	checkRejectedPacket("decision-missing-requirements", packet)
	packet.Requirements = &knowledge.ContextRequirements{
		RulesVersion: agentprofile.ContextRequirementsVersion, HostPolicyVersion: "decision/v1",
		Facts: []knowledge.ContextRequirement{{Kind: "goal", ID: "kt", Observation: o.ID, Content: content}},
	}
	checkRejectedPacket("decision-forged-goal", packet)
	packet.Requirements.Facts[0].Observation = goalObservation.ID
	packet.Requirements.Facts[0].Content = goal
	definition := testPureToolDefinition("read", "executor")
	definitionID, _ := publishTestToolDefinition(t, s, "decision-read-definition", definition)
	spec.ContextPacket, spec.InputFence = publishPacket("decision-packet", packet)
	recoveryCoreControl(t, s, "decision-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: a}})
	recoveryCoreControl(t, s, "decision-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "primary-attempt", Sequence: 1, Authority: a}})
	recoveryCoreControl(t, s, "decision-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "primary-attempt", Authority: a, Evidence: proof}})
	result := agentprofile.ActivityResult{Capture: "full", ID: "primary-result", Activity: spec.Activity, Attempt: "primary-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("read file")}
	recoveryCoreControl(t, s, "decision-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	authorityVersion := uint64(1)
	if reconcile {
		s.reopen()
		recoveryCoreControl(t, s, "decision-fence", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: spec.Execution, ExpectedVersion: authorityVersion, Evidence: proof}})
		a.OwnerEpoch++
		a.Incarnation++
		recoveryCoreControl(t, s, "decision-resume", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: spec.Execution, ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: a, AdapterVersion: "work/v1", Checkpoint: content, CompatibilityEvidence: proof, ResourceFenceEvidence: proof}})
		authorityVersion = 3
		recoveryCoreControl(t, s, "decision-reconcile", agentprofile.ControlCommand{ReconcileAttempt: &agentprofile.ReconcileAttemptCommand{Attempt: result.Attempt, Outcome: "occurred", Evidence: proof}})
	}
	accept := &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: a, Evidence: proof}
	acceptControl := agentprofile.ControlCommand{AcceptActivity: accept}
	if reconcile {
		acceptControl = agentprofile.ControlCommand{ImportActivityResult: accept}
	}
	beforeAccept := integrationSnapshot(t, s.e)
	acceptBody := agentprofile.Command{Control: &acceptControl}
	if out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("decision-direct-accept"), Command: acceptBody}); out.Code != api.CodeRejected || !errors.Is(err, engine.ErrWorkBoundary) || integrationSnapshot(t, s.e) != beforeAccept {
		t.Fatalf("primary acceptance escaped model_decision: %+v %v", out, err)
	}
	checkpoint := workCheckpoint(t, "one", a, spec.InputFence, "dispatch-read", 0, 1)
	checkpointBypass := api.ApplyWorkRequest{Key: agentKey("decision-checkpoint-bypass"), Unit: api.WorkUnit{Kind: "checkpoint", Steps: []api.WorkStep{workControl(t, acceptControl), checkpoint}}}
	if out, err := s.e.ApplyWork(ctx, checkpointBypass); out.Code != api.CodeRejected || !errors.Is(err, engine.ErrWorkBoundary) || integrationSnapshot(t, s.e) != beforeAccept {
		t.Fatalf("generic checkpoint accepted primary result: %+v %v", out, err)
	}
	inv := agentprofile.Invocation{ID: "read-file", Execution: "one", Step: 1, ProviderCallKey: "call-1", Tool: "read", Args: []byte(`{"path":"file.go"}`), Status: agentprofile.InvocationPending, Attempt: 1, AcceptedResult: result.ID, ToolDefinition: definitionID, ToolVersion: "read/v1", DispatchClass: "pure"}
	unit := func(i agentprofile.Invocation) api.WorkUnit {
		return api.WorkUnit{Kind: "model_decision", Decision: &api.DecisionBinding{Activity: spec.Activity, Result: result.ID, Invocations: []api.DecisionInvocation{{Invocation: inv.ID, Definition: definitionID, Version: "read/v1", DispatchClass: "pure"}}}, Steps: []api.WorkStep{
			workControl(t, acceptControl),
			workControl(t, agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: i, Authority: a}}),
			checkpoint,
		}}
	}
	for name, mutate := range map[string]func(*agentprofile.Invocation){
		"accepted-result": func(i *agentprofile.Invocation) { i.AcceptedResult = "other-result" },
		"definition":      func(i *agentprofile.Invocation) { i.ToolDefinition = proof },
		"version":         func(i *agentprofile.Invocation) { i.ToolVersion = "read/v2" },
		"dispatch-class":  func(i *agentprofile.Invocation) { i.DispatchClass = "non_idempotent" },
	} {
		bad := inv
		mutate(&bad)
		before := integrationSnapshot(t, s.e)
		out, err := s.e.ApplyWork(ctx, api.ApplyWorkRequest{Key: agentKey("bad-decision-" + name), Unit: unit(bad)})
		if !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
			t.Fatalf("%s accepted/partially published: %+v %v", name, out, err)
		}
	}
	for name, mutate := range map[string]func(*agentprofile.Invocation){
		"untyped":  func(i *agentprofile.Invocation) { i.ToolDefinition = content },
		"name":     func(i *agentprofile.Invocation) { i.Tool = "write" },
		"version":  func(i *agentprofile.Invocation) { i.ToolVersion = "read/v2" },
		"dispatch": func(i *agentprofile.Invocation) { i.DispatchClass = "non_idempotent" },
	} {
		bad := inv
		mutate(&bad)
		candidate := unit(bad)
		candidate.Decision.Invocations[0] = api.DecisionInvocation{Invocation: bad.ID, Definition: bad.ToolDefinition, Version: bad.ToolVersion, DispatchClass: bad.DispatchClass}
		before := integrationSnapshot(t, s.e)
		out, err := s.e.ApplyWork(ctx, api.ApplyWorkRequest{Key: agentKey("bad-tool-definition-" + name), Unit: candidate})
		if err == nil || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
			t.Fatalf("%s accepted/partially published: %+v %v", name, out, err)
		}
	}
	req := api.ApplyWorkRequest{Key: agentKey("commit-decision"), Unit: unit(inv)}
	before := integrationSnapshot(t, s.e)
	ack := workApply(t, s, req)
	if ack.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("decision published multiple transactions")
	}
	check := func() {
		t.Helper()
		got, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
		if err != nil || len(got.Snapshot.Invocations) != 1 || !reflect.DeepEqual(got.Snapshot.Invocations[0].Args, inv.Args) || got.Snapshot.Invocations[0].AcceptedResult != result.ID || len(got.Snapshot.Lifecycle.Continuations) != 1 || got.Snapshot.Steps[0].Status == agentprofile.StepCompleted {
			t.Fatalf("decision lost contract/recovery point: %+v %v", got, err)
		}
		imports := got.Snapshot.Lifecycle.Recovery.Imports
		if reconcile && (len(imports) != 1 || imports[0].Result != result.ID || imports[0].Authority != a) {
			t.Fatalf("reconciled decision lost import provenance: %+v", imports)
		}
	}
	check()
	s.reopen()
	check()
	if retry := workApply(t, s, req); retry != ack {
		t.Fatal("decision receipt changed")
	}
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
	// Definition reuse remains a current-policy gate even when the tool's
	// framed request is a different object from the definition itself.
	toolBytes := []byte("execute read file.go")
	toolAck := integrationMutate(t, s.e, integrationRequest("tool-request", integrationPut([]byte("tool-request"), api.Value{Kind: api.ValueBytes, Data: toolBytes})))
	toolInput, _ := recoveryValueAt(t, s, toolAck.Fence, "tool-request")
	tool := agentprofile.ActivitySpec{Activity: "tool-read", Execution: "one", Step: 1, Invocation: inv.ID, Role: "external_tool", GoalEpoch: 1, InputFence: toolAck.Fence, Request: toolInput, RequestDigest: agentprofile.ExactRequestDigest(toolBytes), Recipient: definition.Contract.Recipient, Effect: "pure", ResourceScope: "workspace", Contract: definition.BindEffect(definitionID)}
	for name, mutate := range map[string]func(*agentprofile.ActivitySpec){
		"unlinked":     func(s *agentprofile.ActivitySpec) { s.Invocation = "" },
		"wrong-role":   func(s *agentprofile.ActivitySpec) { s.Role = "resource_prepare" },
		"capabilities": func(s *agentprofile.ActivitySpec) { s.Contract.ResultQuery = "strong" },
		"recipient":    func(s *agentprofile.ActivitySpec) { s.Recipient, s.Contract.Recipient = "other", "other" },
	} {
		bad := tool
		mutate(&bad)
		before := integrationSnapshot(t, s.e)
		out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("bad-tool-" + name), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: bad, Authority: a}}}})
		if err == nil || (out.Code != api.CodeRejected && out.Code != api.CodeNotSubmitted) || integrationSnapshot(t, s.e) != before {
			t.Fatalf("%s accepted/partially published: %+v %v", name, out, err)
		}
	}
	recoveryCoreControl(t, s, "tool-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: tool, Authority: a}})
	childTask := agentTask("tool-cannot-spawn-task")
	child := agentprofile.Execution{ID: "tool-cannot-spawn", Task: childTask.ID, Parent: inv.Execution, Actor: "child-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	spawn := agentprofile.SpawnChildCommand{Parent: inv.Execution, Invocation: inv.ID, Task: childTask, Execution: child, InheritedEpoch: 1, Compatibility: proof, Authority: a}
	spawnWork := testChildSpawnWork(t, s, "external-invocation-spawn", spawn, a, "work/v1")
	before = integrationSnapshot(t, s.e)
	if out, err := s.e.ApplyWork(ctx, spawnWork); !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("external invocation also dispatched as a delegation: %+v %v", out, err)
	}
	recoveryCoreControl(t, s, "tool-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: tool.Activity, Attempt: "tool-attempt", Sequence: 1, Authority: a}})
	grant := api.EmissionSpec{ID: "tool-grant", Execution: "one", Task: "kt", GoalEpoch: 1, Authority: a, AuthorityVersion: authorityVersion, Content: toolInput, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: toolAck.Fence}, Projection: "value_bytes", Length: uint64(len(toolBytes)), Destination: tool.Recipient, Activity: tool.Activity, Attempt: "tool-attempt", RequestDigest: tool.RequestDigest}
	issueExactGrant(t, s, grant)
	dispatch := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Authority: a, Evidence: proof}}
	applyMemory(t, s.e, "tool-definition-ban", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: inv.ToolDefinition, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "definition no longer approved"}}})
	before = integrationSnapshot(t, s.e)
	out, err := s.e.ApplyWork(ctx, exactDispatchWork(t, grant, dispatch, "tool-dispatch-denied"))
	if out.Code != api.CodeRejected || !errors.Is(err, memory.ErrUseDenied) || integrationSnapshot(t, s.e) != before {
		t.Fatalf("tool definition withdrawal was ignored: %+v %v", out, err)
	}
}
