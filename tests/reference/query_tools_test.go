package reference_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentQueryMultipleToolsKeepExactContractsAcrossPartialRecovery(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	options := ate.Options{AgentProfile: true, EventProfile: true}
	store, err := ate.Create(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	ack := func(r api.CommitResult, e error) api.CommitResult {
		t.Helper()
		if e != nil || r.Code != api.CodeDurableAck {
			t.Fatalf("%+v %v", r, e)
		}
		return r
	}
	step := func(c agentprofile.ControlCommand) api.WorkStep {
		t.Helper()
		s, e := api.AgentWorkStep(agentprofile.Command{Control: &c})
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	exact := func(id string, c agentprofile.ControlCommand) api.CommitRequest {
		t.Helper()
		raw, e := agentprofile.EncodeCommand(agentprofile.Command{Control: &c})
		if e != nil {
			t.Fatal(e)
		}
		return api.CommitRequest{Space: "space", Principal: "owner", RequestID: model.RequestID(id), Method: api.MethodApplyAgent, Body: raw}
	}
	raw := func(id string, c agentprofile.ControlCommand) api.CommitResult {
		t.Helper()
		return ack(store.Commit(ctx, exact(id, c)))
	}
	work := func(r api.ApplyWorkRequest) api.CommitResult { t.Helper(); return ack(store.ApplyWork(ctx, r)) }
	definition := agentprofile.ToolDefinition{Name: "read", Version: "read/v1", SchemaEncoding: "json-schema/v1", Schema: []byte(`{"type":"object"}`), DispatchMode: "external", DispatchClass: "pure", Contract: agentprofile.EffectContract{
		Version: "effect/v1", Recipient: "executor", VerifiedPure: true, DifferentRequest: "unsupported", ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "none",
	}}
	definitionBytes, err := definition.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("request with two tool definitions and exact arguments")
	inputAck := ack(store.Mutate(ctx, api.MutateRequest{Key: key("multi-input"), Batch: api.StateBatch{Operations: []api.StateMutation{
		{Key: []byte("input"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: input}},
		{Key: []byte("tool-definition"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: definitionBytes}},
	}}}))
	source := model.SnapshotAnchor{Space: "space", Fence: inputAck.Fence}
	view, e := store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "owner", Anchor: &source})
	if e != nil {
		t.Fatal(e)
	}
	entries, e := store.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if e != nil || len(entries.Items) != 2 {
		t.Fatal(e)
	}
	inputID, definitionID := entries.Items[0].Value, entries.Items[1].Value
	_ = store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	packet := knowledge.ContextPacket{Space: "space", Source: knowledge.SourceAnchor{Anchor: source}, Renderer: "query/v1", Tokenizer: "fixture/v1", Input: &knowledge.RecordedInput{Object: inputID, Anchor: source, Encoding: "query/v1"}}
	packet.ID = knowledge.PacketID(packet)
	packetAck := ack(store.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("multi-packet"), Command: knowledge.Command{Packet: &packet}}))
	query := agentprofile.Query{ID: "query", Session: "session", Space: "space", Owner: "owner", Version: 1, Status: "open", Input: []byte("compare two files"), InputFence: inputAck.Fence}
	modelSpec := agentprofile.ActivitySpec{Activity: "model", Query: query.ID, Role: "query_decision", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: inputID, RequestDigest: independentBytesDigest(input), Recipient: "model", Effect: "pure", ResourceScope: "query"}
	claim := exact("multi-model-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: modelSpec.Activity, Attempt: "model-attempt", Sequence: 1}})
	openSource := model.SnapshotAnchor{Space: "space", Fence: packetAck.Fence}
	open := api.ApplyWorkRequest{Key: key("multi-open"), Unit: api.WorkUnit{Kind: "query_open", Query: query.ID, QueryTurn: &api.QueryTurn{Activity: modelSpec.Activity, AdapterVersion: "query/v1", Phase: "model", Source: openSource, Next: &claim}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: modelSpec}})}}}
	openBody, e := api.EncodeWorkUnit(open.Unit)
	if e != nil {
		t.Fatal(e)
	}
	intake := api.ApplyWorkRequest{Key: key("multi-intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: query.ID, Intake: &api.QueryIntake{Query: query, Route: "query/v1", Configuration: []byte("fixed configuration"), Next: api.CommitRequest{Space: "space", Principal: "owner", RequestID: open.Key.RequestID, Method: api.MethodApplyWork, Body: openBody}}}}
	work(intake)
	want := oracle.NewQueryController(query.ID)
	want.Observe("Work.query_intake")
	if err := want.Advance("open", 0, 0, "query/v1", "model", querySourceIdentity(openSource), queryRequestIdentity(&claim), modelSpec.Activity, "", true); err != nil {
		t.Fatal(err)
	}
	openAck := work(open)
	want.Observe("Work.query_open")
	want.Observe("Control.CreateActivity")
	ack(store.Commit(ctx, claim))
	want.Observe("Control.ClaimAttempt")
	dispatchQueryActivity(t, store, want, "multi-model-dispatch", modelSpec, "model-attempt", digest("a"), len(input))
	modelResult := agentprofile.ActivityResult{Capture: "full", ID: "model-result", Activity: modelSpec.Activity, Attempt: "model-attempt", RequestDigest: modelSpec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("read a and b")}
	raw("multi-model-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: modelResult}})
	want.Observe("Control.CaptureResult")
	calls := []api.QueryInvocation{{ID: "a", Activity: "tool-a", ProviderCallKey: "call-a", Tool: "read", Definition: definitionID, ToolVersion: "read/v1", Arguments: []byte(`{"path":"a.go"}`), DispatchClass: "pure"}, {ID: "b", Activity: "tool-b", ProviderCallKey: "call-b", Tool: "read", Definition: definitionID, ToolVersion: "read/v1", Arguments: []byte(`{"path":"b.go"}`), DispatchClass: "pure"}}
	specs := make([]agentprofile.ActivitySpec, 2)
	creates := make([]api.CommitRequest, 2)
	for i, inv := range calls {
		specs[i] = agentprofile.ActivitySpec{Activity: inv.Activity, Query: query.ID, Role: "external_tool", GoalEpoch: 1, InputFence: source.Fence, Request: inputID, RequestDigest: independentBytesDigest(input), Recipient: definition.Contract.Recipient, Effect: inv.DispatchClass, ResourceScope: "query", Contract: definition.BindEffect(definitionID)}
		creates[i] = exact("create-"+inv.ID, agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: specs[i]}})
	}
	abstractCalls := func(in []api.QueryInvocation) []oracle.QueryInvocation {
		var out []oracle.QueryInvocation
		for _, i := range in {
			out = append(out, oracle.QueryInvocation{ID: i.ID, Activity: i.Activity, Call: i.ProviderCallKey, Tool: i.Tool, Definition: string(i.Definition), ToolVersion: i.ToolVersion, Arguments: string(i.Arguments), Effect: i.DispatchClass, AcceptedResult: i.AcceptedResult})
		}
		return out
	}
	bindingMap := func(in []api.DecisionInvocation) map[string]oracle.QueryInvocationBinding {
		out := map[string]oracle.QueryInvocationBinding{}
		for _, b := range in {
			out[b.Invocation] = oracle.QueryInvocationBinding{Definition: string(b.Definition), ToolVersion: b.Version, Effect: b.DispatchClass}
		}
		return out
	}
	bindings := []api.DecisionInvocation{}
	for _, i := range calls {
		bindings = append(bindings, api.DecisionInvocation{Invocation: i.ID, Definition: i.Definition, Version: i.ToolVersion, DispatchClass: i.DispatchClass})
	}
	decision := api.ApplyWorkRequest{Key: key("multi-decision"), Unit: api.WorkUnit{Kind: "query_decision", Query: query.ID, Decision: &api.DecisionBinding{Activity: modelSpec.Activity, Result: modelResult.ID, Invocations: bindings}, QueryTurn: &api.QueryTurn{ExpectedVersion: 1, Sequence: 1, Activity: modelSpec.Activity, Invocations: calls, AdapterVersion: "query/v1", Phase: "create-a", Source: source, Next: &creates[0]}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: modelSpec.Activity, Result: modelResult.ID, ExpectedVersion: 1, Evidence: digest("b")}})}}}
	rejected := func(name string, expected error, request api.ApplyWorkRequest) {
		t.Helper()
		before, _ := store.Snapshot(ctx)
		r, e := store.ApplyWork(ctx, request)
		after, _ := store.Snapshot(ctx)
		if expected == nil || e == nil || r.Code == api.CodeDurableAck || before != after {
			t.Fatalf("%s acceptance/atomicity oracle=%v actual=%+v/%v", name, expected, r, e)
		}
	}
	for _, kind := range []string{"missing binding", "wrong version", "duplicate provider key"} {
		bad := decision
		d := *decision.Unit.Decision
		d.Invocations = append([]api.DecisionInvocation(nil), bindings...)
		turn := *decision.Unit.QueryTurn
		turn.Invocations = append([]api.QueryInvocation(nil), calls...)
		bad.Unit.Decision, bad.Unit.QueryTurn = &d, &turn
		bad.Key.RequestID = model.RequestID("bad-" + kind)
		switch kind {
		case "missing binding":
			d.Invocations = d.Invocations[:1]
		case "wrong version":
			d.Invocations[1].Version = "read/v2"
		case "duplicate provider key":
			turn.Invocations[1].ProviderCallKey = turn.Invocations[0].ProviderCallKey
		}
		rejected(kind, want.Decide(1, 1, "query/v1", "create-a", querySourceIdentity(source), queryRequestIdentity(&creates[0]), modelSpec.Activity, modelResult.ID, true, abstractCalls(turn.Invocations), bindingMap(d.Invocations)), bad)
	}
	if err := want.Decide(1, 1, "query/v1", "create-a", querySourceIdentity(source), queryRequestIdentity(&creates[0]), modelSpec.Activity, modelResult.ID, true, abstractCalls(calls), bindingMap(bindings)); err != nil {
		t.Fatal(err)
	}
	decisionAck := work(decision)
	want.Observe("Work.query_decision")
	want.Observe("Control.AcceptActivity")
	want.Observe("Query.InvocationRecorded")
	want.Observe("Query.InvocationRecorded")
	compare := func(name string) {
		t.Helper()
		got, e := store.GetQueryProgress(ctx, api.ReadQueryProgressRequest{Space: "space", Principal: "owner", Query: query.ID})
		if e != nil || !got.Found {
			t.Fatal(e)
		}
		p := got.Progress
		actual := oracle.QueryProgress{Query: p.Query, Adapter: p.AdapterVersion, Phase: p.Phase, Source: querySourceIdentity(p.Source), Next: queryRequestIdentity(p.Next), Version: p.Version, Sequence: p.Sequence, Completed: p.Completed}
		for _, d := range p.Decisions {
			actual.Decisions = append(actual.Decisions, oracle.QueryDecision{Sequence: d.Sequence, Activity: d.Activity, Result: d.AcceptedResult, Invocations: abstractCalls(d.Invocations)})
		}
		if !reflect.DeepEqual(actual, want.Progress) {
			t.Fatalf("%s Query state mismatch\nactual=%+v\nexpected=%+v", name, actual, want.Progress)
		}
	}
	compare("complete call contract")
	answer := api.ApplyWorkRequest{Key: key("multi-answer"), Unit: api.WorkUnit{Kind: "query_answer", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 6, Sequence: 2, AdapterVersion: "query/v1", Phase: "answered", Source: source}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "final", Kind: "final", Final: true, ExpectedVersion: 1, Body: []byte("a and b compared"), GeneratedFrom: "final-model-result"}})}}}
	answerBody, e := api.EncodeWorkUnit(answer.Unit)
	if e != nil {
		t.Fatal(e)
	}
	answerRequest := api.CommitRequest{Space: "space", Principal: "owner", RequestID: answer.Key.RequestID, Method: api.MethodApplyWork, Body: answerBody}
	finalSpec := modelSpec
	finalSpec.Activity = "final-model"
	finalClaim := exact("final-model-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: finalSpec.Activity, Attempt: "final-model-attempt", Sequence: 1}})
	finalOpen := api.ApplyWorkRequest{Key: key("final-model-open"), Unit: api.WorkUnit{Kind: "query_checkpoint", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 4, Sequence: 1, Activity: finalSpec.Activity, AdapterVersion: "query/v1", Phase: "final-model", Source: source, Next: &finalClaim}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: finalSpec}})}}}
	finalOpenBody, err := api.EncodeWorkUnit(finalOpen.Unit)
	if err != nil {
		t.Fatal(err)
	}
	finalOpenRequest := api.CommitRequest{Space: "space", Principal: "owner", RequestID: finalOpen.Key.RequestID, Method: api.MethodApplyWork, Body: finalOpenBody}
	for i, spec := range specs {
		ack(store.Commit(ctx, creates[i]))
		want.Observe("Control.CreateActivity")
		premature := answer
		turn := *answer.Unit.QueryTurn
		premature.Unit.QueryTurn = &turn
		turn.ExpectedVersion = want.Progress.Version
		premature.Key.RequestID = model.RequestID(fmt.Sprintf("premature-%d", i))
		rejected("unfinished tool blocks answer", want.Advance("answer", want.Progress.Version, 1, "query/v1", "answered", querySourceIdentity(source), "", "", "", true), premature)
		attempt := "attempt-" + calls[i].ID
		raw("claim-"+calls[i].ID, agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: attempt, Sequence: 1}})
		want.Observe("Control.ClaimAttempt")
		dispatchQueryActivity(t, store, want, "dispatch-"+calls[i].ID, spec, attempt, digest("c"), len(input))
		result := agentprofile.ActivityResult{Capture: "full", ID: "result-" + calls[i].ID, Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("file-" + calls[i].ID)}
		raw("capture-"+calls[i].ID, agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
		want.Observe("Control.CaptureResult")
		next := finalOpenRequest
		phase := "final-model"
		if i == 0 {
			next = creates[1]
			phase = "create-b"
		}
		tool := api.ApplyWorkRequest{Key: key("accept-" + calls[i].ID), Unit: api.WorkUnit{Kind: "query_tool", Query: query.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: want.Progress.Version, Sequence: 1, Activity: spec.Activity, AdapterVersion: "query/v1", Phase: phase, Source: source, Next: &next}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: digest("d")}})}}}
		if err := want.ToolResult(want.Progress.Version, 1, "query/v1", phase, querySourceIdentity(source), queryRequestIdentity(&next), spec.Activity, result.ID, true); err != nil {
			t.Fatal(err)
		}
		toolAck := work(tool)
		want.Observe("Work.query_tool")
		want.Observe("Control.AcceptActivity")
		compare("partial tool result")
		if i == 0 {
			if _, e := store.CompactAuditFull(ctx); e != nil {
				t.Fatal(e)
			}
			if e := store.Close(); e != nil {
				t.Fatal(e)
			}
			store, _, err = ate.OpenExisting(path, options)
			if err != nil {
				t.Fatal(err)
			}
			compare("first tool accepted after reopen")
			if got := work(tool); got != toolAck {
				t.Fatal("partial tool receipt changed")
			}
			if got := work(decision); got != decisionAck {
				t.Fatal("partial recovery regenerated decision")
			}
		}
	}
	if err := want.Advance("checkpoint", 4, 1, "query/v1", "final-model", querySourceIdentity(source), queryRequestIdentity(&finalClaim), finalSpec.Activity, "", true); err != nil {
		t.Fatal(err)
	}
	work(finalOpen)
	want.Observe("Work.query_checkpoint")
	want.Observe("Control.CreateActivity")
	ack(store.Commit(ctx, finalClaim))
	want.Observe("Control.ClaimAttempt")
	dispatchQueryActivity(t, store, want, "final-model-dispatch", finalSpec, "final-model-attempt", digest("e"), len(input))
	finalResult := agentprofile.ActivityResult{Capture: "full", ID: "final-model-result", Activity: finalSpec.Activity, Attempt: "final-model-attempt", RequestDigest: finalSpec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("a and b compared")}
	raw("final-model-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: finalResult}})
	want.Observe("Control.CaptureResult")
	finalDecision := api.ApplyWorkRequest{Key: key("final-model-decision"), Unit: api.WorkUnit{Kind: "query_decision", Query: query.ID, Decision: &api.DecisionBinding{Activity: finalSpec.Activity, Result: finalResult.ID}, QueryTurn: &api.QueryTurn{ExpectedVersion: 5, Sequence: 2, Activity: finalSpec.Activity, AdapterVersion: "query/v1", Phase: "answer", Source: source, Next: &answerRequest}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: finalSpec.Activity, Result: finalResult.ID, ExpectedVersion: 1, Evidence: digest("f")}})}}}
	if err := want.Decide(5, 2, "query/v1", "answer", querySourceIdentity(source), queryRequestIdentity(&answerRequest), finalSpec.Activity, finalResult.ID, true, nil, nil); err != nil {
		t.Fatal(err)
	}
	work(finalDecision)
	want.Observe("Work.query_decision")
	want.Observe("Control.AcceptActivity")
	compare("final model accepted")
	if err := want.Advance("answer", 6, 2, "query/v1", "answered", querySourceIdentity(source), "", "", "", true); err != nil {
		t.Fatal(err)
	}
	answerAck := work(answer)
	want.Observe("Work.query_answer")
	want.Observe("Control.AnswerQuery")
	compare("both tools answered")
	compareQueryEventFacts(t, store, want)
	if _, e := store.CompactAuditFull(ctx); e != nil {
		t.Fatal(e)
	}
	if e := store.Close(); e != nil {
		t.Fatal(e)
	}
	store, _, err = ate.OpenExisting(path, options)
	if err != nil {
		t.Fatal(err)
	}
	compare("final replay")
	if got := work(open); got != openAck {
		t.Fatal("open receipt changed")
	}
	if got := work(answer); got != answerAck {
		t.Fatal("answer receipt changed")
	}
	compareRecordedRequestBytes(t, store, source, inputID, input)
}
