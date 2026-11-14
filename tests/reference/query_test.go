package reference_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func queryRequestIdentity(r *api.CommitRequest) string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s/%s/%x/%v", r.Space, r.Principal, r.RequestID, r.Method, r.Body, r.ExpectedFence)
}
func querySourceIdentity(a model.SnapshotAnchor) string {
	return fmt.Sprintf("%s/%s/%d/%s", a.Space, a.Fence.Store, a.Fence.LSN, a.Fence.CommitDigest)
}
func TestIndependentQueryProgressExactRecoveryAndReceiptHistory(t *testing.T) {
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
	ack := func(result api.CommitResult, e error) api.CommitResult {
		t.Helper()
		if e != nil || result.Code != api.CodeDurableAck {
			t.Fatalf("commit=%+v err=%v", result, e)
		}
		return result
	}
	work := func(r api.ApplyWorkRequest) api.CommitResult { t.Helper(); return ack(store.ApplyWork(ctx, r)) }
	control := func(id string, c agentprofile.ControlCommand) api.CommitRequest {
		t.Helper()
		raw, e := agentprofile.EncodeCommand(agentprofile.Command{Control: &c})
		if e != nil {
			t.Fatal(e)
		}
		return api.CommitRequest{Space: "space", Principal: "owner", RequestID: model.RequestID(id), Method: api.MethodApplyAgent, Body: raw}
	}
	step := func(c agentprofile.ControlCommand) api.WorkStep {
		t.Helper()
		s, e := api.AgentWorkStep(agentprofile.Command{Control: &c})
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	input := []byte("exact prior provider request\n多字节输入\x00")
	inputAck := ack(store.Mutate(ctx, api.MutateRequest{Key: key("query-input"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("input"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: input}}}}}))
	source := model.SnapshotAnchor{Space: "space", Fence: inputAck.Fence}
	view, e := store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "owner", Anchor: &source})
	if e != nil {
		t.Fatal(e)
	}
	entries, e := store.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if e != nil || len(entries.Items) != 1 {
		t.Fatal(e)
	}
	inputID := entries.Items[0].Value
	if e := store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); e != nil {
		t.Fatal(e)
	}
	compareRecordedRequestBytes(t, store, source, inputID, input)
	packet := knowledge.ContextPacket{Space: "space", Source: knowledge.SourceAnchor{Anchor: source}, Renderer: "query/v1", Tokenizer: "fixture/v1", Input: &knowledge.RecordedInput{Object: inputID, Anchor: source, Encoding: "query/v1"}}
	packet.ID = knowledge.PacketID(packet)
	packetAck := ack(store.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("query-packet"), Command: knowledge.Command{Packet: &packet}}))
	q := agentprofile.Query{ID: "query", Session: "session", Space: "space", Owner: "owner", Version: 1, Status: "open", Input: []byte("question"), InputFence: inputAck.Fence}
	spec := agentprofile.ActivitySpec{Activity: "query-model", Query: q.ID, Role: "query_decision", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: inputID, RequestDigest: independentBytesDigest(input), Recipient: "model", Effect: "pure", ResourceScope: "query"}
	claim := control("query-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "model-attempt", Sequence: 1}})
	openSource := model.SnapshotAnchor{Space: "space", Fence: packetAck.Fence}
	open := api.ApplyWorkRequest{Key: key("query-open"), Unit: api.WorkUnit{Kind: "query_open", Query: q.ID, QueryTurn: &api.QueryTurn{Activity: spec.Activity, AdapterVersion: "query/v1", Phase: "model", Source: openSource, Next: &claim}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})}}}
	openBody, e := api.EncodeWorkUnit(open.Unit)
	if e != nil {
		t.Fatal(e)
	}
	intake := api.ApplyWorkRequest{Key: key("query-intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: q.ID, Intake: &api.QueryIntake{Query: q, Route: "query/v1", Configuration: []byte(`{"model":"fixture"}`), Next: api.CommitRequest{Space: "space", Principal: "owner", RequestID: open.Key.RequestID, Method: api.MethodApplyWork, Body: openBody}}}}
	work(intake)
	want := oracle.NewQueryController(q.ID)
	want.Observe("Work.query_intake")
	if err := want.Advance("open", 0, 0, "query/v1", "model", querySourceIdentity(openSource), queryRequestIdentity(&claim), spec.Activity, "", true); err != nil {
		t.Fatal(err)
	}
	openAck := work(open)
	want.Observe("Work.query_open")
	want.Observe("Control.CreateActivity")
	firstRead := api.ReadQueryProgressRequest{Space: "space", Principal: "owner", Query: q.ID}
	compare := func(label string, at *model.SnapshotAnchor, expected oracle.QueryProgress) {
		t.Helper()
		read := firstRead
		read.Anchor = at
		got, e := store.GetQueryProgress(ctx, read)
		if e != nil || !got.Found {
			t.Fatal(label, e)
		}
		actual := oracle.QueryProgress{Query: got.Progress.Query, Adapter: got.Progress.AdapterVersion, Phase: got.Progress.Phase, Source: querySourceIdentity(got.Progress.Source), Next: queryRequestIdentity(got.Progress.Next), Version: got.Progress.Version, Sequence: got.Progress.Sequence, Completed: got.Progress.Completed}
		for _, d := range got.Progress.Decisions {
			if len(d.Invocations) != 0 {
				t.Fatal("direct-answer fixture invented tools")
			}
			actual.Decisions = append(actual.Decisions, oracle.QueryDecision{Sequence: d.Sequence, Activity: d.Activity, Result: d.AcceptedResult})
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%s progress mismatch\n actual=%+v\n expected=%+v", label, actual, expected)
		}
		state, e := store.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "space", Principal: "owner"})
		if e != nil || len(state.Snapshot.Tasks)+len(state.Snapshot.Executions)+len(state.Snapshot.Steps) != 0 {
			t.Fatal("query-only created execution", e)
		}
	}
	initialProgress := want.Progress
	compare("open", nil, want.Progress)
	wrong := claim
	wrong.Body = []byte("changed reserved bytes")
	before, _ := store.Snapshot(ctx)
	badResult, badErr := store.Commit(ctx, wrong)
	after, _ := store.Snapshot(ctx)
	if badErr == nil || badResult.Code != api.CodeRejected || before != after {
		t.Fatal("exact reserved action was replaceable", badResult, badErr)
	}
	ack(store.Commit(ctx, claim))
	want.Observe("Control.ClaimAttempt")
	dispatchQueryActivity(t, store, want, "model-dispatch", spec, "model-attempt", digest("a"), len(input))
	result := agentprofile.ActivityResult{Capture: "full", ID: "model-result", Activity: spec.Activity, Attempt: "model-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("persisted answer")}
	ack(store.Commit(ctx, control("model-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})))
	want.Observe("Control.CaptureResult")
	next := control("model-settle", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: spec.Activity, Status: "settled", Evidence: digest("b")}})
	decision := api.ApplyWorkRequest{Key: key("query-decision"), Unit: api.WorkUnit{Kind: "query_decision", Query: q.ID, Decision: &api.DecisionBinding{Activity: spec.Activity, Result: result.ID}, QueryTurn: &api.QueryTurn{ExpectedVersion: 1, Sequence: 1, Activity: spec.Activity, AdapterVersion: "query/v1", Phase: "settle", Source: source, Next: &next}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: digest("c")}})}}}
	for _, version := range []uint64{0, 2} {
		bad := decision
		turn := *decision.Unit.QueryTurn
		bad.Unit.QueryTurn = &turn
		turn.ExpectedVersion = version
		bad.Key.RequestID = model.RequestID(fmt.Sprintf("stale-%d", version))
		expectedErr := want.Advance("decision", version, 1, "query/v1", "settle", querySourceIdentity(source), queryRequestIdentity(&next), spec.Activity, result.ID, true)
		before, _ := store.Snapshot(ctx)
		out, e := store.ApplyWork(ctx, bad)
		after, _ := store.Snapshot(ctx)
		if expectedErr == nil || e == nil || out.Code != api.CodeRejected || before != after {
			t.Fatal("stale controller accepted", out, e)
		}
	}
	if e := want.Advance("decision", 1, 1, "query/v1", "settle", querySourceIdentity(source), queryRequestIdentity(&next), spec.Activity, result.ID, true); e != nil {
		t.Fatal(e)
	}
	decisionAck := work(decision)
	want.Observe("Work.query_decision")
	want.Observe("Control.AcceptActivity")
	compare("decision", nil, want.Progress)
	answer := api.ApplyWorkRequest{Key: key("query-answer"), Unit: api.WorkUnit{Kind: "query_answer", Query: q.ID, QueryTurn: &api.QueryTurn{ExpectedVersion: 2, Sequence: 1, AdapterVersion: "query/v1", Phase: "answered", Source: source}, Steps: []api.WorkStep{step(agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: q.ID, Slot: "final", Kind: "final", Final: true, ExpectedVersion: 1, Body: []byte("persisted answer"), GeneratedFrom: result.ID}})}}}
	if err := want.Advance("answer", 2, 1, "query/v1", "answered", querySourceIdentity(source), "", "", "", false); err == nil {
		t.Fatal("oracle allowed unreceipted continuation")
	}
	before, _ = store.Snapshot(ctx)
	out, e := store.ApplyWork(ctx, answer)
	after, _ = store.Snapshot(ctx)
	if e == nil || out.Code != api.CodeRejected || before != after {
		t.Fatal("unreceipted next action bypassed", out, e)
	}
	ack(store.Commit(ctx, next))
	want.Observe("Control.SettleActivity")
	if err := want.Advance("answer", 2, 1, "query/v1", "answered", querySourceIdentity(source), "", "", "", true); err != nil {
		t.Fatal(err)
	}
	answerAck := work(answer)
	want.Observe("Work.query_answer")
	want.Observe("Control.AnswerQuery")
	compareQueryEventFacts(t, store, want)
	compare("answer", nil, want.Progress)
	// This is a true Query-only completion: no Task was ever started, so the
	// final AnswerQuery is valid without manufacturing a TaskOutcome.
	finalState, e := store.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "space", Principal: "owner"})
	if e != nil {
		t.Fatal(e)
	}
	if len(finalState.Snapshot.Tasks) != 0 || len(finalState.Snapshot.Executions) != 0 || len(finalState.Snapshot.Steps) != 0 {
		t.Fatal("query-only answer crossed into the task lifecycle")
	}
	firstAnchor := model.SnapshotAnchor{Space: "space", Fence: openAck.Fence}
	compare("historical open", &firstAnchor, initialProgress)
	other := firstRead
	other.Principal = "other"
	private, e := store.GetQueryProgress(ctx, other)
	if e != nil || private.Found {
		t.Fatal("query leaked to another principal", private, e)
	}
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
	compare("reopened", nil, want.Progress)
	compareRecordedRequestBytes(t, store, source, inputID, input)
	compareQueryEventFacts(t, store, want)
	compare("reopened historical", &firstAnchor, initialProgress)
	if got := work(open); got != openAck {
		t.Fatal("open receipt changed")
	}
	if got := work(decision); got != decisionAck {
		t.Fatal("decision receipt changed")
	}
	if got := work(answer); got != answerAck {
		t.Fatal("answer receipt changed")
	}
}

func compareQueryEventFacts(t *testing.T, store *ate.Store, expected *oracle.QueryController) {
	t.Helper()
	ctx := context.Background()
	v, err := store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "owner", Mode: "audit_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	page, err := store.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil {
		t.Fatal(err)
	}
	var queryEvents []model.Event
	all := map[model.EventID]model.Event{}
	for _, node := range page.Items {
		detail, e := store.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: node.Ref})
		if e != nil {
			t.Fatal(e)
		}
		var event model.Event
		if e := json.Unmarshal(detail.Data, &event); e != nil {
			t.Fatal(e)
		}
		all[event.ID] = event
		if event.Attribution != nil && event.Attribution.Query == "query" {
			queryEvents = append(queryEvents, event)
		}
	}
	sort.Slice(queryEvents, func(i, j int) bool { return queryEvents[i].Sequence < queryEvents[j].Sequence })
	facts := []oracle.QueryFact{}
	ops := map[string]model.Event{}
	for i, event := range queryEvents {
		a := event.Attribution
		if event.Space != "space" || event.RecordedBy != "owner" || event.Stream.Kind != "query" || event.Stream.Owner != "query" || event.Sequence != uint64(i+1) || a.Session != "session" || a.Execution != "" || a.InitiatingActor != "" || a.Step != 0 {
			t.Fatalf("query fact invented an execution or lost scope: %+v", event)
		}
		if i == 0 {
			if event.PreviousInStream != "" {
				t.Fatal("first Query fact has predecessor")
			}
		} else if event.PreviousInStream != queryEvents[i-1].ID {
			t.Fatal("query stream does not retain publication order")
		}
		facts = append(facts, oracle.QueryFact{Operation: a.Operation})
		ops[a.Operation] = event
	}
	if !reflect.DeepEqual(facts, expected.Facts) {
		t.Fatalf("query facts differ from accepted operations: actual=%+v expected=%+v", facts, expected.Facts)
	}
	for op, cause := range map[string]string{"Control.ClaimAttempt": "Control.CreateActivity", "Control.DispatchAttempt": "Control.ClaimAttempt", "Control.CaptureResult": "Control.DispatchAttempt", "Control.AcceptActivity": "Control.CaptureResult", "Control.SettleActivity": "Control.AcceptActivity"} {
		event, parent := ops[op], ops[cause]
		if event.ID == "" {
			continue
		}
		found := false
		for _, id := range event.CausalParents {
			found = found || id == parent.ID
		}
		if !found {
			t.Fatalf("%s lacks semantic cause %s", op, cause)
		}
	}
	// The transaction's core occurrence authenticates every typed fact exactly
	// once, including both facts of one atomic Work transition.
	for _, event := range all {
		if event.Attribution != nil {
			continue
		}
		for i, id := range event.ProfileEvents {
			fact, exists := all[id]
			if !exists || fact.Transaction != event.Transaction || fact.Attribution == nil || fact.Attribution.Ordinal != uint64(i+1) {
				t.Fatal("core manifest lost typed fact identity or ordinal")
			}
		}
	}
}

func independentBytesDigest(data []byte) model.ObjectID {
	sum := sha256.Sum256(data)
	return model.ObjectID(hex.EncodeToString(sum[:]))
}

func dispatchQueryActivity(t *testing.T, store *ate.Store, want *oracle.QueryController, id string, spec agentprofile.ActivitySpec, attempt string, evidence model.ObjectID, requestBytes int) {
	t.Helper()
	ctx := context.Background()
	grant := api.EmissionSpec{
		ID: id + "-grant", Query: spec.Query, GoalEpoch: spec.GoalEpoch,
		Content: spec.Request, Anchor: model.SnapshotAnchor{Space: "space", Fence: spec.InputFence},
		Projection: "value_bytes", Length: uint64(requestBytes), Destination: spec.Recipient,
		Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest,
	}
	if spec.ContextPacket != "" {
		grant.Content, grant.ContentKind = spec.ContextPacket, "packet"
	}
	issued, err := store.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: key(id + "-issue"), Command: api.EmissionCommand{Issue: &grant}})
	if err != nil || issued.Code != api.CodeDurableAck {
		t.Fatalf("issue %s: %+v %v", id, issued, err)
	}
	want.Observe("Emission.Issue")
	claim, err := api.EmissionWorkStep(api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	command := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: attempt, Evidence: evidence}}
	dispatch, err := api.AgentWorkStep(agentprofile.Command{Control: &command})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := store.ApplyWork(ctx, api.ApplyWorkRequest{Key: key(id), Unit: api.WorkUnit{Kind: "emission_dispatch", Query: spec.Query, Steps: []api.WorkStep{claim, dispatch}}})
	if err != nil || committed.Code != api.CodeDurableAck {
		t.Fatalf("dispatch %s: %+v %v", id, committed, err)
	}
	want.Observe("Work.emission_dispatch")
	want.Observe("Emission.Claim")
	want.Observe("Control.DispatchAttempt")
}

// The host renderer's exact provider request is persisted as bytes. Verify
// byte ranges against the original transcript, independently of Packet IDs or
// any production encoder, before and after compaction/reopen.
func compareRecordedRequestBytes(t *testing.T, store *ate.Store, source model.SnapshotAnchor, id model.ObjectID, input []byte) {
	t.Helper()
	ctx := context.Background()
	view, err := store.OpenView(ctx, api.OpenViewRequest{Space: source.Space, Principal: "owner", Anchor: &source})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	for _, size := range []uint32{1, 7, 256} {
		for offset := 0; offset < len(input); offset += int(size) {
			end := offset + int(size)
			if end > len(input) {
				end = len(input)
			}
			got, err := store.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: id}, Projection: "value_bytes", Offset: uint64(offset), Length: size})
			if err != nil || !bytes.Equal(got.Data, input[offset:end]) || got.TotalBytes != uint64(len(input)) || got.Digest != independentBytesDigest(input) || got.EOF != (end == len(input)) {
				t.Fatalf("recorded input range %d/%d changed: %+v %v", offset, size, got, err)
			}
		}
	}
}
