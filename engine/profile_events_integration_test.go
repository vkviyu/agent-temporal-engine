package engine_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func readProfileOccurrences(t *testing.T, s *integrationStore, principals ...model.PrincipalID) []model.Event {
	t.Helper()
	ctx := context.Background()
	principal := model.PrincipalID("host-principal")
	if len(principals) > 0 {
		principal = principals[0]
	}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: v.Ref, Layer: "events", EntityKind: "Event"})
	if err != nil {
		t.Fatal(err)
	}
	var events []model.Event
	for _, item := range page.Items {
		d, err := s.e.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: item.Ref})
		if err != nil {
			t.Fatal(err)
		}
		var e model.Event
		if err := json.Unmarshal(d.Data, &e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	return events
}

func TestProfileEventsBindWorkSubjectsStreamsAndReplayIdentity(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	intake, start := workIntake(t, s)
	intakeACK := workApply(t, s, intake)
	before := readProfileOccurrences(t, s)
	intakeEvents := 0
	for _, e := range before {
		if e.Transaction != intakeACK.TxID {
			continue // the fixture first publishes exact Query/Goal source bytes
		}
		intakeEvents++
		if e.Attribution != nil && (e.Attribution.Query != "query" || e.Attribution.Session != "session" || e.Attribution.Execution != "" || e.Attribution.InitiatingActor != "" || e.Stream.Kind != "query") {
			t.Fatalf("query intake forged agent attribution: %+v", e)
		}
	}
	if intakeEvents != 2 {
		t.Fatalf("intake transaction did not publish one core plus one Query fact: %+v", before)
	}
	seedSubjects := map[string]bool{}
	for _, step := range start.Unit.Steps {
		if step.Method == api.MethodApplyKnowledge {
			c, err := knowledge.DecodeCommand(step.Body)
			if err != nil || c.Observation == nil {
				t.Fatal("fixture must explicitly seed Query/Goal observations", err)
			}
			seedSubjects[string(c.Observation.ID)] = false
		}
	}
	ack := workApply(t, s, start)
	all := readProfileOccurrences(t, s)
	var manifest *model.Event
	facts := map[model.EventID]model.Event{}
	for i, e := range all {
		if e.Transaction == ack.TxID {
			if e.Attribution == nil {
				v := all[i]
				manifest = &v
			} else {
				facts[e.ID] = e
			}
		}
	}
	if manifest == nil || manifest.SchemaVersion != 2 || manifest.ProfileRegistryVersion != eventlog.ProfileRegistryVersion || len(manifest.ProfileEvents) != len(start.Unit.Steps)+1 || len(facts) != len(start.Unit.Steps)+1 {
		t.Fatalf("Work event manifest=%+v facts=%+v", manifest, facts)
	}
	streams := map[string]bool{}
	ordinals := map[uint64]bool{}
	for _, id := range manifest.ProfileEvents {
		e, ok := facts[id]
		if !ok || ordinals[e.Attribution.Ordinal] || e.RecordedBy != "host-principal" {
			t.Fatal("manifest omitted/duplicated or spoofed occurrence")
		}
		ordinals[e.Attribution.Ordinal] = true
		streams[e.Stream.Kind] = true
		if e.Attribution.Execution != "" && (e.Attribution.Query != "query" || e.Attribution.Session != "session") {
			t.Fatalf("execution fact lost origin: %+v", e)
		}
		if e.Attribution.Operation == "Control.Checkpoint" && e.Attribution.InitiatingActor != "driver" {
			t.Fatal("checked worker transition lost initiating actor")
		}
		if e.Attribution.Operation == "Control.GrantAuthority" && e.Attribution.InitiatingActor != "" {
			t.Fatal("host grant attributed to target actor")
		}
		if e.Attribution.Operation == "Knowledge.Observation" {
			key := string(e.Attribution.Subject.ID)
			if _, exists := seedSubjects[key]; !exists || e.Attribution.Subject.Profile != "knowledge" || e.Attribution.Subject.Kind != "Observation" {
				t.Fatalf("initial Knowledge fact lost its actual seed subject: %+v", e)
			}
			seedSubjects[key] = true
		}
	}
	for id, seen := range seedSubjects {
		if !seen {
			t.Fatalf("missing seed observation fact %s", id)
		}
	}
	if !streams["execution"] || !streams["query"] {
		t.Fatal("Work flattened distinct execution/query streams")
	}
	if again := workApply(t, s, start); again != ack {
		t.Fatal("retry changed receipt")
	}
	if after := readProfileOccurrences(t, s); !reflect.DeepEqual(after, all) {
		t.Fatal("retry generated occurrences")
	}
	s.reopen()
	if after := readProfileOccurrences(t, s); !reflect.DeepEqual(after, all) {
		t.Fatal("replay changed occurrence identity")
	}
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	if after := readProfileOccurrences(t, s); !reflect.DeepEqual(after, all) {
		t.Fatal("compaction changed occurrence identity")
	}
}

func TestProfileAttemptCausalityIsDistinctFromStreamOrder(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	task := agentTask("cause-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("cause-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "cause-execution", Task: task.ID, Actor: "actual-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("cause-exec"), Command: agentprofile.Command{Spawn: &x}})
	input, fence := recoveryCoreContent(t, s)
	authority := agentprofile.AuthorityBinding{Deployment: "host", Holder: "runner", OwnerEpoch: 1, Incarnation: 1}
	proof := model.ObjectID(integrationID('a'))
	recoveryCoreControl(t, s, "cause-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: authority, Status: "active", Evidence: proof}}})
	_, request := recoveryValueAt(t, s, fence, "checkpoint")
	spec := agentprofile.ActivitySpec{Activity: "cause-activity", Execution: x.ID, Role: "resource_prepare", GoalEpoch: 1, InputFence: fence, Request: input, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	recoveryCoreControl(t, s, "cause-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: authority}})
	recoveryCoreControl(t, s, "cause-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "cause-attempt", Sequence: 1, Authority: authority}})
	recoveryCoreControl(t, s, "cause-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "cause-attempt", Authority: authority, Evidence: proof}})
	result := agentprofile.ActivityResult{Capture: "full", ID: "cause-result", Activity: spec.Activity, Attempt: "cause-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("done")}
	recoveryCoreControl(t, s, "cause-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreControl(t, s, "cause-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: authority, Evidence: proof}})
	facts := map[string]model.Event{}
	for _, e := range readProfileOccurrences(t, s) {
		if e.Attribution != nil {
			facts[e.Attribution.Operation] = e
		}
	}
	chain := []string{"Control.CreateActivity", "Control.ClaimAttempt", "Control.DispatchAttempt", "Control.CaptureResult", "Control.AcceptActivity"}
	for i, op := range chain {
		e := facts[op]
		if e.Attribution == nil || e.Attribution.Execution != x.ID || e.RecordedBy != "host-principal" {
			t.Fatalf("invalid attribution %s: %+v", op, e)
		}
		wantActor := x.Actor
		if op == "Control.CaptureResult" {
			wantActor = ""
		}
		if e.Attribution.InitiatingActor != wantActor {
			t.Fatalf("actor/collector attribution mismatch %s: %+v", op, e)
		}
		if i > 0 && (len(e.CausalParents) != 1 || e.CausalParents[0] != facts[chain[i-1]].ID) {
			t.Fatalf("missing exact parent for %s: %+v", op, e)
		}
	}
	accept := facts["Control.AcceptActivity"]
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	for _, relation := range []string{"causal_parent", "stream_previous"} {
		trace, err := s.e.Trace(ctx, api.TraceRequest{View: v.Ref, Start: model.NodeRef{Kind: model.NodeEvent, Event: accept.ID}, Relations: []string{relation}, Limits: api.QueryLimits{MaxDepth: 1}})
		if err != nil || len(trace.Edges) != 1 || trace.Edges[0].Relation != relation {
			t.Fatalf("%s relation lost/merged: %+v %v", relation, trace, err)
		}
	}
	if facts["Control.CaptureResult"].Kind != eventlog.KindAgentTransition {
		t.Fatal("unregistered event kind")
	}
}
