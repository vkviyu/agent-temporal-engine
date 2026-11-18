package engine_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Use persisted profile events as an independent oracle for node projection.
// All reads, cache warming, maintenance and withdrawals go through public APIs.
func taskTraceCacheFixture(t *testing.T) (*integrationStore, api.TaskTraceEvidenceRequest, map[model.EventID]model.Event, model.ObjectID) {
	t.Helper()
	ctx := context.Background()
	// Follow finalAnswerFixture's intake and validated completion setup, but
	// accept a real execution activity before completion. The original fixture
	// completes an artifact directly, leaving no causal path from the intake.
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	intake, start := workIntake(t, s)
	workApply(t, s, intake)
	workApply(t, s, start)
	settleTestWorkCheckpoints(t, s, start)
	content := []byte("trace cache activity request and validated artifact")
	ack := integrationMutate(t, s.e, integrationRequest("trace-cache-artifact",
		integrationPut([]byte("trace-cache-artifact"), api.Value{Kind: api.ValueBytes, Data: content})))
	artifact, _ := recoveryValueAt(t, s, ack.Fence, "trace-cache-artifact")
	authority := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	spec := agentprofile.ActivitySpec{
		Activity: "trace-cache-activity", Execution: "intake-root", Role: "resource_prepare",
		GoalEpoch: 1, InputFence: ack.Fence, Request: artifact,
		RequestDigest: agentprofile.ExactRequestDigest(content),
		Recipient:     "executor", Effect: "pure", ResourceScope: "workspace",
	}
	recoveryCoreControl(t, s, "trace-cache-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: authority}})
	recoveryCoreControl(t, s, "trace-cache-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "trace-cache-attempt", Sequence: 1, Authority: authority}})
	recoveryCoreControl(t, s, "trace-cache-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "trace-cache-attempt", Evidence: artifact, Authority: authority}})
	result := agentprofile.ActivityResult{
		ID: "trace-cache-result", Activity: spec.Activity, Attempt: "trace-cache-attempt",
		RequestDigest: spec.RequestDigest, Incarnation: 1, Capture: "full", Outcome: "success", Payload: content,
	}
	recoveryCoreControl(t, s, "trace-cache-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreControl(t, s, "trace-cache-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: artifact, Authority: authority}})
	recoveryCompleteTask(t, s, "trace-cache-complete", agentprofile.CompleteTaskCommand{Task: "intake-task", Execution: spec.Execution, Epoch: 1, Result: artifact, Authority: authority})
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	input := agentprofile.FinalAnswerInput{Outcomes: state.Snapshot.Lifecycle.Recovery.Outcomes}
	req := api.TaskTraceEvidenceRequest{
		TaskTraceRequest: api.TaskTraceRequest{
			Space: "host-space", Principal: "host-principal",
			Subject: api.TraceSubject{Kind: "query", ID: "query"}, Session: "session",
		},
		Relation: "causal",
	}
	trace, err := s.e.GetTaskTrace(context.Background(), req.TaskTraceRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range trace.Nodes {
		switch node.Operation {
		case "Work.query_intake":
			req.From = node.At.Ref.Event
		case "Control.CompleteTask":
			req.To = node.At.Ref.Event
		}
	}
	if req.From == "" || req.To == "" || len(input.Outcomes) == 0 || input.Outcomes[0].Validation == "" {
		t.Fatal("fixture omitted causal endpoints or outcome validation")
	}
	events := make(map[model.EventID]model.Event)
	for _, event := range readProfileOccurrences(t, s) {
		events[event.ID] = event
	}
	req.Anchor = &trace.Anchor
	req.Limit = 1
	// Make the saved anchor historical before the first evidence read.
	integrationMutate(t, s.e, integrationRequest("trace-cache-advance",
		integrationPut([]byte("trace-cache-advance"), integrationBytes('1'))))
	return s, req, events, input.Outcomes[0].Validation
}

func assertTaskTraceCachePage(t *testing.T, req api.TaskTraceEvidenceRequest, page api.TaskTraceEvidenceResult, events map[model.EventID]model.Event) {
	t.Helper()
	if !reflect.DeepEqual(page.Anchor, *req.Anchor) || len(page.Events) != 1 || len(page.Citations) != 0 {
		t.Fatalf("invalid causal page: %+v", page)
	}
	node := page.Events[0]
	event, ok := events[node.At.Ref.Event]
	if !ok || event.Attribution == nil {
		t.Fatalf("page references an unknown profile event: %+v", node)
	}
	a := event.Attribution
	want := api.TaskTraceNode{
		At: api.TraceCitation{
			Ref: model.NodeRef{Kind: model.NodeEvent, Event: event.ID}, Anchor: *req.Anchor,
		},
		Operation: a.Operation, Subject: a.Subject, Execution: a.Execution,
		Task: a.Task, Query: a.Query, Session: a.Session,
	}
	if !reflect.DeepEqual(node, want) {
		t.Fatalf("causal page lost event projection:\ngot  %+v\nwant %+v", node, want)
	}
}

func warmTaskTraceCache(t *testing.T, s *integrationStore, req api.TaskTraceEvidenceRequest, events map[model.EventID]model.Event) api.TaskTraceEvidenceResult {
	t.Helper()
	first, err := s.e.GetTaskTraceEvidence(context.Background(), req)
	if err != nil {
		t.Fatalf("cold causal read from=%s to=%s: %v", req.From, req.To, err)
	}
	assertTaskTraceCachePage(t, req, first, events)
	if first.Events[0].At.Ref.Event != req.From || first.Continuation == "" {
		t.Fatalf("first causal page omitted intake or continuation: %+v", first)
	}
	again, err := s.e.GetTaskTraceEvidence(context.Background(), req)
	if err != nil || !reflect.DeepEqual(again, first) {
		t.Fatalf("warm first page changed: %+v %v; want %+v", again, err, first)
	}
	return first
}

func TestTaskTraceCacheIntegrationPaginationSurvivesCompactionAndReopen(t *testing.T) {
	ctx := context.Background()
	s, req, events, _ := taskTraceCacheFixture(t)
	first := warmTaskTraceCache(t, s, req, events)
	requests := []api.TaskTraceEvidenceRequest{req}
	pages := []api.TaskTraceEvidenceResult{first}
	nodes := append([]api.TaskTraceNode(nil), first.Events...)
	seen := map[model.EventID]bool{req.From: true}
	for next := first.Continuation; next != ""; {
		if len(pages) >= len(events) {
			t.Fatal("causal pagination failed to terminate")
		}
		pageReq := req
		pageReq.Continuation = next
		page, err := s.e.GetTaskTraceEvidence(ctx, pageReq)
		if err != nil {
			t.Fatal(err)
		}
		assertTaskTraceCachePage(t, pageReq, page, events)
		id := page.Events[0].At.Ref.Event
		if seen[id] {
			t.Fatalf("causal pagination repeated event %s", id)
		}
		seen[id] = true
		requests = append(requests, pageReq)
		pages = append(pages, page)
		nodes = append(nodes, page.Events...)
		next = page.Continuation
	}
	if len(pages) < 3 || nodes[len(nodes)-1].At.Ref.Event != req.To {
		t.Fatalf("causal history omitted intermediate or completion events: %+v", nodes)
	}
	fullReq := req
	fullReq.Limit = 128
	full, err := s.e.GetTaskTraceEvidence(ctx, fullReq)
	if err != nil || full.Continuation != "" || !reflect.DeepEqual(full.Events, nodes) {
		t.Fatalf("paged history differs from complete history: %+v %v; want %+v", full, err, nodes)
	}
	for _, stage := range []string{"compacted", "reopened"} {
		if stage == "compacted" {
			if _, err := s.e.CompactAuditFull(ctx); err != nil {
				t.Fatal(err)
			}
		} else {
			s.reopen()
		}
		// Resume the saved continuation first: after reopen this is a cold
		// cache, and no newly issued token may replace the original token.
		for i := 1; i <= len(requests); i++ {
			index := i % len(requests)
			got, err := s.e.GetTaskTraceEvidence(ctx, requests[index])
			if err != nil || !reflect.DeepEqual(got, pages[index]) {
				t.Fatalf("%s original page %d changed: %+v %v; want %+v", stage, index, got, err, pages[index])
			}
		}
	}
}

func TestTaskTraceCacheIntegrationWithdrawalOutsideCurrentPage(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, req, events, validation := taskTraceCacheFixture(t)
			first := warmTaskTraceCache(t, s, req, events)
			req.Continuation = first.Continuation
			before, err := s.e.GetTaskTraceEvidence(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			assertTaskTraceCachePage(t, req, before, events)
			// The validation belongs to the terminal TaskOutcome. This page
			// precedes that outcome, so authorizing only this page is unsafe.
			node := before.Events[0]
			if node.At.Ref.Event == req.To || node.Subject.Kind == "TaskOutcome" || before.Continuation == "" {
				t.Fatalf("fixture did not put outcome validation outside the resumed page: %+v", before)
			}
			applyMemory(t, s.e, "trace-cache-withdraw-validation", memory.Command{
				RetractSource: &memory.RetractSourceCommand{
					Source:     validation,
					Retraction: memory.Retraction{Kind: kind, Reason: "withdraw validation outside the cached evidence page"},
				},
			})
			for _, stage := range []string{"warm", "compacted", "reopened"} {
				switch stage {
				case "compacted":
					if _, err := s.e.CompactAuditFull(ctx); err != nil {
						t.Fatal(err)
					}
				case "reopened":
					s.reopen()
				}
				out, err := s.e.GetTaskTraceEvidence(ctx, req)
				if !errors.Is(err, memory.ErrHistoryAccessDenied) {
					t.Fatalf("%s historical evidence did not honor current withdrawal: %+v %v", stage, out, err)
				}
				if !reflect.DeepEqual(out, api.TaskTraceEvidenceResult{}) {
					t.Fatalf("%s denied evidence returned output: %+v", stage, out)
				}
			}
		})
	}
}

func TestTaskTraceCacheIntegrationContinuationBindsSelectors(t *testing.T) {
	s, req, events, _ := taskTraceCacheFixture(t)
	first := warmTaskTraceCache(t, s, req, events)
	req.Continuation = first.Continuation
	want, err := s.e.GetTaskTraceEvidence(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	assertTaskTraceCachePage(t, req, want, events)
	for _, tc := range []struct {
		name string
		edit func(*api.TaskTraceEvidenceRequest)
	}{
		{"principal", func(r *api.TaskTraceEvidenceRequest) { r.Principal = "other-principal" }},
		{"session", func(r *api.TaskTraceEvidenceRequest) { r.Session = "other-session" }},
		{"omit-session", func(r *api.TaskTraceEvidenceRequest) { r.Session = "" }},
		{"subject", func(r *api.TaskTraceEvidenceRequest) { r.Subject.ID = "other-query" }},
		{"limit", func(r *api.TaskTraceEvidenceRequest) { r.Limit = 2 }},
		{"missing-anchor", func(r *api.TaskTraceEvidenceRequest) { r.Anchor = nil }},
		{"new-anchor", func(r *api.TaskTraceEvidenceRequest) {
			anchor := model.SnapshotAnchor{Space: r.Space, Fence: integrationSnapshot(t, s.e).Fence}
			r.Anchor = &anchor
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := req
			tc.edit(&changed)
			out, err := s.e.GetTaskTraceEvidence(context.Background(), changed)
			// Scope validation may reject before token decoding; only
			// rejection and absence of disclosed content are contractual.
			if err == nil || len(out.Events) != 0 || len(out.Citations) != 0 || out.Continuation != "" {
				t.Fatalf("changed selector accepted the old token or disclosed evidence: %+v %v", out, err)
			}
		})
	}
	got, err := s.e.GetTaskTraceEvidence(context.Background(), req)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("rejected requests changed the original continuation: %+v %v; want %+v", got, err, want)
	}
}

func TestTaskTraceCacheIntegrationExternalSourceBoundary(t *testing.T) {
	ctx := context.Background()
	s, req, events, _ := taskTraceCacheFixture(t)
	req.Subject = api.TraceSubject{Kind: "task", ID: "intake-task"}
	req.Session = ""
	req.From, req.To = "", ""
	var ordered []model.EventID
	for id := range events {
		ordered = append(ordered, id)
	}
	slices.Sort(ordered)
	// Pick a real cross-scope causal edge. Scope is independent of page size:
	// the source must stay a boundary on both cold and cached first pages.
	for _, id := range ordered {
		event := events[id]
		if event.Attribution == nil || event.Attribution.Task != "intake-task" {
			continue
		}
		for _, parent := range event.CausalParents {
			source, found := events[parent]
			if found && source.Attribution != nil && source.Attribution.Task != "intake-task" && source.Attribution.Execution != "intake-root" {
				req.From, req.To = parent, id
				break
			}
		}
		if req.From != "" {
			break
		}
	}
	if req.From == "" {
		t.Fatal("fixture has no external causal source")
	}
	first, err := s.e.GetTaskTraceEvidence(ctx, req)
	if err != nil || len(first.Events) != 1 || first.Events[0].At.Ref.Event != req.From ||
		!first.Events[0].Boundary || first.Events[0].Operation == "" || first.Continuation == "" {
		t.Fatalf("cold boundary page: %+v %v", first, err)
	}
	next := req
	next.Continuation = first.Continuation
	second, err := s.e.GetTaskTraceEvidence(ctx, next)
	if err != nil || len(second.Events) != 1 || second.Events[0].Boundary || second.Events[0].Operation == "" {
		t.Fatalf("cached in-scope page: %+v %v", second, err)
	}
	for _, stage := range []string{"warm", "compacted", "reopened"} {
		if stage == "compacted" {
			if _, err := s.e.CompactAuditFull(ctx); err != nil {
				t.Fatal(err)
			}
		} else if stage == "reopened" {
			s.reopen()
		}
		for i, request := range []api.TaskTraceEvidenceRequest{next, req} {
			want := []api.TaskTraceEvidenceResult{second, first}[i]
			got, err := s.e.GetTaskTraceEvidence(ctx, request)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s boundary pagination changed: %+v %v; want %+v", stage, got, err, want)
			}
		}
	}
}
