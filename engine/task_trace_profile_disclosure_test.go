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

func traceDisclosureWithdraw(t *testing.T, s *integrationStore, id model.ObjectID, kind memory.RetractionKind) {
	t.Helper()
	if id == "" {
		t.Fatal("missing withdrawal dependency")
	}
	applyMemory(t, s.e, "trace-profile-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{
		Source: id, Retraction: memory.Retraction{Kind: kind, Reason: "withdraw after warming trace projection"},
	}})
}

// The endpoints below deliberately exclude earlier Activity/Attempt/Result
// events that could authorize the same input through a different branch.
func TestTaskTraceProfileDisclosureCausalBranches(t *testing.T) {
	for _, branch := range []struct {
		name, from, to string
		kinds          []model.EntityKind
	}{
		{"Activity", "Work.query_intake", "Control.CreateActivity", []model.EntityKind{"Query", "Activity"}},
		{"Attempt", "Control.ClaimAttempt", "Control.DispatchAttempt", []model.EntityKind{"Attempt", "Attempt"}},
		{"ActivityAcceptance", "Control.AcceptActivity", "Control.SettleActivity", []model.EntityKind{"ActivityAcceptance", "ActivitySettlement"}},
		{"QueryAnswer", "Control.CompleteTask", "Control.AnswerQuery", []model.EntityKind{"TaskOutcome", "QueryAnswer"}},
	} {
		for _, source := range []string{"request", "packet", "final-input", "final-outcome-result", "final-outcome-validation"} {
			// The QueryAnswer path starts at TaskOutcome. Withdrawing that
			// outcome would not isolate QueryAnswer's own provenance.
			if branch.name == "QueryAnswer" && (source == "final-outcome-result" || source == "final-outcome-validation") {
				continue
			}
			for _, kind := range []memory.RetractionKind{memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
				t.Run(branch.name+"/"+source+"/"+string(kind), func(t *testing.T) {
					ctx := context.Background()
					s, spec, input := finalAnswerFixture(t)
					spec.Activity = "disclosure-final"
					recoveryCoreControl(t, s, "disclosure-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})
					recoveryCoreControl(t, s, "disclosure-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "disclosure-attempt", Sequence: 1}})
					recoveryCoreControl(t, s, "disclosure-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "disclosure-attempt", Evidence: input.Request}})
					result := agentprofile.ActivityResult{ID: "disclosure-result", Activity: spec.Activity, Attempt: "disclosure-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Capture: "full", Outcome: "success", Payload: []byte("final answer from recorded inputs")}
					recoveryCoreControl(t, s, "disclosure-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
					recoveryCoreControl(t, s, "disclosure-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: input.Request}})
					recoveryCoreControl(t, s, "disclosure-settle", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: spec.Activity, Status: "settled", Evidence: input.Request}})
					recoveryCoreControl(t, s, "disclosure-answer", agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: "query", Slot: "final", Kind: "final", Final: true, ExpectedVersion: 1, Body: result.Payload, GeneratedFrom: result.ID, Outcomes: input.Outcomes, InputEvidence: spec.FinalAnswerInput}})

					anchor := model.SnapshotAnchor{Space: "host-space", Fence: integrationSnapshot(t, s.e).Fence}
					req := api.TaskTraceEvidenceRequest{TaskTraceRequest: api.TaskTraceRequest{
						Space: "host-space", Principal: "host-principal", Subject: api.TraceSubject{Kind: "query", ID: "query"}, Session: "session", Anchor: &anchor, Limit: 1,
					}, Relation: "causal"}
					for _, event := range readProfileOccurrences(t, s) {
						if event.Attribution == nil {
							continue
						}
						switch event.Attribution.Operation {
						case branch.from:
							req.From = event.ID
						case branch.to:
							req.To = event.ID
						}
					}
					if req.From == "" || req.To == "" {
						t.Fatal("missing API-produced causal endpoints")
					}
					control, err := s.e.GetTaskTrace(ctx, req.TaskTraceRequest)
					if err != nil || len(control.Nodes) != 1 || control.Nodes[0].Operation != "Work.query_intake" {
						t.Fatalf("unrelated intake page: %+v %v", control, err)
					}
					fullReq := req
					fullReq.Limit = 128
					full, err := s.e.GetTaskTraceEvidence(ctx, fullReq)
					if err != nil || len(full.Events) != 2 || full.Continuation != "" {
						t.Fatalf("expected isolated two-event path: %+v %v", full, err)
					}
					for i, node := range full.Events {
						if node.Subject.Profile != "agent" || node.Subject.Kind != branch.kinds[i] || node.Boundary {
							t.Fatalf("unexpected attribution on isolated path: %+v", full)
						}
					}
					first, err := s.e.GetTaskTraceEvidence(ctx, req)
					if err != nil || len(first.Events) != 1 || first.Events[0].At.Ref.Event != req.From || first.Continuation == "" {
						t.Fatalf("first evidence page: %+v %v", first, err)
					}
					resumed := req
					resumed.Continuation = first.Continuation
					last, err := s.e.GetTaskTraceEvidence(ctx, resumed)
					if err != nil || len(last.Events) != 1 || last.Events[0].At.Ref.Event != req.To || last.Continuation != "" {
						t.Fatalf("resumed evidence page: %+v %v", last, err)
					}
					if again, err := s.e.GetTaskTraceEvidence(ctx, resumed); err != nil || !reflect.DeepEqual(again, last) {
						t.Fatalf("warm evidence changed: %+v %v", again, err)
					}

					// This unrelated commit preserves the agent profile object.
					// The second anchor has never had an evidence read: after
					// withdrawal it must rebuild dependencies from the warm
					// profile, rather than reuse the old evidence cache entry.
					advanced := integrationMutate(t, s.e, integrationRequest("disclosure-advance",
						integrationPut([]byte("disclosure-unrelated"), integrationBytes('z'))))
					otherAnchor := model.SnapshotAnchor{Space: req.Space, Fence: advanced.Fence}
					uncached := req
					uncached.Anchor = &otherAnchor
					withdraw := map[string]model.ObjectID{
						"request": spec.Request, "packet": spec.ContextPacket,
						"final-input":              spec.FinalAnswerInput,
						"final-outcome-result":     input.Outcomes[0].Result,
						"final-outcome-validation": input.Outcomes[0].Validation,
					}[source]
					traceDisclosureWithdraw(t, s, withdraw, kind)
					for _, stage := range []string{"warm", "compacted", "reopened"} {
						switch stage {
						case "compacted":
							if _, err := s.e.CompactAuditFull(ctx); err != nil {
								t.Fatal(err)
							}
						case "reopened":
							s.reopen()
						}
						for _, read := range []struct {
							name string
							req  api.TaskTraceEvidenceRequest
						}{{"new-evidence-key", uncached}, {"first-page", req}, {"saved-continuation", resumed}} {
							out, err := s.e.GetTaskTraceEvidence(ctx, read.req)
							if !errors.Is(err, memory.ErrHistoryAccessDenied) || !reflect.DeepEqual(out, api.TaskTraceEvidenceResult{}) {
								t.Fatalf("%s/%s disclosed withdrawn %s: %+v %v", stage, read.name, source, out, err)
							}
						}
						if out, err := s.e.GetTaskTrace(ctx, req.TaskTraceRequest); err != nil || !reflect.DeepEqual(out, control) {
							t.Fatalf("%s withdrawal affected unrelated intake page: %+v %v", stage, out, err)
						}
					}
				})
			}
		}
	}
}

// CaptureResult normally leads to ActivityAcceptance, which shares its input
// dependencies. A real checkpoint Work's outer occurrence carries the first
// step's ActivityResult attribution and has no causal parents. Selecting that
// occurrence tests result -> activity lookup without an acceptance masking it.
func TestTaskTraceProfileDisclosureActivityResult(t *testing.T) {
	for _, source := range []string{"request", "packet", "packet-source", "definition"} {
		for _, kind := range []memory.RetractionKind{memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
			t.Run(source+"/"+string(kind), func(t *testing.T) {
				ctx := context.Background()
				s, payload, fence, authority := emissionFixture(t, engine.Options{AgentProfile: true, EventProfile: true})
				spec := agentprofile.ActivitySpec{Activity: "disclosure-activity", Execution: "emission-exec", Role: "resource_prepare", GoalEpoch: 1, Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
				var version, step uint64
				var packetSource model.ObjectID
				if source == "definition" {
					recoveryCoreControl(t, s, "disclosure-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: agentprofile.Step{Execution: spec.Execution, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: fence}, Authority: authority}})
					inv := agentprofile.Invocation{ID: "disclosure-call", Execution: spec.Execution, Step: 1, ProviderCallKey: "call", Tool: "read", Status: agentprofile.InvocationPending, Attempt: 1}
					bindTestInvocation(t, s, &inv, authority, "external")
					commitTestInvocation(t, s, "disclosure-decision", inv, authority)
					spec.Role, spec.Step, spec.Invocation = "external_tool", inv.Step, inv.ID
					definition := testPureToolDefinition(inv.Tool, "test-tool/v1")
					spec.Contract, spec.Recipient = definition.BindEffect(inv.ToolDefinition), definition.Contract.Recipient
					version, step = 1, 1
				}
				request := []byte("distinct exact activity request")
				published := integrationMutate(t, s.e, integrationRequest("disclosure-request",
					integrationPut([]byte("disclosure-request"), api.Value{Kind: api.ValueBytes, Data: request})))
				spec.Request, _ = recoveryValueAt(t, s, published.Fence, "disclosure-request")
				spec.InputFence, spec.RequestDigest = published.Fence, agentprofile.ExactRequestDigest(request)
				if source != "definition" {
					// The receiving execution must already exist at the
					// observation's source cut, unlike the fixture's first fence.
					observation := knowledgeObservation(payload, published.Fence, spec.Execution, 1)
					observation.Key.Actor = "emitter"
					observation.ID = knowledge.ObservationID(observation)
					applyObservation(t, s.e, "disclosure-observe", observation)
					packet, ack := goalCorePacket(t, s, "disclosure-packet", observation.Key, observation, []knowledge.Observation{observation}, spec.Request, published.Fence)
					spec.ContextPacket, spec.InputFence, packetSource = packet.ID, ack.Fence, observation.Payload
				}
				recoveryCoreControl(t, s, "disclosure-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: authority}})
				recoveryCoreControl(t, s, "disclosure-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "disclosure-attempt", Sequence: 1, Authority: authority}})
				recoveryCoreControl(t, s, "disclosure-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "disclosure-attempt", Evidence: payload, Authority: authority}})
				result := agentprofile.ActivityResult{ID: "disclosure-result", Activity: spec.Activity, Attempt: "disclosure-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Capture: "full", Outcome: "success", Payload: []byte("captured output")}
				work := api.ApplyWorkRequest{Key: agentKey("disclosure-capture-work"), Unit: api.WorkUnit{Kind: "checkpoint", Steps: []api.WorkStep{
					workControl(t, agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}}),
					workCheckpoint(t, spec.Execution, authority, integrationSnapshot(t, s.e).Fence, "disclosure-next", version, step),
				}}}
				captured := workApply(t, s, work)
				var target model.Event
				for _, event := range readProfileOccurrences(t, s) {
					if event.Transaction == captured.TxID && event.Attribution != nil && event.Attribution.Operation == "Work.checkpoint" {
						target = event
					}
				}
				if target.ID == "" || target.Attribution.Subject.Kind != "ActivityResult" || string(target.Attribution.Subject.ID) != result.ID || len(target.CausalParents) != 0 {
					t.Fatalf("checkpoint did not produce an isolated result occurrence: %+v", target)
				}
				// The milestone citation is independent of every withdrawn ID.
				// Citing the result event itself would add another disclosure path.
				citationAck := integrationMutate(t, s.e, integrationRequest("disclosure-label-evidence",
					integrationPut([]byte("disclosure-label-evidence"), api.Value{Kind: api.ValueBytes, Data: []byte("host selected the captured-result checkpoint")})))
				citationID, _ := recoveryValueAt(t, s, citationAck.Fence, "disclosure-label-evidence")
				anchor := model.SnapshotAnchor{Space: "host-space", Fence: citationAck.Fence}
				req := api.TaskTraceRequest{Space: "host-space", Principal: "host-principal", Subject: api.TraceSubject{Kind: "task", ID: "emission-task"}, Limit: 1}
				edit := api.TaskTraceEdit{Subject: req.Subject, Source: anchor, Milestones: []api.TraceMilestone{{
					Event: target.ID, Label: "Captured result", Evidence: []api.TraceCitation{{Ref: model.NodeRef{Kind: model.NodeObject, Object: citationID}, Anchor: anchor}},
				}}}
				ack, err := s.e.ApplyTaskTrace(ctx, api.ApplyTaskTraceRequest{Key: agentKey("disclosure-trace-edit"), Edit: edit})
				if err != nil || ack.Code != api.CodeDurableAck {
					t.Fatalf("select result milestone: %+v %v", ack, err)
				}
				anchor.Fence = ack.Fence
				req.Anchor = &anchor
				var before api.TaskTraceResult
				for i := 0; i < 128; i++ {
					before, err = s.e.GetTaskTrace(ctx, req)
					if err != nil || len(before.Nodes) != 1 {
						t.Fatalf("find result page: %+v %v", before, err)
					}
					if before.Nodes[0].At.Ref.Event == target.ID {
						break
					}
					if before.Continuation == "" {
						t.Fatal("result milestone missing")
					}
					req.Continuation = before.Continuation
				}
				if len(before.Nodes) != 1 || before.Nodes[0].At.Ref.Event != target.ID || len(before.Edges) != 0 {
					t.Fatalf("result page contains another disclosure path: %+v", before)
				}
				if again, err := s.e.GetTaskTrace(ctx, req); err != nil || !reflect.DeepEqual(again, before) {
					t.Fatalf("warm result projection changed: %+v %v", again, err)
				}
				withdraw := map[string]model.ObjectID{"request": spec.Request, "packet": spec.ContextPacket, "packet-source": packetSource, "definition": spec.Contract.Definition}[source]
				if withdraw == citationID || (source == "packet-source" && withdraw == spec.Request) {
					t.Fatal("withdrawal is not independent of citation/request")
				}
				traceDisclosureWithdraw(t, s, withdraw, kind)
				for _, stage := range []string{"warm", "compacted", "reopened"} {
					switch stage {
					case "compacted":
						if _, err := s.e.CompactAuditFull(ctx); err != nil {
							t.Fatal(err)
						}
					case "reopened":
						s.reopen()
					}
					out, err := s.e.GetTaskTrace(ctx, req)
					if !errors.Is(err, memory.ErrHistoryAccessDenied) || !reflect.DeepEqual(out, api.TaskTraceResult{}) {
						t.Fatalf("%s result projection disclosed withdrawn %s: %+v %v", stage, source, out, err)
					}
				}
			})
		}
	}
}
