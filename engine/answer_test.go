package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func finalAnswerFixture(t *testing.T) (*integrationStore, agentprofile.ActivitySpec, agentprofile.FinalAnswerInput) {
	t.Helper()
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	intake, start := workIntake(t, s)
	workApply(t, s, intake)
	workApply(t, s, start)
	settleTestWorkCheckpoints(t, s, start)
	put := func(name string, data []byte) (model.ObjectID, api.CommitResult) {
		t.Helper()
		ack := integrationMutate(t, s.e, integrationRequest(name, integrationPut([]byte(name), api.Value{Kind: api.ValueBytes, Data: data})))
		id, _ := recoveryValueAt(t, s, ack.Fence, name)
		return id, ack
	}
	artifact, _ := put("completed-artifact", []byte("validated exact artifact"))
	authority := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	recoveryCompleteTask(t, s, "completed-task", agentprofile.CompleteTaskCommand{Task: "intake-task", Execution: "intake-root", Epoch: 1, Result: artifact, Authority: authority})
	agent, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	request := []byte("exact final provider wire request")
	input, published := put("final-provider-request", request)
	packet := knowledge.ContextPacket{Space: "host-space", Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: published.Fence}}, Renderer: "final-renderer/v1", Tokenizer: "fixture/v1", Input: &knowledge.RecordedInput{Object: input, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: published.Fence}, Encoding: "provider/json"}}
	packet.ID = knowledge.PacketID(packet)
	ack, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("final-provider-packet"), Command: knowledge.Command{Packet: &packet}})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatal(ack, err)
	}
	witness := agentprofile.FinalAnswerInput{SchemaVersion: 1, Query: "query", Outcomes: agent.Snapshot.Lifecycle.Recovery.Outcomes, ContextPacket: packet.ID, Request: input, RequestDigest: agentprofile.ExactRequestDigest(request), HostPolicyVersion: packet.Renderer}
	raw, err := agentprofile.EncodeFinalAnswerInput(witness)
	if err != nil {
		t.Fatal(err)
	}
	evidence, evidenceAck := put("final-input-witness", raw)
	spec := agentprofile.ActivitySpec{Query: "query", Role: "query_decision", GoalEpoch: 1, InputFence: evidenceAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: witness.RequestDigest, Recipient: "model", Effect: "pure", ResourceScope: "final-answer", FinalAnswerInput: evidence}
	return s, spec, witness
}

func TestFinalAnswerWitnessCannotChangeSourceCutOrAcceptFailedModel(t *testing.T) {
	ctx := context.Background()
	s, spec, input := finalAnswerFixture(t)
	for _, name := range []string{"wrong-query", "changed-outcome", "wrong-renderer", "unpublished-at-source"} {
		t.Run(name, func(t *testing.T) {
			witness := input
			witness.Outcomes = append([]agentprofile.TaskOutcome(nil), input.Outcomes...)
			switch name {
			case "wrong-query":
				witness.Query = "another-query"
			case "changed-outcome":
				witness.Outcomes[0].Result = input.Request
			case "wrong-renderer", "unpublished-at-source":
				witness.HostPolicyVersion = "unrecognized-renderer/v1"
			}
			raw, err := agentprofile.EncodeFinalAnswerInput(witness)
			if err != nil {
				t.Fatal(err)
			}
			published := integrationMutate(t, s.e, integrationRequest(name+"-witness", integrationPut([]byte(name), api.Value{Kind: api.ValueBytes, Data: raw})))
			id, _ := recoveryValueAt(t, s, published.Fence, name)
			candidate := spec
			candidate.Activity, candidate.FinalAnswerInput, candidate.InputFence = name, id, published.Fence
			if name == "unpublished-at-source" {
				candidate.InputFence = spec.InputFence
			}
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey(name), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: candidate}}}})
			if err == nil || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("final model accepted invalid input evidence: %+v %v", out, err)
			}
		})
	}
	spec.Activity = "failed-final-model"
	recoveryCoreControl(t, s, "failed-final-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})
	recoveryCoreControl(t, s, "failed-final-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "failed-final-attempt", Sequence: 1}})
	recoveryCoreControl(t, s, "failed-final-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "failed-final-attempt", Evidence: input.Request}})
	result := agentprofile.ActivityResult{Capture: "full", ID: "accepted-failed-final-result", Activity: spec.Activity, Attempt: "failed-final-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "failure", Payload: []byte("provider failed")}
	recoveryCoreControl(t, s, "failed-final-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreControl(t, s, "failed-final-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: input.Request}})
	answer := agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: "query", Slot: "final", Kind: "final", Final: true, ExpectedVersion: 1, Body: result.Payload, GeneratedFrom: result.ID, Outcomes: input.Outcomes, InputEvidence: spec.FinalAnswerInput}}
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("answer-from-accepted-failure"), Command: agentprofile.Command{Control: &answer}})
	if !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatalf("accepted failure became a successful final response: %+v %v", out, err)
	}
}

func TestFinalAnswerWitnessWithdrawalGatesClaimDispatchAndPublication(t *testing.T) {
	for _, source := range []string{"artifact", "validation"} {
		for _, kind := range []memory.RetractionKind{memory.RetractionUseBan, memory.RetractionPrivacyErasure} {
			t.Run(source+"/"+string(kind), func(t *testing.T) {
				ctx := context.Background()
				s, spec, input := finalAnswerFixture(t)
				for _, activity := range []string{"before-claim", "before-dispatch", "before-answer"} {
					candidate := spec
					candidate.Activity = activity
					recoveryCoreControl(t, s, activity+"-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: candidate}})
				}
				for _, activity := range []string{"before-dispatch", "before-answer"} {
					recoveryCoreControl(t, s, activity+"-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: activity, Attempt: activity + "-attempt", Sequence: 1}})
				}
				recoveryCoreControl(t, s, "answer-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "before-answer-attempt", Evidence: input.Request}})
				result := agentprofile.ActivityResult{Capture: "full", ID: "final-response", Activity: "before-answer", Attempt: "before-answer-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("accepted final body")}
				recoveryCoreControl(t, s, "answer-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
				recoveryCoreControl(t, s, "answer-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: result.Activity, Result: result.ID, ExpectedVersion: 1, Evidence: input.Request}})
				grant := api.EmissionSpec{ID: "final-dispatch-grant", Query: "query", Activity: "before-dispatch", Attempt: "before-dispatch-attempt", GoalEpoch: 1, Content: spec.ContextPacket, ContentKind: "packet", Anchor: model.SnapshotAnchor{Space: "host-space", Fence: spec.InputFence}, Length: uint64(len("exact final provider wire request")), Destination: spec.Recipient, RequestDigest: spec.RequestDigest}
				issueExactGrant(t, s, grant)
				withdraw := input.Outcomes[0].Result
				if source == "validation" {
					withdraw = input.Outcomes[0].Validation
				}
				applyMemory(t, s.e, "withdraw-completion-evidence", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: withdraw, Retraction: memory.Retraction{Kind: kind, Reason: "completed result may no longer be used in a reply"}}})
				before := integrationSnapshot(t, s.e)
				denial := memory.ErrUseDenied
				if kind == memory.RetractionPrivacyErasure {
					denial = memory.ErrHistoryAccessDenied
				}
				claim := agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: "before-claim", Attempt: "late-attempt", Sequence: 1}}
				out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("withdrawn-final-claim"), Command: agentprofile.Command{Control: &claim}})
				if out.Code != api.CodeRejected || !errors.Is(err, denial) {
					t.Fatalf("withdrawn final evidence remained claimable: %+v %v", out, err)
				}
				dispatch := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Evidence: input.Request}}
				out, err = s.e.ApplyWork(ctx, exactDispatchWork(t, grant, dispatch, "withdrawn-final-dispatch"))
				if out.Code != api.CodeRejected || !errors.Is(err, denial) {
					t.Fatalf("withdrawn final evidence remained dispatchable: %+v %v", out, err)
				}
				answer := agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: "query", Slot: "final", Kind: "final", Final: true, ExpectedVersion: 1, Body: result.Payload, GeneratedFrom: result.ID, Outcomes: input.Outcomes, InputEvidence: spec.FinalAnswerInput}}
				out, err = s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("withdrawn-final-answer"), Command: agentprofile.Command{Control: &answer}})
				if out.Code != api.CodeRejected || !errors.Is(err, denial) || integrationSnapshot(t, s.e) != before {
					t.Fatalf("withdrawn final evidence escaped through publication: %+v %v", out, err)
				}
				s.reopen()
				out, err = s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("withdrawn-final-after-reopen"), Command: agentprofile.Command{Control: &answer}})
				if out.Code != api.CodeRejected || !errors.Is(err, denial) || integrationSnapshot(t, s.e) != before {
					t.Fatalf("recovery lost final evidence withdrawal: %+v %v", out, err)
				}
			})
		}
	}
}
