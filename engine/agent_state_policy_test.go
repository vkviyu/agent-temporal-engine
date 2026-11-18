package engine_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// The result is not an untracked copy: ActivitySpec explicitly identifies its
// ContextPacket, which in turn identifies the source being withdrawn.
func TestAgentStateHistoricalDisclosureChecksKnownModelProvenance(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionFactualCorrection, memory.RetractionUseBan, memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, payload, source := knowledgeFixture(t)
			o := knowledgeObservation(payload, source, "one", 1)
			applyObservation(t, s.e, "state-policy-adopt", o)
			requestBytes := []byte("model request derived from the explicitly adopted source")
			requestAck := integrationMutate(t, s.e, integrationRequest("state-policy-input", integrationPut([]byte("state-policy-input"), api.Value{Kind: api.ValueBytes, Data: requestBytes})))
			input, _ := recoveryValueAt(t, s, requestAck.Fence, "state-policy-input")
			packet, packetAck := goalCorePacket(t, s, "state-policy-packet", o.Key, o, []knowledge.Observation{o}, input, requestAck.Fence)
			binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "model-worker"}
			proof := model.ObjectID(integrationID('a'))
			recoveryCoreControl(t, s, "state-policy-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: binding, Status: "active", Evidence: proof}}})
			spec := agentprofile.ActivitySpec{Activity: "state-model", Execution: "one", Role: "aux_summary", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: agentprofile.ExactRequestDigest(requestBytes), Recipient: "provider", Effect: "pure", ResourceScope: "session"}
			recoveryCoreControl(t, s, "state-policy-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding}})
			recoveryCoreControl(t, s, "state-policy-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "state-attempt", Sequence: 1, Authority: binding}})
			recoveryCoreControl(t, s, "state-policy-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "state-attempt", Authority: binding, Evidence: proof}})
			secret := []byte("sensitive model result derived from the source")
			result := agentprofile.ActivityResult{Capture: "full", ID: "state-result", Activity: spec.Activity, Attempt: "state-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: secret}
			recoveryCoreControl(t, s, "state-policy-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
			recoveryCoreControl(t, s, "state-policy-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: binding, Evidence: proof}})
			read := api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"}
			before, err := s.e.GetAgentState(ctx, read)
			if err != nil || len(before.Snapshot.Lifecycle.Results) != 1 || !bytes.Equal(before.Snapshot.Lifecycle.Results[0].Payload, secret) {
				t.Fatalf("setup did not retain exact accepted output: %+v %v", before, err)
			}
			pageReq := api.ReadAgentStateRecordsRequest{Space: read.Space, Principal: read.Principal, Anchor: &before.Anchor, Limit: 1}
			page, err := s.e.GetAgentStateRecords(ctx, pageReq)
			if err != nil || page.Continuation == "" {
				t.Fatalf("first record page: %+v %v", page, err)
			}
			pageReq.Continuation = page.Continuation
			// Select a next page that does not itself contain the sensitive
			// result. A page-only disclosure check would miss its provenance.
			for {
				next, err := s.e.GetAgentStateRecords(ctx, pageReq)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(next.Records[0].Raw, []byte("state-result")) {
					break
				}
				if next.Continuation == "" {
					t.Fatal("fixture has no non-result continuation page")
				}
				pageReq.Continuation = next.Continuation
			}
			applyMemory(t, s.e, "state-policy-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: kind, Reason: "withdraw explicit model source"}}})
			privacy := kind == memory.RetractionPrivacyErasure || kind == memory.RetractionACLRevocation
			cut, err := s.e.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			next, pageErr := s.e.GetAgentStateRecords(ctx, pageReq)
			if privacy {
				if !errors.Is(pageErr, memory.ErrHistoryAccessDenied) || !reflect.DeepEqual(next, api.AgentStateRecordsPage{}) {
					t.Fatalf("continuation bypassed full disclosure: %+v %v", next, pageErr)
				}
			} else if pageErr != nil {
				t.Fatal(pageErr)
			}
			afterRead, err := s.e.Snapshot(ctx)
			if err != nil || !reflect.DeepEqual(cut, afterRead) {
				t.Fatal("page disclosure check changed publication")
			}
			for _, historical := range []bool{false, true} {
				if historical {
					anchor := before.Anchor.Clone()
					read.Anchor = &anchor
				}
				got, err := s.e.GetAgentState(ctx, read)
				if privacy {
					if !errors.Is(err, memory.ErrHistoryAccessDenied) || got.Found || len(got.Snapshot.Lifecycle.Results) != 0 {
						t.Errorf("historical=%v: GetAgentState disclosed known withdrawn model provenance: found=%v result_count=%d err=%v", historical, got.Found, len(got.Snapshot.Lifecycle.Results), err)
					}
				} else if err != nil || len(got.Snapshot.Lifecycle.Results) != 1 || !bytes.Equal(got.Snapshot.Lifecycle.Results[0].Payload, secret) {
					t.Errorf("historical=%v: correction/use-ban changed audit bytes: %+v %v", historical, got, err)
				}
			}
			// A policy denial is an ordinary read outcome, never journal
			// corruption. It must leave unrelated reads and publication usable.
			if _, err := s.e.Snapshot(ctx); err != nil {
				t.Fatalf("disclosure denial fenced the store: %v", err)
			}
			s.reopen()
			got, err := s.e.GetAgentState(ctx, read)
			if privacy && (!errors.Is(err, memory.ErrHistoryAccessDenied) || got.Found) {
				t.Fatalf("reopen bypassed current disclosure policy: %+v %v", got, err)
			}
			if !privacy && (err != nil || len(got.Snapshot.Lifecycle.Results) != 1) {
				t.Fatalf("reopen lost audit result: %+v %v", got, err)
			}
		})
	}
}
