package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestCurrentWithdrawalsGateClaimDispatchAndHistoricalDisclosure(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionFactualCorrection, memory.RetractionUseBan, memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, payload, source := knowledgeFixture(t)
			o := knowledgeObservation(payload, source, "one", 1)
			applyObservation(t, s.e, "policy-adopt", o)
			view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
			if err != nil {
				t.Fatal(err)
			}
			defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
			binding := agentprofile.AuthorityBinding{Deployment: "policy-host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
			recoveryCoreControl(t, s, "policy-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: binding, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}})
			a := agentprofile.ActivitySpec{Execution: "one", Role: "resource_prepare", GoalEpoch: 1, InputFence: source, Request: payload, RequestDigest: agentprofile.ExactRequestDigest([]byte{'a'}), Recipient: "test-adapter", Effect: "pure", ResourceScope: "test"}
			for _, id := range []string{"before-claim", "before-dispatch"} {
				spec := a
				spec.Activity = id
				recoveryCoreControl(t, s, "policy-create-"+id, agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding}})
			}
			recoveryCoreControl(t, s, "policy-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: "before-dispatch", Attempt: "claimed", Sequence: 1, Authority: binding}})
			grant := api.EmissionSpec{
				ID: "policy-grant", Execution: a.Execution, Task: "kt", GoalEpoch: a.GoalEpoch,
				Authority: binding, AuthorityVersion: 1,
				Content: a.Request, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: a.InputFence},
				Projection: "value_bytes", Length: 1, Destination: a.Recipient,
				Activity: "before-dispatch", Attempt: "claimed", RequestDigest: a.RequestDigest,
			}
			issueExactGrant(t, s, grant)
			applyMemory(t, s.e, "policy-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: kind, Reason: "recorded source withdrawn"}}})
			before := integrationSnapshot(t, s.e)
			privacy := kind == memory.RetractionPrivacyErasure || kind == memory.RetractionACLRevocation
			denial := memory.ErrUseDenied
			if privacy {
				denial = memory.ErrHistoryAccessDenied
			}
			claim := agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: "before-claim", Attempt: "late-claim", Sequence: 1, Authority: binding}}
			if out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("denied-claim"), Command: agentprofile.Command{Control: &claim}}); out.Code != api.CodeRejected || !errors.Is(err, denial) {
				t.Fatalf("withdrawn source still claimable or fenced store: %+v %v", out, err)
			}
			dispatch := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Authority: binding, Evidence: model.ObjectID(integrationID('b'))}}
			if out, err := s.e.ApplyWork(ctx, exactDispatchWork(t, grant, dispatch, "denied-dispatch")); out.Code != api.CodeRejected || !errors.Is(err, denial) {
				t.Fatalf("withdrawn source still dispatchable or fenced store: %+v %v", out, err)
			}
			value, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("evidence")})
			if privacy {
				if !errors.Is(err, memory.ErrHistoryAccessDenied) || value.Found {
					t.Fatalf("private raw content disclosed: %+v %v", value, err)
				}
			} else if err != nil || string(value.Value.Data) != "a" {
				t.Fatalf("audit bytes silently changed: %+v %v", value, err)
			}
			_, nodeErr := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: payload}})
			_, chunkErr := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: payload}, Projection: "value_bytes", Length: 1})
			_, knowledgeErr := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &o.Key})
			_, entriesErr := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
			for _, err := range []error{nodeErr, chunkErr, knowledgeErr, entriesErr} {
				if privacy && !errors.Is(err, memory.ErrHistoryAccessDenied) {
					t.Fatalf("fixed historical view bypassed disclosure ban: %v", err)
				}
				if !privacy && err != nil {
					t.Fatalf("audit view incorrectly denied after correction/UseBan: %v", err)
				}
			}
			if privacy {
				out, err := s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: agentKey("policy-downgrade"), Command: memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "attempt downgrade"}}}})
				if out.Code != api.CodeRejected || !errors.Is(err, memory.ErrInvalidTransition) {
					t.Fatalf("privacy policy weakened by later UseBan: %+v %v", out, err)
				}
			}
			if after := integrationSnapshot(t, s.e); after != before {
				t.Fatal("policy denial changed or fenced the ledger")
			}
			if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
				t.Fatal(err)
			}
			s.reopen()
			if _, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("evidence")}); privacy != errors.Is(err, memory.ErrHistoryAccessDenied) {
				t.Fatalf("withdrawal changed across replay: %v", err)
			}
		})
	}
}

func TestModelPacketSourceWithdrawalRecheckedAfterClaim(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	o := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "model-policy-adopt", o)
	requestBytes := []byte("framed model request derived from adopted evidence")
	requestAck := integrationMutate(t, s.e, integrationRequest("model-policy-input", integrationPut([]byte("model-policy-input"), api.Value{Kind: api.ValueBytes, Data: requestBytes})))
	input, _ := recoveryValueAt(t, s, requestAck.Fence, "model-policy-input")
	packet, packetAck := goalCorePacket(t, s, "model-policy-packet", o.Key, o, []knowledge.Observation{o}, input, requestAck.Fence)
	binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "model-worker"}
	recoveryCoreControl(t, s, "model-policy-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: binding, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}})
	a := agentprofile.ActivitySpec{Activity: "model-policy-activity", Execution: "one", Role: "aux_summary", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: agentprofile.ExactRequestDigest(requestBytes), Recipient: "provider", Effect: "pure", ResourceScope: "session"}
	recoveryCoreControl(t, s, "model-policy-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: a, Authority: binding}})
	recoveryCoreControl(t, s, "model-policy-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: a.Activity, Attempt: "model-policy-attempt", Sequence: 1, Authority: binding}})
	grant := api.EmissionSpec{
		ID: "model-policy-grant", Execution: a.Execution, Task: "kt", GoalEpoch: a.GoalEpoch,
		Authority: binding, AuthorityVersion: 1,
		Content: a.ContextPacket, ContentKind: "packet", Anchor: model.SnapshotAnchor{Space: "host-space", Fence: a.InputFence},
		Length: uint64(len(requestBytes)), Destination: a.Recipient,
		Activity: a.Activity, Attempt: "model-policy-attempt", RequestDigest: a.RequestDigest,
	}
	issueExactGrant(t, s, grant)
	applyMemory(t, s.e, "model-policy-ban", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "upstream source no longer reusable"}}})
	before := integrationSnapshot(t, s.e)
	control := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Authority: binding, Evidence: model.ObjectID(integrationID('b'))}}
	out, err := s.e.ApplyWork(ctx, exactDispatchWork(t, grant, control, "model-policy-denied"))
	if out.Code != api.CodeRejected || !errors.Is(err, memory.ErrUseDenied) || integrationSnapshot(t, s.e) != before {
		t.Fatalf("upstream ban ignored because request bytes themselves were not banned: %+v %v", out, err)
	}
}

func TestHistoricalKnowledgeViewHonorsPacketSourceWithdrawal(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	adopted := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "withdraw-packet-adopt", adopted)
	requestBytes := []byte("recorded model input with its own content identity")
	ack := integrationMutate(t, s.e, integrationRequest("withdraw-packet-input", integrationPut([]byte("withdraw-packet-input"), api.Value{Kind: api.ValueBytes, Data: requestBytes})))
	input, _ := recoveryValueAt(t, s, ack.Fence, "withdraw-packet-input")
	packet, _ := goalCorePacket(t, s, "withdraw-packet-record", adopted.Key, adopted, []knowledge.Observation{adopted}, input, ack.Fence)
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Knowledge: &adopted.Key, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	packetRequest := api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: packet.ID}}
	contentRequest := api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: input}, Projection: "value_bytes"}
	if _, err := s.e.GetNode(ctx, packetRequest); err != nil {
		t.Fatal(err)
	}
	if chunk, err := s.e.ReadContent(ctx, contentRequest); err != nil || string(chunk.Data) != string(requestBytes) {
		t.Fatalf("recorded input before withdrawal = %+v, %v", chunk, err)
	}
	applyMemory(t, s.e, "withdraw-packet-source", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "packet source withdrawn"}}})
	before := integrationSnapshot(t, s.e)
	if node, err := s.e.GetNode(ctx, packetRequest); !errors.Is(err, memory.ErrHistoryAccessDenied) || len(node.Data) != 0 {
		t.Fatalf("fixed view exposed withdrawn packet = %+v, %v", node, err)
	}
	if chunk, err := s.e.ReadContent(ctx, contentRequest); !errors.Is(err, memory.ErrHistoryAccessDenied) || len(chunk.Data) != 0 {
		t.Fatalf("fixed view exposed input through withdrawn packet = %+v, %v", chunk, err)
	}
	state, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("withdraw-packet-input")})
	if !errors.Is(err, memory.ErrHistoryAccessDenied) || state.Found {
		t.Fatalf("raw state exposed input through withdrawn packet source = %+v, %v", state, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("denying historical disclosure changed or fenced the store")
	}
}
