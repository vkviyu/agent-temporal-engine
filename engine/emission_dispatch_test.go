package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func exactEmissionFixture(t *testing.T, query bool) (*integrationStore, api.EmissionSpec, agentprofile.ControlCommand) {
	t.Helper()
	s, content, source, binding := emissionFixture(t, engine.Options{AgentProfile: true, EventProfile: true})
	spec := agentprofile.ActivitySpec{Activity: "exact-tool", Execution: "emission-exec", Role: "resource_prepare", GoalEpoch: 1, InputFence: source, Request: content, RequestDigest: agentprofile.ExactRequestDigest([]byte{'a'}), Recipient: "tool/v1", Effect: "pure", ResourceScope: "workspace"}
	if query {
		q := agentprofile.Query{ID: "emission-query", Session: "s", Space: "host-space", Owner: "host-principal", Input: []byte("question"), InputFence: source, Status: "open", Version: 1}
		coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("emission-query"), Command: agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &q}}})
		spec.Execution, spec.Query, binding = "", q.ID, agentprofile.AuthorityBinding{}
	}
	recoveryCoreControl(t, s, "exact-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding}})
	recoveryCoreControl(t, s, "exact-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "exact-attempt", Sequence: 1, Authority: binding}})
	grant := api.EmissionSpec{ID: "exact-grant", Execution: spec.Execution, Query: spec.Query, GoalEpoch: 1, Authority: binding, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Projection: "value_bytes", Length: 1, Destination: spec.Recipient, Activity: spec.Activity, Attempt: "exact-attempt", RequestDigest: spec.RequestDigest}
	if !query {
		grant.Task, grant.AuthorityVersion = "emission-task", 1
	}
	return s, grant, agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Authority: binding, Evidence: content}}
}
func exactDispatchWork(t *testing.T, grant api.EmissionSpec, dispatch agentprofile.ControlCommand, id string) api.ApplyWorkRequest {
	t.Helper()
	claim, err := api.EmissionWorkStep(api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: grant.Authority})
	if err != nil {
		t.Fatal(err)
	}
	return api.ApplyWorkRequest{Key: agentKey(id), Unit: api.WorkUnit{Kind: "emission_dispatch", Query: grant.Query, Steps: []api.WorkStep{claim, workControl(t, dispatch)}}}
}
func issueExactGrant(t *testing.T, s *integrationStore, g api.EmissionSpec) api.CommitResult {
	t.Helper()
	out, err := s.e.ApplyEmission(context.Background(), api.ApplyEmissionRequest{Key: agentKey("issue-" + g.ID), Command: api.EmissionCommand{Issue: &g}})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("issue exact: %+v %v", out, err)
	}
	return out
}
func TestEmissionExactDispatchProtectsBothScopesAndSurvivesReplay(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(fmt.Sprintf("query=%v", query), func(t *testing.T) {
			s, g, dispatch := exactEmissionFixture(t, query)
			issueExactGrant(t, s, g)
			before := integrationSnapshot(t, s.e)
			if out, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: agentKey("direct-dispatch"), Command: agentprofile.Command{Control: &dispatch}}); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("direct dispatch: %+v %v", out, err)
			}
			if out, err := s.e.ApplyEmission(context.Background(), api.ApplyEmissionRequest{Key: agentKey("direct-claim"), Command: api.EmissionCommand{ClaimID: g.ID, ClaimVersion: 1, ClaimAuthority: g.Authority}}); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("direct claim: %+v %v", out, err)
			}
			work := exactDispatchWork(t, g, dispatch, "exact-dispatch")
			bad := work
			bad.Key = agentKey("wrong-attempt")
			wrong := *dispatch.DispatchAttempt
			wrong.Attempt = "foreign"
			bad.Unit.Steps = append([]api.WorkStep(nil), work.Unit.Steps...)
			bad.Unit.Steps[1] = workControl(t, agentprofile.ControlCommand{DispatchAttempt: &wrong})
			if out, err := s.e.ApplyWork(context.Background(), bad); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("wrong attempt: %+v %v", out, err)
			}
			bad = work
			bad.Key = agentKey("checkpoint-bypass")
			bad.Unit.Kind = "checkpoint"
			if out, err := s.e.ApplyWork(context.Background(), bad); err == nil || out.Code == api.CodeDurableAck || integrationSnapshot(t, s.e) != before {
				t.Fatalf("checkpoint bypass: %+v %v", out, err)
			}
			ack := workApply(t, s, work)
			if ack.Fence.LSN != before.Fence.LSN+1 {
				t.Fatal("claim/dispatch split across publications")
			}
			check := func() {
				got, err := s.e.GetEmission(context.Background(), api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: g.ID})
				if err != nil || got.Record.Status != "claimed" || got.Record.Version != 2 {
					t.Fatalf("grant: %+v %v", got, err)
				}
				state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, a := range state.Snapshot.Lifecycle.Recovery.AttemptControls {
					if a.Attempt == g.Attempt {
						found = a.Status == "dispatch_recorded" && a.DispatchEvidence == dispatch.DispatchAttempt.Evidence
					}
				}
				if !found {
					t.Fatal("grant claimed without attempt dispatch")
				}
				if retry := workApply(t, s, work); retry != ack {
					t.Fatal("original work receipt changed")
				}
			}
			check()
			s.reopen()
			check()
			if _, err := s.e.CompactAuditFull(context.Background()); err != nil {
				t.Fatal(err)
			}
			s.reopen()
			check()
		})
	}
}

func TestEmissionQueryDecisionRequiresExactDispatchWork(t *testing.T) {
	for _, effect := range []string{"pure", "non_idempotent"} {
		t.Run(effect, func(t *testing.T) {
			ctx := context.Background()
			s := newIntegrationStore(t, engine.Options{AgentProfile: true})
			inputAck := integrationMutate(t, s.e, integrationRequest("query-request", integrationPut([]byte("query-request"), integrationBytes('a'))))
			input, data := recoveryValueAt(t, s, inputAck.Fence, "query-request")
			query := agentprofile.Query{ID: "emission-query", Session: "session", Space: "host-space", Owner: "host-principal", Input: []byte("question"), InputFence: inputAck.Fence, Status: "open", Version: 1}
			coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("emission-query"), Command: agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &query}}})
			anchor := model.SnapshotAnchor{Space: query.Space, Fence: inputAck.Fence}
			publishPacket := func(renderer string) (model.ObjectID, model.CommitFence) {
				t.Helper()
				packet := knowledge.ContextPacket{
					Space: query.Space, Source: knowledge.SourceAnchor{Anchor: anchor}, Renderer: renderer, Tokenizer: "test/v1",
					Input: &knowledge.RecordedInput{Object: input, Anchor: anchor, Encoding: "text/plain"},
				}
				packet.ID = knowledge.PacketID(packet)
				ack, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey(renderer), Command: knowledge.Command{Packet: &packet}})
				if err != nil || ack.Code != api.CodeDurableAck {
					t.Fatal(ack, err)
				}
				return packet.ID, ack.Fence
			}
			otherPacket, _ := publishPacket("other-renderer/v1")
			packet, packetFence := publishPacket("query/v1")
			spec := agentprofile.ActivitySpec{
				Activity: "query-decision", Query: query.ID, Role: "query_decision", GoalEpoch: query.Version,
				InputFence: packetFence, ContextPacket: packet, Request: input, RequestDigest: agentprofile.ExactRequestDigest(data),
				Recipient: "provider/v1", Effect: effect, ResourceScope: "query",
			}
			recoveryCoreControl(t, s, "decision-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})
			recoveryCoreControl(t, s, "decision-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "decision-attempt", Sequence: 1}})
			grant := api.EmissionSpec{
				ID: "decision-grant", Query: query.ID, GoalEpoch: query.Version, Content: packet, ContentKind: "packet",
				Anchor: model.SnapshotAnchor{Space: query.Space, Fence: packetFence}, Length: uint64(len(data)), Destination: spec.Recipient,
				Activity: spec.Activity, Attempt: "decision-attempt", RequestDigest: spec.RequestDigest,
			}
			dispatch := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Evidence: input}}
			rejectDirect := func(id string) {
				t.Helper()
				before := integrationSnapshot(t, s.e)
				if out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey(id), Command: agentprofile.Command{Control: &dispatch}}); !errors.Is(err, engine.ErrWorkBoundary) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
					t.Fatalf("query decision bypassed emission Work: %+v %v", out, err)
				}
			}
			rejectDirect("direct-before-grant")
			for _, test := range []struct {
				id, kind, projection string
				content              model.ObjectID
			}{
				{"other-packet", "packet", "", otherPacket},
				{"raw-input", "", "value_bytes", input},
			} {
				bad := grant
				bad.ID, bad.ContentKind, bad.Projection, bad.Content = test.id, test.kind, test.projection, test.content
				before := integrationSnapshot(t, s.e)
				out, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey(test.id), Command: api.EmissionCommand{Issue: &bad}})
				if !errors.Is(err, engine.ErrEmissionRejected) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
					t.Fatalf("selector %s substituted for the Activity packet: %+v %v", test.id, out, err)
				}
			}
			issueExactGrant(t, s, grant)
			rejectDirect("direct-after-grant")
			before := integrationSnapshot(t, s.e)
			ack := workApply(t, s, exactDispatchWork(t, grant, dispatch, "decision-dispatch"))
			if ack.Fence.LSN != before.Fence.LSN+1 {
				t.Fatal("claim and dispatch did not share one publication")
			}
			got, err := s.e.GetEmission(ctx, api.ReadEmissionRequest{Space: query.Space, Principal: query.Owner, ID: grant.ID})
			if err != nil || got.Record.Status != "claimed" || got.Record.Version != 2 {
				t.Fatalf("query grant: %+v %v", got, err)
			}
			proof := got.Record.Proof
			if proof == nil || proof.TotalBytes != uint64(len(data)) || proof.ContentDigest != spec.RequestDigest || proof.Projection != "value_bytes" {
				t.Fatalf("packet did not project the recorded request: %+v", proof)
			}
		})
	}
}

func TestEmissionIssueEnforcesExactAttemptRecipientBytesAndExpiry(t *testing.T) {
	s, g, _ := exactEmissionFixture(t, false)
	for name, mutate := range map[string]func(*api.EmissionSpec){
		"wrong-activity":         func(g *api.EmissionSpec) { g.Activity = "missing" },
		"wrong-attempt":          func(g *api.EmissionSpec) { g.Attempt = "missing" },
		"wrong-destination":      func(g *api.EmissionSpec) { g.Destination = "another/tool" },
		"wrong-digest":           func(g *api.EmissionSpec) { g.RequestDigest = model.ObjectID(integrationID('f')) },
		"wrong-projection-bytes": func(g *api.EmissionSpec) { g.Projection = "payload" },
		"expired":                func(g *api.EmissionSpec) { g.NotAfterLSN = integrationSnapshot(t, s.e).Fence.LSN },
	} {
		t.Run(name, func(t *testing.T) {
			bad := g
			bad.ID = name
			mutate(&bad)
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplyEmission(context.Background(), api.ApplyEmissionRequest{Key: agentKey(name), Command: api.EmissionCommand{Issue: &bad}})
			if err == nil || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("invalid binding changed/fenced store: %+v %v", out, err)
			}
		})
	}
}
func TestEmissionDispatchRechecksExpiryWithdrawalAndAuthority(t *testing.T) {
	for _, kind := range []string{"expiry", "withdrawal", "authority", "revoke"} {
		t.Run(kind, func(t *testing.T) {
			s, g, dispatch := exactEmissionFixture(t, false)
			if kind == "expiry" {
				g.NotAfterLSN = integrationSnapshot(t, s.e).Fence.LSN + 1
			}
			issueExactGrant(t, s, g)
			wantErr := engine.ErrEmissionRejected
			switch kind {
			case "withdrawal":
				wantErr = memory.ErrUseDenied
				applyMemory(t, s.e, "withdraw-grant", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: g.Content, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "withdrawn"}}})
			case "authority":
				recoveryCoreControl(t, s, "fence-emitter", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: g.Execution, ExpectedVersion: 1, Evidence: g.Content}})
			case "revoke":
				wantErr = engine.ErrWorkBoundary
				out, err := s.e.ApplyEmission(context.Background(), api.ApplyEmissionRequest{Key: agentKey("revoke"), Command: api.EmissionCommand{RevokeID: g.ID, RevokeVersion: 1}})
				if err != nil || out.Code != api.CodeDurableAck {
					t.Fatal(out, err)
				}
			}
			s.reopen()
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplyWork(context.Background(), exactDispatchWork(t, g, dispatch, "blocked-dispatch"))
			if !errors.Is(err, wantErr) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("stale grant bypassed %s: %+v %v", kind, out, err)
			}
		})
	}
}
