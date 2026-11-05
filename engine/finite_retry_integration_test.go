package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func finiteRetryFixture(t *testing.T) (*integrationStore, api.EmissionSpec, agentprofile.IdempotencyGuarantee) {
	t.Helper()
	s, content, source, binding := emissionFixture(t, engine.Options{AgentProfile: true})
	contract := agentprofile.EffectContract{
		Version: "tool-contract/v1", Recipient: "tool/v1", AllowsMutation: true,
		KeyNamespace: "tool-operations", RetentionNanos: 100, DifferentRequest: "reject",
		ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "request_deadline",
	}
	spec := agentprofile.ActivitySpec{
		Activity: "finite-retry", Execution: "emission-exec", Role: "resource_prepare", GoalEpoch: 1,
		InputFence: source, Request: content, RequestDigest: agentprofile.ExactRequestDigest([]byte("a")),
		Recipient: contract.Recipient, Effect: "idempotent", IdempotencyKey: "operation-1", ResourceScope: "workspace",
		Contract: contract,
	}
	recoveryCoreControl(t, s, "create", agentprofile.ControlCommand{
		CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding},
	})
	first := "first-attempt"
	recoveryCoreControl(t, s, "claim-first", agentprofile.ControlCommand{
		ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: first, Sequence: 1, Authority: binding},
	})
	recoveryCoreControl(t, s, "dispatch-first", agentprofile.ControlCommand{
		DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: first, Authority: binding, Evidence: content},
	})
	recoveryCoreControl(t, s, "capture-unknown", agentprofile.ControlCommand{
		CaptureResult: &agentprofile.CaptureResultCommand{Result: agentprofile.ActivityResult{
			ID: "unknown-result", Activity: spec.Activity, Attempt: first,
			RequestDigest: spec.RequestDigest, Incarnation: binding.Incarnation, Outcome: "unknown",
		}},
	})
	emission := api.EmissionSpec{
		ID: "retry-emission", Execution: spec.Execution, Task: "emission-task", GoalEpoch: spec.GoalEpoch,
		Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source},
		Projection: "value_bytes", Length: 1, Destination: spec.Recipient, Activity: spec.Activity,
		Attempt: "second-attempt", RequestDigest: spec.RequestDigest,
	}
	guarantee := agentprofile.IdempotencyGuarantee{
		Activity: spec.Activity, Version: 1, Status: "active", Contract: agentprofile.EffectContractDigest(contract),
		RequestDigest: spec.RequestDigest, KeyNamespace: contract.KeyNamespace, IdempotencyKey: spec.IdempotencyKey,
		ReceiverEpoch: "tool-epoch-1", CoveredAttempts: []string{first}, NextSequence: 2, ValidFrom: 100, ValidUntil: 200,
		TerminalCondition: contract.LateArrivalFence, TerminalEvidence: content, Evidence: content, Authority: binding,
	}
	guarantee.ID = agentprofile.IdempotencyGuaranteeID(guarantee)
	return s, emission, guarantee
}

func assertFiniteRetryRejection(t *testing.T, s *integrationStore, key model.RequestKey, before api.StateSnapshot, out api.CommitResult, err, wantErr error) {
	t.Helper()
	if !errors.Is(err, wantErr) || out.Code != api.CodeRejected || out.Fence != (model.CommitFence{}) {
		t.Fatalf("%s: got %+v %v, want rejection with %v", key.RequestID, out, err, wantErr)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatalf("%s changed publication: before=%+v after=%+v", key.RequestID, before, after)
	}
	if lookup := integrationLookup(t, s.e, key); lookup.Code != api.CodeNotFoundAtFence || lookup.ObservedFence != before.Fence || lookup.Result != (api.CommitResult{}) {
		t.Fatalf("%s left a receipt or fenced reads: %+v", key.RequestID, lookup)
	}
}

func rejectFiniteRetryControl(t *testing.T, s *integrationStore, id string, c agentprofile.ControlCommand, wantErr error) {
	t.Helper()
	before := integrationSnapshot(t, s.e)
	key := agentKey(id)
	out, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: key, Command: agentprofile.Command{Control: &c}})
	assertFiniteRetryRejection(t, s, key, before, out, err, wantErr)
}

func TestFiniteRetryRejectsMismatchedGuarantee(t *testing.T) {
	s, grant, guarantee := finiteRetryFixture(t)
	for _, test := range []struct {
		name   string
		mutate func(*agentprofile.IdempotencyGuarantee)
	}{
		{"operation-key", func(g *agentprofile.IdempotencyGuarantee) { g.IdempotencyKey = "foreign-key" }},
		{"key-namespace", func(g *agentprofile.IdempotencyGuarantee) { g.KeyNamespace = "foreign-namespace" }},
		{"request-digest", func(g *agentprofile.IdempotencyGuarantee) {
			g.RequestDigest = agentprofile.ExactRequestDigest([]byte("foreign-request"))
		}},
		{"authority", func(g *agentprofile.IdempotencyGuarantee) { g.Authority.Holder = "foreign-worker" }},
		{"missing-predecessor", func(g *agentprofile.IdempotencyGuarantee) { g.CoveredAttempts = nil }},
		{"wrong-predecessor", func(g *agentprofile.IdempotencyGuarantee) { g.CoveredAttempts = []string{"foreign-attempt"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := guarantee
			test.mutate(&bad)
			bad.ID = agentprofile.IdempotencyGuaranteeID(bad)
			rejectFiniteRetryControl(t, s, test.name, agentprofile.ControlCommand{
				PutIdempotencyGuarantee: &agentprofile.PutIdempotencyGuaranteeCommand{Guarantee: bad},
			}, agentprofile.ErrConflict)
		})
	}
	recoveryCoreControl(t, s, "put-guarantee", agentprofile.ControlCommand{
		PutIdempotencyGuarantee: &agentprofile.PutIdempotencyGuaranteeCommand{Guarantee: guarantee},
	})
	recoveryCoreControl(t, s, "fence-worker", agentprofile.ControlCommand{
		FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: grant.Execution, ExpectedVersion: 1, Evidence: grant.Content},
	})
	next := grant.Authority
	next.Holder = "replacement"
	next.OwnerEpoch++
	next.Incarnation++
	recoveryCoreControl(t, s, "resume-worker", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{
		Execution: grant.Execution, ExpectedIncarnation: grant.Authority.Incarnation, ExpectedAuthorityVersion: 2, Next: next,
		AdapterVersion: "fixture/v1", Checkpoint: grant.Content, CompatibilityEvidence: grant.Content, ResourceFenceEvidence: grant.Content,
	}})
	s.reopen()
	auth := agentprofile.RetryAuthorization{
		Activity: grant.Activity, ControlVersion: 2, Sequence: 2, Guarantee: guarantee.ID,
		CheckedAt: 120, NotBefore: 130, NotAfter: guarantee.ValidUntil, Evidence: grant.Content, Authority: next,
	}
	auth.ID = agentprofile.RetryAuthorizationID(auth)
	rejectFiniteRetryControl(t, s, "stale-guarantee-after-takeover", agentprofile.ControlCommand{
		AuthorizeRetry: &agentprofile.AuthorizeRetryCommand{Authorization: auth},
	}, agentprofile.ErrActionUnknown)
}

func TestFiniteRetryRechecksEvidenceAtExactEmissionDispatch(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		name := "expired"
		if revoked {
			name = "revoked"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, grant, guarantee := finiteRetryFixture(t)
			recoveryCoreControl(t, s, "put-guarantee", agentprofile.ControlCommand{
				PutIdempotencyGuarantee: &agentprofile.PutIdempotencyGuaranteeCommand{Guarantee: guarantee},
			})
			auth := agentprofile.RetryAuthorization{
				Activity: grant.Activity, ControlVersion: 2, Sequence: 2, Guarantee: guarantee.ID,
				CheckedAt: 120, NotBefore: 130, NotAfter: guarantee.ValidUntil, Evidence: grant.Content, Authority: grant.Authority,
			}
			auth.ID = agentprofile.RetryAuthorizationID(auth)
			recoveryCoreControl(t, s, "authorize-retry", agentprofile.ControlCommand{
				AuthorizeRetry: &agentprofile.AuthorizeRetryCommand{Authorization: auth},
			})
			s.reopen()

			use := agentprofile.RetryUse{Authorization: auth.ID, ObservedAt: auth.NotBefore, SendNotAfter: auth.NotAfter, Evidence: grant.Content}
			expired := use
			expired.ObservedAt, expired.SendNotAfter = auth.NotAfter, auth.NotAfter+1
			retry := agentprofile.RetryActivityCommand{Activity: grant.Activity, ExpectedVersion: 1, Authority: grant.Authority}
			rejectFiniteRetryControl(t, s, "retry-without-evidence", agentprofile.ControlCommand{RetryActivity: &retry}, agentprofile.ErrActionUnknown)
			retry.Use = expired
			rejectFiniteRetryControl(t, s, "expired-retry", agentprofile.ControlCommand{RetryActivity: &retry}, agentprofile.ErrActionUnknown)
			use.ObservedAt++
			retry.Use = use
			recoveryCoreControl(t, s, "retry", agentprofile.ControlCommand{RetryActivity: &retry})
			s.reopen()

			claim := agentprofile.ClaimAttemptCommand{Activity: grant.Activity, Attempt: grant.Attempt, Sequence: 2, Authority: grant.Authority, Use: expired}
			rejectFiniteRetryControl(t, s, "expired-claim", agentprofile.ControlCommand{ClaimAttempt: &claim}, agentprofile.ErrActionUnknown)
			claim.Use = use
			claim.Use.ObservedAt--
			rejectFiniteRetryControl(t, s, "claim-before-retry-check", agentprofile.ControlCommand{ClaimAttempt: &claim}, agentprofile.ErrActionUnknown)
			claim.Use = use
			claim.Use.SendNotAfter++
			rejectFiniteRetryControl(t, s, "claim-with-late-send-cutoff", agentprofile.ControlCommand{ClaimAttempt: &claim}, agentprofile.ErrActionUnknown)
			use.ObservedAt++
			claim.Use = use
			recoveryCoreControl(t, s, "claim-retry", agentprofile.ControlCommand{ClaimAttempt: &claim})
			issueExactGrant(t, s, grant)

			use.ObservedAt++
			dispatch := agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Authority: grant.Authority, Evidence: grant.Content, Use: use}
			if revoked {
				guarantee.Version++
				guarantee.Status = "revoked"
				guarantee.CoveredAttempts = []string{"first-attempt", grant.Attempt}
				guarantee.NextSequence++
				guarantee.ID = agentprofile.IdempotencyGuaranteeID(guarantee)
				recoveryCoreControl(t, s, "revoke-guarantee", agentprofile.ControlCommand{
					PutIdempotencyGuarantee: &agentprofile.PutIdempotencyGuaranteeCommand{Guarantee: guarantee},
				})
			}
			s.reopen()

			before := integrationSnapshot(t, s.e)
			bad := dispatch
			if !revoked {
				bad.Use = expired
			}
			blocked := exactDispatchWork(t, grant, agentprofile.ControlCommand{DispatchAttempt: &bad}, "blocked-dispatch")
			out, err := s.e.ApplyWork(ctx, blocked)
			assertFiniteRetryRejection(t, s, blocked.Key, before, out, err, agentprofile.ErrActionUnknown)
			read := api.ReadEmissionRequest{Space: blocked.Key.Space, Principal: blocked.Key.Principal, ID: grant.ID}
			if got, err := s.e.GetEmission(ctx, read); err != nil || !got.Found || got.Record.Status != "issued" || got.Record.Version != 1 {
				t.Fatalf("failed dispatch changed emission: %+v %v", got, err)
			}
			if revoked {
				return
			}

			work := exactDispatchWork(t, grant, agentprofile.ControlCommand{DispatchAttempt: &dispatch}, "dispatch-retry")
			ack := workApply(t, s, work)
			if ack.Fence.LSN != before.Fence.LSN+1 {
				t.Fatalf("dispatch and grant claim advanced more than one fence: before=%+v after=%+v", before.Fence, ack.Fence)
			}
			s.reopen()
			if got, err := s.e.GetEmission(ctx, read); err != nil || !got.Found || got.Record.Status != "claimed" || got.Record.Version != 2 {
				t.Fatalf("dispatch did not claim exact emission: %+v %v", got, err)
			}
			state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: read.Space, Principal: read.Principal})
			if err != nil {
				t.Fatal(err)
			}
			recovery := state.Snapshot.Lifecycle.Recovery
			if recovery == nil || !reflect.DeepEqual(recovery.IdempotencyGuarantees, []agentprofile.IdempotencyGuarantee{guarantee}) ||
				!reflect.DeepEqual(recovery.RetryAuthorizations, []agentprofile.RetryAuthorization{auth}) {
				t.Fatalf("reopen changed guarantee or authorization: %+v", recovery)
			}
			if len(recovery.ActivityControls) != 1 || recovery.ActivityControls[0].Version != auth.ControlVersion ||
				recovery.ActivityControls[0].RetryAuthorization != auth.ID || recovery.ActivityControls[0].RetryObservedAt != retry.Use.ObservedAt {
				t.Fatalf("reopen changed retry control or timing: %+v", recovery.ActivityControls)
			}
			for _, attempt := range recovery.AttemptControls {
				if attempt.Attempt == grant.Attempt && attempt.Status == "dispatch_recorded" &&
					attempt.ClaimUse == claim.Use && attempt.DispatchUse == dispatch.Use && attempt.DispatchEvidence == dispatch.Evidence {
					if replay := workApply(t, s, work); replay != ack {
						t.Fatalf("dispatch receipt changed on replay: %+v", replay)
					}
					return
				}
			}
			t.Fatalf("reopen did not recover exact dispatch and timing evidence: %+v", recovery.AttemptControls)
		})
	}
}
