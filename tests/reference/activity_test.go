package reference_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func retryCheck(use agentprofile.RetryUse) oracle.RetryCheck {
	return oracle.RetryCheck{Permit: string(use.Authorization), At: use.ObservedAt, SendBefore: use.SendNotAfter}
}

func projectGuarantee(g agentprofile.IdempotencyGuarantee, incarnation uint64) oracle.FiniteGuarantee {
	return oracle.FiniteGuarantee{
		ID: string(g.ID), Version: g.Version, Incarnation: incarnation, Sequence: g.NextSequence,
		From: g.ValidFrom, Until: g.ValidUntil, Revoked: g.Status == "revoked",
	}
}

func projectPermit(a agentprofile.RetryAuthorization) oracle.RetryPermit {
	return oracle.RetryPermit{ID: string(a.ID), Guarantee: string(a.Guarantee), Version: a.ControlVersion, From: a.NotBefore, Until: a.NotAfter}
}

func TestIndependentActivityUnknownEffectsAndStaleResults(t *testing.T) {
	for _, effect := range []string{"pure", "idempotent", "non_idempotent"} {
		t.Run(effect, func(t *testing.T) {
			h := newHarness(t)
			h.root()
			want := oracle.NewActivity(effect)
			auth := h.expected.Authorities["root"]
			spec := agentprofile.ActivitySpec{Activity: "external", Execution: "root", Role: "resource_prepare", GoalEpoch: 1, InputFence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digest("f"))}, Request: digest("a"), RequestDigest: digest("b"), Recipient: "executor", Effect: effect, ResourceScope: "workspace"}
			if effect == "idempotent" {
				spec.IdempotencyKey = "stable-operation"
				spec.Contract = agentprofile.EffectContract{Version: "contract/v1", Recipient: spec.Recipient, KeyNamespace: "fixture", RetentionNanos: 1, DifferentRequest: "reject", ResultQuery: "none", ResourceFence: "supported", Compensation: "none", LateArrivalFence: "receiver_epoch"}
			}
			h.raw(h.control(agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding(auth)}}))
			compare := func(name string) {
				t.Helper()
				s := h.actual.Snapshot()
				r := s.Lifecycle.Recovery
				control := r.ActivityControls[0]
				if control.AcceptedResult != want.Accepted || control.Version != want.Version || control.Incarnation != want.DecisionIncarnation || len(r.AttemptControls) != len(want.Attempts) || len(s.Lifecycle.Results) != len(want.Results) {
					t.Fatalf("%s control/results mismatch %+v / %+v", name, r, want)
				}
				for _, attempt := range r.AttemptControls {
					if attempt.Status != want.Status(attempt.Attempt) {
						t.Fatalf("%s attempt=%s actual=%s expected=%s", name, attempt.Attempt, attempt.Status, want.Status(attempt.Attempt))
					}
				}
				for _, capture := range r.Captures {
					if capture.Quarantined != want.Results[capture.Result].Quarantined {
						t.Fatalf("%s quarantine differs", name)
					}
				}
			}
			sequence := 0
			apply := func(name string, expected func(*oracle.Activity) error, c agentprofile.ControlCommand) {
				t.Helper()
				sequence++
				before, _ := agentprofile.EncodeSnapshot(h.actual.Snapshot())
				wantErr := want.Batch(expected)
				_, gotErr := h.actual.ApplyCommand(key(fmt.Sprintf("activity-%d", sequence)), h.control(c))
				if (wantErr == nil) != (gotErr == nil) {
					t.Fatalf("%s mismatch expected=%v actual=%v", name, wantErr, gotErr)
				}
				if gotErr != nil {
					after, _ := agentprofile.EncodeSnapshot(h.actual.Snapshot())
					if !bytes.Equal(before, after) {
						t.Fatalf("%s rejected transition mutated state", name)
					}
				}
				compare(name)
			}
			retryUse := agentprofile.RetryUse{}
			claim := func(id string, seq uint64) {
				use := agentprofile.RetryUse{}
				if effect == "idempotent" && id != "first" && id != "concurrent" {
					use = retryUse
				}
				apply("claim-"+id, func(a *oracle.Activity) error { return a.Claim(id, seq, auth.Incarnation, retryCheck(use)) }, agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: id, Sequence: seq, Authority: binding(auth), Use: use}})
			}
			dispatch := func(id string) {
				use := agentprofile.RetryUse{}
				if effect == "idempotent" && id != "first" {
					use = retryUse
				}
				apply("dispatch-"+id, func(a *oracle.Activity) error { return a.Dispatch(id, auth.Incarnation, retryCheck(use)) }, agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: id, Authority: binding(auth), Evidence: digest("c"), Use: use}})
			}
			capture := func(id, attempt, outcome string, inc uint64) {
				apply("capture-"+id, func(a *oracle.Activity) error { return a.Capture(id, attempt, outcome) }, agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: agentprofile.ActivityResult{Capture: "full", ID: id, Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest, Incarnation: inc, Outcome: outcome, Payload: []byte(id)}}})
			}
			retryCount := 0
			retry := func() {
				v := want.Version
				use := agentprofile.RetryUse{}
				if effect == "idempotent" {
					retryCount++
					covered := []string{"first"}
					if retryCount > 1 {
						covered = []string{"first", "second"}
					}
					guarantee := agentprofile.IdempotencyGuarantee{
						Activity:          spec.Activity,
						Version:           uint64(retryCount),
						Status:            "active",
						Contract:          agentprofile.EffectContractDigest(spec.Contract),
						RequestDigest:     spec.RequestDigest,
						KeyNamespace:      spec.Contract.KeyNamespace,
						IdempotencyKey:    spec.IdempotencyKey,
						ReceiverEpoch:     fmt.Sprintf("epoch-%d", retryCount),
						CoveredAttempts:   covered,
						NextSequence:      v + 1,
						ValidFrom:         1,
						ValidUntil:        2,
						TerminalCondition: spec.Contract.LateArrivalFence,
						TerminalEvidence:  digest("c"),
						Evidence:          digest("d"),
						Authority:         binding(auth),
					}
					guarantee.ID = agentprofile.IdempotencyGuaranteeID(guarantee)
					apply("put-guarantee", func(a *oracle.Activity) error {
						return a.PublishGuarantee(projectGuarantee(guarantee, auth.Incarnation), guarantee.CoveredAttempts)
					}, agentprofile.ControlCommand{PutIdempotencyGuarantee: &agentprofile.PutIdempotencyGuaranteeCommand{Guarantee: guarantee}})
					authorization := agentprofile.RetryAuthorization{
						Activity:       spec.Activity,
						ControlVersion: v + 1,
						Sequence:       v + 1,
						Guarantee:      guarantee.ID,
						CheckedAt:      1,
						NotBefore:      1,
						NotAfter:       2,
						Evidence:       digest("e"),
						Authority:      binding(auth),
					}
					authorization.ID = agentprofile.RetryAuthorizationID(authorization)
					apply("authorize-retry", func(a *oracle.Activity) error {
						return a.AuthorizeRetry(projectPermit(authorization))
					}, agentprofile.ControlCommand{AuthorizeRetry: &agentprofile.AuthorizeRetryCommand{Authorization: authorization}})
					use = agentprofile.RetryUse{Authorization: authorization.ID, ObservedAt: 1, SendNotAfter: 2, Evidence: digest("f")}
					retryUse = use
				}
				apply("retry", func(a *oracle.Activity) error { return a.Retry(v, auth.Incarnation, retryCheck(use)) }, agentprofile.ControlCommand{RetryActivity: &agentprofile.RetryActivityCommand{Activity: spec.Activity, ExpectedVersion: v, Authority: binding(auth), Use: use}})
			}
			claim("first", 1)
			dispatch("first")
			claim("concurrent", 2)
			capture("unknown", "first", "unknown", 1)
			apply("accept-unknown", func(a *oracle.Activity) error { return a.Accept("unknown", 1) }, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: "unknown", ExpectedVersion: 1, Authority: binding(auth), Evidence: digest("d")}})
			retry()
			if effect == "non_idempotent" {
				apply("unfenced-no-effect", func(a *oracle.Activity) error { return a.Reconcile("first", true, false) }, agentprofile.ControlCommand{ReconcileAttempt: &agentprofile.ReconcileAttemptCommand{Attempt: "first", Outcome: "no_effect", Evidence: digest("e")}})
				apply("fenced-no-effect", func(a *oracle.Activity) error { return a.Reconcile("first", true, true) }, agentprofile.ControlCommand{ReconcileAttempt: &agentprofile.ReconcileAttemptCommand{Attempt: "first", Outcome: "no_effect", FutureEffectFenced: true, Evidence: digest("e")}})
				retry()
			}
			claim("second", 2)
			dispatch("second")
			// Authority loss does not turn an unknown external effect into absence.
			h.raw(h.control(agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "root", ExpectedVersion: 1, Evidence: digest("a")}}))
			next := auth
			next.OwnerEpoch++
			next.Incarnation++
			next.Holder = "replacement"
			h.raw(h.control(agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: "root", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: binding(next), AdapterVersion: "fixture/v1", Checkpoint: digest("b"), CompatibilityEvidence: digest("c"), ResourceFenceEvidence: digest("d")}}))
			auth = next
			want.Takeover()
			compare("takeover")
			capture("stale-success", "second", "success", 1)
			apply("accept-stale", func(a *oracle.Activity) error { return a.Accept("stale-success", 2) }, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: "stale-success", ExpectedVersion: want.Version, Authority: binding(auth), Evidence: digest("d")}})
			if effect == "non_idempotent" {
				apply("no-effect-second", func(a *oracle.Activity) error { return a.Reconcile("second", true, true) }, agentprofile.ControlCommand{ReconcileAttempt: &agentprofile.ReconcileAttemptCommand{Attempt: "second", Outcome: "no_effect", FutureEffectFenced: true, Evidence: digest("e")}})
			}
			retry()
			claim("third", 3)
			dispatch("third")
			capture("success", "third", "success", 2)
			apply("accept-success", func(a *oracle.Activity) error { return a.Accept("success", 2) }, agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: "success", ExpectedVersion: want.Version, Authority: binding(auth), Evidence: digest("d")}})
			claim("after-accept", 4)
			raw, err := agentprofile.EncodeSnapshot(h.actual.Snapshot())
			if err != nil {
				t.Fatal(err)
			}
			h.actual, err = agentprofile.RestoreSnapshot(raw)
			if err != nil {
				t.Fatal(err)
			}
			compare("restored")
		})
	}
}
