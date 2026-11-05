package reference_test

import (
	"context"
	"errors"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentFiniteRetryThroughEmissionWork(t *testing.T) {
	ctx := context.Background()
	path, options := t.TempDir(), ate.Options{AgentProfile: true}
	store, err := ate.Create(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	snapshot := func() api.StateSnapshot {
		t.Helper()
		got, err := store.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	ack := func(got api.CommitResult, err error) api.CommitResult {
		t.Helper()
		if err != nil || got.Code != api.CodeDurableAck {
			t.Fatalf("expected durable acknowledgement, got %+v %v", got, err)
		}
		return got
	}
	apply := func(name string, c agentprofile.Command) (api.CommitResult, error) {
		return store.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key(name), Command: c})
	}
	want := oracle.NewActivity("idempotent")
	check := func(name string, expected func(*oracle.Activity) error, actual func() (api.CommitResult, error)) {
		t.Helper()
		before := snapshot()
		wantErr := want.Batch(expected)
		got, err := actual()
		after := snapshot()
		if wantErr == nil {
			if ack(got, err).Fence != after.Fence || after.Fence.LSN != before.Fence.LSN+1 {
				t.Fatalf("%s should publish exactly once: before=%+v after=%+v", name, before, after)
			}
			return
		}
		if !errors.Is(err, agentprofile.ErrActionUnknown) || got.Code != api.CodeRejected || before != after {
			t.Fatalf("%s oracle rejected, got %+v %v, before=%+v after=%+v", name, got, err, before, after)
		}
		receipt, err := store.LookupRequest(ctx, api.LookupRequestRequest{Space: "space", Principal: "owner", RequestID: key(name).RequestID})
		if err != nil || receipt.Code != api.CodeNotFoundAtFence || receipt.ObservedFence != before.Fence {
			t.Fatalf("%s rejection changed receipts: %+v %v", name, receipt, err)
		}
	}
	control := func(name string, expected func(*oracle.Activity) error, command agentprofile.ControlCommand) {
		t.Helper()
		check(name, expected, func() (api.CommitResult, error) {
			return apply(name, agentprofile.Command{Control: &command})
		})
	}

	request := []byte("submit stable operation")
	input := ack(store.Mutate(ctx, api.MutateRequest{Key: key("input"), Batch: api.StateBatch{Operations: []api.StateMutation{
		{Key: []byte("request"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: request}},
	}}}))
	source := model.SnapshotAnchor{Space: "space", Fence: input.Fence}
	view, err := store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "owner", Anchor: &source})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := store.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("read request object: %+v %v", entries, err)
	}
	if err := store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	content := entries.Items[0].Value
	query := agentprofile.Query{ID: "query", Session: "session", Space: "space", Owner: "owner", Input: request, InputFence: input.Fence, Version: 1, Status: "open"}
	ack(apply("query", agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &query}}))
	contract := agentprofile.EffectContract{
		Version: "tool/v1", Recipient: "executor", AllowsMutation: true, KeyNamespace: "operation", RetentionNanos: 100,
		DifferentRequest: "reject", ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "request_deadline",
	}
	spec := agentprofile.ActivitySpec{
		Activity: "external", Query: query.ID, GoalEpoch: 1, InputFence: input.Fence, Role: "resource_prepare",
		Request: content, RequestDigest: independentBytesDigest(request), Recipient: contract.Recipient,
		Effect: "idempotent", IdempotencyKey: "operation-1", ResourceScope: "query", Contract: contract,
	}
	create := agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}}
	ack(apply("activity", agentprofile.Command{Control: &create}))

	claim := func(name, attempt string, seq uint64, use agentprofile.RetryUse) {
		t.Helper()
		control(name, func(a *oracle.Activity) error { return a.Claim(attempt, seq, 1, retryCheck(use)) },
			agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: attempt, Sequence: seq, Use: use}})
	}
	issue := func(attempt string) api.EmissionSpec {
		t.Helper()
		grant := api.EmissionSpec{
			ID: "grant-" + attempt, Query: query.ID, GoalEpoch: 1, Content: content, Anchor: source,
			Projection: "value_bytes", Length: uint64(len(request)), Destination: spec.Recipient,
			Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest,
		}
		ack(store.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: key(grant.ID), Command: api.EmissionCommand{Issue: &grant}}))
		return grant
	}
	dispatch := func(name string, grant api.EmissionSpec, use agentprofile.RetryUse) {
		t.Helper()
		claimStep, err := api.EmissionWorkStep(api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1})
		if err != nil {
			t.Fatal(err)
		}
		command := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: grant.Attempt, Evidence: content, Use: use}}
		dispatchStep, err := api.AgentWorkStep(agentprofile.Command{Control: &command})
		if err != nil {
			t.Fatal(err)
		}
		check(name, func(a *oracle.Activity) error { return a.Dispatch(grant.Attempt, 1, retryCheck(use)) }, func() (api.CommitResult, error) {
			return store.ApplyWork(ctx, api.ApplyWorkRequest{Key: key(name), Unit: api.WorkUnit{
				Kind: "emission_dispatch", Query: query.ID, Steps: []api.WorkStep{claimStep, dispatchStep},
			}})
		})
	}
	claim("claim-first", "first", 1, agentprofile.RetryUse{})
	dispatch("dispatch-first", issue("first"), agentprofile.RetryUse{})
	result := agentprofile.ActivityResult{
		ID: "unknown-result", Activity: spec.Activity, Attempt: "first",
		RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "unknown",
	}
	control("capture-unknown", func(a *oracle.Activity) error { return a.Capture(result.ID, result.Attempt, result.Outcome) },
		agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})

	publish := func(name string, version uint64, status string) agentprofile.IdempotencyGuarantee {
		t.Helper()
		g := agentprofile.IdempotencyGuarantee{
			Activity: spec.Activity, Version: version, Status: status, Contract: agentprofile.EffectContractDigest(contract),
			RequestDigest: spec.RequestDigest, KeyNamespace: contract.KeyNamespace, IdempotencyKey: spec.IdempotencyKey,
			ReceiverEpoch: "receiver", CoveredAttempts: []string{"first"}, NextSequence: 2, ValidFrom: 100, ValidUntil: 200,
			TerminalCondition: contract.LateArrivalFence, TerminalEvidence: content, Evidence: content,
		}
		g.ID = agentprofile.IdempotencyGuaranteeID(g)
		control(name, func(a *oracle.Activity) error { return a.PublishGuarantee(projectGuarantee(g, 1), g.CoveredAttempts) },
			agentprofile.ControlCommand{PutIdempotencyGuarantee: &agentprofile.PutIdempotencyGuaranteeCommand{Guarantee: g}})
		return g
	}
	authorize := func(name string, g agentprofile.IdempotencyGuarantee) agentprofile.RetryUse {
		t.Helper()
		a := agentprofile.RetryAuthorization{
			Activity: spec.Activity, ControlVersion: 2, Sequence: 2, Guarantee: g.ID,
			CheckedAt: 110, NotBefore: 120, NotAfter: 190, Evidence: content,
		}
		a.ID = agentprofile.RetryAuthorizationID(a)
		control(name, func(activity *oracle.Activity) error { return activity.AuthorizeRetry(projectPermit(a)) },
			agentprofile.ControlCommand{AuthorizeRetry: &agentprofile.AuthorizeRetryCommand{Authorization: a}})
		return agentprofile.RetryUse{Authorization: a.ID, ObservedAt: 130, SendNotAfter: a.NotAfter, Evidence: content}
	}
	retry := func(name string, use agentprofile.RetryUse) {
		t.Helper()
		control(name, func(a *oracle.Activity) error { return a.Retry(1, 1, retryCheck(use)) },
			agentprofile.ControlCommand{RetryActivity: &agentprofile.RetryActivityCommand{Activity: spec.Activity, ExpectedVersion: 1, Use: use}})
	}
	retry("missing-authorization", agentprofile.RetryUse{})
	old := authorize("authorize-first", publish("publish-first", 1, "active"))
	publish("revoke", 2, "revoked")
	retry("revoked-authorization", old)
	latest := publish("publish-renewal", 3, "active")
	retry("superseded-authorization", old)
	valid := authorize("authorize-renewal", latest)
	expired := valid
	expired.ObservedAt, expired.SendNotAfter = 190, 191
	retry("expired-retry", expired)
	retry("valid-retry", valid)
	claim("expired-claim", "second", 2, expired)
	valid.ObservedAt++
	claim("valid-claim", "second", 2, valid)
	grant := issue("second")

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, _, err = ate.OpenExisting(path, options)
	if err != nil {
		t.Fatal(err)
	}
	dispatch("expired-dispatch", grant, expired)
	valid.ObservedAt++
	dispatch("valid-dispatch", grant, valid)
	agent, err := store.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "space", Principal: "owner"})
	if err != nil || !agent.Found {
		t.Fatalf("read recovered agent: %+v %v", agent, err)
	}
	recovery := agent.Snapshot.Lifecycle.Recovery
	if recovery == nil || len(recovery.ActivityControls) != 1 || recovery.ActivityControls[0].Version != want.Version ||
		len(recovery.AttemptControls) != len(want.Attempts) {
		t.Fatalf("recovered retry state differs: got %+v want %+v", recovery, want)
	}
	for _, attempt := range recovery.AttemptControls {
		expected := want.Attempts[attempt.Attempt]
		if attempt.Status != want.Status(attempt.Attempt) ||
			retryCheck(attempt.ClaimUse) != expected.ClaimCheck || retryCheck(attempt.DispatchUse) != expected.DispatchCheck {
			t.Fatalf("recovered attempt differs: got %+v want %+v", attempt, expected)
		}
	}
}
