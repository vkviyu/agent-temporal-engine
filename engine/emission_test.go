package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func emissionFixture(t *testing.T, options ...engine.Options) (*integrationStore, model.ObjectID, model.CommitFence, agentprofile.AuthorityBinding) {
	t.Helper()
	s, payload, source := knowledgeFixture(t, options...)
	task := agentTask("emission-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("emission-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "emission-exec", Task: task.ID, Actor: "emitter", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("emission-exec"), Command: agentprofile.Command{Spawn: &x}})
	binding := agentprofile.AuthorityBinding{Deployment: "deploy", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("emission-authority"), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}}}})
	return s, payload, source, binding
}

func TestEmissionGrantClaimRevokeIsVersionedAndHistorical(t *testing.T) {
	ctx := context.Background()
	s, content, source, binding := emissionFixture(t)
	grant := api.EmissionSpec{ID: "grant-1", Execution: "emission-exec", Task: "emission-task", GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Offset: 0, Length: 5, Destination: "local-audit-sink"}
	before := integrationSnapshot(t, s.e)
	issued, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("emit-issue"), Command: api.EmissionCommand{Issue: &grant}})
	if err != nil || issued.Code != api.CodeDurableAck {
		t.Fatalf("issue=%+v %v", issued, err)
	}
	read := api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: grant.ID}
	got, err := s.e.GetEmission(ctx, read)
	if err != nil || !got.Found || got.Record.Status != "issued" || got.Record.Version != 1 {
		t.Fatalf("issued=%+v %v", got, err)
	}
	proof := got.Record.Proof
	wantDigest, digestErr := api.EmissionDependencyDigest([]model.ObjectID{content})
	if proof == nil || proof.PolicyFence != before.Fence || proof.PolicyRoot != before.Roots.ExecutionKnowledge || proof.Space != read.Space || proof.Principal != read.Principal || len(proof.Dependencies) != 1 || proof.Dependencies[0] != content || digestErr != nil || proof.DependencyDigest != wantDigest {
		t.Fatalf("issue proof=%+v; expected policy cut=%+v", proof, before.Fence)
	}
	claimed, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("emit-claim"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: binding}})
	if err != nil || claimed.Code != api.CodeDurableAck {
		t.Fatalf("claim=%+v %v", claimed, err)
	}
	claimedRead, err := s.e.GetEmission(ctx, read)
	if err != nil || claimedRead.Record.Status != "claimed" || claimedRead.Record.Version != 2 {
		t.Fatalf("claimed=%+v %v", claimedRead, err)
	}
	// Each chunk/range is a separate immutable grant; the stored spec cannot
	// be enlarged or redirected by Claim.
	if out, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("emit-claim-again"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: binding}}); !errors.Is(err, engine.ErrEmissionRejected) || out.Code != api.CodeRejected {
		t.Fatalf("reclaim=%+v %v", out, err)
	}
	ban := memory.Command{RetractSource: &memory.RetractSourceCommand{Source: content, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "sink denied"}}}
	applyMemory(t, s.e, "emit-ban", ban)
	newGrant := grant
	newGrant.ID = "grant-2"
	if out, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("emit-new-after-ban"), Command: api.EmissionCommand{Issue: &newGrant}}); !errors.Is(err, memory.ErrUseDenied) || out.Code != api.CodeRejected {
		t.Fatalf("reuse after ban=%+v %v", out, err)
	}
	revoked, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("emit-revoke"), Command: api.EmissionCommand{RevokeID: grant.ID, RevokeVersion: 2}})
	if err != nil || revoked.Code != api.CodeDurableAck {
		t.Fatalf("revoke=%+v %v", revoked, err)
	}
	out, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("emit-claim-revoked"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 3, ClaimAuthority: binding}})
	if !errors.Is(err, engine.ErrEmissionRejected) || out.Code != api.CodeRejected {
		t.Fatalf("claim revoked=%+v %v", out, err)
	}
	past := read
	past.Anchor = &model.SnapshotAnchor{Space: "host-space", Fence: claimed.Fence}
	history, err := s.e.GetEmission(ctx, past)
	if err != nil || !history.Found || history.Record.Status != "claimed" {
		t.Fatalf("history=%+v %v", history, err)
	}
	s.reopen()
	latest, err := s.e.GetEmission(ctx, read)
	if err != nil || latest.Record.Status != "revoked" {
		t.Fatalf("reopen=%+v %v", latest, err)
	}
	if _, err = s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	latest, err = s.e.GetEmission(ctx, read)
	if err != nil || latest.Record.Status != "revoked" {
		t.Fatalf("compact=%+v %v", latest, err)
	}
	if latest.Record.Proof == nil || latest.Record.Proof.DependencyDigest != proof.DependencyDigest || latest.Record.Proof.PolicyFence != proof.PolicyFence || latest.Record.Proof.PolicyRoot != proof.PolicyRoot {
		t.Fatalf("compaction changed issuance evidence: before=%+v after=%+v", proof, latest.Record.Proof)
	}
}

func TestEmissionNewAdmissionCannotDowngradeProofSchema(t *testing.T) {
	ctx := context.Background()
	s, content, source, binding := emissionFixture(t)
	grant := api.EmissionSpec{ID: "legacy-grant", Execution: "emission-exec", Task: "emission-task", GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Length: 1, Destination: "legacy-sink"}
	command := api.EmissionCommand{SchemaVersion: 1, Issue: &grant}
	request := api.ApplyEmissionRequest{Key: agentKey("legacy-issue"), Command: command}
	before := integrationSnapshot(t, s.e)
	ack, err := s.e.ApplyEmission(ctx, request)
	if !errors.Is(err, api.ErrInvalidRequest) || ack.Code != api.CodeNotSubmitted || integrationSnapshot(t, s.e) != before {
		t.Fatalf("legacy downgrade changed/fenced store: %+v %v", ack, err)
	}
	request.Key = agentKey("current-issue")
	request.Command.SchemaVersion = 3
	ack, err = s.e.ApplyEmission(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("current issue after rejected downgrade=%+v %v", ack, err)
	}
}

func TestEmissionClaimRechecksPolicyAfterProofIssuance(t *testing.T) {
	ctx := context.Background()
	s, content, source, binding := emissionFixture(t)
	grant := api.EmissionSpec{ID: "proof-withdrawal", Execution: "emission-exec", Task: "emission-task", GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Length: 1, Destination: "test-sink"}
	if ack, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("proof-issue"), Command: api.EmissionCommand{Issue: &grant}}); err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("issue=%+v %v", ack, err)
	}
	applyMemory(t, s.e, "proof-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: content, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "withdraw after issue"}}})
	s.reopen()
	ack, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("proof-claim-after-withdraw"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: binding}})
	if !errors.Is(err, memory.ErrUseDenied) || ack.Code != api.CodeRejected {
		t.Fatalf("old proof bypassed current policy: %+v %v", ack, err)
	}
	got, err := s.e.GetEmission(ctx, api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: grant.ID})
	if err != nil || got.Record.Status != "issued" || got.Record.Version != 1 || got.Record.Proof == nil {
		t.Fatalf("rejected claim mutated grant: %+v %v", got, err)
	}
}

func TestEmissionReadRejectsWrongScopeSelectorsAndCurrentPrivacyWithdrawal(t *testing.T) {
	ctx := context.Background()
	s, content, source, binding := emissionFixture(t)
	grant := api.EmissionSpec{ID: "private-grant", Execution: "emission-exec", Task: "emission-task", GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Length: 1, Destination: "private-sink"}
	ack, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("private-issue"), Command: api.EmissionCommand{Issue: &grant}})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("issue=%+v %v", ack, err)
	}
	read := api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: grant.ID}
	base := model.SnapshotAnchor{Space: read.Space, Fence: ack.Fence}
	for _, change := range []func(*model.SnapshotAnchor){
		func(a *model.SnapshotAnchor) { a.Space = "foreign" },
		func(a *model.SnapshotAnchor) {
			a.Knowledge = &model.KnowledgeKey{Actor: "actor", Execution: "execution"}
		},
		func(a *model.SnapshotAnchor) {
			a.Cursor = &model.Cursor{Store: a.Fence.Store, Space: a.Space, Revision: model.RevisionID(integrationID('a'))}
		},
	} {
		anchor := base.Clone()
		change(&anchor)
		bad := read
		bad.Anchor = &anchor
		if _, err := s.e.GetEmission(ctx, bad); !errors.Is(err, model.ErrAnchorMismatch) {
			t.Fatalf("invalid anchor accepted: %+v %v", anchor, err)
		}
	}
	applyMemory(t, s.e, "private-withdrawal", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: content, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "private content"}}})
	s.reopen()
	read.Anchor = &base
	if _, err := s.e.GetEmission(ctx, read); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatalf("historical grant disclosed after privacy withdrawal: %v", err)
	}
	withoutProfile := newIntegrationStore(t, engine.Options{})
	if _, err := withoutProfile.e.GetEmission(ctx, api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: "missing"}); !errors.Is(err, engine.ErrUnsupportedMethod) {
		t.Fatalf("disabled profile accepted emission read: %v", err)
	}
}

func TestEmissionProjectionBindsExactBytesAndRejectsTruncatedRanges(t *testing.T) {
	ctx := context.Background()
	s, content, source, binding := emissionFixture(t)
	base := api.EmissionSpec{ID: "range-grant", Execution: "emission-exec", Task: "emission-task", GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}, Projection: "value_bytes", Length: 1, Destination: "range-sink"}
	before := integrationSnapshot(t, s.e)
	for i, mutate := range []func(*api.EmissionSpec){
		func(s *api.EmissionSpec) { s.Offset = 1 }, // no bytes remain; no implicit EOF truncation
		func(s *api.EmissionSpec) { s.Length = 2 },
		func(s *api.EmissionSpec) { s.Offset, s.Length = 1<<40, 1<<30 },
		func(s *api.EmissionSpec) { s.Projection = "arbitrary" },
		func(s *api.EmissionSpec) { s.Anchor.Knowledge = &model.KnowledgeKey{Actor: "a", Execution: "e"} },
		func(s *api.EmissionSpec) {
			s.Anchor.Cursor = &model.Cursor{Store: s.Anchor.Fence.Store, Space: s.Anchor.Space, Revision: model.RevisionID(integrationID('a'))}
		},
	} {
		bad := base
		bad.Anchor = base.Anchor.Clone()
		mutate(&bad)
		ack, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey(fmt.Sprintf("bad-range-%d", i)), Command: api.EmissionCommand{Issue: &bad}})
		if err == nil || ack.Code == api.CodeDurableAck || integrationSnapshot(t, s.e) != before {
			t.Fatalf("invalid range/projection/selector %d changed store: %+v %v", i, ack, err)
		}
	}
	ack, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("exact-range"), Command: api.EmissionCommand{Issue: &base}})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("exact=%+v %v", ack, err)
	}
	got, err := s.e.GetEmission(ctx, api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: base.ID})
	if err != nil || got.Record.Proof == nil {
		t.Fatalf("read=%+v %v", got, err)
	}
	// Fixture's logical byte value is exactly 'a'; codec payload framing is not
	// part of value_bytes and cannot silently appear in the authorized request.
	wantSum := sha256.Sum256([]byte{'a'})
	want := model.ObjectID(hex.EncodeToString(wantSum[:]))
	p := got.Record.Proof
	if p.Projection != "value_bytes" || p.TotalBytes != 1 || p.ContentDigest != want || p.RangeDigest != want {
		t.Fatalf("wrong logical-byte proof=%+v expected digest=%s", p, want)
	}
	s.reopen()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	got, err = s.e.GetEmission(ctx, api.ReadEmissionRequest{Space: "host-space", Principal: "host-principal", ID: base.ID})
	if err != nil || got.Record.Proof == nil || got.Record.Proof.ContentDigest != want || got.Record.Proof.RangeDigest != want || got.Record.Proof.TotalBytes != 1 {
		t.Fatalf("range proof lost on compaction: %+v %v", got, err)
	}
}
