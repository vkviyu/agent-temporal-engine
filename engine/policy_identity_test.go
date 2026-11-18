package engine

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

var policyIdentityWithdrawals = []struct {
	kind         memory.RetractionKind
	audit, reuse error
}{
	{memory.RetractionFactualCorrection, nil, memory.ErrUseDenied},
	{memory.RetractionUseBan, nil, memory.ErrUseDenied},
	{memory.RetractionPrivacyErasure, memory.ErrHistoryAccessDenied, memory.ErrHistoryAccessDenied},
	{memory.RetractionACLRevocation, memory.ErrHistoryAccessDenied, memory.ErrHistoryAccessDenied},
}

// These are selector aliases at the policy boundary, not canonical records
// with fabricated colliding hashes. Resolution and authorization are tested
// separately below; converting a selector is never evidence of either.
func policyIdentityAliases(id model.ObjectID) []policyDependency {
	return []policyDependency{
		{kind: policyObject, id: id},
		{kind: policyObservation, id: id},
		{kind: policyPacket, id: id},
		{kind: policySummary, id: id},
		{kind: policyMemory, id: id},
		{kind: policyPath, id: id},
		{kind: policyAnnotation, id: id},
		{kind: policyNode, id: id},
		{kind: policyRevision, id: id},
		{kind: policyControl, id: id},
	}
}

func TestPolicyIdentityConversionPreservesDigestAndSequence(t *testing.T) {
	id := model.ObjectID(fmt.Sprintf("%x", sha256.Sum256([]byte("policy selector"))))
	other := model.ObjectID(fmt.Sprintf("%x", sha256.Sum256([]byte("other selector"))))
	dependencies := policyIdentityAliases(id)
	dependencies = append(dependencies, policyDependency{kind: policyObject, id: other},
		policyDependency{kind: policyNode}, policyDependency{kind: policyMemory, id: id})
	want := []model.ObjectID{id, id, id, id, id, id, id, id, id, id, other, "", id}
	if got := policyIdentities(dependencies); !reflect.DeepEqual(got, want) {
		t.Fatalf("conversion changed digest, order, duplicates or empty identity: got %v want %v", got, want)
	}
	for _, empty := range [][]policyDependency{nil, {}} {
		if got := policyIdentities(empty); len(got) != 0 {
			t.Fatalf("empty conversion introduced identities: %v", got)
		}
	}
	for _, test := range policyIdentityWithdrawals {
		t.Run(string(test.kind), func(t *testing.T) {
			store := memory.New()
			for _, alias := range policyIdentityAliases(id) {
				for _, purpose := range []memory.AccessPurpose{memory.AccessAudit, memory.AccessReuse} {
					if err := store.AccessPolicy().Check(policyIdentities([]policyDependency{alias}), purpose); err != nil {
						t.Fatalf("unwithdrawn %s/%s: %v", alias.kind, purpose, err)
					}
				}
			}
			if _, err := store.RetractSource(id, memory.Retraction{Kind: test.kind, Reason: "shared policy identity"}); err != nil {
				t.Fatal(err)
			}
			policy := store.AccessPolicy()
			for _, alias := range policyIdentityAliases(id) {
				ids := policyIdentities([]policyDependency{alias})
				if err := policy.Check(ids, memory.AccessAudit); !errors.Is(err, test.audit) {
					t.Errorf("%s audit = %v, want %v", alias.kind, err, test.audit)
				}
				if err := policy.Check(ids, memory.AccessReuse); !errors.Is(err, test.reuse) {
					t.Errorf("%s reuse = %v, want %v", alias.kind, err, test.reuse)
				}
			}
			for _, purpose := range []memory.AccessPurpose{memory.AccessAudit, memory.AccessReuse} {
				if err := policy.Check(policyIdentities(objectPolicyDependencies(other)), purpose); err != nil {
					t.Fatalf("withdrawal affected a different digest: %v", err)
				}
			}
		})
	}
}

func policyIdentityWithdraw(t *testing.T, e *Engine, space model.SpaceID, id model.ObjectID, kind memory.RetractionKind) {
	t.Helper()
	out, err := e.ApplyMemory(t.Context(), api.ApplyMemoryRequest{
		Key: model.RequestKey{Space: space, Principal: "host", RequestID: "policy-identity-withdraw"},
		Command: memory.Command{RetractSource: &memory.RetractSourceCommand{
			Source: id, Retraction: memory.Retraction{Kind: kind, Reason: "withdraw after warming policy reads"},
		}},
	})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("withdrawal = %+v, %v", out, err)
	}
}

func TestPolicyIdentityReadPolicySpaceIsolationAndWarmCut(t *testing.T) {
	for _, test := range policyIdentityWithdrawals {
		t.Run(string(test.kind), func(t *testing.T) {
			e, _ := coordinatorStore(t, Options{})
			ctx := t.Context()
			seed := coordinatorRequest(t, "policy-seed", "evidence", "policy evidence", api.ValueCondition{})
			ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, seed)))
			packet := knowledgeFormatPacket(seed.Space, ack.Fence, "policy-identity/v1")
			// Use the same selector digest in both Spaces. No cross-Space
			// canonical object collision or object visibility is asserted.
			spaces := []model.SpaceID{seed.Space, "other-space"}
			warm, closeWarm, err := e.beginReadCut(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer closeWarm()
			for _, space := range spaces {
				for i := 0; i < 2; i++ {
					policy, err := e.readPolicy(warm, space)
					if err != nil {
						t.Fatal(err)
					}
					if err := policy.Check([]model.ObjectID{packet.ID}, memory.AccessReuse); err != nil {
						t.Fatalf("warm %s: %v", space, err)
					}
				}
			}
			policyIdentityWithdraw(t, e, seed.Space, packet.ID, test.kind)
			e.mu.Lock()
			staleErr := e.checkReadCutLocked(warm)
			e.mu.Unlock()
			if !errors.Is(staleErr, api.ErrReadAuthorizationChanged) {
				t.Fatalf("warmed pre-withdrawal cut could still authorize delivery: %v", staleErr)
			}
			closeWarm()
			// Both cache population orders matter: a cache missing its Space
			// key could either spread a denial or hide one.
			for _, order := range [][]model.SpaceID{spaces, {spaces[1], spaces[0]}} {
				fresh, closeFresh, err := e.beginReadCut(ctx)
				if err != nil {
					t.Fatal(err)
				}
				func() {
					defer closeFresh()
					for i := 0; i < 2; i++ {
						for _, space := range order {
							policy, err := e.readPolicy(fresh, space)
							if err != nil {
								t.Fatal(err)
							}
							audit, reuse := test.audit, test.reuse
							if space != seed.Space {
								audit, reuse = nil, nil
							}
							for _, alias := range policyIdentityAliases(packet.ID) {
								ids := policyIdentities([]policyDependency{alias})
								if err := policy.Check(ids, memory.AccessAudit); !errors.Is(err, audit) {
									t.Errorf("%s/%s audit = %v, want %v", space, alias.kind, err, audit)
								}
								if err := policy.Check(ids, memory.AccessReuse); !errors.Is(err, reuse) {
									t.Errorf("%s/%s reuse = %v, want %v", space, alias.kind, err, reuse)
								}
							}
						}
					}
				}()
			}
		})
	}
}

func TestPolicyIdentitySharedWithdrawalPreservesOwnerAuthorization(t *testing.T) {
	for _, test := range policyIdentityWithdrawals {
		t.Run(string(test.kind), func(t *testing.T) {
			e, _ := coordinatorStore(t, Options{AgentProfile: true})
			ctx := t.Context()
			owners := []model.PrincipalID{"host", "other-owner"}
			key := model.KnowledgeKey{Actor: "worker", Execution: "shared-local-execution"}
			var ack api.CommitResult
			for _, owner := range owners {
				task := profileCoordinatorTask("shared-local-task")
				task.Owner = owner
				execution := agentprofile.Execution{ID: key.Execution, Task: task.ID, Actor: key.Actor, Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
				for i, command := range []agentprofile.Command{{CreateTask: &task}, {Spawn: &execution}} {
					req := profileCoordinatorRequest(t, fmt.Sprintf("policy-owner-%d", i), command)
					req.Principal = owner
					ack = coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, req)))
				}
			}
			space := model.SpaceID("coordinator-space")
			packet := knowledgeFormatPacket(space, ack.Fence, "shared-owner-packet/v1")
			packetBytes, err := packet.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			// This query packet has no execution ViewDigest. Owner is catalog
			// scope, so both owners can publish identical canonical content.
			// Execution selector authorization is checked independently below.
			publish := func(owner model.PrincipalID, id model.RequestID, value knowledge.ContextPacket) {
				t.Helper()
				out, err := e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{
					Key:     model.RequestKey{Space: space, Principal: owner, RequestID: id},
					Command: knowledge.Command{Packet: &value},
				})
				if err != nil || out.Code != api.CodeDurableAck {
					t.Fatalf("publish for %s = %+v, %v", owner, out, err)
				}
			}
			for _, owner := range owners {
				publish(owner, "shared-packet", packet)
			}
			private := packet
			private.Renderer = "private-owner-packet/v1"
			private.ID = knowledge.PacketID(private)
			publish(owners[0], "private-packet", private)
			before, err := e.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			anchor := model.SnapshotAnchor{Space: space, Fence: before.Fence}
			checkAuthorization := func() {
				t.Helper()
				cut, closeCut, err := e.beginReadCut(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer closeCut()
				roots := e.readRoots(cut)
				for _, owner := range owners {
					if err := e.authorizeKnowledge(cut, roots, space, owner, key); err != nil {
						t.Fatalf("authorized owner %s rejected: %v", owner, err)
					}
					got, err := e.packetAt(cut, roots, space, owner, packet.ID)
					if err != nil {
						t.Fatalf("shared packet missing for %s: %+v, %v", owner, got, err)
					}
					raw, err := got.packet.Canonical()
					if err != nil || !bytes.Equal(raw, packetBytes) {
						t.Fatalf("shared canonical packet changed for %s: %v", owner, err)
					}
				}
				if got, err := e.packetAt(cut, roots, space, owners[1], private.ID); !errors.Is(err, agentprofile.ErrUnknown) || got.packet.ID != "" {
					t.Fatalf("private packet crossed owner partition: %+v, %v", got, err)
				}
				if got, err := e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: space, Principal: "outsider", Knowledge: &key}); !errors.Is(err, agentprofile.ErrCommandScope) || !reflect.DeepEqual(got, api.KnowledgeStateResult{}) {
					t.Fatalf("digest equality bypassed execution authorization: %+v, %v", got, err)
				}
			}
			checkAuthorization()
			baseline := make(map[model.PrincipalID]api.KnowledgeStateResult)
			for _, owner := range owners {
				req := api.ReadKnowledgeStateRequest{Space: space, Principal: owner, Anchor: &anchor}
				for i := 0; i < 2; i++ {
					got, err := e.GetKnowledgeState(ctx, req)
					wantCount := 1
					if owner == owners[0] {
						wantCount = 2
					}
					if err != nil || !got.Found || len(got.Snapshot.Packets) != wantCount {
						t.Fatalf("warm owner read %s = %+v, %v", owner, got, err)
					}
					baseline[owner] = got
				}
			}
			policyIdentityWithdraw(t, e, space, packet.ID, test.kind)
			checkAuthorization()
			after, err := e.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, owner := range owners {
				got, err := e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: space, Principal: owner, Anchor: &anchor})
				if !errors.Is(err, test.audit) {
					t.Fatalf("historical audit for %s = %v, want %v", owner, err, test.audit)
				}
				if test.audit == nil && !reflect.DeepEqual(got, baseline[owner]) {
					t.Fatalf("UseBan/correction changed %s audit history", owner)
				}
				if test.audit != nil && !reflect.DeepEqual(got, api.KnowledgeStateResult{}) {
					t.Fatalf("withdrawn history disclosed for %s: %+v", owner, got)
				}
				cut, closeCut, err := e.beginReadCut(ctx)
				if err != nil {
					t.Fatal(err)
				}
				record, err := e.packetAt(cut, e.readRoots(cut), space, owner, packet.ID)
				if err == nil {
					err = e.checkPolicyIDs(cut, space, policyIdentities([]policyDependency{{kind: policyPacket, id: record.packet.ID}}), memory.AccessReuse)
				}
				closeCut()
				if !errors.Is(err, test.reuse) {
					t.Fatalf("authorized owner %s escaped shared reuse denial: %v", owner, err)
				}
			}
			if final, err := e.Snapshot(ctx); err != nil || final != after {
				t.Fatalf("policy reads changed or fenced the engine: %+v, %v", final, err)
			}
		})
	}
}
