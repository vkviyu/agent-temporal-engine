package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func TestEmissionStoredProofRejectsMalformedDependencyAndPolicyEvidence(t *testing.T) {
	a, b := model.ObjectID(strings.Repeat("a", 64)), model.ObjectID(strings.Repeat("b", 64))
	fence := model.CommitFence{Store: "store", LSN: 5, CommitDigest: model.CommitDigest(a)}
	valid := func() api.EmissionRecord {
		digest, err := api.EmissionDependencyDigest([]model.ObjectID{a, b})
		if err != nil {
			t.Fatal(err)
		}
		return api.EmissionRecord{Spec: api.EmissionSpec{ID: "grant", Content: a, Length: 1, Anchor: model.SnapshotAnchor{Space: "space", Fence: fence}}, Status: "issued", Version: 1, Proof: &api.EmissionProof{Space: "space", Principal: "owner", PolicyFence: fence, Dependencies: []model.ObjectID{a, b}, DependencyDigest: digest, Projection: "payload", ContentDigest: a, RangeDigest: b, TotalBytes: 2}}
	}
	if _, err := encodeEmissionRecord(valid()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*api.EmissionRecord)
	}{
		{"changed_dependency", func(r *api.EmissionRecord) { r.Proof.Dependencies[1] = a }},
		{"wrong_digest", func(r *api.EmissionRecord) { r.Proof.DependencyDigest = b }},
		{"wrong_space", func(r *api.EmissionRecord) { r.Proof.Space = "other" }},
		{"missing_owner", func(r *api.EmissionRecord) { r.Proof.Principal = "" }},
		{"wrong_store", func(r *api.EmissionRecord) { r.Proof.PolicyFence.Store = "other" }},
		{"predates_content", func(r *api.EmissionRecord) { r.Proof.PolicyFence.LSN-- }},
		{"invalid_root", func(r *api.EmissionRecord) { r.Proof.PolicyRoot = "invalid" }},
		{"invalid_projection", func(r *api.EmissionRecord) { r.Proof.Projection = "other" }},
		{"projection_mismatch", func(r *api.EmissionRecord) { r.Proof.Projection = "value_bytes" }},
		{"invalid_content_digest", func(r *api.EmissionRecord) { r.Proof.ContentDigest = "invalid" }},
		{"invalid_range_digest", func(r *api.EmissionRecord) { r.Proof.RangeDigest = "invalid" }},
		{"offset_out_of_bounds", func(r *api.EmissionRecord) { r.Spec.Offset = 3 }},
		{"length_truncated", func(r *api.EmissionRecord) { r.Spec.Length = 3 }},
		{"omitted_content", func(r *api.EmissionRecord) {
			r.Proof.Dependencies = []model.ObjectID{b}
			r.Proof.DependencyDigest, _ = api.EmissionDependencyDigest(r.Proof.Dependencies)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := valid()
			tc.mutate(&r)
			raw, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeEmissionRecord(raw); err == nil {
				t.Fatal("malformed stored proof accepted")
			}
		})
	}
}

func emissionCoordinatorFixture(t *testing.T) (*Engine, *coordinatorIO, api.EmissionSpec) {
	t.Helper()
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 16}})
	task := profileCoordinatorTask("emission-task")
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "emission-task", agentprofile.Command{CreateTask: &task}))))
	x := agentprofile.Execution{ID: "emission-execution", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "emission-execution", agentprofile.Command{Spawn: &x}))))
	binding := agentprofile.AuthorityBinding{Deployment: "deploy", OwnerEpoch: 1, Incarnation: 1, Holder: "holder"}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "emission-authority", agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(strings.Repeat("a", 64))}}}}))))
	contentACK := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, "emission-content", "content", "bytes", api.ValueCondition{}))))
	root, err := domainRoot(ctx, e.index, e.roots, task.Space)
	if err != nil {
		t.Fatal(err)
	}
	content, found, err := state.Get(ctx, e.index, task.Space, root, []byte("content"))
	if err != nil || !found {
		t.Fatalf("content=%v %v", found, err)
	}
	return e, disk, api.EmissionSpec{ID: "grant", Execution: x.ID, Task: task.ID, GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: content, Anchor: model.SnapshotAnchor{Space: task.Space, Fence: contentACK.Fence}, Length: 1, Destination: "sink"}
}

func TestEmissionPolicyProofNamesCoalescedCandidatePredecessor(t *testing.T) {
	ctx := context.Background()
	e, disk, spec := emissionCoordinatorFixture(t)
	_, _, currentsBefore, _ := disk.counts()
	gate := newCoordinatorGate()
	disk.arm("current", gate)
	base := coordinatorCommit(e, ctx, coordinatorRequest(t, "emission-group-base", "base", "base", api.ValueCondition{}))
	coordinatorWait(t, gate.entered)
	memoryBody, err := memory.EncodeCommand(memory.Command{RetractSource: &memory.RetractSourceCommand{Source: model.ObjectID(strings.Repeat("b", 64)), Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "unrelated source"}}})
	if err != nil {
		t.Fatal(err)
	}
	memoryRequest := api.CommitRequest{Space: spec.Anchor.Space, Principal: "host", RequestID: "emission-group-policy", Method: api.MethodApplyMemory, Body: memoryBody}
	policy := coordinatorCommit(e, ctx, memoryRequest)
	coordinatorQueued(t, e, memoryRequest)
	emissionBody, err := api.EncodeEmissionCommand(api.EmissionCommand{Issue: &spec})
	if err != nil {
		t.Fatal(err)
	}
	emissionRequest := api.CommitRequest{Space: spec.Anchor.Space, Principal: "host", RequestID: "emission-group-issue", Method: api.MethodApplyEmission, Body: emissionBody}
	emission := coordinatorCommit(e, ctx, emissionRequest)
	coordinatorQueued(t, e, emissionRequest)
	gate.unblock()
	coordinatorAck(t, coordinatorWait(t, base))
	policyACK := coordinatorAck(t, coordinatorWait(t, policy))
	coordinatorAck(t, coordinatorWait(t, emission))
	_, _, currentsAfter, _ := disk.counts()
	if currentsAfter-currentsBefore != 2 {
		t.Fatalf("policy and Issue did not share the second physical publication: %d", currentsAfter-currentsBefore)
	}
	got, err := e.GetEmission(ctx, api.ReadEmissionRequest{Space: spec.Anchor.Space, Principal: "host", ID: spec.ID})
	if err != nil || got.Record.Proof == nil {
		t.Fatalf("proof=%+v %v", got, err)
	}
	e.mu.Lock()
	policyRoots, _, err := e.rootsAtFence(ctx, policyACK.Fence, false)
	e.mu.Unlock()
	if err != nil || got.Record.Proof.PolicyFence != policyACK.Fence || got.Record.Proof.PolicyRoot != policyRoots.ExecutionKnowledge {
		t.Fatalf("proof mixed publication/candidate cuts: proof=%+v policy=%+v roots=%+v %v", got.Record.Proof, policyACK.Fence, policyRoots, err)
	}
}

// A current V1 emission remains idempotent across reopen and compaction. Legacy
// envelopes are rejected before they can enter the journal.
func TestEmissionJournalReplaysAndRetainsCurrentReceipt(t *testing.T) {
	ctx := context.Background()
	e, disk, spec := emissionCoordinatorFixture(t)
	body, err := api.EncodeEmissionCommand(api.EmissionCommand{SchemaVersion: 3, Issue: &spec})
	if err != nil {
		t.Fatal(err)
	}
	req := api.CommitRequest{Space: spec.Anchor.Space, Principal: "host", RequestID: "current-emission", Method: api.MethodApplyEmission, Body: body}
	original, err := e.Commit(ctx, req)
	if err != nil || original.Code != api.CodeDurableAck {
		t.Fatalf("current emission commit=%+v %v", original, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var current *Engine
	t.Cleanup(func() {
		if current != nil {
			_ = current.Close()
		}
	})
	for pass := 0; pass < 2; pass++ {
		current, _, err = OpenExisting(disk, e.options)
		if err != nil {
			t.Fatalf("current journal reopen=%v", err)
		}
		got, err := current.GetEmission(ctx, api.ReadEmissionRequest{Space: req.Space, Principal: req.Principal, ID: spec.ID})
		if err != nil || !got.Found || got.Record.Proof == nil {
			t.Fatalf("current proof changed=%+v %v", got, err)
		}
		if retry, err := current.Commit(ctx, req); err != nil || retry != original {
			t.Fatalf("current retry=%+v expected=%+v %v", retry, original, err)
		}
		if pass == 0 {
			if _, err := current.CompactAuditFull(ctx); err != nil {
				t.Fatal(err)
			}
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
