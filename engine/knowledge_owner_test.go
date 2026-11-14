package engine_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestKnowledgeOwnerPartitionsPermitDuplicateLocalExecutionIdentities(t *testing.T) {
	ctx := context.Background()
	s, payload, _ := knowledgeFixture(t)
	const other model.PrincipalID = "other-owner"
	task := agentTask("kt")
	task.Owner = other
	key := func(id string) model.RequestKey {
		return model.RequestKey{Space: "host-space", Principal: other, RequestID: model.RequestID(id)}
	}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: key("other-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "one", Task: task.ID, Actor: "same-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: key("other-execution"), Command: agentprofile.Command{Spawn: &x}})
	source := integrationSnapshot(t, s.e).Fence
	one := knowledgeObservation(payload, source, "one", 1)
	one.Label = "first owner's observation"
	one.ID = ""
	// Recreate the canonical content identity after changing the label.
	one.ID = knowledge.ObservationID(one)
	_, ack := applyObservation(t, s.e, "owner-one", one)
	two := knowledgeObservation(payload, source, "one", 1)
	two.Label = "second owner's observation"
	two.ID = knowledge.ObservationID(two)
	request := api.ApplyKnowledgeRequest{Key: key("owner-two"), Command: knowledge.Command{Observation: &two}}
	otherAck, err := s.e.ApplyKnowledge(ctx, request)
	if err != nil || otherAck.Code != api.CodeDurableAck {
		t.Fatalf("another owner collided with the same local sequence: %+v %v", otherAck, err)
	}
	check := func() {
		t.Helper()
		for _, test := range []struct {
			principal model.PrincipalID
			want      model.ObjectID
		}{{"host-principal", one.ID}, {other, two.ID}} {
			for _, selector := range []*model.KnowledgeKey{nil, &one.Key} {
				got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: test.principal, Knowledge: selector})
				if err != nil || len(got.Snapshot.Observations) != 1 || got.Snapshot.Observations[0].ID != test.want {
					t.Fatalf("principal=%s selector=%+v read another owner's knowledge: %+v %v", test.principal, selector, got, err)
				}
			}
		}
	}
	check()
	s.reopen()
	check()
	if again, err := s.e.ApplyKnowledge(ctx, request); err != nil || again != otherAck {
		t.Fatalf("owner-bound retry changed after reopen: %+v %v", again, err)
	}
	pastAnchor := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence, Knowledge: &one.Key}
	past, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: other, Anchor: &pastAnchor})
	if err != nil || past.Found || len(past.Snapshot.Observations) != 0 {
		t.Fatalf("historical read exposed first owner's observation: %+v %v", past, err)
	}
}

func TestKnowledgeInvalidPrincipalNeverFencesPublishedStore(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	o := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "principal-source", o)
	before := integrationSnapshot(t, s.e)
	for _, principal := range []model.PrincipalID{"", model.PrincipalID(string([]byte{255})), model.PrincipalID(strings.Repeat("p", model.MaxIdentityBytes+1))} {
		got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: principal, Knowledge: &o.Key})
		if err == nil || !reflect.DeepEqual(got, api.KnowledgeStateResult{}) {
			t.Fatalf("invalid principal got data: %+v %v", got, err)
		}
		class := engine.ClassifyError(err)
		if class != api.ErrorInvalidRequest && class != api.ErrorUnauthorized {
			t.Fatalf("caller error was misclassified as storage failure: %s %v", class, err)
		}
		if current := integrationSnapshot(t, s.e); current != before {
			t.Fatal("invalid principal changed the published cut")
		}
	}
}

func TestMemoryKnowledgeSourceCannotClaimUnobservedDomainContent(t *testing.T) {
	ctx := context.Background()
	s, observedPayload, source := knowledgeFixture(t)
	o := knowledgeObservation(observedPayload, source, "one", 1)
	applyObservation(t, s.e, "adopt-for-memory", o)
	newSource := integrationMutate(t, s.e, integrationRequest("unobserved-data", integrationPut([]byte("unobserved"), integrationBytes('z'))))
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	items, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	var unobserved model.ObjectID
	for _, item := range items.Items {
		if string(item.Key) == "unobserved" {
			unobserved = item.Value
		}
	}
	if unobserved == "" {
		t.Fatal("missing fixture value")
	}
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: newSource.Fence, Knowledge: &o.Key}
	bad := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("pretend the execution read this"), Sources: []memory.Source{{SourceID: unobserved, Anchor: anchor}}}
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: agentKey("unobserved-memory"), Command: memory.Command{PutCandidate: &bad}})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("unobserved K source was accepted: %+v %v", out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("rejection changed or fenced the store")
	}
	good := bad
	good.Claim = []byte("the execution adopted this source")
	good.Sources = []memory.Source{{SourceID: observedPayload, Anchor: anchor}}
	applyMemory(t, s.e, "observed-memory", memory.Command{PutCandidate: &good})
}

func TestKnowledgeExplicitlyBridgesPromotedMemoryByTypedLogicalID(t *testing.T) {
	s, payload, source := knowledgeFixture(t)
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("promoted bridge fact"), Sources: []memory.Source{{SourceID: payload, Anchor: sourceAnchor(source)}}}
	applyMemory(t, s.e, "bridge-memory", memory.Command{PutCandidate: &m})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "bridge-validate", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{{SourceID: payload, Anchor: sourceAnchor(source)}}}}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	applyMemory(t, s.e, "bridge-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	cut := integrationSnapshot(t, s.e).Fence
	key := model.KnowledgeKey{Actor: "same-actor", Execution: "one"}
	o := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: cut}}, Payload: promoted.ID, PayloadKind: "memory", Sequence: 1, Label: "promoted memory"}
	o.ID = knowledge.ObservationID(o)
	applyObservation(t, s.e, "bridge-observation", o)
	pathAck := applyMemory(t, s.e, "bridge-path", memory.Command{BuildPath: &memory.BuildPathCommand{Space: "host-space", Anchor: sourceAnchor(cut), Steps: []memory.Step{{Node: model.NodeRef{Kind: model.NodeObject, Object: payload}, Memory: promoted.ID, Order: 0, Outcome: "confirmed"}}, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{{SourceID: payload, Anchor: sourceAnchor(source)}}}}})
	memoryState, err := s.e.GetMemoryState(context.Background(), api.ReadMemoryStateRequest{Space: "host-space"})
	if err != nil || len(memoryState.Snapshot.Paths) != 1 {
		t.Fatalf("typed path missing: %+v %v", memoryState, err)
	}
	path := memoryState.Snapshot.Paths[0]
	pathObservation := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: pathAck.Fence}}, Payload: path.ID, PayloadKind: "experience_path", Sequence: 2, Label: "promoted path"}
	pathObservation.ID = knowledge.ObservationID(pathObservation)
	applyObservation(t, s.e, "bridge-path-observation", pathObservation)
	state, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
	if err != nil || len(state.Snapshot.Observations) != 2 || state.Snapshot.Observations[0].PayloadKind == state.Snapshot.Observations[1].PayloadKind {
		t.Fatalf("typed observation=%+v %v", state, err)
	}
	view, err := s.e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := s.e.GetNode(context.Background(), api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "memory", Kind: "memory", ID: model.EntityID(promoted.ID)}}})
	_ = s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref})
	if err != nil || detail.Encoding != "memory/Memory/v1+json" || len(detail.Data) == 0 {
		t.Fatalf("typed node=%+v %v", detail, err)
	}
}

func sourceAnchor(fence model.CommitFence) model.SnapshotAnchor {
	return model.SnapshotAnchor{Space: "host-space", Fence: fence}
}
