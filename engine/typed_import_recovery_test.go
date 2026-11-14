package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func beginMemorySession(t *testing.T, s *integrationStore, session, input string) (model.KnowledgeKey, knowledge.Observation, api.CommitResult) {
	t.Helper()
	task := agentTask(model.EntityID(session + "-task"))
	execution := agentprofile.Execution{ID: model.ExecutionID(session + "-execution"), Actor: "memory-agent", Task: task.ID, Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	key := model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID}
	binding := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: session}
	goal, err := agentprofile.GoalKnowledge(task)
	if err != nil {
		t.Fatal(err)
	}
	inputKey, goalKey := session+"-input", session+"-goal"
	published := integrationMutate(t, s.e, integrationRequest(session+"-source",
		integrationPut([]byte(inputKey), api.Value{Kind: api.ValueBytes, Data: []byte(input)}),
		integrationPut([]byte(goalKey), api.Value{Kind: api.ValueBytes, Data: goal}),
	))
	inputID, _ := recoveryValueAt(t, s, published.Fence, inputKey)
	goalID, _ := recoveryValueAt(t, s, published.Fence, goalKey)
	query := agentprofile.Query{ID: session + "-query", Session: session, Space: "host-space", Owner: "host-principal", Input: []byte(input), InputFence: published.Fence, Status: "open", Version: 1}
	goalObservation := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: sourceAnchor(published.Fence)}, Payload: goalID, Sequence: 2, Label: "initial goal"}
	goalObservation.ID = knowledge.ObservationID(goalObservation)
	start := api.ApplyWorkRequest{Key: agentKey(session + "-start"), Unit: api.WorkUnit{Kind: "query_start", Query: query.ID, Steps: []api.WorkStep{
		workControl(t, agentprofile.ControlCommand{StartWork: &agentprofile.StartWorkCommand{Query: query.ID, Slot: "root", ExpectedQueryVersion: 1, Task: task, Execution: execution}}),
		workControl(t, agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: execution.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}}),
		workSeedObservation(t, key, inputID, published.Fence, 1, "initial query"),
		workSeedObservation(t, key, goalID, published.Fence, 2, "initial goal"),
		workCheckpoint(t, execution.ID, binding, published.Fence, session+"-continuation", 0, 0),
	}}}
	raw, err := api.EncodeWorkUnit(start.Unit)
	if err != nil {
		t.Fatal(err)
	}
	workApply(t, s, api.ApplyWorkRequest{Key: agentKey(session + "-intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: query.ID, Intake: &api.QueryIntake{
		Query: query, Route: "test/v1", Configuration: []byte(`{"model":"test"}`),
		Next: api.CommitRequest{Space: start.Key.Space, Principal: start.Key.Principal, RequestID: start.Key.RequestID, Method: api.MethodApplyWork, Body: raw},
	}}})
	return key, goalObservation, workApply(t, s, start)
}

func TestPromotedMemoryRequiresExplicitAdoptionAcrossSessions(t *testing.T) {
	ctx := t.Context()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	old, goal, published := beginMemorySession(t, s, "first-session", "find where the loader reads resources")
	anchored := memory.Source{SourceID: goal.Payload, Anchor: goal.Source.Anchor}
	applyMemory(t, s.e, "session-memory-candidate", memory.Command{PutCandidate: &memory.Memory{
		Space: "host-space", Kind: memory.KindFact, Claim: []byte("inspect the loader for resource reads"), Sources: []memory.Source{anchored},
	}})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "session-memory-validate", memory.Command{Validate: &memory.ValidateCommand{
		ID: candidate.ID, Validation: memory.Validation{Passed: true, StrategyVersion: 1, Evidence: []memory.Source{anchored}},
	}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	applyMemory(t, s.e, "session-memory-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	origin := knowledge.Observation{Key: old, Source: knowledge.SourceAnchor{Anchor: sourceAnchor(integrationSnapshot(t, s.e).Fence)}, Payload: promoted.ID, PayloadKind: "memory", Sequence: 3, Label: "promoted knowledge from first session"}
	origin.ID = knowledge.ObservationID(origin)
	applyObservation(t, s.e, "first-session-adopt", origin)
	oldState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &old})
	if err != nil || oldState.Revision == nil || oldState.Revision.Sequence != 3 {
		t.Fatalf("first session = %+v, %v", oldState, err)
	}
	target, targetGoal, targetStart := beginMemorySession(t, s, "second-session", "apply the previous finding to this sample")
	before := model.SnapshotAnchor{Space: "host-space", Fence: targetStart.Fence, Knowledge: &target}
	ref := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "memory", Kind: "memory", ID: model.EntityID(promoted.ID)}}
	read := func(anchor *model.SnapshotAnchor, visible bool) {
		t.Helper()
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Knowledge: &target, Anchor: anchor})
		if err != nil {
			t.Fatal(err)
		}
		defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		_, err = s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref})
		if (err == nil) != visible {
			t.Fatalf("memory view expected visible=%v, got %v", visible, err)
		}
	}
	read(nil, false)
	anchor := oldState.Anchor
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: oldState.Revision.ID}
	request := api.ApplyKnowledgeRequest{Key: agentKey("second-session-adopt"), Command: knowledge.Command{Import: &knowledge.ImportCommand{
		Target: target, ExpectedSequence: 2, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: oldState.Frontier}, Observations: []model.ObjectID{origin.ID},
	}}}
	receipt, err := s.e.ApplyKnowledge(ctx, request)
	if err != nil || receipt.Code != api.CodeDurableAck {
		t.Fatalf("cross-session import = %+v, %v", receipt, err)
	}
	read(nil, true)
	read(&before, false)
	targetState, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &target})
	if err != nil {
		t.Fatal(err)
	}
	adopted := byImportedOrigin(targetState.Snapshot.Observations, origin.ID)
	if adopted.Key != target || adopted.Payload != promoted.ID || adopted.Sequence != 3 || !reflect.DeepEqual(adopted.Source.Anchor, anchor) {
		t.Fatalf("cross-session adoption lost exact provenance: %+v", adopted)
	}
	packet, _ := goalCorePacket(t, s, "second-session-packet", target, targetGoal, []knowledge.Observation{targetGoal, adopted}, goal.Payload, published.Fence)
	s.reopen()
	read(&before, false)
	read(nil, true)
	current, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &target})
	if err != nil || len(current.Snapshot.Packets) != 1 || !reflect.DeepEqual(current.Snapshot.Packets[0], packet) {
		t.Fatalf("cross-session packet did not survive replay = %+v, %v", current, err)
	}
	if retry, err := s.e.ApplyKnowledge(ctx, request); err != nil || retry != receipt {
		t.Fatalf("cross-session receipt changed = %+v, %v", retry, err)
	}
}

// This exercises the actual knowledge Import reducer, rather than relabeling
// a direct schema3 Observation as an import. Both typed payload kinds must
// survive canonical decoding, publication, reopen and physical compaction.
func TestTypedKnowledgeImportReopensCompactsAndRetainsOriginalReceipt(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	provenance := memory.Source{SourceID: payload, Anchor: sourceAnchor(source)}
	m := memory.Memory{Space: "host-space", Kind: memory.KindProcedure, Claim: []byte("retain the original evidence and failed alternatives"), Sources: []memory.Source{provenance}}
	applyMemory(t, s.e, "typed-import-candidate", memory.Command{PutCandidate: &m})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	validation := memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{provenance}}
	applyMemory(t, s.e, "typed-import-validation", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: validation}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	applyMemory(t, s.e, "typed-import-promotion", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	applyMemory(t, s.e, "typed-import-path", memory.Command{BuildPath: &memory.BuildPathCommand{Space: "host-space", Anchor: sourceAnchor(integrationSnapshot(t, s.e).Fence), Steps: []memory.Step{{Node: model.NodeRef{Kind: model.NodeObject, Object: payload}, Memory: promoted.ID, Outcome: "confirmed"}}, Validation: validation}})
	state, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "host-space"})
	if err != nil || len(state.Snapshot.Paths) != 1 {
		t.Fatalf("experience path fixture: %+v %v", state, err)
	}
	typed := []struct {
		kind string
		id   model.ObjectID
	}{{"memory", promoted.ID}, {"experience_path", state.Snapshot.Paths[0].ID}}
	origins := []knowledge.Observation{knowledgeObservation(payload, source, "one", 1)}
	applyObservation(t, s.e, "typed-import-goal", origins[0])
	for i, value := range typed {
		o := knowledgeObservation(value.id, integrationSnapshot(t, s.e).Fence, "one", uint64(i+2))
		o.PayloadKind = value.kind
		o.ID = knowledge.ObservationID(o)
		applyObservation(t, s.e, "typed-origin-"+value.kind, o)
		origins = append(origins, o)
	}
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &origins[0].Key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	target := model.KnowledgeKey{Actor: "same-actor", Execution: "two"}
	command := knowledge.ImportCommand{Target: target, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: known.Frontier}}
	for _, o := range origins {
		command.Observations = append(command.Observations, o.ID)
	}
	request := api.ApplyKnowledgeRequest{Key: agentKey("typed-import-command"), Command: knowledge.Command{Import: &command}}
	ack, err := s.e.ApplyKnowledge(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("typed import: %+v %v", ack, err)
	}
	check := func(packet *knowledge.ContextPacket) []knowledge.Observation {
		t.Helper()
		got, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &target})
		if err != nil || len(got.Snapshot.Observations) != len(origins) {
			t.Fatalf("target knowledge: %+v %v", got, err)
		}
		byOrigin := make(map[model.ObjectID]knowledge.Observation)
		for _, o := range got.Snapshot.Observations {
			byOrigin[o.ImportedFrom] = o
			encoded, err := o.Canonical()
			decoded, decodeErr := knowledge.DecodeObservation(encoded)
			if err != nil || decodeErr != nil || !reflect.DeepEqual(decoded, o) {
				t.Fatalf("canonical imported Observation: %+v %v %v", decoded, err, decodeErr)
			}
		}
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Knowledge: &target})
		if err != nil {
			t.Fatal(err)
		}
		defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		for _, origin := range origins {
			o := byOrigin[origin.ID]
			if o.PayloadKind != origin.PayloadKind || o.Payload != origin.Payload || o.Key != target {
				t.Fatalf("imported provenance changed: %+v", o)
			}
			if o.PayloadKind == "" {
				continue
			}
			ref := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "memory", Kind: model.EntityKind(o.PayloadKind), ID: model.EntityID(o.Payload)}}
			if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: o.Payload}}); err == nil {
				t.Fatal("logical memory ID granted raw object access")
			}
		}
		if retry, err := s.e.ApplyKnowledge(ctx, request); err != nil || retry != ack {
			t.Fatalf("original import receipt changed: %+v %v", retry, err)
		}
		if packet != nil && (len(got.Snapshot.Packets) != 1 || !reflect.DeepEqual(got.Snapshot.Packets[0], *packet)) {
			t.Fatalf("typed context packet changed: %+v", got.Snapshot.Packets)
		}
		return got.Snapshot.Observations
	}
	adopted := check(nil)
	packet, _ := goalCorePacket(t, s, "typed-import-packet", target, byImportedOrigin(adopted, origins[0].ID), adopted, payload, source)
	s.reopen()
	check(&packet)
	before := integrationSnapshot(t, s.e)
	if report, err := s.e.CompactAuditFull(ctx); err != nil || report.Before.Fence() != report.After.Fence() {
		t.Fatalf("typed import compaction: %+v %v", report, err)
	}
	s.reopen()
	check(&packet)
	if after := integrationSnapshot(t, s.e); after.Fence != before.Fence || after.RootSet != before.RootSet {
		t.Fatal("typed import maintenance changed logical publication")
	}
}

func byImportedOrigin(observations []knowledge.Observation, id model.ObjectID) knowledge.Observation {
	for _, observation := range observations {
		if observation.ImportedFrom == id {
			return observation
		}
	}
	return knowledge.Observation{}
}
