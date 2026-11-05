package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

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
