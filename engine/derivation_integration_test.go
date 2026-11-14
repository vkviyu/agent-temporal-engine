package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestDerivationPacketDependenciesRemainTransitiveAcrossRecovery(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "chain-one-adopt", one)
	first := integrationMutate(t, s.e, integrationRequest("chain-first-input", integrationPut([]byte("chain-first-input"), api.Value{Kind: api.ValueBytes, Data: []byte("first rendered request")})))
	firstID, _ := recoveryValueAt(t, s, first.Fence, "chain-first-input")
	goalCorePacket(t, s, "chain-first-packet", one.Key, one, []knowledge.Observation{one}, firstID, first.Fence)
	two := knowledgeObservation(firstID, first.Fence, "two", 1)
	applyObservation(t, s.e, "chain-two-adopt", two)
	second := integrationMutate(t, s.e, integrationRequest("chain-second-input", integrationPut([]byte("chain-second-input"), api.Value{Kind: api.ValueBytes, Data: []byte("second request derived from first")})))
	secondID, _ := recoveryValueAt(t, s, second.Fence, "chain-second-input")
	goalCorePacket(t, s, "chain-second-packet", two.Key, two, []knowledge.Observation{two}, secondID, second.Fence)
	applyMemory(t, s.e, "chain-retract-origin", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "upstream evidence withdrawn"}}})
	check := func() {
		t.Helper()
		got, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("chain-second-input")})
		if got.Found || !errors.Is(err, memory.ErrHistoryAccessDenied) {
			t.Fatalf("transitive request disclosed: %+v %v", got, err)
		}
	}
	check()
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func TestDerivationSharedRawInputRetainsAllDeclaredSources(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	one := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "alias-one-adopt", one)
	other := integrationMutate(t, s.e, integrationRequest("alias-other-evidence", integrationPut([]byte("alias-other-evidence"), api.Value{Kind: api.ValueBytes, Data: []byte("independent source")})))
	otherID, _ := recoveryValueAt(t, s, other.Fence, "alias-other-evidence")
	two := knowledgeObservation(otherID, other.Fence, "two", 1)
	applyObservation(t, s.e, "alias-two-adopt", two)
	input := integrationMutate(t, s.e, integrationRequest("alias-input", integrationPut([]byte("alias-input"), api.Value{Kind: api.ValueBytes, Data: []byte("same exact recorded request reused by two renderers")})))
	inputID, _ := recoveryValueAt(t, s, input.Fence, "alias-input")
	goalCorePacket(t, s, "alias-first-packet", one.Key, one, []knowledge.Observation{one}, inputID, input.Fence)
	goalCorePacket(t, s, "alias-later-packet", two.Key, two, []knowledge.Observation{two}, inputID, input.Fence)
	applyMemory(t, s.e, "alias-retract-earlier-source", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "earlier declaration remains binding"}}})
	s.reopen()
	got, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("alias-input")})
	if got.Found || !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatalf("later declaration erased earlier provenance: %+v %v", got, err)
	}
}

func TestSummaryDerivationPublicationUnknownRecoversAtomically(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[after], func(t *testing.T) {
			ctx := context.Background()
			s, summary := summaryCoreFixture(t)
			if err := s.e.Close(); err != nil {
				t.Fatal(err)
			}
			fault := &workCurrentFaultIO{DurableIO: s.d, after: after}
			var err error
			s.e, _, err = engine.OpenExisting(fault, s.options)
			if err != nil {
				t.Fatal(err)
			}
			fault.armed = true
			out, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey("atomic-derived-summary"), Summary: summary})
			if out.Code != api.CodeCommitUnknown || out.RootSet != "" || !errors.Is(err, errWorkCurrent) {
				t.Fatalf("ambiguous publication leaked an ACK: %+v %v", out, err)
			}
			s.options.Journal.TailPolicy = storage.TailQuarantine
			s.reopen()
			got, err := s.e.GetSummary(ctx, api.ReadSummaryRequest{Space: "host-space", Principal: "host-principal", ID: summary.ID})
			if err != nil || got.Found != after {
				t.Fatalf("primary Summary publication differs: %+v %v", got, err)
			}
			applyMemory(t, s.e, "atomic-derived-retract", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: summary.Sources[0].Content, Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "test atomic adjacency"}}})
			raw, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte("summary-output")})
			if after && (raw.Found || !errors.Is(err, memory.ErrHistoryAccessDenied)) || !after && (err != nil || !raw.Found) {
				t.Fatalf("primary Summary and derived index split across publication: %+v %v", raw, err)
			}
		})
	}
}
