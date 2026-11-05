package reference_test

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func selectorFixture(t *testing.T, n int) (knowledge.ContextSelectionRequest, []model.ObjectID) {
	t.Helper()
	key := model.KnowledgeKey{Actor: "actor", Execution: "exec"}
	fence := model.CommitFence{Store: "store", LSN: 10, CommitDigest: model.CommitDigest(digest("f"))}
	snap := knowledge.Snapshot{}
	ids := []model.ObjectID{}
	for i := range n {
		o := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "space", Fence: fence}}, Payload: digest(fmt.Sprintf("%x", i+1)), Sequence: uint64(i + 1), Label: fmt.Sprintf("segment-%d", i)}
		o.ID = knowledge.ObservationID(o)
		snap.Observations = append(snap.Observations, o)
		ids = append(ids, o.ID)
	}
	revisions, err := knowledge.Revisions(snap, "space", key)
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := knowledge.Frontier(snap, key)
	if err != nil {
		t.Fatal(err)
	}
	anchor := model.SnapshotAnchor{Space: "space", Fence: fence, Knowledge: &key, Cursor: &model.Cursor{Store: "store", Space: "space", Revision: revisions[len(revisions)-1].ID}}
	return knowledge.ContextSelectionRequest{Snapshot: snap, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: frontier, ViewDigest: digest("e")}, Capacity: knowledge.ContextCapacity{Window: 30}, Tokenizer: "framed/v1", CountQuality: knowledge.TokenCountExact, PolicyVersion: knowledge.ContextSelectionPolicy}, ids
}
func oracleSelection(req knowledge.ContextSelectionRequest) (oracle.SelectionPlan, error) {
	p := oracle.SelectionProposal{Costs: map[string]uint64{}, Limit: req.Capacity.Window - req.Capacity.OutputReserve - req.Capacity.FramingReserve}
	for _, c := range req.Costs {
		p.Costs[string(c.Observation)] = c.Tokens
	}
	for _, id := range req.Mandatory {
		p.Mandatory = append(p.Mandatory, string(id))
	}
	for _, b := range req.Bundles {
		x := oracle.SelectionBundle{ID: string(b.ID), Conclusion: string(b.Conclusion), Benefit: b.Benefit}
		for _, id := range b.Evidence {
			x.Evidence = append(x.Evidence, string(id))
		}
		for _, id := range b.Counterexamples {
			x.Counters = append(x.Counters, string(id))
		}
		p.Bundles = append(p.Bundles, x)
	}
	return oracle.Select(p)
}
func compareSelection(t *testing.T, req knowledge.ContextSelectionRequest) {
	t.Helper()
	want, werr := oracleSelection(req)
	got, gerr := knowledge.SelectContext(req)
	if (werr == nil) != (gerr == nil) {
		t.Fatalf("acceptance: oracle=%v production=%v", werr, gerr)
	}
	if werr != nil {
		if !errors.Is(gerr, knowledge.ErrContextBudgetUnsatisfied) {
			t.Fatal(gerr)
		}
		return
	}
	stringsOf := func(in []model.ObjectID) []string {
		out := make([]string, len(in))
		for i, id := range in {
			out[i] = string(id)
		}
		return out
	}
	omitted := map[string]string{}
	for _, o := range got.Omitted {
		omitted[string(o.Observation)] = o.Reason
	}
	if got.UsedTokens != want.Used || !reflect.DeepEqual(stringsOf(got.Mandatory), want.Mandatory) || !reflect.DeepEqual(stringsOf(got.Selected), want.Selected) || !reflect.DeepEqual(stringsOf(got.SelectedBundles), want.Bundles) || !reflect.DeepEqual(omitted, want.Omitted) {
		t.Fatalf("selector mismatch\n got=%+v\n want=%+v", got, want)
	}
}
func TestIndependentSelectorFiniteSetsAndRecordedRendering(t *testing.T) {
	rng := rand.New(rand.NewSource(90210))
	for trial := 0; trial < 120; trial++ {
		req, ids := selectorFixture(t, 6)
		req.Capacity = knowledge.ContextCapacity{Window: uint64(2 + rng.Intn(22)), OutputReserve: 1, FramingReserve: 1}
		for _, id := range ids {
			req.Costs = append(req.Costs, knowledge.ContextCost{Observation: id, Tokens: uint64(rng.Intn(6))})
		}
		if trial%3 == 0 {
			req.Mandatory = []model.ObjectID{ids[0]}
		}
		for i := 0; i < 3; i++ {
			b := knowledge.ContextBundle{Conclusion: ids[i], Evidence: []model.ObjectID{ids[3]}, Counterexamples: []model.ObjectID{ids[4]}, Benefit: uint64(rng.Intn(10))}
			if i == 0 && trial%4 == 0 {
				b.Evidence = []model.ObjectID{ids[1]}
			}
			b.ID = knowledge.ContextBundleID(b)
			req.Bundles = append(req.Bundles, b)
		}
		compareSelection(t, req)
		// Persisted packets must retain the independent presentation order and exact
		// token accounting; a later renderer may not silently reorder the decision.
		want, err := oracleSelection(req)
		if err != nil || len(want.Mandatory) == 0 {
			continue
		}
		record, err := knowledge.RecordContextSelection(req)
		if err != nil {
			t.Fatal(err)
		}
		packet := knowledge.ContextPacket{Space: "space", Key: req.Source.Anchor.Knowledge, Source: req.Source, GoalRef: req.Snapshot.Observations[0].Payload, Renderer: "fixture/framed-v1", Tokenizer: req.Tokenizer, EstimatedTokens: want.Used + req.Capacity.FramingReserve, Selection: &record}
		observations := map[string]knowledge.Observation{}
		for _, o := range req.Snapshot.Observations {
			observations[string(o.ID)] = o
		}
		for i, id := range want.Selected {
			o := observations[id]
			packet.Items = append(packet.Items, knowledge.ContextItem{Ordinal: uint64(i), Observation: o.ID, Content: o.Payload, Label: o.Label})
		}
		packet.ID = knowledge.PacketID(packet)
		if err := knowledge.VerifyPacketSelection(packet, req.Snapshot); err != nil {
			t.Fatal(err)
		}
		raw, err := packet.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		restored, err := knowledge.DecodePacket(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(restored.Items, packet.Items) || restored.EstimatedTokens != packet.EstimatedTokens {
			t.Fatal("recorded render order or count changed")
		}
		if len(packet.Items) > 1 {
			bad := packet.Clone()
			bad.Items[0], bad.Items[1] = bad.Items[1], bad.Items[0]
			bad.Items[0].Ordinal, bad.Items[1].Ordinal = 0, 1
			if err := knowledge.VerifyPacketSelection(bad, req.Snapshot); err == nil {
				t.Fatal("renderer changed independent selection order")
			}
		}
	}
}
