package knowledge

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func selectionPacketFixture(t *testing.T) (ContextSelectionRequest, ContextPacket) {
	t.Helper()
	snapshot := Snapshot{}
	for sequence := uint64(1); sequence <= 5; sequence++ {
		o := snapshotObservation("space", "one", sequence)
		o.Payload = dig('a' + byte(sequence))
		o.ID = ObservationID(o)
		snapshot.Observations = append(snapshot.Observations, o)
	}
	key := snapshot.Observations[0].Key
	chain, err := Revisions(snapshot, "space", key)
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := Frontier(snapshot, key)
	if err != nil {
		t.Fatal(err)
	}
	anchor := model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "store", LSN: 8, CommitDigest: model.CommitDigest(dig('f'))}, Knowledge: &key, Cursor: &model.Cursor{Store: "store", Space: "space", Revision: chain[len(chain)-1].ID}}
	req := ContextSelectionRequest{Snapshot: snapshot, Source: SourceAnchor{Anchor: anchor, ViewDigest: dig('e'), Frontier: frontier}, Capacity: ContextCapacity{Window: 14, OutputReserve: 4, FramingReserve: 2}, Mandatory: []model.ObjectID{snapshot.Observations[0].ID}, Tokenizer: "segment/v1", CountQuality: TokenCountEstimated}
	for _, observation := range snapshot.Observations {
		req.Costs = append(req.Costs, ContextCost{Observation: observation.ID, Tokens: 2})
	}
	bundle := ContextBundle{Conclusion: snapshot.Observations[1].ID, Evidence: []model.ObjectID{snapshot.Observations[2].ID}, Counterexamples: []model.ObjectID{snapshot.Observations[3].ID}, Benefit: 10}
	bundle.ID = ContextBundleID(bundle)
	req.Bundles = []ContextBundle{bundle}
	record, err := RecordContextSelection(req)
	if err != nil {
		t.Fatal(err)
	}
	p := ContextPacket{Space: "space", Key: &key, Source: req.Source, GoalRef: snapshot.Observations[0].Payload, Renderer: "render/v1", Tokenizer: req.Tokenizer, EstimatedTokens: record.Result.UsedTokens + req.Capacity.FramingReserve, Selection: &record}
	known := map[model.ObjectID]Observation{}
	for _, observation := range snapshot.Observations {
		known[observation.ID] = observation
	}
	for i, id := range record.Result.Selected {
		o := known[id]
		p.Items = append(p.Items, ContextItem{Ordinal: uint64(i), Observation: id, Content: o.Payload, Label: o.Label})
	}
	p.ID = PacketID(p)
	return req, p
}

func TestSelectedPacketPersistsFullProposalAndRecomputableResult(t *testing.T) {
	req, packet := selectionPacketFixture(t)
	if len(packet.Selection.Proposal.Mandatory) != 1 || len(packet.Selection.Proposal.Costs) != 5 || len(packet.Selection.Result.Selected) != 4 || len(packet.Selection.Result.Omitted) != 1 || packet.EstimatedTokens != 10 {
		t.Fatalf("incomplete recorded selection: %+v", packet.Selection)
	}
	if err := VerifyPacketSelection(packet, req.Snapshot); err != nil {
		t.Fatal(err)
	}
	raw, err := packet.Canonical()
	if err != nil || raw[0] != 4 {
		t.Fatalf("schema4 packet: %x %v", raw, err)
	}
	decoded, err := DecodePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decoded.Canonical(); err != nil || !bytes.Equal(got, raw) {
		t.Fatal("selection canonical bytes changed", err)
	}
	if err := VerifyPacketSelection(decoded, req.Snapshot); err != nil {
		t.Fatal("decoded selection cannot be recomputed", err)
	}
	withInput := packet.Clone()
	withInput.Input = &RecordedInput{Object: dig('a'), Anchor: model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "store", LSN: 9, CommitDigest: model.CommitDigest(dig('b'))}}, Encoding: "model/v1"}
	withInput.ID = PacketID(withInput)
	inputRaw, err := withInput.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if decodedInput, err := DecodePacket(inputRaw); err != nil || !reflect.DeepEqual(decodedInput.Input, withInput.Input) {
		t.Fatal("schema3 input lost", decodedInput, err)
	}
	next, err := ApplyCommand(req.Snapshot, Command{Packet: &packet})
	if err != nil {
		t.Fatal(err)
	}
	snapshotRaw, err := EncodeSnapshot(next)
	if err != nil {
		t.Fatal(err)
	}
	if restored, err := DecodeSnapshot(snapshotRaw); err != nil || len(restored.Packets) != 1 || restored.Packets[0].ID != packet.ID {
		t.Fatal("selection snapshot cannot restore", err)
	}
	for i := 0; i < len(raw); i++ {
		if _, err := DecodePacket(raw[:i]); err == nil {
			t.Fatalf("accepted schema3 truncation at %d", i)
		}
	}
}

func TestSelectedPacketRejectsForgedResultInputAndGoal(t *testing.T) {
	req, packet := selectionPacketFixture(t)
	for name, mutate := range map[string]func(*ContextPacket){
		"tokens":             func(p *ContextPacket) { p.Selection.Result.UsedTokens++; p.EstimatedTokens++ },
		"omission_reason":    func(p *ContextPacket) { p.Selection.Result.Omitted[0].Reason = "capacity" },
		"goal_not_mandatory": func(p *ContextPacket) { p.GoalRef = req.Snapshot.Observations[1].Payload },
		"item_order": func(p *ContextPacket) {
			p.Items[0], p.Items[1] = p.Items[1], p.Items[0]
			p.Items[0].Ordinal, p.Items[1].Ordinal = 0, 1
		},
		"source":          func(p *ContextPacket) { p.Selection.Proposal.Source.ViewDigest = dig('a') },
		"output_capacity": func(p *ContextPacket) { p.Selection.Proposal.Capacity.OutputReserve++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := packet.Clone()
			mutate(&bad)
			if err := VerifyPacketSelection(bad, req.Snapshot); err == nil {
				t.Fatal("forged selection verified")
			}
		})
	}
	bad := packet.Clone()
	bad.Selection.Result.Omitted[0].Reason = "capacity"
	bad.ID = PacketID(bad)
	if _, err := ApplyCommand(req.Snapshot, Command{Packet: &bad}); !errors.Is(err, ErrContextSelectionInvalid) {
		t.Fatal("pure snapshot accepted invented omission", err)
	}
	owned := packet.Clone()
	owned.Selection.Proposal.Mandatory[0] = dig('a')
	owned.Selection.Proposal.Costs[0].Tokens++
	owned.Selection.Proposal.Bundles[0].Counterexamples[0] = dig('b')
	owned.Selection.Result.Selected[0] = dig('c')
	owned.Selection.Result.Source.Anchor.Cursor.Revision = model.RevisionID(dig('d'))
	if err := VerifyPacketSelection(packet, req.Snapshot); err != nil {
		t.Fatal("clone aliased original selection", err)
	}
}

func TestSelectedPacketDecoderRejectsOversizedCountsBeforeAllocation(t *testing.T) {
	_, p := selectionPacketFixture(t)
	_, proposal, err := canonicalContextProposal(p.Selection.Proposal)
	if err != nil {
		t.Fatal(err)
	}
	raw := codec.AppendUint([]byte("ATE-CONTEXT-SELECTION\x00"), 1)
	raw, _ = codec.AppendBytes(raw, proposal)
	raw = codec.AppendUint(raw, p.Selection.Result.InputLimit)
	raw = codec.AppendUint(raw, p.Selection.Result.UsedTokens)
	raw = codec.AppendUint(raw, ^uint64(0))
	if _, err := decodeContextSelection(raw); err == nil {
		t.Fatal("accepted adversarial mandatory count")
	}
}

func TestPreV1PacketIndependentBytesAreRejected(t *testing.T) {
	goldens := []string{
		"0105737061636500017302406666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666605737061636500000000000001720174004061373036626134306632626462656262666461663236623463613863373762373133363662643834656163636662643934646132316164336363396438643562",
		"0205737061636500017302406666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666605737061636500000000000001720174004061616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161017303406565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656505737061636500000000046a736f6e4062663237333663346533363238633039303862643861623632616134303039326364343266376633663236613330393061373936346266353630336530383461",
	}
	for i, golden := range goldens {
		raw, err := hex.DecodeString(golden)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodePacket(raw); err == nil {
			t.Fatal("pre-V1 packet decoded", i)
		}
	}
}

func FuzzSelectionPacketDecoder(f *testing.F) {
	f.Add([]byte("ATE-CONTEXT-SELECTION\x00\x01"))
	f.Add([]byte{3})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if p, err := DecodePacket(raw); err == nil {
			canonical, err := p.Canonical()
			if err != nil || !bytes.Equal(canonical, raw) {
				t.Fatal("decoder accepted noncanonical packet")
			}
		}
		if s, err := decodeContextSelection(raw); err == nil {
			canonical, err := encodeContextSelection(s)
			if err != nil || !bytes.Equal(canonical, raw) {
				t.Fatal("decoder accepted noncanonical selection")
			}
		}
	})
}
