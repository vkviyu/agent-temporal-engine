package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func knowledgeFormatPacket(space model.SpaceID, fence model.CommitFence, renderer string) knowledge.ContextPacket {
	p := knowledge.ContextPacket{
		Space: space, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: space, Fence: fence}},
		Renderer: renderer, Tokenizer: "test/v1",
	}
	p.ID = knowledge.PacketID(p)
	return p
}

// Keep immutable old roots in the same catalog as a populated current v2 root.
// The legacy value is deliberately opaque: rejection depends on key presence,
// and must not require resurrecting the removed owner-snapshot codec.
func knowledgeFormatRoots(t *testing.T) (*Engine, map[string]model.RootSet, map[string]model.ObjectID, knowledge.ContextPacket) {
	t.Helper()
	ctx := t.Context()
	e := stagingEngine(t)
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	put := func(raw []byte) model.ObjectID {
		t.Helper()
		obj, err := makeValue("format-space", api.Value{Kind: api.ValueBytes, Data: raw})
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.add(obj, storage.KindObject)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	apply := func(space model.SpaceID, root model.ObjectID, mutations ...state.Mutation) model.ObjectID {
		t.Helper()
		delta, err := state.Apply(ctx, b, space, root, mutations)
		if err != nil {
			t.Fatal(err)
		}
		for _, obj := range delta.Objects {
			if _, err := b.add(obj, storage.KindObject); err != nil {
				t.Fatal(err)
			}
		}
		return delta.Root
	}
	packet := knowledgeFormatPacket("format-space", e.publication.Fence(), "current")
	raw, err := packet.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	packetValue := put(raw)
	indexRoot := apply("format-space", "",
		state.Mutation{Key: []byte("packet/" + string(packet.ID)), Value: packetValue},
		state.Mutation{Key: []byte("query/packet/" + string(packet.ID)), Value: packetValue})
	v2 := apply("format-space", "", state.Mutation{Key: knowledgeIndexKey("format-space", "owner"), Value: indexRoot})
	legacy := state.Mutation{Key: legacyKnowledgeStateKey("format-space", "owner"), Value: put([]byte("unsupported owner snapshot"))}
	ownerRoots := map[string]model.ObjectID{
		"empty":   "",
		"v2":      v2,
		"v1-only": apply("format-space", "", legacy),
		"mixed":   apply("format-space", v2, legacy),
	}
	roots := make(map[string]model.RootSet)
	for name, root := range ownerRoots {
		if root == state.EmptyRoot {
			roots[name] = model.RootSet{}
			continue
		}
		roots[name] = model.RootSet{ExecutionKnowledge: apply(systemSpace, "", state.Mutation{Key: []byte("format-space"), Value: root})}
	}
	e.index = newStagingIndex(e.index)
	for id, raw := range b.staged {
		e.index.staged[id] = bytes.Clone(raw)
	}
	e.roots = roots["v2"]
	return e, roots, ownerRoots, packet
}

func TestKnowledgeFormatRejectsLegacyAndMixedRoots(t *testing.T) {
	e, roots, ownerRoots, packet := knowledgeFormatRoots(t)
	ctx := t.Context()
	for _, name := range []string{"v1-only", "mixed"} {
		t.Run(name, func(t *testing.T) {
			before := maps.Clone(e.index.staged)
			publication, currentRoots, currentRootSet := e.publication, e.roots, e.rootSet
			index, err := openKnowledgeIndex(ctx, e.index, ownerRoots[name], "format-space", "owner")
			if !errors.Is(err, knowledge.ErrUnsupportedSchema) || index.root != state.EmptyRoot {
				t.Fatalf("legacy index exposed as empty or partially opened: root=%s err=%v", index.root, err)
			}
			snapshot, root, found, err := e.loadKnowledge(ctx, roots[name], "format-space", "owner")
			if !errors.Is(err, knowledge.ErrUnsupportedSchema) || found || root != "" || !reflect.DeepEqual(snapshot, knowledge.Snapshot{}) {
				t.Fatalf("historical knowledge was hidden or partially returned: %+v root=%s found=%v err=%v", snapshot, root, found, err)
			}
			recorded, err := e.packetAt(ctx, roots[name], "format-space", "owner", packet.ID)
			if !errors.Is(err, knowledge.ErrUnsupportedSchema) ||
				!reflect.DeepEqual(recorded, recordedKnowledgePacket{roots: roots[name]}) {
				t.Fatalf("historical packet returned data or not-found instead of schema rejection: %+v err=%v", recorded, err)
			}
			if e.publication != publication || e.roots != currentRoots || e.rootSet != currentRootSet || !reflect.DeepEqual(e.index.staged, before) {
				t.Fatal("rejected historical read changed current publication, roots or objects")
			}
		})
	}
}

func TestKnowledgeFormatAcceptsCurrentAndEmptyRoots(t *testing.T) {
	e, roots, ownerRoots, packet := knowledgeFormatRoots(t)
	ctx := t.Context()
	for _, name := range []string{"empty", "v2"} {
		t.Run(name, func(t *testing.T) {
			index, err := openKnowledgeIndex(ctx, e.index, ownerRoots[name], "format-space", "owner")
			if err != nil || (index.root != state.EmptyRoot) != (name == "v2") {
				t.Fatalf("supported index: root=%s err=%v", index.root, err)
			}
			snapshot, root, found, err := e.loadKnowledge(ctx, roots[name], "format-space", "owner")
			if err != nil || found != (name == "v2") || root != ownerRoots[name] || len(snapshot.Observations) != 0 {
				t.Fatalf("supported snapshot: %+v root=%s found=%v err=%v", snapshot, root, found, err)
			}
			recorded, err := e.packetAt(ctx, roots[name], "format-space", "owner", packet.ID)
			if name == "empty" {
				if len(snapshot.Packets) != 0 || !errors.Is(err, agentprofile.ErrUnknown) ||
					!reflect.DeepEqual(recorded, recordedKnowledgePacket{roots: roots[name]}) {
					t.Fatalf("empty root did not retain normal not-found semantics: %+v err=%v", recorded, err)
				}
				return
			}
			if err != nil || recorded.roots != roots[name] || len(snapshot.Packets) != 1 ||
				len(recorded.selected.Observations) != 0 || len(recorded.selected.Packets) != 0 {
				t.Fatalf("v2 packet missing: snapshot=%+v packet=%+v err=%v", snapshot, recorded, err)
			}
			want, err := packet.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range []knowledge.ContextPacket{snapshot.Packets[0], recorded.packet} {
				raw, err := got.Canonical()
				if err != nil || !bytes.Equal(raw, want) {
					t.Fatalf("v2 packet changed: %v", err)
				}
			}
		})
	}
	// A legacy key for a different owner must not poison this owner's absence.
	index, err := openKnowledgeIndex(ctx, e.index, ownerRoots["mixed"], "format-space", "other-owner")
	if err != nil || index.root != state.EmptyRoot {
		t.Fatalf("legacy rejection crossed owner boundary: root=%s err=%v", index.root, err)
	}
}

func TestKnowledgeFormatPreservesReaderErrors(t *testing.T) {
	e, roots, ownerRoots, packet := knowledgeFormatRoots(t)
	ctx := t.Context()
	root := ownerRoots["v2"]
	legacyReads := 0
	counter := traceProfileReaderFunc(func(ctx context.Context, id model.ObjectID) ([]byte, error) {
		legacyReads++
		return e.index.ReadObject(ctx, id)
	})
	if _, found, err := state.Get(ctx, counter, "format-space", root, legacyKnowledgeStateKey("format-space", "owner")); err != nil || found || legacyReads == 0 {
		t.Fatalf("legacy absence fixture: found=%v reads=%d err=%v", found, legacyReads, err)
	}
	for _, phase := range []struct {
		name   string
		failAt int
	}{{"legacy-lookup", 1}, {"v2-lookup", legacyReads + 1}} {
		t.Run(phase.name, func(t *testing.T) {
			for _, cause := range []error{io.ErrUnexpectedEOF, ErrMissingObject, context.Canceled} {
				calls := 0
				reader := traceProfileReaderFunc(func(ctx context.Context, id model.ObjectID) ([]byte, error) {
					calls++
					if calls == phase.failAt {
						return nil, cause
					}
					return e.index.ReadObject(ctx, id)
				})
				index, err := openKnowledgeIndex(ctx, reader, root, "format-space", "owner")
				if !errors.Is(err, cause) || errors.Is(err, knowledge.ErrUnsupportedSchema) || index.root != state.EmptyRoot || calls != phase.failAt {
					t.Fatalf("reader failure replaced or partially opened index: cause=%v root=%s calls=%d err=%v", cause, index.root, calls, err)
				}
			}
		})
	}
	// Exercise propagation through the historical read entry points as well.
	cause := errors.Join(ErrObjectIO, io.ErrClosedPipe)
	reader := traceProfileReaderFunc(func(context.Context, model.ObjectID) ([]byte, error) { return nil, cause })
	scoped := context.WithValue(ctx, readCutKey{}, &readCut{owner: e, reader: reader})
	snapshot, root, found, err := e.loadKnowledge(scoped, roots["v2"], "format-space", "owner")
	if !errors.Is(err, ErrObjectIO) || !errors.Is(err, io.ErrClosedPipe) || found || root != "" || !reflect.DeepEqual(snapshot, knowledge.Snapshot{}) {
		t.Fatalf("loadKnowledge lost reader error: %+v found=%v err=%v", snapshot, found, err)
	}
	recorded, err := e.packetAt(scoped, roots["v2"], "format-space", "owner", packet.ID)
	if !errors.Is(err, ErrObjectIO) || !errors.Is(err, io.ErrClosedPipe) ||
		!reflect.DeepEqual(recorded, recordedKnowledgePacket{roots: roots["v2"]}) {
		t.Fatalf("packetAt lost reader error or returned partial data: %+v err=%v", recorded, err)
	}
}

func TestKnowledgeFormatV1JournalReplayRejectsWithoutPartialInstallation(t *testing.T) {
	ctx := t.Context()
	e, disk := coordinatorStore(t, Options{})
	seed := coordinatorRequest(t, "format-seed", "seed", "value", api.ValueCondition{})
	seedAck := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, seed)))
	packet := knowledgeFormatPacket(seed.Space, seedAck.Fence, "v2-prefix")
	body, err := knowledge.EncodeCommand(knowledge.Command{Packet: &packet})
	if err != nil {
		t.Fatal(err)
	}
	req := api.CommitRequest{Space: seed.Space, Principal: seed.Principal, RequestID: "format-v2", Method: api.MethodApplyKnowledge, Body: body}
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, req)))

	packet = knowledgeFormatPacket(seed.Space, ack.Fence, "rejected-v1")
	body, err = knowledge.EncodeCommand(knowledge.Command{Packet: &packet})
	if err != nil {
		t.Fatal(err)
	}
	const versionOffset = len("ATE-KNOWLEDGE-COMMAND\x00")
	if len(body) <= versionOffset || body[versionOffset] != 2 {
		t.Fatal("expected canonical v2 command")
	}
	badReq := req
	badReq.RequestID, badReq.Body = "format-v1", bytes.Clone(body)
	badReq.Body[versionOffset] = 1

	// Bypass only command decoding to construct a physically valid journal
	// member with consistent command digest, receipt, StateOps and roots.
	// This uses the current record builders, never an old knowledge codec.
	prepareRecords := func(request api.CommitRequest) *commitPlan {
		t.Helper()
		digest, err := CommandDigest(request)
		if err != nil {
			t.Fatal(err)
		}
		index, _, err := e.knowledgeIndexAt(ctx, e.roots, req.Space, req.Principal)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := e.prepareKnowledgeRecords(ctx, request, digest, model.TxID(strings.Repeat("e", 32)),
			ack.Fence.LSN+1, nil, index, nil, &packet, knowledge.Snapshot{})
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	goodReq := badReq
	goodReq.Body = body
	goodPlan, badPlan := prepareRecords(goodReq), prepareRecords(badReq)
	// The identical v2 command must produce exactly the same plan through the
	// normal reducer, proving this fixture can reach semantic replay.
	goodDigest, err := CommandDigest(goodReq)
	if err != nil {
		t.Fatal(err)
	}
	normal, err := e.prepare(ctx, goodReq, goodDigest, goodPlan.transaction.TxID, goodPlan.transaction.LSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := normal.compare(goodPlan.transaction); err != nil {
		t.Fatalf("v2 control does not match normal reduction: %v", err)
	}
	if _, err := e.journal.PublishWithRecords(ctx, badPlan.transaction); err != nil {
		t.Fatalf("v1 fixture failed physical publication: %v", err)
	}

	verifier, prefix := newAuditVerifier(e.options), newAuditVerifier(e.options)
	seen := uint64(0)
	_, _, err = e.journal.VerifyPublished(ctx, func(tx storage.ScannedTransaction, source io.ReaderAt) error {
		seen++
		if tx.LSN <= ack.Fence.LSN {
			if err := prefix.replay(tx, source); err != nil {
				return err
			}
			return verifier.replay(tx, source)
		}
		beforeEntries := maps.Clone(verifier.index.entries)
		beforePublication := verifier.publication
		err := verifier.replay(tx, source)
		if !errors.Is(err, knowledge.ErrUnsupportedSchema) {
			t.Fatalf("v1 journal failed for the wrong reason or was accepted: %v", err)
		}
		assertAuditSemanticState(t, verifier, prefix)
		assertAuditIndexesAbsent(t, verifier)
		if verifier.publication != beforePublication || !maps.Equal(verifier.index.entries, beforeEntries) ||
			verifier.index.staged != nil || verifier.index.source != nil || verifier.replayingEvents || verifier.replayEventRegistry != 0 {
			t.Fatal("rejected replay installed objects/publication or retained transient replay state")
		}
		// The retained prefix still resolves its receipt and knowledge, but
		// exposes neither the rejected receipt nor its candidate packet.
		verifier.index.source = source
		defer func() { verifier.index.source = nil }()
		if got, found, err := verifier.lookup(ctx, req.Key()); err != nil || !found || got != ack {
			t.Fatalf("rejected replay changed prefix receipt: %+v found=%v err=%v", got, found, err)
		}
		if _, found, err := verifier.lookup(ctx, badReq.Key()); err != nil || found {
			t.Fatalf("rejected replay installed receipt: found=%v err=%v", found, err)
		}
		snapshot, _, found, readErr := verifier.loadKnowledge(ctx, verifier.roots, req.Space, req.Principal)
		if readErr != nil || !found || len(snapshot.Packets) != 1 || snapshot.Packets[0].Renderer != "v2-prefix" {
			t.Fatalf("rejected replay changed prefix knowledge: %+v err=%v", snapshot, readErr)
		}
		if _, readErr := verifier.packetAt(ctx, verifier.roots, req.Space, req.Principal, packet.ID); !errors.Is(readErr, agentprofile.ErrUnknown) {
			t.Fatalf("rejected replay exposed candidate packet: %v", readErr)
		}
		return err
	})
	if !errors.Is(err, knowledge.ErrUnsupportedSchema) || seen != ack.Fence.LSN+1 {
		t.Fatalf("journal verification did not reject the v1 member: seen=%d err=%v", seen, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := OpenExisting(disk, e.options)
	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("failed recovery exposed a partially replayed engine")
	}
	if !errors.Is(err, knowledge.ErrUnsupportedSchema) {
		t.Fatalf("reopen lost unsupported knowledge schema: %v", err)
	}
}
