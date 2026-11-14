package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func integrationID(b byte) string { return strings.Repeat(string(b), 64) }

func knowledgeFixture(t *testing.T, options ...engine.Options) (*integrationStore, model.ObjectID, model.CommitFence) {
	t.Helper()
	o := engine.Options{AgentProfile: true}
	if len(options) > 0 {
		o = options[0]
	}
	s := newIntegrationStore(t, o)
	task := agentTask("kt")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("kt"), Command: agentprofile.Command{CreateTask: &task}})
	for _, id := range []model.ExecutionID{"one", "two"} {
		x := agentprofile.Execution{ID: id, Task: task.ID, Actor: "same-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
		coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey(string(id)), Command: agentprofile.Command{Spawn: &x}})
	}
	ack := integrationMutate(t, s.e, integrationRequest("evidence", integrationPut([]byte("evidence"), integrationBytes('a'))))
	v, err := s.e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("evidence=%+v %v", page, err)
	}
	if err = s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	return s, page.Items[0].Value, ack.Fence
}
func knowledgeObservation(payload model.ObjectID, fence model.CommitFence, execution model.ExecutionID, sequence uint64) knowledge.Observation {
	o := knowledge.Observation{Key: model.KnowledgeKey{Actor: "same-actor", Execution: execution}, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: fence}}, Payload: payload, Sequence: sequence, Label: "read evidence"}
	o.ID = knowledge.ObservationID(o)
	return o
}
func applyObservation(t *testing.T, e *engine.Engine, id string, o knowledge.Observation) (api.ApplyKnowledgeRequest, api.CommitResult) {
	t.Helper()
	r := api.ApplyKnowledgeRequest{Key: agentKey(id), Command: knowledge.Command{Observation: &o}}
	a, err := e.ApplyKnowledge(context.Background(), r)
	if err != nil || a.Code != api.CodeDurableAck {
		t.Fatalf("apply %s: %+v %v", id, a, err)
	}
	return r, a
}

func TestKnowledgeCoreHistoricalKeyIsolationRetriesAndRecovery(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	genesisStore := newIntegrationStore(t, engine.Options{})
	genesis := integrationSnapshot(t, genesisStore.e)
	// Genesis is empty, not a missing object that fences the store.
	empty, err := genesisStore.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: genesis.Fence}})
	if err != nil || empty.Found || empty.RootSet != "" || empty.Fence != genesis.Fence {
		t.Fatalf("genesis=%+v %v", empty, err)
	}
	one := knowledgeObservation(payload, source, "one", 1)
	r, first := applyObservation(t, s.e, "observe-one", one)
	applyObservation(t, s.e, "observe-two", knowledgeObservation(payload, source, "two", 1))
	key := one.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: first.Fence, Knowledge: &key}
	read := api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor}
	check := func() {
		t.Helper()
		got, err := s.e.GetKnowledgeState(ctx, read)
		if err != nil || len(got.Snapshot.Observations) != 1 || got.Snapshot.Observations[0].Key != key || got.Fence != first.Fence || got.RootSet != first.RootSet || !reflect.DeepEqual(got.Anchor, anchor) {
			t.Fatalf("past=%+v err=%v", got, err)
		}
		latest, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &key})
		if err != nil || len(latest.Snapshot.Observations) != 2 {
			t.Fatalf("latest=%+v %v", latest, err)
		}
		bad := read
		bad.Principal = "another-owner"
		if _, err = s.e.GetKnowledgeState(ctx, bad); !errors.Is(err, agentprofile.ErrCommandScope) {
			t.Fatalf("unauthorized=%v", err)
		}
		wrong := key
		wrong.Actor = "not-the-actor"
		bad = read
		bad.Anchor = nil
		bad.Knowledge = &wrong
		if _, err = s.e.GetKnowledgeState(ctx, bad); !errors.Is(err, agentprofile.ErrCommandScope) {
			t.Fatalf("actor mismatch=%v", err)
		}
	}
	applyObservation(t, s.e, "observe-one-next", knowledgeObservation(payload, source, "one", 2))
	check()
	if retry, err := s.e.ApplyKnowledge(ctx, r); err != nil || retry != first {
		t.Fatalf("original retry=%+v %v", retry, err)
	}
	before := integrationSnapshot(t, s.e)
	r.Command.Observation.Label = "different"
	r.Command.Observation.ID = knowledge.ObservationID(*r.Command.Observation)
	if out, err := s.e.ApplyKnowledge(ctx, r); err != nil || out.Code != api.CodeConflict || out.Conflict != api.ConflictIdempotency {
		t.Fatalf("changed key=%+v %v", out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("retry/denial advanced prefix")
	}
	s.reopen()
	check()
	r.Command.Observation = &one
	if retry, err := s.e.ApplyKnowledge(ctx, r); err != nil || retry != first {
		t.Fatalf("reopen retry=%+v %v", retry, err)
	}
}

func TestKnowledgeCoreRejectsUnpublishedSourceAndRawStateBypass(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	before := integrationSnapshot(t, s.e)
	body, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("arbitrary"), integrationBytes('x'))}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.e.Commit(ctx, api.CommitRequest{Space: "host-space", Principal: "host-principal", RequestID: "bypass", Method: api.MethodApplyKnowledge, Body: body})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("bypass=%+v %v", out, err)
	}
	bad := knowledgeObservation(payload, source, "one", 1)
	bad.Source.Anchor.Fence.CommitDigest = model.CommitDigest(integrationID('e'))
	bad.ID = knowledge.ObservationID(bad)
	out, err = s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("bad-source"), Command: knowledge.Command{Observation: &bad}})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("forged source=%+v %v", out, err)
	}
	bad = knowledgeObservation(model.ObjectID(integrationID('f')), source, "one", 1)
	out, err = s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("unknown-object"), Command: knowledge.Command{Observation: &bad}})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("unobserved=%+v %v", out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("bad request fenced or modified store")
	}
	applyObservation(t, s.e, "ready-after-rejections", knowledgeObservation(payload, source, "one", 1))
}

func TestKnowledgePacketRequiresAdoptionAtSourceFence(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	o := knowledgeObservation(payload, source, "one", 1)
	_, adopt := applyObservation(t, s.e, "adopt", o)
	packet := knowledge.ContextPacket{Space: "host-space", Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}}, Renderer: "plain/v1", Tokenizer: "test/v1", Items: []knowledge.ContextItem{{Ordinal: 0, Observation: o.ID, Content: payload, Label: "evidence"}}}
	packet.ID = knowledge.PacketID(packet)
	out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("premature-render"), Command: knowledge.Command{Packet: &packet}})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("future adoption leaked=%+v %v", out, err)
	}
	// A knowledge View binds the key and exact frontier at the adopted cut.
	key := o.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: adopt.Fence, Knowledge: &key}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	selected, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &v.Anchor})
	if err != nil {
		t.Fatal(err)
	}
	packet.Key = &key
	packet.Source = knowledge.SourceAnchor{Anchor: v.Anchor, ViewDigest: v.ViewDigest, Frontier: selected.Frontier}
	packet.GoalRef = payload
	packet.ID = knowledge.PacketID(packet)
	r := api.ApplyKnowledgeRequest{Key: agentKey("render"), Command: knowledge.Command{Packet: &packet}}
	ack, err := s.e.ApplyKnowledge(ctx, r)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("render=%+v %v", ack, err)
	}
	if _, err = s.e.GetViewState(ctx, api.ReadViewStateRequest{View: v.Ref, Key: []byte("evidence")}); !errors.Is(err, engine.ErrUnsupportedMethod) {
		t.Fatalf("knowledge view escaped to domain=%v", err)
	}
	packet.Source.Frontier = model.ObjectID(integrationID('d'))
	packet.ID = knowledge.PacketID(packet)
	out, err = s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("wrong-frontier"), Command: knowledge.Command{Packet: &packet}})
	if err == nil || out.Code != api.CodeRejected {
		t.Fatalf("forged frontier=%+v %v", out, err)
	}
	s.reopen()
	packet.Source.Frontier = selected.Frontier
	packet.ID = knowledge.PacketID(packet)
	r.Command.Packet = &packet
	if again, err := s.e.ApplyKnowledge(ctx, r); err != nil || again != ack {
		t.Fatalf("packet retry=%+v %v", again, err)
	}
}

func TestStateReadPublishesOneCoherentCutUnderConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{})
	key := []byte("coherent")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 12; i++ {
			_, err := s.e.Mutate(ctx, integrationRequest(fmt.Sprintf("race-%d", i), integrationPut(key, integrationBytes(byte(i)))))
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 30; i++ {
		got, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: key})
		if errors.Is(err, api.ErrReadAuthorizationChanged) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		anchor := model.SnapshotAnchor{Space: "host-space", Fence: got.Fence}
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
		if errors.Is(err, api.ErrReadAuthorizationChanged) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		past, err := s.e.GetViewState(ctx, api.ReadViewStateRequest{View: view.Ref, Key: key})
		_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		if errors.Is(err, api.ErrReadAuthorizationChanged) {
			continue
		}
		if err != nil || !reflect.DeepEqual(got, past) {
			t.Fatalf("read cut mixed: got=%+v past=%+v err=%v", got, past, err)
		}
	}
	wg.Wait()
}

func TestKnowledgeRejectsPacketWhoseSourceHasDifferentSelector(t *testing.T) {
	ctx := context.Background()
	s, _, source := knowledgeFixture(t)
	k := model.KnowledgeKey{Actor: "same-actor", Execution: "one"}
	p := knowledge.ContextPacket{Space: "host-space", Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source, Knowledge: &k}}, Renderer: "query", Tokenizer: "test"}
	p.ID = knowledge.PacketID(p)
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("packet-selector-forgery"), Command: knowledge.Command{Packet: &p}})
	if !errors.Is(err, knowledge.ErrKnowledgeMismatch) || out.Code != api.CodeRejected {
		t.Fatalf("source selector escaped: %+v %v", out, err)
	}
	if after := integrationSnapshot(t, s.e); after != before {
		t.Fatal("invalid source selector fenced/modified store")
	}
}
