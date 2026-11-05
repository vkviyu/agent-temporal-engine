package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func memoryFixture(t *testing.T) (*integrationStore, memory.Source) {
	t.Helper()
	s := newIntegrationStore(t, engine.Options{})
	ack := integrationMutate(t, s.e, integrationRequest("memory-evidence", integrationPut([]byte("memory-evidence"), integrationBytes('e'))))
	v, err := s.e.OpenView(context.Background(), api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("source=%+v %v", entries, err)
	}
	if err = s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref}); err != nil {
		t.Fatal(err)
	}
	return s, memory.Source{SourceID: entries.Items[0].Value, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}}
}
func applyMemory(t *testing.T, e *engine.Engine, id string, c memory.Command) api.CommitResult {
	t.Helper()
	out, err := e.ApplyMemory(context.Background(), api.ApplyMemoryRequest{Key: agentKey(id), Command: c})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("memory %s=%+v %v", id, out, err)
	}
	return out
}
func memoryWithStatus(t *testing.T, e *engine.Engine, status memory.Status) memory.Memory {
	t.Helper()
	s, err := e.GetMemoryState(context.Background(), api.ReadMemoryStateRequest{Space: "host-space"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Snapshot.Memories {
		if m.Status == status {
			return m
		}
	}
	t.Fatalf("status %s missing in %+v", status, s)
	return memory.Memory{}
}
func TestMemoryCoreTypedIntentRetriesAndRecovery(t *testing.T) {
	s, source := memoryFixture(t)
	ctx := context.Background()
	before := integrationSnapshot(t, s.e)
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("source-backed memory"), Sources: []memory.Source{source}}
	r := api.ApplyMemoryRequest{Key: agentKey("memory-put"), Command: memory.Command{PutCandidate: &m}, ExpectedFence: &before.Fence}
	first, err := s.e.ApplyMemory(ctx, r)
	if err != nil || first.Code != api.CodeDurableAck {
		t.Fatalf("put=%+v %v", first, err)
	}
	after := integrationSnapshot(t, s.e)
	if after.Roots.ExecutionKnowledge == "" || after.Roots.ExecutionKnowledge == before.Roots.ExecutionKnowledge || after.Roots.DomainStates != before.Roots.DomainStates || after.Roots.BranchRefs != before.Roots.BranchRefs {
		t.Fatalf("wrong root slots before=%+v after=%+v", before.Roots, after.Roots)
	}
	created := memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "memory-validate", memory.Command{Validate: &memory.ValidateCommand{ID: created.ID, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{source}}}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	applyMemory(t, s.e, "memory-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	applyMemory(t, s.e, "memory-ban", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: source.SourceID, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "source withdrawn"}}})
	final := integrationSnapshot(t, s.e)
	for _, reopen := range []bool{false, true} {
		if reopen {
			s.reopen()
		}
		ack, err := s.e.ApplyMemory(ctx, r)
		if err != nil || !reflect.DeepEqual(ack, first) {
			t.Fatalf("retry reopen=%v: %+v %v", reopen, ack, err)
		}
		if snapshot := integrationSnapshot(t, s.e); snapshot != final {
			t.Fatal("retry changed publication")
		}
		got, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "host-space"})
		if err != nil || !got.Found || got.Fence != final.Fence || len(got.Snapshot.Bans) == 0 {
			t.Fatalf("recovery state=%+v %v", got, err)
		}
		storeRaw, err := memory.EncodeSnapshot(got.Snapshot)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := memory.RestoreSnapshot(storeRaw)
		if err != nil {
			t.Fatal(err)
		}
		if audit, err := restored.Get(promoted.ID); err != nil || audit.Status != memory.StatusPromoted {
			t.Fatal("audit version lost on withdrawal")
		}
	}
	changed := m
	changed.Claim = []byte("changed under same request")
	conflict, err := s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: r.Key, Command: memory.Command{PutCandidate: &changed}, ExpectedFence: r.ExpectedFence})
	if err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
		t.Fatalf("same key changed body=%+v %v", conflict, err)
	}
}

func TestMemoryCoreRejectsRawSnapshotsForgedSourcesWithoutFencing(t *testing.T) {
	s, source := memoryFixture(t)
	ctx := context.Background()
	before := integrationSnapshot(t, s.e)
	batchRaw, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("ate/memory/v1/host-space"), api.Value{Kind: api.ValueBytes, Data: []byte("arbitrary snapshot")})}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.e.Commit(ctx, api.CommitRequest{Space: "host-space", Principal: "host-principal", RequestID: "memory-raw", Method: api.MethodApplyMemory, Body: batchRaw})
	if err == nil && raw.Code == api.CodeDurableAck {
		t.Fatal("arbitrary state batch bypassed memory reducer")
	}
	for _, mutate := range []func(*memory.Source){
		func(s *memory.Source) { s.SourceID = model.ObjectID(strings.Repeat("b", 64)) },
		func(s *memory.Source) { s.Anchor.Fence.CommitDigest = model.CommitDigest(strings.Repeat("b", 64)) },
		func(s *memory.Source) { s.Anchor.Space = "another" },
	} {
		bad := source
		mutate(&bad)
		m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("forged"), Sources: []memory.Source{bad}}
		ack, err := s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: agentKey("bad-" + string(bad.SourceID) + string(bad.Anchor.Space) + string(bad.Anchor.Fence.CommitDigest)), Command: memory.Command{PutCandidate: &m}})
		if err == nil && ack.Code == api.CodeDurableAck {
			t.Fatal("forged provenance committed")
		}
		if after := integrationSnapshot(t, s.e); after != before {
			t.Fatal("business rejection changed or fenced store")
		}
	}
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("valid after rejected"), Sources: []memory.Source{source}}
	applyMemory(t, s.e, "valid-memory", memory.Command{PutCandidate: &m})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	before = integrationSnapshot(t, s.e)
	ack, err := s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: agentKey("bypass-validation"), Command: memory.Command{Transition: &memory.TransitionCommand{ID: candidate.ID, To: memory.StatusPromoted, Why: "skip validation"}}})
	if !errors.Is(err, memory.ErrCommandShape) || ack.Code == api.CodeDurableAck {
		t.Fatalf("privilege shortcut=%+v %v", ack, err)
	}
	if integrationSnapshot(t, s.e) != before {
		t.Fatal("invalid transition changed publication")
	}
}

func TestMemoryCoreConcurrentCandidatesDoNotLoseUpdates(t *testing.T) {
	s, source := memoryFixture(t)
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte(fmt.Sprintf("parallel fact %d", i)), Sources: []memory.Source{source}}
			ack, err := s.e.ApplyMemory(context.Background(), api.ApplyMemoryRequest{Key: agentKey(fmt.Sprintf("parallel-memory-%d", i)), Command: memory.Command{PutCandidate: &m}})
			if err == nil && ack.Code != api.CodeDurableAck {
				err = fmt.Errorf("unexpected ack %s", ack.Code)
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, reopen := range []bool{false, true} {
		if reopen {
			s.reopen()
		}
		got, err := s.e.GetMemoryState(context.Background(), api.ReadMemoryStateRequest{Space: "host-space"})
		if err != nil || len(got.Snapshot.Memories) != 12 {
			t.Fatalf("lost candidates reopen=%v state=%+v %v", reopen, got, err)
		}
	}
}
