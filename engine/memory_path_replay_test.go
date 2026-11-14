package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func TestLegacyExperiencePathReplaysWithoutInventingProvenance(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{EventProfile: true})
	put := coordinatorRequest(t, "path-origin", "origin", "initial evidence", api.ValueCondition{})
	initial := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, put)))
	root, err := domainRoot(ctx, e.index, e.roots, put.Space)
	if err != nil {
		t.Fatal(err)
	}
	object, found, err := state.Get(ctx, e.index, put.Space, root, []byte("origin"))
	if err != nil || !found {
		t.Fatalf("initial evidence: %v %v", found, err)
	}
	source := memory.Source{SourceID: object, Anchor: model.SnapshotAnchor{Space: put.Space, Fence: initial.Fence}}
	validation := memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{source}}
	makeRequest := func(id string, c memory.Command) api.CommitRequest {
		t.Helper()
		raw, err := memory.EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		return api.CommitRequest{Space: put.Space, Principal: put.Principal, RequestID: model.RequestID(id), Method: api.MethodApplyMemory, Body: raw}
	}
	apply := func(id string, c memory.Command) {
		t.Helper()
		coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, makeRequest(id, c))))
	}
	find := func(status memory.Status) memory.Memory {
		t.Helper()
		got, err := e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: put.Space})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range got.Snapshot.Memories {
			if record.Status == status {
				return record
			}
		}
		t.Fatalf("no memory at %s", status)
		return memory.Memory{}
	}
	apply("legacy-member", memory.Command{PutCandidate: &memory.Memory{Space: put.Space, Kind: memory.KindFact, Claim: []byte("remember the legacy result"), Sources: []memory.Source{source}}})
	candidate := find(memory.StatusCandidate)
	apply("legacy-validate", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: validation}})
	validated := find(memory.StatusValidated)
	apply("legacy-promote", memory.Command{Promote: &validated.ID})
	member := find(memory.StatusPromoted)
	snapshot, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	unpublished := model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("f", 64))}
	command := memory.Command{BuildPath: &memory.BuildPathCommand{
		Space: put.Space, Anchor: model.SnapshotAnchor{Space: put.Space, Fence: snapshot.Fence}, Validation: validation,
		Steps: []memory.Step{{Memory: member.ID, Node: unpublished}},
	}}
	request := makeRequest("legacy-build", command)
	// Write the exact previous reducer's single-key plan through P1. New
	// requests cannot select this plan via the unchanged BuildPath encoding.
	var old api.CommitResult
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		store, _, _, err := e.loadMemory(ctx, e.roots, put.Space)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ApplyCommand(request.Key(), command); err != nil {
			t.Fatal(err)
		}
		raw, err := memory.EncodeSnapshot(store.Snapshot())
		if err != nil {
			t.Fatal(err)
		}
		digest, err := CommandDigest(request)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := e.prepareTypedValue(ctx, request, digest, model.TxID(strings.Repeat("a", 32)), snapshot.Fence.LSN+1, nil, memoryStateKey(put.Space), raw)
		if err != nil {
			t.Fatal(err)
		}
		publication, err := e.journal.PublishWithRecords(ctx, plan.transaction)
		if err != nil {
			t.Fatal(err)
		}
		old = api.CommitResult{Code: api.CodeDurableAck, RequestID: request.RequestID, TxID: plan.transaction.TxID, Fence: publication.Publication.Fence(), RequestDigest: digest, ResultDigest: plan.receipt, StateRoot: plan.stateRoot, RootSet: plan.rootSet}
	}()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := OpenExisting(disk, e.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if retry, err := reopened.Commit(ctx, request); err != nil || retry != old {
		t.Fatalf("legacy receipt changed: %+v %v", retry, err)
	}
	got, err := reopened.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: put.Space})
	if err != nil || len(got.Snapshot.Paths) != 1 || got.Snapshot.Paths[0].Steps[0].Node != unpublished {
		t.Fatalf("legacy audit state changed: %+v %v", got, err)
	}
	request.RequestID = "legacy-republication"
	if ack, err := reopened.Commit(ctx, request); !errors.Is(err, memory.ErrSourceUnavailable) || ack.Code != api.CodeRejected {
		t.Fatalf("fresh request bypassed strict admission: %+v %v", ack, err)
	}
}
