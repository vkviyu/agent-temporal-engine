package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func transferCoreFixture(t *testing.T) (*integrationStore, api.ExportTransferRequest, model.ObjectID) {
	t.Helper()
	s, payload, source := knowledgeFixture(t)
	src := memory.Source{SourceID: payload, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}}
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("portable claim"), Sources: []memory.Source{src}}
	applyMemory(t, s.e, "xfer-candidate", memory.Command{PutCandidate: &m})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "xfer-validate", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{src}}}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	ack := applyMemory(t, s.e, "xfer-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
	return s, api.ExportTransferRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Roots: []transfer.Reference{{Kind: "memory", ID: promoted.ID, Anchor: anchor}}, Policy: transfer.RedactionPolicy{ID: "whole-record-omission", Version: "1"}}, payload
}

func TestTransferArchiveCanBeExplicitlyObservedAndAdoptedWithoutForeignAuthority(t *testing.T) {
	ctx := context.Background()
	s, export, _ := transferCoreFixture(t)
	manifest, err := s.e.ExportTransfer(ctx, export)
	if err != nil {
		t.Fatal(err)
	}
	key := func(id string) model.RequestKey { k := agentKey(id); k.Space = "destination"; return k }
	r := api.ImportTransferRequest{Key: key("xfer-import"), Manifest: manifest, ApprovedDigest: manifest.ID}
	before := integrationSnapshot(t, s.e)
	ack, err := s.e.ImportTransfer(ctx, r)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatal(ack, err)
	}
	after := integrationSnapshot(t, s.e)
	if ack.Fence.LSN != before.Fence.LSN+1 || after.Roots.DomainStates != before.Roots.DomainStates || after.Roots.LiveControl != before.Roots.LiveControl || after.Roots.ExecutionKnowledge == before.Roots.ExecutionKnowledge {
		t.Fatal("archive did not publish one isolated knowledge transition")
	}
	read := api.ReadTransferRequest{Space: "destination", Principal: "host-principal", ID: manifest.ID}
	got, err := s.e.GetTransfer(ctx, read)
	if err != nil || !got.Found {
		t.Fatal(got, err)
	}
	var target model.ObjectID
	for _, mapping := range got.Record.Mappings {
		if mapping.Source.Kind == "memory" {
			target = mapping.Target
		}
		if mapping.Source.ID == mapping.Target {
			t.Fatal("foreign ID reused")
		}
	}
	if target == "" {
		t.Fatal("missing root mapping")
	}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "destination", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: target}})
	archive, archiveErr := transfer.DecodeArchiveEntry(detail.Data)
	if err != nil || archiveErr != nil || detail.Encoding != "transfer/ArchiveEntry/v1+json" || archive.Source.Source.Anchor.Space != "host-space" {
		t.Fatalf("archive is not navigable as foreign evidence: %v %v", err, archiveErr)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	other, err := s.e.GetTransfer(ctx, api.ReadTransferRequest{Space: "destination", Principal: "other", ID: manifest.ID})
	if err != nil || other.Found {
		t.Fatal("foreign principal read archive", err)
	}
	task := agentprofile.Task{ID: "local-task", Space: "destination", Owner: "host-principal", Goal: "assess imported evidence", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	execution := agentprofile.Execution{ID: "local-exec", Task: task.ID, Actor: "local-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	for i, c := range []agentprofile.Command{{CreateTask: &task}, {Spawn: &execution}} {
		k := key("local-task")
		if i == 1 {
			k = key("local-exec")
		}
		out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: k, Command: c})
		if err != nil || out.Code != api.CodeDurableAck {
			t.Fatalf("agent step %d: %+v %v", i, out, err)
		}
	}
	localSource := model.SnapshotAnchor{Space: "destination", Fence: integrationSnapshot(t, s.e).Fence}
	observation := knowledge.Observation{Key: model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID}, Source: knowledge.SourceAnchor{Anchor: localSource}, Payload: target, Sequence: 1, Label: "foreign evidence explicitly adopted"}
	observation.ID = knowledge.ObservationID(observation)
	out, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("adopt-import"), Command: knowledge.Command{Observation: &observation}})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatal(out, err)
	}
	candidate := memory.Memory{Space: "destination", Kind: memory.KindFact, Claim: []byte("local hypothesis based on foreign evidence"), Sources: []memory.Source{{SourceID: target, Anchor: localSource}}}
	out, err = s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("candidate-from-import"), Command: memory.Command{PutCandidate: &candidate}})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatal(out, err)
	}
	mem, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "destination"})
	if err != nil || len(mem.Snapshot.Memories) != 1 || mem.Snapshot.Memories[0].Status != memory.StatusCandidate {
		t.Fatal("foreign validation became local promotion", mem, err)
	}
	check := func() {
		t.Helper()
		state, err := s.e.GetTransfer(ctx, read)
		if err != nil || !reflect.DeepEqual(state.Record, got.Record) {
			t.Fatal("archive changed on recovery", err)
		}
		retry, err := s.e.ImportTransfer(ctx, r)
		if err != nil || retry != ack {
			t.Fatal("import receipt changed", retry, err)
		}
	}
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func TestTransferRejectsForgedClosureAndCurrentSourceWithdrawal(t *testing.T) {
	ctx := context.Background()
	s, export, payload := transferCoreFixture(t)
	manifest, err := s.e.ExportTransfer(ctx, export)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := manifest.Canonical()
	for name, mutate := range map[string]func(*transfer.Manifest){"bytes": func(m *transfer.Manifest) { m.Entries[0].Data[0] ^= 1 }, "dependencies": func(m *transfer.Manifest) {
		for i := range m.Entries {
			if len(m.Entries[i].Dependencies) > 0 {
				m.Entries[i].Dependencies = nil
				return
			}
		}
	}, "source-space": func(m *transfer.Manifest) { m.Source.Space = "forged" }, "future-source": func(m *transfer.Manifest) { m.Entries[0].Source.Anchor.Fence.LSN = m.Source.Fence.LSN + 1 }} {
		t.Run(name, func(t *testing.T) {
			bad, _ := transfer.DecodeManifest(raw)
			mutate(&bad)
			bad.ID = bad.Digest()
			before := integrationSnapshot(t, s.e)
			key := agentKey("bad-xfer-" + name)
			key.Space = "destination"
			out, err := s.e.ImportTransfer(ctx, api.ImportTransferRequest{Key: key, Manifest: bad, ApprovedDigest: bad.ID})
			if err == nil || out.Code == api.CodeDurableAck || integrationSnapshot(t, s.e) != before {
				t.Fatal("invalid archive published", out, err)
			}
		})
	}
	export.Omit = []model.ObjectID{payload}
	redacted, err := s.e.ExportTransfer(ctx, export)
	if err != nil || redacted.ID == manifest.ID || len(redacted.Gaps) == 0 {
		t.Fatal("explicit omission did not change manifest", err)
	}
	for _, e := range redacted.Entries {
		if e.Source.ID == payload {
			t.Fatal("redacted source bytes leaked")
		}
	}
	applyMemory(t, s.e, "xfer-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "withdraw source"}}})
	if _, err := s.e.ExportTransfer(ctx, export); !errors.Is(err, memory.ErrHistoryAccessDenied) && !errors.Is(err, memory.ErrNotEligible) {
		t.Fatalf("historical cut bypassed current withdrawal: %v", err)
	}
}
