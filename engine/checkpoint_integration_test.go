package engine_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestCheckpointFiveRootsHistorySuffixAndCompaction(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true, Journal: storage.JournalOptions{SegmentTargetBytes: 1024}})
	task := agentTask("checkpoint-task")
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("checkpoint-task"), Command: agentprofile.Command{CreateTask: &task}})
	x := agentprofile.Execution{ID: "checkpoint-exec", Task: task.ID, Actor: "checkpoint-actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("checkpoint-spawn"), Command: agentprofile.Command{Spawn: &x}})
	req := integrationRequest("checkpoint-source", integrationPut([]byte("source"), integrationBytes('a')))
	ack := integrationMutate(t, s.e, req)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatal(entries, err)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	r := graphRevisionFromCurrent(t, s, "checkpoint-revision", "")
	graphCoreApply(t, s.e, "checkpoint-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r.ID}})
	k := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
	observation := knowledge.Observation{Key: k, Source: knowledge.SourceAnchor{Anchor: anchor}, Payload: entries.Items[0].Value, Sequence: 1, Label: "checkpoint source"}
	observation.ID = knowledge.ObservationID(observation)
	kr, ka := applyObservation(t, s.e, "checkpoint-observation", observation)
	before := integrationSnapshot(t, s.e)
	for _, root := range []model.ObjectID{before.Roots.DomainStates, before.Roots.ExecutionKnowledge, before.Roots.BranchRefs, before.Roots.LiveControl, before.Roots.CommandReceipts} {
		if root == "" {
			t.Fatal("fixture lacks a root")
		}
	}
	certified, err := s.e.CreateRecoveryCheckpoint(ctx)
	if err != nil || certified.Fence() != before.Fence {
		t.Fatal(certified, err)
	}
	if report := s.reopen(); report.CheckpointTransactions != before.Fence.LSN {
		t.Fatal("checkpoint not consumed", report)
	}
	next := integrationMutate(t, s.e, integrationRequest("checkpoint-suffix", integrationPut([]byte("source"), integrationBytes('b'))))
	want := integrationSnapshot(t, s.e)
	// Reopen with a suffix before compaction, after compaction, and with full
	// replay; every historical envelope and receipt must remain usable.
	for _, stage := range []string{"suffix", "compact", "full", "warm-again"} {
		t.Run(stage, func(t *testing.T) {
			if stage == "compact" {
				if _, err := s.e.CompactAuditFull(ctx); err != nil {
					t.Fatal(err)
				}
			}
			s.options.DisableRecoveryCheckpoints = stage == "full"
			report := s.reopen()
			skipped := before.Fence.LSN
			if stage == "full" {
				skipped = 0
			}
			if report.CheckpointTransactions != skipped || report.ScannedTransactions != next.Fence.LSN {
				t.Fatal("wrong replay coverage", report)
			}
			if got := integrationSnapshot(t, s.e); got != want {
				t.Fatal("warm/full replay roots differ")
			}
			if got := integrationMutate(t, s.e, req); got != ack {
				t.Fatal("historical receipt changed")
			}
			if got, err := s.e.ApplyKnowledge(ctx, kr); err != nil || got != ka {
				t.Fatal("knowledge receipt changed", err)
			}
			v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
			if err != nil || !reflect.DeepEqual(got, entries) {
				t.Fatal("historical object locations changed", err)
			}
			if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Recertification always starts at genesis, including after a warm open.
	if _, err := s.e.CreateRecoveryCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if report := s.reopen(); report.CheckpointTransactions != next.Fence.LSN {
		t.Fatal("replacement certificate did not cover suffix", report)
	}
}

func TestCheckpointTaskTraceIndexesAliasesAndProfileGates(t *testing.T) {
	ctx := context.Background()
	s, spec, _ := finalAnswerFixture(t)
	req := api.TaskTraceRequest{Space: "host-space", Principal: "host-principal", Subject: api.TraceSubject{Kind: "query", ID: "query"}, Session: "session"}
	historical, err := s.e.GetTaskTrace(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := s.e.CreateRecoveryCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report := s.reopen(); report.CheckpointTransactions != checkpoint.Position.LSN {
		t.Fatal(report)
	}
	// This reducer consumes pre-checkpoint event heads/fact aliases.
	spec.Activity = "checkpoint-next-activity"
	recoveryCoreControl(t, s, "checkpoint-next-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})
	want, err := s.e.GetTaskTrace(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		s.options.DisableRecoveryCheckpoints = full
		s.reopen()
		got, err := s.e.GetTaskTrace(ctx, req)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("trace projection changed", err)
		}
		oldReq := req
		oldReq.Anchor = &historical.Anchor
		old, err := s.e.GetTaskTrace(ctx, oldReq)
		if err != nil || !reflect.DeepEqual(old, historical) {
			t.Fatal("historical trace changed", err)
		}
	}
	if err := s.e.Close(); err != nil {
		t.Fatal(err)
	}
	s.e = nil
	for _, o := range []engine.Options{{AgentProfile: true}, {EventProfile: true}, {}} {
		opened, _, err := engine.OpenExisting(s.d, o)
		if opened != nil {
			opened.Close()
			t.Fatal("checkpoint bypassed disabled profile")
		}
		if !errors.Is(err, engine.ErrUnsupportedSchema) {
			t.Fatal("profile mismatch did not use historical replay", err)
		}
	}
}

func TestCheckpointEmptyStore(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	before := integrationSnapshot(t, s.e)
	if _, err := s.e.CreateRecoveryCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	if got := integrationSnapshot(t, s.e); got != before {
		t.Fatal("empty checkpoint changed genesis")
	}
	integrationMutate(t, s.e, integrationRequest("after-empty-checkpoint", integrationPut([]byte("k"), integrationBytes('v'))))
	s.reopen()
}

func TestCheckpointRecordStateThenSuffixWorkMatchesFullReplay(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	intake, start := workIntake(t, s)
	intakeAck := workApply(t, s, intake)
	before, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	// Verify the fixture actually uses the new representation in its physical
	// journal. All state was produced by public Mutate/ApplyWork commands.
	if err := s.e.Close(); err != nil {
		t.Fatal(err)
	}
	s.e = nil
	var records, roots int
	j, _, err := storage.OpenJournalWithReplay(s.d, s.options.Journal, func(tx storage.ScannedTransaction, _ io.ReaderAt) error {
		for _, record := range tx.Records {
			obj, err := codec.DecodeObject(record.Payload)
			if err != nil {
				return err
			}
			switch obj.Type {
			case "ate.agent.record":
				records++
			case "ate.agent.record-root":
				roots++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if records == 0 || roots == 0 {
		t.Fatal("fixture did not persist record state")
	}
	s.e, _, err = engine.OpenExisting(s.d, s.options)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.e.CreateRecoveryCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report := s.reopen(); report.CheckpointTransactions != p.Position.LSN {
		t.Fatal(report)
	}
	// StartWork, GrantAuthority, knowledge seed and continuation publish as
	// one Work transaction after restoring the certified record-state prefix.
	ack := workApply(t, s, start)
	wantRoots := integrationSnapshot(t, s.e)
	read := api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"}
	wantAgent, err := s.e.GetAgentState(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	key := model.KnowledgeKey{Actor: "driver", Execution: "intake-root"}
	readKnowledge := api.ReadKnowledgeStateRequest{Space: read.Space, Principal: read.Principal, Knowledge: &key}
	wantKnowledge, err := s.e.GetKnowledgeState(ctx, readKnowledge)
	if err != nil {
		t.Fatal(err)
	}
	traceReq := api.TaskTraceRequest{Space: read.Space, Principal: read.Principal, Subject: api.TraceSubject{Kind: "query", ID: "query"}, Session: "session"}
	wantTrace, err := s.e.GetTaskTrace(ctx, traceReq)
	if err != nil {
		t.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		s.options.DisableRecoveryCheckpoints = full
		report := s.reopen()
		skipped := p.Position.LSN
		if full {
			skipped = 0
		}
		if report.CheckpointTransactions != skipped || report.ScannedTransactions != ack.Fence.LSN {
			t.Fatal(report)
		}
		if got := integrationSnapshot(t, s.e); got != wantRoots {
			t.Fatal("roots differ")
		}
		gotAgent, err := s.e.GetAgentState(ctx, read)
		if err != nil || !reflect.DeepEqual(gotAgent, wantAgent) {
			t.Fatal("record state differs", err)
		}
		gotKnowledge, err := s.e.GetKnowledgeState(ctx, readKnowledge)
		if err != nil || !reflect.DeepEqual(gotKnowledge, wantKnowledge) {
			t.Fatal("Work knowledge differs", err)
		}
		gotTrace, err := s.e.GetTaskTrace(ctx, traceReq)
		if err != nil || !reflect.DeepEqual(gotTrace, wantTrace) {
			t.Fatal("Work event indexes differ", err)
		}
		historicalRead := read
		historicalRead.Anchor = &before.Anchor
		gotBefore, err := s.e.GetAgentState(ctx, historicalRead)
		if err != nil || !reflect.DeepEqual(gotBefore, before) {
			t.Fatal("historical record state differs", err)
		}
		if got := workApply(t, s, intake); got != intakeAck {
			t.Fatal("intake receipt differs")
		}
		if got := workApply(t, s, start); got != ack {
			t.Fatal("Work receipt differs")
		}
	}
}

func TestCheckpointReconstructsBranchAndImportedObservationAliases(t *testing.T) {
	ctx := context.Background()
	t.Run("branch-position", func(t *testing.T) {
		s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
		authority := localGraphExecution(t, s, "checkpoint-branch")
		base := graphRevisionFromCurrent(t, s, "checkpoint-branch-base", "")
		graphCoreApply(t, s.e, "checkpoint-branch-fork", graph.Command{ForkLocal: &graph.ForkLocalCommand{
			Branch: "local", Source: base.ID, Authority: authority,
		}})
		if _, err := s.e.CreateRecoveryCheckpoint(ctx); err != nil {
			t.Fatal(err)
		}
		s.reopen()
		pos := localGraphPosition(t, s, "local")
		graphCoreApply(t, s.e, "checkpoint-branch-write", graph.Command{WriteBranch: &graph.WriteBranchCommand{
			Target: pos, Authority: authority, Batch: localGraphBatch(t, integrationPut([]byte("after"), integrationBytes('v'))),
		}})
		if _, err := s.e.CreateRecoveryCheckpoint(ctx); err != nil {
			t.Fatal(err)
		}
		want := integrationSnapshot(t, s.e)
		s.reopen()
		s.options.DisableRecoveryCheckpoints = true
		s.reopen()
		if integrationSnapshot(t, s.e) != want {
			t.Fatal("branch alias changed replay")
		}
	})
	t.Run("imported-observation", func(t *testing.T) {
		s, payload, source := knowledgeFixture(t, engine.Options{AgentProfile: true, EventProfile: true})
		o := knowledgeObservation(payload, source, "one", 1)
		applyObservation(t, s.e, "checkpoint-import-source", o)
		known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &o.Key})
		if err != nil {
			t.Fatal(err)
		}
		target := model.KnowledgeKey{Actor: "same-actor", Execution: "two"}
		anchor := known.Anchor.Clone()
		anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
		req := api.ApplyKnowledgeRequest{Key: agentKey("checkpoint-import"), Command: knowledge.Command{Import: &knowledge.ImportCommand{
			Target: target, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: known.Frontier}, Observations: []model.ObjectID{o.ID},
		}}}
		ack, err := s.e.ApplyKnowledge(ctx, req)
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatal(ack, err)
		}
		if _, err := s.e.CreateRecoveryCheckpoint(ctx); err != nil {
			t.Fatal(err)
		}
		want := integrationSnapshot(t, s.e)
		s.reopen()
		s.options.DisableRecoveryCheckpoints = true
		s.reopen()
		if integrationSnapshot(t, s.e) != want {
			t.Fatal("import alias changed replay")
		}
	})
}
