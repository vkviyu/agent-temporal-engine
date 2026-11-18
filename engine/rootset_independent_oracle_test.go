package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentRootSetCompositionAcrossDomainsAndRecovery(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	want := oracle.NewRootProjection()

	assertTransition := func(name string, request string, fn func() api.CommitResult, changed ...oracle.RootSlot) {
		t.Helper()
		before := integrationSnapshot(t, s.e)
		versions := want.Versions
		ack := fn()
		if ack.Code != api.CodeDurableAck {
			t.Fatalf("%s did not commit: %+v", name, ack)
		}
		after := integrationSnapshot(t, s.e)
		if got := want.Accept(request, changed...); got != ack.Fence.LSN {
			t.Fatalf("%s fence=%d oracle=%d", name, ack.Fence.LSN, got)
		}
		if ack.RootSet != after.RootSet || ack.Fence != after.Fence {
			t.Fatalf("%s ACK is detached from snapshot: ack=%+v snapshot=%+v", name, ack, after)
		}
		if after.Fence.LSN != before.Fence.LSN+1 {
			t.Fatalf("%s advanced more than one publication", name)
		}
		if err := compareRootVersions(before, after, versions, want.Versions); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	assertTransition(
		"domain",
		"root-domain",
		func() api.CommitResult {
			return integrationMutate(t, s.e, integrationRequest("root-domain", integrationPut([]byte("root-domain"), integrationBytes('d'))))
		},
		oracle.DomainRoot,
	)
	payload := pathTestStateObject(t, s.e, "root-domain")

	task := agentTask("root-task")
	assertTransition(
		"control-create",
		"root-create",
		func() api.CommitResult {
			return coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("root-create"), Command: agentprofile.Command{CreateTask: &task}})
		},
		oracle.ControlRoot,
	)
	execution := agentprofile.Execution{
		ID: "root-execution", Task: task.ID, Actor: "root-actor",
		Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
	}
	assertTransition(
		"control-spawn",
		"root-spawn",
		func() api.CommitResult {
			return coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("root-spawn"), Command: agentprofile.Command{Spawn: &execution}})
		},
		oracle.ControlRoot,
	)

	source := integrationSnapshot(t, s.e).Fence
	observation := knowledge.Observation{
		Key:      model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID},
		Source:   knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}},
		Payload:  payload,
		Sequence: 1,
		Label:    "rootset oracle",
	}
	observation.ID = knowledge.ObservationID(observation)
	assertTransition(
		"knowledge",
		"root-knowledge",
		func() api.CommitResult {
			_, ack := applyObservation(t, s.e, "root-knowledge", observation)
			return ack
		},
		oracle.KnowledgeRoot,
	)

	m := memory.Memory{
		Space: "host-space",
		Kind:  memory.KindFact,
		Claim: []byte("rootset memory"),
		Sources: []memory.Source{{
			SourceID: observation.Payload,
			Anchor:   observation.Source.Anchor,
		}},
	}
	assertTransition(
		"memory",
		"root-memory",
		func() api.CommitResult {
			return applyMemory(t, s.e, "root-memory", memory.Command{PutCandidate: &m})
		},
		oracle.KnowledgeRoot,
	)

	revision := graphRevisionFromCurrent(t, s, "root-revision", "")
	// AddRevision is a separate accepted graph publication performed by the
	// fixture helper before the branch operation under test.
	want.Accept("root-revision", oracle.BranchRoot)
	assertTransition(
		"graph",
		"root-graph",
		func() api.CommitResult {
			return graphCoreApply(t, s.e, "root-graph", graph.Command{Fork: &graph.ForkCommand{Branch: "root-branch", Source: revision.ID}})
		},
		oracle.BranchRoot,
	)

	beforeRetry := integrationSnapshot(t, s.e)
	retry := integrationMutate(t, s.e, integrationRequest("root-domain", integrationPut([]byte("root-domain"), integrationBytes('d'))))
	if retry.Code != api.CodeDurableAck || retry.Fence.LSN != want.Retry("root-domain") || integrationSnapshot(t, s.e) != beforeRetry {
		t.Fatalf("retry changed root projection: %+v", retry)
	}
	rejected, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{
		Key:     agentKey("root-rejected"),
		Command: agentprofile.Command{CreateTask: &task},
	})
	want.Reject()
	if err == nil || rejected.Code != api.CodeRejected || integrationSnapshot(t, s.e) != beforeRetry {
		t.Fatalf("duplicate task changed projection: %+v %v", rejected, err)
	}

	final := integrationSnapshot(t, s.e)
	if final.Fence.LSN != want.Fence || final.Roots.DomainStates == "" ||
		final.Roots.ExecutionKnowledge == "" || final.Roots.BranchRefs == "" ||
		final.Roots.LiveControl == "" || final.Roots.CommandReceipts == "" {
		t.Fatalf("incomplete root projection: snapshot=%+v oracle=%+v", final, want)
	}

	check := func() {
		t.Helper()
		got := integrationSnapshot(t, s.e)
		if got.Fence != final.Fence || got.Roots != final.Roots || got.RootSet != final.RootSet {
			t.Fatalf("recovery changed roots: before=%+v after=%+v", final, got)
		}
		retry := integrationMutate(t, s.e, integrationRequest("root-domain", integrationPut([]byte("root-domain"), integrationBytes('d'))))
		if retry.Fence.LSN != want.Retry("root-domain") || integrationSnapshot(t, s.e) != got {
			t.Fatalf("recovery changed receipt: %+v", retry)
		}
	}
	s.reopen()
	check()
	if _, err := s.e.CreateRecoveryCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if report := s.reopen(); report.CheckpointTransactions != want.Fence {
		t.Fatalf("checkpoint not used: %+v", report)
	}
	check()
	s.options.DisableRecoveryCheckpoints = true
	if report := s.reopen(); report.CheckpointTransactions != 0 || report.ScannedTransactions != want.Fence {
		t.Fatalf("full replay not used: %+v", report)
	}
	s.options.DisableRecoveryCheckpoints = false
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func compareRootVersions(before, after api.StateSnapshot, oldVersions, newVersions [5]uint64) error {
	values := func(snapshot api.StateSnapshot) map[oracle.RootSlot]model.ObjectID {
		return map[oracle.RootSlot]model.ObjectID{
			oracle.DomainRoot:    snapshot.Roots.DomainStates,
			oracle.KnowledgeRoot: snapshot.Roots.ExecutionKnowledge,
			oracle.BranchRoot:    snapshot.Roots.BranchRefs,
			oracle.ControlRoot:   snapshot.Roots.LiveControl,
			oracle.ReceiptRoot:   snapshot.Roots.CommandReceipts,
		}
	}
	beforeRoots, afterRoots := values(before), values(after)
	for _, slot := range []oracle.RootSlot{
		oracle.DomainRoot, oracle.KnowledgeRoot, oracle.BranchRoot,
		oracle.ControlRoot, oracle.ReceiptRoot,
	} {
		changedInStore := beforeRoots[slot] != afterRoots[slot]
		expected := oldVersions[slot] != newVersions[slot]
		if changedInStore != expected {
			return fmt.Errorf("slot=%d changed=%v want=%v", slot, changedInStore, expected)
		}
	}
	return nil
}

func TestIndependentRootSetOracleDetectsWrongSlot(t *testing.T) {
	want := oracle.NewRootProjection()
	before := api.StateSnapshot{}
	old := want.Versions
	want.Accept("domain", oracle.DomainRoot)
	after := before
	after.Roots.DomainStates = "domain"
	after.Roots.CommandReceipts = "receipt"
	if err := compareRootVersions(before, after, old, want.Versions); err != nil {
		t.Fatal(err)
	}
	after.Roots.LiveControl = "unexpected"
	if err := compareRootVersions(before, after, old, want.Versions); err == nil {
		t.Fatal("oracle accepted an unrelated control root change")
	}
}
