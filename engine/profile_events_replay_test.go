package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestProfileEventsMixedLegacyJournalReplaysWithoutInventingOldFacts(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, EventProfile: true})
	task := profileCoordinatorTask("legacy-event-task")
	req := profileCoordinatorRequest(t, "legacy-event-task", agentprofile.Command{CreateTask: &task})
	digest, err := CommandDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	var old api.CommitResult
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.replayingEvents = true
		e.replayEventRegistry = 0
		defer func() { e.replayingEvents = false }()
		plan, err := e.prepare(ctx, req, digest, model.TxID(strings.Repeat("a", 32)), 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if plan.event == nil || plan.event.SchemaVersion != 1 || len(plan.profileEvents) != 0 {
			t.Fatal("fixture did not emit historical core1")
		}
		published, err := e.journal.PublishWithRecords(ctx, plan.transaction)
		if err != nil {
			t.Fatal(err)
		}
		old = api.CommitResult{Code: api.CodeDurableAck, RequestID: req.RequestID, TxID: plan.transaction.TxID, Fence: published.Publication.Fence(), RequestDigest: digest, ResultDigest: plan.receipt, StateRoot: plan.stateRoot, RootSet: plan.rootSet}
	}()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	current, _, err := OpenExisting(disk, e.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if current != nil {
			_ = current.Close()
		}
	})
	if len(current.events) != 1 {
		t.Fatal("upgrade invented profile facts for legacy transaction")
	}
	if retry, err := current.Commit(ctx, req); err != nil || retry != old {
		t.Fatalf("legacy retry changed: %+v %v", retry, err)
	}
	x := agentprofile.Execution{ID: "new-execution", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	modern := profileCoordinatorRequest(t, "new-execution", agentprofile.Command{Spawn: &x})
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(current, ctx, modern)))
	if len(current.events) != 3 {
		t.Fatalf("expected old core + new profile/core, got %d", len(current.events))
	}
	if _, err := current.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	current = nil
	current, _, err = OpenExisting(disk, e.options)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.events) != 3 {
		t.Fatal("mixed replay changed event identities")
	}
	if retry, err := current.Commit(ctx, modern); err != nil || retry != ack {
		t.Fatalf("modern receipt changed: %+v %v", retry, err)
	}
}

func TestProfileEventReplayRejectsCanonicalButForgedAttribution(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, EventProfile: true})
	task := profileCoordinatorTask("forged-event-task")
	req := profileCoordinatorRequest(t, "forged-event-task", agentprofile.Command{CreateTask: &task})
	digest, err := CommandDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		plan, err := e.prepare(ctx, req, digest, model.TxID(strings.Repeat("c", 32)), 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.profileEvents) != 1 {
			t.Fatal("missing profile event")
		}
		forged := plan.profileEvents[0]
		a := *forged.Attribution
		forged.Attribution = &a
		forged.ID = ""
		forged.Attribution.InitiatingActor = "spoofed-actor"
		obj, err := eventlog.Object(forged)
		if err != nil {
			t.Fatal(err)
		}
		id, err := codec.ObjectID(obj)
		if err != nil {
			t.Fatal(err)
		}
		core := *plan.event
		core.ID = ""
		core.ProfileEvents = []model.EventID{model.EventID(id)}
		coreObj, err := eventlog.Object(core)
		if err != nil {
			t.Fatal(err)
		}
		for i, record := range plan.transaction.Records {
			original, err := codec.DecodeObject(record.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if original.Type == eventlog.ProfileType {
				raw, _ := codec.EncodeObject(obj)
				plan.transaction.Records[i].Payload = raw
			}
			if original.Type == eventlog.Type {
				raw, _ := codec.EncodeObject(coreObj)
				plan.transaction.Records[i].Payload = raw
			}
		}
		if _, err := e.journal.PublishWithRecords(ctx, plan.transaction); err != nil {
			t.Fatal(err)
		}
	}()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	current, _, err := OpenExisting(disk, e.options)
	if current != nil {
		_ = current.Close()
	}
	if !errors.Is(err, ErrSemanticCorruption) {
		t.Fatalf("canonical forged event accepted on replay: %v", err)
	}
}
