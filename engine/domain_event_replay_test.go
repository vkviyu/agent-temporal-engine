package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestDomainEventReplayRejectsCanonicalButWrongSubject(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{EventProfile: true})
	req := graphCoordinatorRequest(t, "forged-revision-subject", graph.Command{AddRevision: &graph.AddRevisionCommand{}})
	digest, err := CommandDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		plan, err := e.prepare(ctx, req, digest, model.TxID(strings.Repeat("d", 32)), 1, nil)
		if err != nil || len(plan.profileEvents) != 1 {
			t.Fatalf("prepare domain fact: %v", err)
		}
		forged := plan.profileEvents[0]
		a := *forged.Attribution
		forged.Attribution, forged.ID = &a, ""
		forged.Attribution.Subject.ID = model.EntityID(strings.Repeat("f", 64))
		object, err := eventlog.Object(forged)
		if err != nil {
			t.Fatal(err) // the envelope is intentionally valid in isolation
		}
		id, err := codec.ObjectID(object)
		if err != nil {
			t.Fatal(err)
		}
		core := *plan.event
		core.ID, core.ProfileEvents = "", []model.EventID{model.EventID(id)}
		coreObject, err := eventlog.Object(core)
		if err != nil {
			t.Fatal(err)
		}
		for i, record := range plan.transaction.Records {
			previous, err := codec.DecodeObject(record.Payload)
			if err != nil {
				t.Fatal(err)
			}
			switch previous.Type {
			case eventlog.ProfileType:
				plan.transaction.Records[i].Payload, err = codec.EncodeObject(object)
			case eventlog.Type:
				plan.transaction.Records[i].Payload, err = codec.EncodeObject(coreObject)
			}
			if err != nil {
				t.Fatal(err)
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
		t.Fatalf("canonical but unrelated domain subject accepted on recovery: %v", err)
	}
}
