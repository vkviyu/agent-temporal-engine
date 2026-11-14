package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func investigationRequest(id string, update investigation.Update) api.UpdateInvestigationRequest {
	return api.UpdateInvestigationRequest{Key: agentKey(id), Command: update}
}

func publishInvestigation(t *testing.T, e *engine.Engine, req api.UpdateInvestigationRequest) api.CommitResult {
	t.Helper()
	ack, err := e.UpdateInvestigation(context.Background(), req)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("investigation commit=%+v err=%v", ack, err)
	}
	return ack
}

func investigationEvent(t *testing.T, e *engine.Engine, view model.ViewRef, tx model.TxID) model.Event {
	t.Helper()
	ctx := context.Background()
	page, err := e.ListNodes(ctx, api.ListNodesRequest{View: view, Layer: "events"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range page.Items {
		detail, err := e.GetNode(ctx, api.GetNodeRequest{View: view, Ref: item.Ref})
		if err != nil {
			t.Fatal(err)
		}
		var event model.Event
		if err := json.Unmarshal(detail.Data, &event); err != nil {
			t.Fatal(err)
		}
		if event.Transaction == tx {
			return event
		}
	}
	t.Fatal("published event was not found")
	return model.Event{}
}

func TestInvestigationConcurrentUpdateAndRestart(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	firstFact := integrationMutate(t, s.e, integrationRequest("investigation-event", integrationPut([]byte("artifact"), integrationBytes('a'))))
	view := nodeView(t, s.e, "host-principal", nil)
	event := investigationEvent(t, s.e, view.Ref, firstFact.TxID)
	subject := investigation.Citation{Ref: model.NodeRef{Kind: model.NodeEvent, Event: event.ID}, Anchor: view.Anchor}
	create := investigationRequest("investigation-create", investigation.Update{
		ID: "inspect", Subject: &subject,
		Position: &investigation.Position{Focus: subject, Mode: "retrospective", Sections: []string{"node", "downstream"}},
	})
	before := integrationSnapshot(t, s.e)
	foreign := create
	foreign.Key.Principal = "foreign-principal"
	if out, err := s.e.UpdateInvestigation(ctx, foreign); out.Code != api.CodeRejected || !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("foreign event accepted: %+v %v", out, err)
	}
	initial := publishInvestigation(t, s.e, create)
	state := integrationSnapshot(t, s.e)
	if state.Roots.DomainStates != before.Roots.DomainStates || state.Roots.ExecutionKnowledge == before.Roots.ExecutionKnowledge || state.Roots.LiveControl != before.Roots.LiveControl || state.Roots.BranchRefs != before.Roots.BranchRefs {
		t.Fatalf("unexpected investigation roots: %+v", state.Roots)
	}

	// The current publication can move independently of the investigation version.
	integrationMutate(t, s.e, integrationRequest("unrelated-publish", integrationPut([]byte("later"), integrationBytes('b'))))
	type attempt struct {
		request api.UpdateInvestigationRequest
		ack     api.CommitResult
		err     error
	}
	start := make(chan struct{})
	results := make(chan attempt, 2)
	for _, text := range []string{"inspect one", "inspect two"} {
		request := investigationRequest(text, investigation.Update{ID: "inspect", ExpectedVersion: 1, NextFocus: &text})
		go func() {
			<-start
			ack, err := s.e.UpdateInvestigation(ctx, request)
			results <- attempt{request: request, ack: ack, err: err}
		}()
	}
	close(start)
	accepted, conflict := <-results, <-results
	if accepted.ack.Code != api.CodeDurableAck {
		accepted, conflict = conflict, accepted
	}
	if accepted.err != nil || conflict.err != nil || accepted.ack.Code != api.CodeDurableAck || conflict.ack.Code != api.CodeConflict || conflict.ack.Conflict != api.ConflictPrecondition {
		t.Fatalf("accepted=%+v conflict=%+v", accepted, conflict)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	retry := publishInvestigation(t, s.e, accepted.request)
	if retry != accepted.ack {
		t.Fatalf("retry receipt changed: %+v vs %+v", retry, accepted.ack)
	}
	changed := accepted.request
	different := "different command under same key"
	changed.Command.NextFocus = &different
	if out, err := s.e.UpdateInvestigation(ctx, changed); err != nil || out.Code != api.CodeConflict || out.Conflict != api.ConflictIdempotency {
		t.Fatalf("same key with different command: %+v %v", out, err)
	}
	read := api.GetInvestigationRequest{Space: "host-space", Principal: "host-principal", ID: "inspect"}
	latest, err := s.e.GetInvestigation(ctx, read)
	if err != nil || !latest.Found || latest.Checkpoint.Version != 2 || latest.Checkpoint.NextFocus != *accepted.request.Command.NextFocus || latest.Checkpoint.Position == nil {
		t.Fatalf("recovered checkpoint=%+v err=%v", latest, err)
	}
	p := latest.Checkpoint.Position
	resumed, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: read.Space, Principal: read.Principal, Anchor: &p.Focus.Anchor, Mode: p.ViewMode()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: resumed.Ref})
	if resumed.Ref == view.Ref || resumed.Anchor.Fence != firstFact.Fence {
		t.Fatalf("invalid resumed view: %+v", resumed)
	}
	page, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{
		View: resumed.Ref, Ref: p.Focus.Ref, Mode: p.Mode, Sections: p.Sections,
		Limits: api.QueryLimits{MaxNodes: 1, MaxEdges: 1, MaxDepth: 1, MaxBytes: 4096},
	})
	if err != nil || page.Ref != subject.Ref || page.Anchor.Fence != firstFact.Fence || len(page.Sections["node"]) != 1 || len(page.Sections["downstream"]) != 0 {
		t.Fatalf("resumed context changed historical cut: %+v %v", page, err)
	}
	read.Anchor = &model.SnapshotAnchor{Space: read.Space, Fence: initial.Fence}
	old, err := s.e.GetInvestigation(ctx, read)
	if err != nil || !old.Found || old.Checkpoint.Version != 1 || old.Checkpoint.NextFocus != "" {
		t.Fatalf("historical checkpoint=%+v err=%v", old, err)
	}
}

func TestInvestigationSourceWithdrawalProtectsHistoricalCommand(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	seed := integrationMutate(t, s.e, integrationRequest("investigation-sources",
		integrationPut([]byte("subject"), integrationBytes('a')),
		integrationPut([]byte("private"), integrationBytes('b')),
	))
	source := model.SnapshotAnchor{Space: "host-space", Fence: seed.Fence}
	cite := func(key string) investigation.Citation {
		t.Helper()
		v := nodeView(t, s.e, "host-principal", &source)
		entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range entries.Items {
			if string(item.Key) == key {
				return investigation.Citation{Ref: model.NodeRef{Kind: model.NodeObject, Object: item.Value}, Anchor: source}
			}
		}
		t.Fatal("missing source key", key)
		return investigation.Citation{}
	}
	subject, private := cite("subject"), cite("private")
	create := investigationRequest("investigation-derived-text", investigation.Update{
		ID: "privacy", Subject: &subject,
		Findings: []investigation.Finding{{ID: "private-finding", Text: "derived from the private artifact", Evidence: []investigation.Citation{private}}},
	})
	first := publishInvestigation(t, s.e, create)
	publishInvestigation(t, s.e, investigationRequest("investigation-remove-finding", investigation.Update{
		ID: "privacy", ExpectedVersion: 1, RemoveFindings: []string{"private-finding"},
	}))
	integrationMutate(t, s.e, integrationRequest("investigation-remove-source", api.StateMutation{Key: []byte("private"), Kind: api.MutationDelete}))
	current := nodeView(t, s.e, "host-principal", nil)
	command := investigationEvent(t, s.e, current.Ref, first.TxID).Command
	raw := api.ReadContentRequest{View: current.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: command}}
	if _, err := s.e.ReadContent(ctx, raw); err != nil {
		t.Fatalf("raw audit command was unavailable before withdrawal: %v", err)
	}
	applyMemory(t, s.e, "investigation-withdraw-source", memory.Command{RetractSource: &memory.RetractSourceCommand{
		Source: private.Ref.Object, Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "source permission withdrawn"},
	}})
	if _, err := s.e.ReadContent(ctx, raw); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatalf("raw command bypassed current policy: %v", err)
	}
	read := api.GetInvestigationRequest{Space: "host-space", Principal: "host-principal", ID: "privacy"}
	verify := func() {
		t.Helper()
		latest, err := s.e.GetInvestigation(ctx, read)
		if err != nil || !latest.Found || latest.Checkpoint.Version != 2 || len(latest.Checkpoint.Findings) != 0 {
			t.Fatalf("independent later version unavailable: %+v %v", latest, err)
		}
		historical := read
		historical.Anchor = &model.SnapshotAnchor{Space: read.Space, Fence: first.Fence}
		if got, err := s.e.GetInvestigation(ctx, historical); got.Found || !errors.Is(err, memory.ErrHistoryAccessDenied) {
			t.Fatalf("historical derived text disclosed: %+v %v", got, err)
		}
		if retry := publishInvestigation(t, s.e, create); retry != first {
			t.Fatalf("withdrawal changed durable acknowledgement: %+v", retry)
		}
	}
	verify()
	s.reopen()
	verify()
}
