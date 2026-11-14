package service

import (
	"context"
	"reflect"
	"slices"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestInvestigationServiceOwnershipAndRecovery(t *testing.T) {
	ctx := context.Background()
	core, path := nativeAgentStore(t, false)
	server := newServer(t, core, agentCredentials())
	owner := pipeClient(t, server, "owner-token")
	other := pipeClient(t, server, "other-token")
	if !slices.Contains(owner.Identity().Capabilities, "investigation/v1") {
		t.Fatal("investigation capability was not advertised")
	}

	ack, err := owner.Mutate(ctx, api.MutateRequest{
		Key: domainKey("investigation-evidence"),
		Batch: api.StateBatch{Operations: []api.StateMutation{{
			Key: []byte("tool-result"), Kind: api.MutationPut,
			Value: api.Value{Kind: api.ValueBytes, Data: []byte("observed state")},
		}}},
	})
	domainAck(t, ack, err)
	source := model.SnapshotAnchor{Space: "s", Fence: ack.Fence}
	view, err := owner.OpenView(ctx, api.OpenViewRequest{Space: source.Space, Anchor: &source})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := owner.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("evidence entries=%+v err=%v", entries, err)
	}
	citation := investigation.Citation{Ref: model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}, Anchor: source}
	create := api.UpdateInvestigationRequest{
		Key:     domainKey("investigation-create"),
		Command: investigation.Update{ID: "inspect-result", Subject: &citation},
	}
	created, err := owner.UpdateInvestigation(ctx, create)
	domainAck(t, created, err)

	read := api.GetInvestigationRequest{Space: "s", ID: create.Command.ID}
	initial, err := owner.GetInvestigation(ctx, read)
	if err != nil || !initial.Found || initial.Checkpoint.Version != 1 || initial.Fence != created.Fence {
		t.Fatalf("initial investigation=%+v err=%v", initial, err)
	}
	foreign, err := other.GetInvestigation(ctx, read)
	if err != nil || foreign.Found {
		t.Fatalf("foreign investigation=%+v err=%v", foreign, err)
	}
	if _, err := owner.GetInvestigation(ctx, api.GetInvestigationRequest{Space: "s", ID: read.ID, Principal: "other"}); err != client.ErrPrincipal {
		t.Fatalf("principal spoof accepted: %v", err)
	}

	nextFocus := "Check what changed after this result"
	advance := api.UpdateInvestigationRequest{
		Key: domainKey("investigation-advance"),
		Command: investigation.Update{
			ID: read.ID, ExpectedVersion: 1, NextFocus: &nextFocus, Sources: []investigation.Citation{citation},
			Position: &investigation.Position{Focus: citation, Mode: "evidence_neighborhood", Sections: []string{"node"}},
		},
	}
	advanced, err := owner.UpdateInvestigation(ctx, advance)
	domainAck(t, advanced, err)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, report, err := ate.OpenExisting(path, ate.Options{})
	if err != nil || !report.Verified {
		t.Fatalf("reopen report=%+v err=%v", report, err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	resumed := pipeClient(t, newServer(t, reopened, agentCredentials()), "owner-token")
	retried, err := resumed.UpdateInvestigation(ctx, advance)
	if err != nil || retried != advanced {
		t.Fatalf("retry after restart=%+v want=%+v err=%v", retried, advanced, err)
	}
	latest, err := resumed.GetInvestigation(ctx, read)
	embedded, embeddedErr := reopened.GetInvestigation(ctx, api.GetInvestigationRequest{Space: "s", Principal: "owner", ID: read.ID})
	if err != nil || embeddedErr != nil || !reflect.DeepEqual(latest, embedded) ||
		!latest.Found || latest.Checkpoint.Version != 2 || latest.Checkpoint.NextFocus != nextFocus {
		t.Fatalf("resumed=%+v embedded=%+v errors=%v/%v", latest, embedded, err, embeddedErr)
	}
	read.Anchor = &initial.Anchor
	old, err := resumed.GetInvestigation(ctx, read)
	if err != nil || !reflect.DeepEqual(initial, old) {
		t.Fatalf("historical=%+v want=%+v err=%v", old, initial, err)
	}
	position := latest.Checkpoint.Position
	if position == nil {
		t.Fatal("recovery lost the saved evidence position")
	}
	historyView, err := resumed.OpenView(ctx, api.OpenViewRequest{Space: position.Focus.Anchor.Space, Anchor: &position.Focus.Anchor, Mode: position.ViewMode()})
	if err != nil || !reflect.DeepEqual(historyView.Anchor, source) {
		t.Fatalf("reopened evidence view=%+v err=%v", historyView, err)
	}
	page, err := resumed.GetNodeContext(ctx, api.NodeContextRequest{
		View: historyView.Ref, Ref: position.Focus.Ref, Mode: position.Mode, Sections: position.Sections,
		Limits: api.QueryLimits{MaxNodes: 1, MaxEdges: 1, MaxDepth: 1, MaxBytes: 4096},
	})
	if err != nil || page.Ref != citation.Ref || len(page.Sections["node"]) != 1 || page.Anchor.Fence != source.Fence {
		t.Fatalf("reopened evidence context=%+v err=%v", page, err)
	}
	withdrawn, err := resumed.ApplyMemory(ctx, api.ApplyMemoryRequest{
		Key: domainKey("investigation-withdraw-evidence"),
		Command: memory.Command{RetractSource: &memory.RetractSourceCommand{
			Source: citation.Ref.Object,
			Retraction: memory.Retraction{
				Kind: memory.RetractionPrivacyErasure, Reason: "withdraw investigation source",
			},
		}},
	})
	domainAck(t, withdrawn, err)
	for _, anchor := range []*model.SnapshotAnchor{nil, &initial.Anchor} {
		read.Anchor = anchor
		denied, err := resumed.GetInvestigation(ctx, read)
		remoteCode(t, err, api.ErrorUnauthorized)
		if denied.Found {
			t.Fatalf("withdrawn source exposed checkpoint: %+v", denied)
		}
	}
}
