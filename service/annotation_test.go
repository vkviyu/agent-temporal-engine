package service

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestAnnotationServiceSDKIsolationHistoryAndAuditAccess(t *testing.T) {
	ctx := context.Background()
	core, _ := nativeAgentStore(t, true)
	server := newServer(t, core, agentCredentials())
	owner := pipeClient(t, server, "owner-token")
	other := pipeClient(t, server, "other-token")
	advertised := false
	for _, capability := range owner.Identity().Capabilities {
		if capability == "annotation/v1" {
			advertised = true
		}
	}
	if !advertised {
		t.Fatal("annotation capability was not advertised")
	}
	ack, err := owner.Mutate(ctx, api.MutateRequest{Key: domainKey("annotation-source"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("failure"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("original tool failure remains auditable")}}}}})
	domainAck(t, ack, err)
	source := model.SnapshotAnchor{Space: "s", Fence: ack.Fence}
	prior, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s", Anchor: &source})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := owner.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: prior.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("source entries=%+v %v", entries, err)
	}
	target := model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}
	record, err := annotation.NewRecord(annotation.Record{Target: target, Label: "irrelevant", Reason: "host accepted repeated failure judgement", Hide: true, Source: source, Evidence: []model.NodeRef{target}})
	if err != nil {
		t.Fatal(err)
	}
	request := api.ApplyAnnotationRequest{Key: domainKey("annotation-hide"), Command: record}
	hidden, err := owner.ApplyAnnotation(ctx, request)
	domainAck(t, hidden, err)
	retry, err := owner.ApplyAnnotation(ctx, request)
	if err != nil || !reflect.DeepEqual(hidden, retry) {
		t.Fatalf("annotation retry=%+v want=%+v %v", retry, hidden, err)
	}
	read := api.ReadAnnotationsRequest{Space: "s", Principal: "owner"}
	got, err := owner.GetAnnotations(ctx, read)
	embedded, embeddedErr := core.GetAnnotations(ctx, read)
	if err != nil || embeddedErr != nil || !reflect.DeepEqual(got, embedded) || len(got.Snapshot.Records) != 1 || got.Snapshot.Records[0].ID != record.ID {
		t.Fatalf("SDK=%+v embedded=%+v errors=%v/%v", got, embedded, err, embeddedErr)
	}
	historical, err := owner.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "s", Anchor: &source})
	if err != nil || len(historical.Snapshot.Records) != 0 {
		t.Fatalf("future annotation leaked into source: %+v %v", historical, err)
	}
	foreign, err := other.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "s"})
	if err != nil || len(foreign.Snapshot.Records) != 0 {
		t.Fatalf("foreign annotations=%+v %v", foreign, err)
	}
	if _, err := owner.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "s", Principal: "other"}); err != client.ErrPrincipal {
		t.Fatalf("principal spoof accepted: %v", err)
	}
	current, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.CloseView(ctx, api.CloseViewRequest{Ref: prior.Ref})
	defer owner.CloseView(ctx, api.CloseViewRequest{Ref: current.Ref})
	assertTarget := func(view model.ViewRef, includeHidden bool, want bool) {
		t.Helper()
		page, err := owner.ListNodes(ctx, api.ListNodesRequest{View: view, Layer: "content", IncludeHidden: includeHidden})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, node := range page.Items {
			found = found || node.Ref == target
		}
		if found != want {
			t.Fatalf("target present=%v want=%v includeHidden=%v page=%+v", found, want, includeHidden, page)
		}
	}
	assertTarget(prior.Ref, false, true)
	assertTarget(current.Ref, false, false)
	assertTarget(current.Ref, true, true)
	assertSearch := func(c *client.Client, view model.ViewRef, includeHidden, want bool) {
		t.Helper()
		result, err := c.SearchNodes(ctx, api.SearchNodesRequest{View: view, Layer: "content", IncludeHidden: includeHidden})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, item := range result.Items {
			found = found || item.Node.Ref == target
		}
		if found != want {
			t.Fatalf("search target present=%v want=%v includeHidden=%v result=%+v", found, want, includeHidden, result)
		}
	}
	assertSearch(owner, prior.Ref, false, true)
	assertSearch(owner, current.Ref, false, false)
	assertSearch(owner, current.Ref, true, true)
	foreignView, err := other.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer other.CloseView(ctx, api.CloseViewRequest{Ref: foreignView.Ref})
	foreignPage, err := other.ListNodes(ctx, api.ListNodesRequest{View: foreignView.Ref, Layer: "content"})
	if err != nil {
		t.Fatal(err)
	}
	foreignFound := false
	for _, item := range foreignPage.Items {
		foreignFound = foreignFound || item.Ref == target
	}
	if !foreignFound {
		t.Fatal("owner annotation changed another principal's navigation")
	}
	assertSearch(other, foreignView.Ref, false, true)
	detail, err := owner.GetNode(ctx, api.GetNodeRequest{View: current.Ref, Ref: target})
	if err != nil || detail.Ref != target {
		t.Fatalf("explicit audit detail=%+v %v", detail, err)
	}
	retraction, err := annotation.NewRecord(annotation.Record{Target: target, Reason: "host withdrew the noise judgment", Source: model.SnapshotAnchor{Space: "s", Fence: hidden.Fence}, Evidence: []model.NodeRef{target}, Retracts: record.ID})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := owner.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: domainKey("annotation-retract"), Command: retraction})
	domainAck(t, restored, err)
	withdrawn, err := owner.GetAnnotations(ctx, read)
	if err != nil || len(withdrawn.Snapshot.Records) != 2 || withdrawn.Snapshot.Records[0].ID != record.ID || withdrawn.Snapshot.Records[1].Retracts != record.ID {
		t.Fatalf("append-only retraction=%+v %v", withdrawn, err)
	}
	oldCut := model.SnapshotAnchor{Space: "s", Fence: hidden.Fence}
	oldJudgment, err := owner.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "s", Anchor: &oldCut})
	if err != nil || len(oldJudgment.Snapshot.Records) != 1 || !oldJudgment.Snapshot.Hidden()[target] {
		t.Fatalf("historical judgment changed=%+v %v", oldJudgment, err)
	}
	assertTarget(current.Ref, false, false)
	after, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.CloseView(ctx, api.CloseViewRequest{Ref: after.Ref})
	assertTarget(after.Ref, false, true)
}
