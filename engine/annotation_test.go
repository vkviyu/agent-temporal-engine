package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func newAnnotation(t *testing.T, target model.NodeRef, source model.SnapshotAnchor, reason string) annotation.Record {
	t.Helper()
	r, err := annotation.NewRecord(annotation.Record{Target: target, Label: annotation.LabelIrrelevant, Reason: reason, Hide: true, Source: source, Evidence: []model.NodeRef{target}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func applyAnnotation(t *testing.T, e *engine.Engine, id string, r annotation.Record) api.CommitResult {
	t.Helper()
	ack, err := e.ApplyAnnotation(context.Background(), api.ApplyAnnotationRequest{Key: agentKey(id), Command: r})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("annotation=%+v %v", ack, err)
	}
	return ack
}

func TestAnnotationRetainedThroughReplayAndCompactionWithTypedEvents(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	mutation := integrationMutate(t, s.e, integrationRequest("annotation-fact", integrationPut([]byte("tool-failure"), integrationBytes('f'))))
	old := nodeView(t, s.e, "host-principal", nil)
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: old.Ref})
	if err != nil {
		t.Fatal(err)
	}
	target := model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}
	before := integrationSnapshot(t, s.e)
	record := newAnnotation(t, target, old.Anchor, "host accepted duplicate failure judgment")
	ack := applyAnnotation(t, s.e, "annotation-review", record)
	after := integrationSnapshot(t, s.e)
	if before.Roots.DomainStates != after.Roots.DomainStates || before.Roots.LiveControl != after.Roots.LiveControl || before.Roots.BranchRefs != after.Roots.BranchRefs || before.Roots.ExecutionKnowledge == after.Roots.ExecutionKnowledge {
		t.Fatal("annotation changed facts/control or selected wrong root")
	}
	check := func() {
		t.Helper()
		hiddenCut := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
		got, err := s.e.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "host-space", Principal: "host-principal", Anchor: &hiddenCut})
		if err != nil || len(got.Snapshot.Records) != 1 || !reflect.DeepEqual(got.Snapshot.Records[0], record) {
			t.Fatalf("retained annotation=%+v %v", got, err)
		}
		historic := nodeView(t, s.e, "host-principal", &hiddenCut)
		page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: historic.Ref, Layer: "content"})
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range page.Items {
			if n.Ref == target {
				t.Fatal("hidden failure returned in navigation")
			}
		}
		detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: historic.Ref, Ref: target})
		if err != nil || detail.Ref != target {
			t.Fatalf("explicit fact erased=%+v %v", detail, err)
		}
		data, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: historic.Ref, Ref: target, Projection: "value_bytes"})
		if err != nil || string(data.Data) != "f" {
			t.Fatalf("original failure bytes changed=%+v %v", data, err)
		}
		events, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: historic.Ref, Layer: "events", EventOperation: "Annotation.Apply", IncludeHidden: true})
		if err != nil || len(events.Items) != 1 {
			t.Fatalf("missing typed annotation fact=%+v %v", events, err)
		}
		prior := model.SnapshotAnchor{Space: "host-space", Fence: mutation.Fence}
		noFuture, err := s.e.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "host-space", Principal: "host-principal", Anchor: &prior})
		if err != nil || len(noFuture.Snapshot.Records) != 0 {
			t.Fatalf("future judgment leaked into old cut=%+v %v", noFuture, err)
		}
	}
	check()
	// Close all views before reopen/compaction, including helper cleanup views.
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: old.Ref}); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func TestAnnotationRejectsForgedSourcesUnknownEvidenceAndNoncausalWithdrawal(t *testing.T) {
	ctx := context.Background()
	s, source := memoryFixture(t)
	target := model.NodeRef{Kind: model.NodeObject, Object: source.SourceID}
	good := newAnnotation(t, target, source.Anchor, "host review")
	ack := applyAnnotation(t, s.e, "valid-annotation", good)
	before := integrationSnapshot(t, s.e)
	invalids := []annotation.Record{}
	for _, mutate := range []func(*annotation.Record){
		func(r *annotation.Record) { r.Source.Fence.CommitDigest = model.CommitDigest(strings.Repeat("0", 64)) },
		func(r *annotation.Record) { r.Source.Fence.LSN += 100 },
		func(r *annotation.Record) { r.Source.Space = "other-space" },
		func(r *annotation.Record) { r.Target.Object = model.ObjectID(strings.Repeat("a", 64)) },
		func(r *annotation.Record) {
			r.Evidence = []model.NodeRef{{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("c", 64))}}
		},
	} {
		r := good.Clone()
		mutate(&r)
		r, err := annotation.NewRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		invalids = append(invalids, r)
	}
	// Its target existed at the old Source, but the accepted judgment did not.
	staleWithdrawal, err := annotation.NewRecord(annotation.Record{Target: target, Reason: "withdraw", Source: source.Anchor, Retracts: good.ID})
	if err != nil {
		t.Fatal(err)
	}
	invalids = append(invalids, staleWithdrawal, good)
	for i, r := range invalids {
		key := agentKey("rejected-annotation-" + string(rune('a'+i)))
		out, err := s.e.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: key, Command: r})
		if err == nil || out.Code != api.CodeRejected {
			t.Fatalf("invalid annotation %d accepted=%+v %v", i, out, err)
		}
		if engine.ClassifyError(err) == api.ErrorInternal || engine.ClassifyError(err) == api.ErrorCorruption {
			t.Fatalf("caller error fenced core=%v", err)
		}
	}
	if integrationSnapshot(t, s.e) != before {
		t.Fatal("annotation rejection mutated/fenced store")
	}
	current := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
	retract, err := annotation.NewRecord(annotation.Record{Target: target, Reason: "withdraw", Source: current, Retracts: good.ID})
	if err != nil {
		t.Fatal(err)
	}
	applyAnnotation(t, s.e, "valid-withdrawal", retract)
}

func TestAnnotationCurrentPrivacyCannotBeBypassedByHistoricalSourceOrRetry(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, source := memoryFixture(t)
			target := model.NodeRef{Kind: model.NodeObject, Object: source.SourceID}
			record := newAnnotation(t, target, source.Anchor, "reviewed original failure")
			ack := applyAnnotation(t, s.e, "privacy-annotation", record)
			fixed := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}
			applyMemory(t, s.e, "annotation-privacy-ban", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: source.SourceID, Retraction: memory.Retraction{Kind: kind, Reason: "source privacy revoked"}}})
			before := integrationSnapshot(t, s.e)
			read := func() {
				t.Helper()
				if _, err := s.e.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "host-space", Principal: "host-principal", Anchor: &fixed}); !errors.Is(err, memory.ErrHistoryAccessDenied) {
					t.Fatalf("old annotation disclosed protected provenance: %v", err)
				}
			}
			read()
			retry, err := s.e.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: agentKey("privacy-annotation"), Command: record})
			if err != nil || retry != ack {
				t.Fatalf("accepted receipt changed under policy=%+v %v", retry, err)
			}
			record.Reason = "new review"
			record, err = annotation.NewRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			denied, err := s.e.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: agentKey("privacy-new-review"), Command: record})
			if denied.Code != api.CodeRejected || !errors.Is(err, memory.ErrHistoryAccessDenied) {
				t.Fatalf("new source bypassed current ACL=%+v %v", denied, err)
			}
			if integrationSnapshot(t, s.e) != before {
				t.Fatal("ACL denial mutated/fenced store")
			}
			s.reopen()
			read()
		})
	}
}

func TestAnnotationExecutionKnowledgeSourceCannotImportUnadoptedOrForeignNodes(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	observation := knowledgeObservation(payload, source, "one", 1)
	_, ack := applyObservation(t, s.e, "annotation-adopted", observation)
	key := observation.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence, Knowledge: &key}
	good := newAnnotation(t, model.NodeRef{Kind: model.NodeObject, Object: payload}, anchor, "execution-local review")
	reviewAck := applyAnnotation(t, s.e, "knowledge-annotation", good)
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil || known.Revision == nil {
		t.Fatalf("knowledge revision=%+v %v", known, err)
	}
	cut := model.SnapshotAnchor{Space: "host-space", Fence: reviewAck.Fence, Knowledge: &key, Cursor: &model.Cursor{Store: reviewAck.Fence.Store, Space: "host-space", Revision: known.Revision.ID}}
	viewKnowledge, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &cut, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: viewKnowledge.Ref})
	page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: viewKnowledge.Ref, Layer: "content"})
	if err != nil {
		t.Fatal(err)
	}
	adoptedStillVisible := false
	for _, node := range page.Items {
		adoptedStillVisible = adoptedStillVisible || node.Ref == good.Target
	}
	if !adoptedStillVisible {
		t.Fatal("unadopted owner navigation judgment changed an execution knowledge cut")
	}
	annotationRef := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "annotation", Kind: "Annotation", ID: model.EntityID(good.ID)}}
	if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: viewKnowledge.Ref, Ref: annotationRef}); !errors.Is(err, engine.ErrNodeNotFound) && !errors.Is(err, engine.ErrUnsupportedMethod) {
		t.Fatalf("owner annotation bypassed knowledge adoption: %v", err)
	}
	foreign := good.Clone()
	foreign.Reason = "foreign review"
	foreign, err = annotation.NewRecord(foreign)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.e.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: model.RequestKey{Space: "host-space", Principal: "other", RequestID: "foreign-review"}, Command: foreign})
	if out.Code != api.CodeRejected || engine.ClassifyError(err) != api.ErrorUnauthorized {
		t.Fatalf("foreign execution annotated=%+v %v", out, err)
	}
	mutation := integrationMutate(t, s.e, integrationRequest("unadopted-annotation-input", integrationPut([]byte("unadopted"), integrationBytes('u'))))
	view := nodeView(t, s.e, "host-principal", nil)
	raw, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil {
		t.Fatal(err)
	}
	foreign = good.Clone()
	var unadopted model.ObjectID
	for _, entry := range raw.Items {
		if string(entry.Key) == "unadopted" {
			unadopted = entry.Value
		}
	}
	foreign.Target = model.NodeRef{Kind: model.NodeObject, Object: unadopted}
	foreign.Source.Fence = mutation.Fence
	foreign.Reason = "unadopted review"
	foreign, err = annotation.NewRecord(foreign)
	if err != nil {
		t.Fatal(err)
	}
	out, err = s.e.ApplyAnnotation(ctx, api.ApplyAnnotationRequest{Key: agentKey("unadopted-review"), Command: foreign})
	if out.Code != api.CodeRejected || !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("unadopted hash granted visibility=%+v %v", out, err)
	}
}

func TestAnnotationEventTargetPreservesTraceAndRequiredContext(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	integrationMutate(t, s.e, integrationRequest("annotation-event-source", integrationPut([]byte("failure"), integrationBytes('f'))))
	source := nodeView(t, s.e, "host-principal", nil)
	events, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: source.Ref, Layer: "events"})
	if err != nil || len(events.Items) == 0 {
		t.Fatalf("events=%+v %v", events, err)
	}
	target := events.Items[0].Ref
	before, err := s.e.Trace(ctx, api.TraceRequest{View: source.Ref, Start: target, Direction: "both"})
	if err != nil {
		t.Fatal(err)
	}
	record := newAnnotation(t, target, source.Anchor, "host marks the original event noisy")
	applyAnnotation(t, s.e, "event-target-review", record)
	current := nodeView(t, s.e, "host-principal", nil)
	after, err := s.e.Trace(ctx, api.TraceRequest{View: current.Ref, Start: target, Direction: "both"})
	if err != nil || len(after.Nodes) < len(before.Nodes) {
		t.Fatalf("trace erased=%+v %v", after, err)
	}
	found := false
	for _, node := range after.Nodes {
		found = found || node.Ref == target
	}
	if !found {
		t.Fatal("hidden original event disappeared from trace")
	}
	context, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: current.Ref, Ref: target, Mode: "evidence_neighborhood"})
	if err != nil || len(context.Sections["node"]) != 1 {
		t.Fatalf("hidden original event context erased=%+v %v", context, err)
	}
	page, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: current.Ref, Layer: "events", Limit: 1, IncludeHidden: true})
	if err != nil || !page.Truncated {
		t.Fatalf("audit page=%+v %v", page, err)
	}
	if _, err := s.e.ListNodes(ctx, api.ListNodesRequest{View: current.Ref, Layer: "events", Limit: 1, Continuation: page.Continuation}); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatalf("IncludeHidden token rebound: %v", err)
	}
}

func TestAnnotationEventAndDetailRemainProtectedAfterSourceLeavesDomain(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	integrationMutate(t, s.e, integrationRequest("annotation-private-source", integrationPut([]byte("sensitive"), integrationBytes('s'))))
	source := nodeView(t, s.e, "host-principal", nil)
	entries, err := s.e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: source.Ref})
	if err != nil {
		t.Fatal(err)
	}
	target := model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value}
	record := newAnnotation(t, target, source.Anchor, "derived judgment retains original provenance")
	applyAnnotation(t, s.e, "annotation-private-review", record)
	integrationMutate(t, s.e, integrationRequest("annotation-remove-live-key", api.StateMutation{Key: []byte("sensitive"), Kind: api.MutationDelete}))
	current := nodeView(t, s.e, "host-principal", nil)
	annotationRef := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "annotation", Kind: "Annotation", ID: model.EntityID(record.ID)}}
	if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: current.Ref, Ref: annotationRef}); err != nil {
		t.Fatalf("annotation not inspectable before ban: %v", err)
	}
	applyMemory(t, s.e, "annotation-privacy-after-delete", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: target.Object, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "current privacy restriction"}}})
	for _, read := range []func() error{
		func() error {
			_, err := s.e.GetNode(ctx, api.GetNodeRequest{View: current.Ref, Ref: annotationRef})
			return err
		},
		func() error {
			_, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: current.Ref, Layer: "events", IncludeHidden: true})
			return err
		},
		func() error {
			_, err := s.e.GetAnnotations(ctx, api.ReadAnnotationsRequest{Space: "host-space", Principal: "host-principal"})
			return err
		},
	} {
		if err := read(); !errors.Is(err, memory.ErrHistoryAccessDenied) {
			t.Fatalf("derived annotation bytes bypassed current privacy: %v", err)
		}
	}
}
