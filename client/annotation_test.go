package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func annotationResponse(t *testing.T) api.AnnotationsResult {
	t.Helper()
	source := model.SnapshotAnchor{Space: "s", Fence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}}
	ref := model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("b", 64))}
	record, err := annotation.NewRecord(annotation.Record{Target: ref, Label: "irrelevant", Reason: "repeated tool failure", Hide: true, Source: source, Evidence: []model.NodeRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	anchor := model.SnapshotAnchor{Space: "s", Fence: model.CommitFence{Store: "store", LSN: 2, CommitDigest: model.CommitDigest(strings.Repeat("c", 64))}}
	return api.AnnotationsResult{Snapshot: annotation.Snapshot{Records: []annotation.Record{record}}, Anchor: anchor, Fence: anchor.Fence, RootSet: model.ObjectID(strings.Repeat("d", 64))}
}

func TestAnnotationClientRejectsMalformedRemoteState(t *testing.T) {
	for name, mutate := range map[string]func(*api.AnnotationsResult){
		"different-fence":         func(r *api.AnnotationsResult) { r.Fence.LSN++ },
		"foreign-space":           func(r *api.AnnotationsResult) { r.Anchor.Space = "foreign" },
		"invalid-anchor":          func(r *api.AnnotationsResult) { r.Anchor.Fence.CommitDigest = "bad" },
		"unrequested-publication": func(r *api.AnnotationsResult) { r.Anchor.Fence.LSN++; r.Fence = r.Anchor.Fence },
		"unexpected-selector": func(r *api.AnnotationsResult) {
			r.Anchor.Knowledge = &model.KnowledgeKey{Actor: "actor", Execution: "execution"}
		},
		"invalid-root":            func(r *api.AnnotationsResult) { r.RootSet = "bad" },
		"missing-root":            func(r *api.AnnotationsResult) { r.RootSet = "" },
		"invalid-record-id":       func(r *api.AnnotationsResult) { r.Snapshot.Records[0].ID = "bad" },
		"duplicate-record":        func(r *api.AnnotationsResult) { r.Snapshot.Records = append(r.Snapshot.Records, r.Snapshot.Records[0]) },
		"foreign-source-space":    func(r *api.AnnotationsResult) { r.Snapshot.Records[0].Source.Space = "foreign" },
		"foreign-source-store":    func(r *api.AnnotationsResult) { r.Snapshot.Records[0].Source.Fence.Store = "foreign" },
		"future-source":           func(r *api.AnnotationsResult) { r.Snapshot.Records[0].Source.Fence.LSN = 3 },
		"same-publication-source": func(r *api.AnnotationsResult) { r.Snapshot.Records[0].Source = r.Anchor.Clone() },
	} {
		t.Run(name, func(t *testing.T) {
			response := annotationResponse(t)
			anchor := response.Anchor.Clone()
			mutate(&response)
			// Keep source attacks structurally valid so the transport's
			// publication/space checks are independently exercised.
			if strings.Contains(name, "source") {
				response.Snapshot.Records[0].ID = ""
				rebuilt, err := annotation.NewRecord(response.Snapshot.Records[0])
				if err != nil {
					t.Fatal(err)
				}
				response.Snapshot.Records[0] = rebuilt
			}
			c := nodeResponseClient(t, response)
			if _, err := c.GetAnnotations(context.Background(), api.ReadAnnotationsRequest{Space: "s", Anchor: &anchor}); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("malformed annotation success accepted: %v", err)
			}
			if !c.closed.Load() {
				t.Fatal("protocol-invalid connection remains reusable")
			}
		})
	}
}

func TestAnnotationClientAcceptsValidAppendOnlyState(t *testing.T) {
	response := annotationResponse(t)
	c := nodeResponseClient(t, response)
	if _, err := c.GetAnnotations(context.Background(), api.ReadAnnotationsRequest{Space: "s", Anchor: &response.Anchor}); err != nil {
		t.Fatal(err)
	}
}

func TestAnnotationClientRejectsSuccessAtItsOwnSourcePublication(t *testing.T) {
	response := annotationResponse(t)
	record := response.Snapshot.Records[0]
	ack := api.CommitResult{Code: api.CodeDurableAck, RequestID: "write", TxID: model.TxID(strings.Repeat("a", 32)), Fence: record.Source.Fence, RequestDigest: model.ObjectID(strings.Repeat("c", 64)), ResultDigest: model.ObjectID(strings.Repeat("d", 64)), RootSet: model.ObjectID(strings.Repeat("e", 64))}
	c := nodeResponseClient(t, ack)
	_, err := c.ApplyAnnotation(context.Background(), api.ApplyAnnotationRequest{Key: model.RequestKey{Space: "s", RequestID: "write"}, Command: record})
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, protocol.ErrProtocol) || !c.closed.Load() {
		t.Fatalf("invalid source publication success accepted: %v", err)
	}
}
