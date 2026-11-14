package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestViewRetainsPhysicalGenerationUntilClosed(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{Journal: storage.JournalOptions{SegmentTargetBytes: 1024}})
	integrationMutate(t, s.e, integrationRequest("physical-pin-first", integrationPut([]byte("before"), integrationBytes('a'))))
	view := nodeView(t, s.e, "host-principal", nil)
	read := api.ReadViewEntriesRequest{View: view.Ref}
	before, err := s.e.GetViewEntries(ctx, read)
	if err != nil || len(before.Items) != 1 {
		t.Fatal(before, err)
	}
	integrationMutate(t, s.e, integrationRequest("physical-pin-later", integrationPut([]byte("later"), integrationBytes('b'))))
	report, err := s.e.CompactAuditFull(ctx)
	if err != nil || report.PinnedSegments == 0 {
		t.Fatalf("live View did not retain its physical generation: %+v %v", report, err)
	}
	removed, pinned, err := s.e.ReclaimRetired(ctx)
	if err != nil || len(removed) != 0 || pinned != report.PinnedSegments {
		t.Fatalf("reclaim bypassed live View: removed=%v pinned=%d err=%v", removed, pinned, err)
	}
	after, err := s.e.GetViewEntries(ctx, read)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("compaction changed pinned logical cut: %+v %v", after, err)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	removed, pinned, err = s.e.ReclaimRetired(ctx)
	if err != nil || pinned != 0 || len(removed) != report.PinnedSegments {
		t.Fatalf("closed View leaked physical retention: removed=%v pinned=%d err=%v", removed, pinned, err)
	}
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal("second CloseView is not idempotent", err)
	}
}
