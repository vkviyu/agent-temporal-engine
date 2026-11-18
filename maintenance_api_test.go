package ate_test

import (
	"context"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
)

func TestPublicRecoveryCheckpointAndSuffix(t *testing.T) {
	ctx := context.Background()
	path := nativeFacadeDirectory(t)
	s, err := ate.Create(path, ate.Options{EventProfile: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	first := facadeRequest()
	ack, err := s.Mutate(ctx, first)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("first: %+v %v", ack, err)
	}
	checkpoint, err := s.CreateRecoveryCheckpoint(ctx)
	if err != nil || checkpoint.Fence() != ack.Fence {
		t.Fatalf("checkpoint changed fence: %+v %v", checkpoint, err)
	}
	next := facadeRequest()
	next.Key.RequestID = "suffix"
	tip, err := s.Mutate(ctx, next)
	if err != nil || tip.Code != api.CodeDurableAck {
		t.Fatalf("suffix: %+v %v", tip, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		opened, report, err := ate.OpenExisting(path, ate.Options{
			EventProfile: true, DisableRecoveryCheckpoints: full,
		})
		if err != nil {
			t.Fatal(err)
		}
		// Register cleanup before assertions so failed checks release ownership.
		t.Cleanup(func() { _ = opened.Close() })
		expected := uint64(1)
		if full {
			expected = 0
		}
		if report.ScannedTransactions != 2 || report.CheckpointTransactions != expected ||
			report.Publication.Fence() != tip.Fence {
			t.Fatalf("full=%t: %+v", full, report)
		}
		retry, err := opened.Mutate(ctx, first)
		if err != nil || retry != ack {
			t.Fatalf("historical receipt: %+v %v", retry, err)
		}
		if err := opened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
