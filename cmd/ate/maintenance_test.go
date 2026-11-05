package main

import (
	"context"
	"encoding/json"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestCompactStateBinaryPreservesHistoryAndRejectsBusyOwner(t *testing.T) {
	dir, _ := initStateFixture(t)
	s, _, err := ate.OpenExisting(dir, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := api.MutateRequest{Key: model.RequestKey{Space: "s", Principal: "p", RequestID: "original"}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("a"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("history")}}}}}
	ack, err := s.Mutate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if code, _, _ := runJournalCommand(t, "compact-state", dir); code != 1 {
		t.Fatal("compacted another owner's open store")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := runJournalCommand(t, "compact-state", dir)
	if code != 0 {
		t.Fatal(code, string(out), errOut)
	}
	var report storage.CompactionReport
	if err = json.Unmarshal(out, &report); err != nil || report.After.Fence() != ack.Fence || report.After.Generation != report.Before.Generation+1 || len(report.ReclaimedSegments) != 1 {
		t.Fatal(report, err)
	}
	s, _, err = ate.OpenExisting(dir, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if again, err := s.Mutate(context.Background(), req); err != nil || again != ack {
		t.Fatal("binary compaction changed receipt", again, err)
	}
}
