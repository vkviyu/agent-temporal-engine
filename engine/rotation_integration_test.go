package engine_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestDurableStateAcrossMidTransactionRotations(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{Journal: storage.JournalOptions{SegmentTargetBytes: 1024}, Batch: engine.BatchOptions{MaxCount: 1}})
	requests := make([]api.MutateRequest, 6)
	acks := make([]api.CommitResult, len(requests))
	for i := range requests {
		requests[i] = integrationRequest(fmt.Sprintf("rotation-%d", i), integrationPut([]byte(fmt.Sprintf("key-%d", i)), api.Value{Kind: api.ValueBytes, Data: bytes.Repeat([]byte{byte(i + 1)}, 1800+i*7)}))
		acks[i] = integrationMutate(t, s.e, requests[i])
		// Every new command reads tree/receipt objects from earlier sealed
		// segments, while its own multi-frame transaction crosses more segments.
		for n := 0; n <= i; n++ {
			integrationRead(t, s.e, requests[n].Key.Space, requests[n].Batch.Operations[0].Key, true, requests[n].Batch.Operations[0].Value)
		}
		if i == 2 || i == 5 {
			report := s.reopen()
			if report.ScannedTransactions != uint64(i+1) {
				t.Fatalf("missing transactions after rotation: %+v", report)
			}
		}
	}
	for i, request := range requests {
		if lookup := integrationLookup(t, s.e, request.Key); lookup.Result != acks[i] || lookup.ObservedFence != acks[len(acks)-1].Fence {
			t.Fatalf("historical proof changed after rotation: %+v != %+v", lookup, acks[i])
		}
		if retry := integrationMutate(t, s.e, request); retry != acks[i] {
			t.Fatal("rotation replaced original receipt with current roots")
		}
	}
	report := s.reopen()
	if report.ScannedTransactions != uint64(len(requests)) || report.Publication.Generation != uint64(len(requests)) {
		t.Fatalf("retries after rotations wrote another transaction: %+v", report)
	}
}
