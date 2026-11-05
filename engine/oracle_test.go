package engine_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// This oracle uses only Go maps and the public value DTO. It never invokes the
// production radix algorithm, production replay, or production delta builder
// to decide which values should exist.
func TestDurableStateIndependentMapOracleAcrossReopens(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{})
	rng := rand.New(rand.NewSource(1942703))
	spaces := []model.SpaceID{"oracle-one", "oracle-two", "oracle\x00three"}
	keys := [][]byte{nil, {0}, {0, 1}, []byte("a"), []byte("ab"), []byte("ac"), []byte("xb"), {255, 0, 255}}
	for i := 0; i < 12; i++ {
		key := make([]byte, rng.Intn(15)+1)
		_, _ = rng.Read(key)
		if i%2 == 0 {
			key = append([]byte("shared/"), key...)
		}
		keys = append(keys, key)
	}
	oracle := make(map[model.SpaceID]map[string]api.Value)
	roots := make(map[model.SpaceID]model.ObjectID)
	for _, space := range spaces {
		oracle[space] = make(map[string]api.Value)
	}
	type receipt struct {
		key model.RequestKey
		ack api.CommitResult
	}
	var receipts []receipt
	for batch := 0; batch < 30; batch++ {
		space := spaces[rng.Intn(len(spaces))]
		before := integrationSnapshot(t, s.e)
		expectedRoot := roots[space]
		request := api.MutateRequest{
			Key:   model.RequestKey{Space: space, Principal: "oracle-host", RequestID: model.RequestID(fmt.Sprintf("oracle-%02d", batch))},
			Batch: api.StateBatch{ExpectedRoot: &expectedRoot},
		}
		if batch%2 == 0 {
			request.ExpectedFence = &before.Fence
		}
		permutation := rng.Perm(len(keys))
		for _, i := range permutation[:rng.Intn(6)+1] {
			key := keys[i]
			old, exists := oracle[space][string(key)]
			mutation := api.StateMutation{Key: bytes.Clone(key)}
			if rng.Intn(3) == 0 {
				mutation.Kind = api.MutationDelete
			} else {
				mutation.Kind = api.MutationPut
				switch rng.Intn(3) {
				case 0:
					mutation.Value = api.Value{Kind: api.ValueNull}
				case 1:
					mutation.Value = integrationBytes()
				case 2:
					data := make([]byte, rng.Intn(20)+1)
					_, _ = rng.Read(data)
					mutation.Value = api.Value{Kind: api.ValueBytes, Data: data}
				}
			}
			if rng.Intn(2) == 0 {
				if exists {
					mutation.Condition = api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: old.Kind, Data: bytes.Clone(old.Data)}}
				} else {
					mutation.Condition.Kind = api.ConditionAbsent
				}
			}
			request.Batch.Operations = append(request.Batch.Operations, mutation)
		}
		ack := integrationMutate(t, s.e, request)
		if ack.Fence.LSN != before.Fence.LSN+1 {
			t.Fatal("successful oracle batch did not publish exactly once")
		}
		for _, mutation := range request.Batch.Operations {
			if mutation.Kind == api.MutationDelete {
				delete(oracle[space], string(mutation.Key))
			} else {
				oracle[space][string(mutation.Key)] = api.Value{Kind: mutation.Value.Kind, Data: bytes.Clone(mutation.Value.Data)}
			}
		}
		roots[space] = ack.StateRoot
		receipts = append(receipts, receipt{request.Key, ack})
		if batch%6 == 5 {
			report := s.reopen()
			if report.ScannedTransactions != uint64(batch+1) {
				t.Fatalf("oracle replay transaction count = %d, want %d", report.ScannedTransactions, batch+1)
			}
			for _, prior := range receipts {
				lookup := integrationLookup(t, s.e, prior.key)
				if lookup.Code != api.CodeDurableAck || lookup.Result != prior.ack || lookup.ObservedFence != ack.Fence {
					t.Fatalf("oracle replay changed a durable receipt: %+v", lookup)
				}
			}
		}
		for _, checkSpace := range spaces {
			for _, key := range keys {
				want, found := oracle[checkSpace][string(key)]
				got := integrationRead(t, s.e, checkSpace, key, found, want)
				if got.StateRoot != roots[checkSpace] || got.RootSet != ack.RootSet || got.Fence != ack.Fence {
					t.Fatalf("oracle read returned mismatched publication at batch %d: %+v", batch, got)
				}
				// A caller modifying a read must not mutate committed value bytes.
				if len(got.Value.Data) != 0 {
					got.Value.Data[0] ^= 255
					integrationRead(t, s.e, checkSpace, key, found, want)
				}
			}
		}
	}
}
