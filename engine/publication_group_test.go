package engine

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestSourcePublicationResolvesItsOwnMemberWithinOnePhysicalGroup(t *testing.T) {
	ctx := context.Background()
	e, d := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 8}})
	gate := newCoordinatorGate()
	d.arm("current", gate)
	base := coordinatorCommit(e, ctx, coordinatorRequest(t, "source-base", "seed", "base", api.ValueCondition{}))
	coordinatorWait(t, gate.entered)
	oneReq := coordinatorRequest(t, "source-one", "one", "one", api.ValueCondition{})
	twoReq := coordinatorRequest(t, "source-two", "two", "two", api.ValueCondition{})
	onePending := coordinatorCommit(e, ctx, oneReq)
	twoPending := coordinatorCommit(e, ctx, twoReq)
	coordinatorQueued(t, e, oneReq, twoReq)
	gate.unblock()
	coordinatorAck(t, coordinatorWait(t, base))
	one := coordinatorAck(t, coordinatorWait(t, onePending))
	two := coordinatorAck(t, coordinatorWait(t, twoPending))
	if one.Fence.LSN == two.Fence.LSN || one.RootSet == two.RootSet {
		t.Fatal("group did not create distinct logical source cuts")
	}
	for _, ack := range []api.CommitResult{one, two} {
		ref := model.SourcePublicationRef{Store: ack.Fence.Store, Space: oneReq.Space, ProducerTx: ack.TxID}
		result, err := e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: oneReq.Principal, Source: ref})
		if err != nil || result.Anchor.Fence != ack.Fence || result.RootSet != ack.RootSet {
			t.Fatalf("member substituted group tip: %+v %v", result, err)
		}
	}
}
