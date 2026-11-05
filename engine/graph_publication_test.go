package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func graphCoordinatorRequest(t *testing.T, id string, c graph.Command) api.CommitRequest {
	t.Helper()
	b, err := graph.EncodeCommand("coordinator-space", c)
	if err != nil {
		t.Fatal(err)
	}
	return api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: model.RequestID(id), Method: api.MethodApplyGraph, Body: b}
}

func TestGraphPublicationUnknownDoesNotInstallBranchCandidate(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			options := Options{Batch: BatchOptions{MaxCount: 1}}
			e, disk := coordinatorStore(t, options)
			r, _ := graph.BuildRevision("coordinator-space", "", nil, "", nil, nil)
			base := graphCoordinatorRequest(t, "graph-fault-revision", graph.Command{AddRevision: &graph.AddRevisionCommand{}})
			baseACK := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), base)))
			before, err := e.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			objects := e.index.count()
			req := graphCoordinatorRequest(t, "graph-fault-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r.ID}})
			gate := newCoordinatorGate()
			gate.err = errors.New("graph CURRENT publication unavailable")
			gate.after = after
			disk.arm("current", gate)
			pending := coordinatorCommit(e, context.Background(), req)
			coordinatorWait(t, gate.entered)
			gate.unblock()
			unknown := coordinatorWait(t, pending)
			if unknown.result.Code != api.CodeCommitUnknown || !errors.Is(unknown.err, storage.ErrCommitUnknown) {
				t.Fatalf("not unknown: %+v %v", unknown.result, unknown.err)
			}
			e.mu.Lock()
			unchanged := e.roots == before.Roots && e.rootSet == before.RootSet && e.index.count() == objects
			e.mu.Unlock()
			if !unchanged {
				t.Fatal("unpublished graph candidate installed")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			options.Journal.TailPolicy = storage.TailQuarantine
			opened, _, err := OpenExisting(disk, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = opened.Close() })
			lookup := coordinatorLookup(t, opened, req)
			if after {
				if lookup.Code != api.CodeDurableAck {
					t.Fatalf("lost published graph receipt: %+v", lookup)
				}
				again := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(opened, context.Background(), req)))
				if again != lookup.Result {
					t.Fatal("retry changed fork receipt")
				}
			} else if lookup.Code != api.CodeNotFoundAtFence || lookup.ObservedFence != baseACK.Fence {
				t.Fatalf("promoted unpublished graph state: %+v", lookup)
			}
		})
	}
}

func TestGraphGroupUsesPrivatePredecessorAndSinglePublisher(t *testing.T) {
	e, disk := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 2, MaxDelay: time.Second}})
	r, _ := graph.BuildRevision("coordinator-space", "", nil, "", nil, nil)
	first := graphCoordinatorRequest(t, "graph-group-revision", graph.Command{AddRevision: &graph.AddRevisionCommand{}})
	second := graphCoordinatorRequest(t, "graph-group-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: r.ID}})
	gate := newCoordinatorGate()
	disk.arm("current", gate)
	_, _, before, _ := disk.counts()
	one, two, oneFlight, twoFlight := speculativePair(t, e, first, second)
	coordinatorWait(t, gate.entered)
	speculativePending(t, oneFlight)
	speculativePending(t, twoFlight)
	gate.unblock()
	a := coordinatorAck(t, coordinatorWait(t, one))
	b := coordinatorAck(t, coordinatorWait(t, two))
	_, _, after, _ := disk.counts()
	if after-before != 1 || a.Fence.LSN+1 != b.Fence.LSN {
		t.Fatalf("graph group publication split: %d %+v %+v", after-before, a, b)
	}
	if lookup := coordinatorLookup(t, e, first); lookup.Code != api.CodeDurableAck || lookup.Result != a {
		t.Fatalf("first graph transaction lost original proof: %+v", lookup)
	}
}
