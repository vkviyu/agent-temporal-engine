package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func knowledgeQueryPacketRequest(t *testing.T, e *Engine, id string, fence model.CommitFence) api.CommitRequest {
	t.Helper()
	p := knowledge.ContextPacket{Space: "coordinator-space", Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "coordinator-space", Fence: fence}}, Renderer: id, Tokenizer: "test/v1"}
	p.ID = knowledge.PacketID(p)
	b, err := knowledge.EncodeCommand(knowledge.Command{Packet: &p})
	if err != nil {
		t.Fatal(err)
	}
	return api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: model.RequestID(id), Method: api.MethodApplyKnowledge, Body: b}
}

func TestKnowledgePublicationUnknownDoesNotInstallPrivateCandidate(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			o := Options{Batch: BatchOptions{MaxCount: 1}}
			e, io := coordinatorStore(t, o)
			before, err := e.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			r := knowledgeQueryPacketRequest(t, e, "unknown-knowledge", before.Fence)
			gate := newCoordinatorGate()
			gate.after = after
			gate.err = errors.New("knowledge CURRENT response lost")
			io.arm("current", gate)
			pending := coordinatorCommit(e, context.Background(), r)
			coordinatorWait(t, gate.entered)
			if got := coordinatorLookup(t, e, r); got.Code != api.CodeInFlight {
				t.Fatalf("private receipt escaped: %+v", got)
			}
			gate.unblock()
			out := coordinatorWait(t, pending)
			if out.result.Code != api.CodeCommitUnknown || !errors.Is(out.err, storage.ErrCommitUnknown) {
				t.Fatalf("unknown=%+v %v", out.result, out.err)
			}
			e.mu.Lock()
			roots := e.roots
			e.mu.Unlock()
			if roots != before.Roots {
				t.Fatal("failed P1 installed knowledge candidate")
			}
			if got, err := e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "coordinator-space", Principal: "host"}); !errors.Is(err, storage.ErrStoreFenced) || !reflect.DeepEqual(got, api.KnowledgeStateResult{}) {
				t.Fatalf("fenced output=%+v %v", got, err)
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			o.Journal.TailPolicy = storage.TailQuarantine
			restored, _, err := OpenExisting(io, o)
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			got, err := restored.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "coordinator-space", Principal: "host"})
			if err != nil {
				t.Fatal(err)
			}
			if after {
				if !got.Found || len(got.Snapshot.Packets) != 1 {
					t.Fatalf("published state lost: %+v", got)
				}
				receipt := coordinatorLookup(t, restored, r)
				ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(restored, context.Background(), r)))
				if receipt.Code != api.CodeDurableAck || ack != receipt.Result {
					t.Fatalf("original receipt changed: %+v %+v", receipt, ack)
				}
			} else {
				if got.Found || got.Fence != before.Fence {
					t.Fatalf("unpublished state promoted: %+v", got)
				}
			}
		})
	}
}

func TestKnowledgeGroupCandidateSharesOnePublication(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 2, MaxDelay: time.Second}})
	before, _ := e.Snapshot(context.Background())
	_, _, beforeCurrents, _ := io.counts()
	a := knowledgeQueryPacketRequest(t, e, "query-packet-a", before.Fence)
	b := knowledgeQueryPacketRequest(t, e, "query-packet-b", before.Fence)
	first := coordinatorCommit(e, context.Background(), a)
	coordinatorFlightWaiters(t, e, a, 1)
	second := coordinatorCommit(e, context.Background(), b)
	ax := coordinatorAck(t, coordinatorWait(t, first))
	bx := coordinatorAck(t, coordinatorWait(t, second))
	if bx.Fence.LSN != ax.Fence.LSN+1 || ax.RootSet == bx.RootSet {
		t.Fatalf("transaction proofs mixed: %+v %+v", ax, bx)
	}
	got, err := e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil || len(got.Snapshot.Packets) != 2 {
		t.Fatalf("group lost update=%+v %v", got, err)
	}
	_, _, currents, _ := io.counts()
	if currents-beforeCurrents != 1 {
		t.Fatalf("group used multiple publications: %d", currents-beforeCurrents)
	}
}
