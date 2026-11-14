package engine

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type publishedDirectoryGate struct {
	storage.DurableIO
	armed atomic.Bool
	gate  *coordinatorGate
}

type planningObjectGate struct {
	io.ReaderAt
	armed atomic.Bool
	gate  *coordinatorGate
}

func (r *planningObjectGate) ReadAt(data []byte, offset int64) (int, error) {
	if r.armed.CompareAndSwap(true, false) {
		close(r.gate.entered)
		<-r.gate.release
	}
	return r.ReaderAt.ReadAt(data, offset)
}

func TestCoordinatorPlanningIOReleasesCoreAndHonorsConcurrentFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(map[bool]string{false: "read-old-cut", true: "fence"}[fenced], func(t *testing.T) {
			e, disk := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
			first := coordinatorRequest(t, "planning-before", "value", "old", api.ValueCondition{})
			coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), first)))
			before, err := e.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			gate := newCoordinatorGate()
			reader := &planningObjectGate{ReaderAt: e.index.source, gate: gate}
			reader.armed.Store(true)
			e.index.source = reader
			disk.mu.Lock()
			disk.gates = append(disk.gates, gate)
			disk.mu.Unlock()
			_, _, currentsBefore, _ := disk.counts()
			pending := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "planning-after", "value", "new", api.ValueCondition{}))
			coordinatorWait(t, gate.entered)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if got, err := e.Snapshot(ctx); err != nil || got != before {
				t.Fatalf("planning IO blocked or exposed its candidate: %+v %v", got, err)
			}
			if got, err := e.GetState(ctx, api.ReadStateRequest{Space: "coordinator-space", Key: []byte("value")}); err != nil || !got.Found || string(got.Value.Data) != "old" {
				t.Fatalf("planning IO blocked the published input cut: %+v %v", got, err)
			}
			if fenced {
				e.mu.Lock()
				err = e.readFailure(errors.New("trusted semantic verification failure"))
				e.mu.Unlock()
				if !errors.Is(err, storage.ErrStoreFenced) {
					t.Fatal(err)
				}
			}
			gate.unblock()
			out := coordinatorWait(t, pending)
			if fenced {
				if out.result.Code != api.CodeStoreFenced || !errors.Is(out.err, storage.ErrStoreFenced) {
					t.Fatalf("concurrent semantic fence permitted publication: %+v %v", out.result, out.err)
				}
				_, _, currentsAfter, _ := disk.counts()
				if currentsAfter != currentsBefore || e.journal.Status().LastPublication.Fence() != before.Fence {
					t.Fatal("fenced candidate reached CURRENT")
				}
			} else {
				coordinatorAck(t, out)
			}
		})
	}
}

func (d *publishedDirectoryGate) SyncDir(name string) error {
	if err := d.DurableIO.SyncDir(name); err != nil {
		return err
	}
	if name == "." && d.armed.CompareAndSwap(true, false) {
		// CURRENT and its directory have already synchronized. The journal
		// publisher is still inside P1 and has not returned to the coordinator.
		close(d.gate.entered)
		<-d.gate.release
		return d.gate.err
	}
	return nil
}

func armPublishedDirectory(t *testing.T, io *coordinatorIO, cause error) *coordinatorGate {
	t.Helper()
	gate := newCoordinatorGate()
	gate.err = cause
	wrapped := &publishedDirectoryGate{DurableIO: io.DurableIO, gate: gate}
	wrapped.armed.Store(true)
	io.DurableIO = wrapped
	io.mu.Lock()
	io.gates = append(io.gates, gate)
	io.mu.Unlock()
	return gate
}

func TestCoordinatorP1ReleasesCoreAndClosesOldPolicyOutput(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "ack", true: "unknown"}[unknown], func(t *testing.T) {
			e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
			first := coordinatorRequest(t, "initial-policy", "value", "old", api.ValueCondition{})
			coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), first)))
			before, err := e.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var cause error
			if unknown {
				cause = errors.New("directory sync response lost")
			}
			gate := armPublishedDirectory(t, io, cause)
			next := coordinatorRequest(t, "new-policy", "value", "new", api.ValueCondition{})
			pending := coordinatorCommit(e, context.Background(), next)
			coordinatorWait(t, gate.entered)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := e.mu.LockContext(ctx); err != nil {
				t.Fatalf("P1 retained the core lock: %v", err)
			}
			roots, rootSet := e.roots, e.rootSet
			e.mu.Unlock()
			if roots != before.Roots || rootSet != before.RootSet {
				t.Fatal("private candidate became public before P1 returned")
			}
			if got, err := e.Snapshot(ctx); !errors.Is(err, api.ErrReadAuthorizationChanged) || got != (api.StateSnapshot{}) {
				t.Fatalf("pending publication returned a stale snapshot: %+v %v", got, err)
			}
			if got, err := e.GetState(ctx, api.ReadStateRequest{Space: "coordinator-space", Key: []byte("value")}); !errors.Is(err, api.ErrReadAuthorizationChanged) || got.Found {
				t.Fatalf("pending publication disclosed old-policy content: %+v %v", got, err)
			}
			missing, err := e.LookupRequest(ctx, api.LookupRequestRequest{Space: "coordinator-space", Principal: "host", RequestID: "missing"})
			if !errors.Is(err, api.ErrReadAuthorizationChanged) || missing.Code != api.CodeIndeterminate || missing.ObservedFence != (model.CommitFence{}) {
				t.Fatalf("pending publication asserted absence: %+v %v", missing, err)
			}
			if err := e.viewOutputGate(); !errors.Is(err, api.ErrReadAuthorizationChanged) {
				t.Fatalf("View output accepted an unresolved durable publication: %v", err)
			}
			gate.unblock()
			out := coordinatorWait(t, pending)
			if unknown {
				if out.result.Code != api.CodeCommitUnknown || !errors.Is(out.err, storage.ErrCommitUnknown) {
					t.Fatalf("uncertain publication returned an ACK: %+v %v", out.result, out.err)
				}
				if _, err := e.Snapshot(ctx); !errors.Is(err, storage.ErrStoreFenced) {
					t.Fatalf("unknown outcome did not fence output: %v", err)
				}
			} else {
				ack := coordinatorAck(t, out)
				if got, err := e.GetState(ctx, api.ReadStateRequest{Space: "coordinator-space", Key: []byte("value")}); err != nil || !got.Found || string(got.Value.Data) != "new" || got.Fence != ack.Fence {
					t.Fatalf("successful publication did not install its matching cut: %+v %v", got, err)
				}
			}
		})
	}
}

func TestCoordinatorCompletionDoesNotNestAdmissionAndCoreLocks(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	gate := armPublishedDirectory(t, io, nil)
	pending := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "lock-order", "value", "new", api.ValueCondition{}))
	coordinatorWait(t, gate.entered)
	func() {
		e.admission.mu.Lock()
		defer e.admission.mu.Unlock()
		gate.unblock()
		deadline := time.Now().Add(time.Second)
		for e.publicationPending.Load() && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		if e.publicationPending.Load() {
			t.Fatal("published roots could not install while admission was busy")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := e.mu.LockContext(ctx); err != nil {
			t.Fatalf("completion held the core while waiting for admission: %v", err)
		}
		e.mu.Unlock()
	}()
	coordinatorAck(t, coordinatorWait(t, pending))
}

func TestWriterGateSerializesMaintenanceAndCloseWithP1(t *testing.T) {
	e, io := coordinatorStore(t, Options{Batch: BatchOptions{MaxCount: 1}})
	gate := armPublishedDirectory(t, io, nil)
	pending := coordinatorCommit(e, context.Background(), coordinatorRequest(t, "close-during-p1", "value", "new", api.ValueCondition{}))
	coordinatorWait(t, gate.entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.CompactAuditFull(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance bypassed the writer gate: %v", err)
	}
	if _, _, err := e.ReclaimRetired(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reclaim bypassed the writer gate: %v", err)
	}
	if err := e.CloseContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("close did not preserve its canceled wait: %v", err)
	}
	if _, _, _, closes := io.counts(); closes != 0 {
		t.Fatal("Close released ownership while P1 was unresolved")
	}
	gate.unblock()
	coordinatorAck(t, coordinatorWait(t, pending))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, closes := io.counts(); closes != 1 {
		t.Fatalf("owner did not retire exactly once: %d", closes)
	}
}
