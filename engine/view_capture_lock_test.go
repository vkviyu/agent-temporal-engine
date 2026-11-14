package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
)

// OpenView calls LockContext once to capture the publication and once more to
// record a transport capture. Pause before the second acquire without adding a
// scheduling hook to production code.
type viewCaptureLockContext struct {
	context.Context
	calls   atomic.Int32
	waiting chan struct{}
	resume  chan struct{}
}

func (c *viewCaptureLockContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 2 {
		close(c.waiting)
		<-c.resume
	}
	return c.Context.Done()
}

func TestOpenViewCaptureReleasesRegistryBeforeCoreLock(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	defer e.stopViews()
	gate := &viewCaptureLockContext{Context: context.Background(), waiting: make(chan struct{}), resume: make(chan struct{})}
	capture := &readDeliveryCapture{engine: e, binding: api.ReadDeliveryBinding{
		Method: "open_view", Destination: api.ReadDestination{Principal: "host"},
	}}
	ctx := context.WithValue(gate, readDeliveryKey{}, capture)
	type result struct {
		view api.ViewDescriptor
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		view, err := e.OpenView(ctx, api.OpenViewRequest{Space: "coordinator-space", Principal: "host"})
		finished <- result{view: view, err: err}
	}()
	coordinatorWait(t, gate.waiting)
	// An ordinary OpenView acquires these locks in this order. At the capture
	// boundary it must be able to acquire the registry while owning the core.
	e.mu.Lock()
	registryAvailable := e.views.mu.TryLock()
	if registryAvailable {
		e.views.mu.Unlock()
	}
	e.mu.Unlock()
	close(gate.resume)
	out := coordinatorWait(t, finished)
	if out.err != nil {
		t.Fatal(out.err)
	}
	if err := e.CloseView(context.Background(), api.CloseViewRequest{Ref: out.view.Ref}); err != nil {
		t.Fatal(err)
	}
	if !registryAvailable {
		t.Fatal("OpenView held the registry while waiting to acquire the core lock")
	}
}

func TestOpenViewFailedCaptureRetiresUnreturnedView(t *testing.T) {
	for _, failure := range []string{"canceled-lock", "binding-mismatch"} {
		t.Run(failure, func(t *testing.T) {
			e, _ := coordinatorStore(t, Options{})
			defer e.stopViews()
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			gate := &viewCaptureLockContext{Context: base, waiting: make(chan struct{}), resume: make(chan struct{})}
			capture := &readDeliveryCapture{engine: e, binding: api.ReadDeliveryBinding{
				Method: "open_view", Destination: api.ReadDestination{Principal: "host"},
			}}
			if failure == "binding-mismatch" {
				capture.binding.Method = "get_state"
			}
			ctx := context.WithValue(gate, readDeliveryKey{}, capture)
			finished := make(chan error, 1)
			go func() {
				view, err := e.OpenView(ctx, api.OpenViewRequest{Space: "coordinator-space", Principal: "host"})
				if view.Ref != "" {
					finished <- errors.New("failed capture returned a view reference")
					return
				}
				finished <- err
			}()
			coordinatorWait(t, gate.waiting)
			want := api.ErrReadEmissionDenied
			if failure == "canceled-lock" {
				cancel()
				want = context.Canceled
			}
			close(gate.resume)
			if err := coordinatorWait(t, finished); !errors.Is(err, want) {
				t.Fatalf("capture failure: got %v, want %v", err, want)
			}
			e.views.mu.Lock()
			entries, pending := len(e.views.entries), e.views.pending
			e.views.mu.Unlock()
			if entries != 0 || pending != 0 {
				t.Fatalf("failed capture retained an unreachable view: entries=%d pending=%d", entries, pending)
			}
		})
	}
}
