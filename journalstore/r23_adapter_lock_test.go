package journalstore

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type r23SyncGateIO struct {
	storage.DurableIO
	entered     chan struct{}
	release     chan struct{}
	enabled     atomic.Bool
	fired       atomic.Bool
	releaseOnce sync.Once
}

func (d *r23SyncGateIO) SyncDir(name string) error {
	if err := d.DurableIO.SyncDir(name); err != nil {
		return err
	}
	if name == "." && d.enabled.Load() && d.fired.CompareAndSwap(false, true) {
		close(d.entered)
		<-d.release
	}
	return nil
}

func TestR23AppendHoldsAdapterLockAcrossPublicationIO(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("adapter lock baseline requires native directory IO")
	}
	d, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	gate := &r23SyncGateIO{
		DurableIO: d,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	s, err := Create(gate, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.enabled.Store(true)
	defer func() {
		gate.releaseOnce.Do(func() { close(gate.release) })
		_ = s.Close()
	}()

	key := model.RequestKey{Space: "r23-space", Principal: "r23-principal", RequestID: "r23-append"}
	appendDone := make(chan error, 1)
	go func() {
		_, appendErr := s.Append(context.Background(), Record{
			Kind:    storage.KindEvent,
			Payload: []byte("r23-payload"),
			Request: key,
		})
		appendDone <- appendErr
	}()

	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("append did not reach the gated publication boundary")
	}

	lookupDone := make(chan error, 1)
	go func() {
		_, found, lookupErr := s.Lookup(key)
		if lookupErr == nil && !found {
			lookupErr = storage.ErrCorruption
		}
		lookupDone <- lookupErr
	}()
	select {
	case err := <-lookupDone:
		t.Fatalf("Lookup bypassed adapter lock during publication IO: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	gate.releaseOnce.Do(func() { close(gate.release) })
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("append did not finish after publication gate release")
	}
	select {
	case err := <-lookupDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Lookup remained blocked after publication completed")
	}
}
