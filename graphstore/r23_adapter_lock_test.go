package graphstore

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type r23StateGate struct {
	entries     []graph.Entry
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (r *r23StateGate) ReadEntries(model.SpaceID, model.ObjectID) ([]graph.Entry, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return append([]graph.Entry(nil), r.entries...), nil
}

func TestR23AddRevisionHoldsAdapterLockAcrossStateReader(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("adapter lock baseline requires native directory IO")
	}
	d, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	entries := []graph.Entry{{Key: []byte("r23-key"), Value: gid('1')}}
	reader := &r23StateGate{
		entries: entries,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	s, err := CreateVerified(d, storage.JournalOptions{}, reader)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.releaseOnce.Do(func() { close(reader.release) })
		_ = s.Close()
	}()

	addDone := make(chan error, 1)
	go func() {
		_, _, addErr := s.AddRevision(
			context.Background(),
			"r23-space",
			"",
			nil,
			gid('a'),
			nil,
			entries,
		)
		addDone <- addErr
	}()

	select {
	case <-reader.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("AddRevision did not reach the gated StateReader")
	}

	getDone := make(chan error, 1)
	go func() {
		_, getErr := s.Get(model.RevisionID("missing"))
		getDone <- getErr
	}()
	select {
	case err := <-getDone:
		t.Fatalf("Get bypassed adapter lock during StateReader IO: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	reader.releaseOnce.Do(func() { close(reader.release) })
	select {
	case err := <-addDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AddRevision did not finish after StateReader release")
	}
	select {
	case err := <-getDone:
		if err == nil {
			t.Fatal("Get unexpectedly found a missing revision")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get remained blocked after StateReader completed")
	}
}
