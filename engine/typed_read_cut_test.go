package engine

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// Gate the actual journal segment capability rather than the mutable object
// index source: a public read must retain its own physical reader.
type typedReadCutIO struct {
	storage.DurableIO
	armed atomic.Bool
	gate  *coordinatorGate
}

type typedReadCutFile struct {
	storage.DurableFile
	disk *typedReadCutIO
}

func (f *typedReadCutFile) ReadAt(p []byte, offset int64) (int, error) {
	if f.disk.armed.CompareAndSwap(true, false) {
		close(f.disk.gate.entered)
		<-f.disk.gate.release
	}
	return f.DurableFile.ReadAt(p, offset)
}

func (d *typedReadCutIO) wrap(name string, file storage.DurableFile) storage.DurableFile {
	if file != nil && strings.HasPrefix(name, "segments/SEGMENT.") {
		return &typedReadCutFile{DurableFile: file, disk: d}
	}
	return file
}

func (d *typedReadCutIO) CreateExclusive(name string) (storage.DurableFile, error) {
	file, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(name, file), err
}

func (d *typedReadCutIO) OpenAppend(name string) (storage.DurableFile, error) {
	file, err := d.DurableIO.OpenAppend(name)
	return d.wrap(name, file), err
}

func TestTypedReadPhysicalIOReleasesPublicationAndPreservesHistoricalCut(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native durability profile requires Darwin or Linux")
	}
	directory, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	disk := &typedReadCutIO{DurableIO: directory, gate: newCoordinatorGate()}
	e, err := Create(disk, Options{AgentProfile: true})
	if err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		disk.gate.unblock()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
		if err := directory.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	task := profileCoordinatorTask("typed-read-task")
	before := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "typed-read-create-task", agentprofile.Command{CreateTask: &task}))))
	request := api.ReadAgentStateRequest{Space: task.Space, Principal: task.Owner}
	disk.armed.Store(true)
	type outcome struct {
		value api.AgentStateResult
		err   error
	}
	result := make(chan outcome, 1)
	go func() {
		value, err := e.GetAgentState(ctx, request)
		result <- outcome{value, err}
	}()
	coordinatorWait(t, disk.gate.entered)
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	execution := agentprofile.Execution{ID: "typed-read-execution", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	after := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, writeCtx, profileCoordinatorRequest(t, "typed-read-spawn", agentprofile.Command{Spawn: &execution}))))
	if after.Fence.LSN != before.Fence.LSN+1 {
		t.Fatalf("concurrent writer did not publish: before=%+v after=%+v", before.Fence, after.Fence)
	}
	disk.gate.unblock()
	got := coordinatorWait(t, result)
	if !errors.Is(got.err, api.ErrReadAuthorizationChanged) || !reflect.DeepEqual(got.value, api.AgentStateResult{}) {
		t.Fatalf("obsolete typed read released a result: %+v %v", got.value, got.err)
	}
	anchor := model.SnapshotAnchor{Space: task.Space, Fence: before.Fence}
	request.Anchor = &anchor
	historical, err := e.GetAgentState(ctx, request)
	if err != nil || !historical.Found || historical.Fence != before.Fence || historical.RootSet != before.RootSet || len(historical.Snapshot.Executions) != 0 || len(historical.Snapshot.Tasks) != 1 || historical.Snapshot.Tasks[0] != task {
		t.Fatalf("typed historical read moved to the later publication: %+v %v", historical, err)
	}
}

func TestQueryProgressUnpublishedFenceDenialLeavesStoreReady(t *testing.T) {
	e, _ := coordinatorStore(t, Options{AgentProfile: true})
	ctx := context.Background()
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, "query-progress-fence-source", "key", "value", api.ValueCondition{}))))
	before, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"future", "digest", "store"} {
		t.Run(kind, func(t *testing.T) {
			anchor := model.SnapshotAnchor{Space: "coordinator-space", Fence: ack.Fence}
			switch kind {
			case "future":
				anchor.Fence.LSN++
			case "digest":
				anchor.Fence.CommitDigest = model.CommitDigest(strings.Repeat("e", 64))
			case "store":
				anchor.Fence.Store = "foreign-store"
			}
			got, err := e.GetQueryProgress(ctx, api.ReadQueryProgressRequest{Space: anchor.Space, Principal: "host", Query: "missing", Anchor: &anchor})
			if !errors.Is(err, model.ErrAnchorMismatch) || !reflect.DeepEqual(got, api.QueryProgressResult{}) {
				t.Fatalf("unpublished fence established query absence: %+v %v", got, err)
			}
			if after, err := e.Snapshot(ctx); err != nil || after != before {
				t.Fatalf("caller anchor rejection changed or fenced store: %+v %v", after, err)
			}
		})
	}
}
