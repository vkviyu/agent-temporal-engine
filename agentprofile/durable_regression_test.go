package agentprofile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/journalstore"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func durableNative(t *testing.T) (string, *storage.DirectoryIO) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native profile requires Darwin/Linux")
	}
	path := t.TempDir()
	d, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, d
}
func durableKey(id string) model.RequestKey {
	return model.RequestKey{Space: "space", Principal: "owner", RequestID: model.RequestID(id)}
}
func durableTask() CreateTask {
	return CreateTask{Task: Task{ID: "task", Space: "space", Owner: "owner", Goal: "original", GoalEpoch: 1, Status: TaskOpen}, Request: durableKey("create")}
}
func durableExecution(task model.EntityID) SpawnRequest {
	return SpawnRequest{Execution: Execution{ID: "execution", Task: task, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: ExecutionReady}, Request: durableKey("spawn")}
}
func requireEqual[T any](t *testing.T, got T, err error, want T) {
	t.Helper()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v error=%v, want %+v", got, err, want)
	}
}

func TestDurableTypedCommandsReopenAndReturnOriginalResults(t *testing.T) {
	path, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	create := durableTask()
	originalTask, err := s.CreateTask(context.Background(), create)
	if err != nil {
		t.Fatal(err)
	}
	ack, found, err := s.store.Lookup(create.Request)
	if err != nil || !found {
		t.Fatal(err)
	}
	anchor := ack.Publication.Fence()
	spawn := durableExecution(create.Task.ID)
	originalExecution, err := s.Spawn(context.Background(), spawn)
	if err != nil {
		t.Fatal(err)
	}
	step1 := StepRequest{Step: Step{Execution: spawn.Execution.ID, Sequence: 1, GoalEpoch: 1, Status: StepOpen, InputAnchor: anchor}, Request: durableKey("step-1")}
	originalStep, err := s.StartStep(context.Background(), step1)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := s.CompleteStep(context.Background(), spawn.Execution.ID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	step2 := StepRequest{Step: Step{Execution: spawn.Execution.ID, Sequence: 2, GoalEpoch: 1, Status: StepOpen, InputAnchor: anchor}, Request: durableKey("step-2")}
	if _, err = s.StartStep(context.Background(), step2); err != nil {
		t.Fatal(err)
	}
	invocation := InvocationRequest{Invocation: Invocation{ID: "invocation", Execution: spawn.Execution.ID, Step: 2, ProviderCallKey: "call", Tool: "grep", Args: []byte("original arguments"), Status: InvocationPending, Attempt: 1, AcceptedResult: "result", ToolDefinition: digest('d'), ToolVersion: "grep/v1", DispatchClass: "pure"}, Request: durableKey("invocation")}
	if _, err := s.RecordInvocation(context.Background(), invocation); !errors.Is(err, ErrConflict) {
		t.Fatalf("standalone adapter admitted an invocation without an accepted primary: %v", err)
	}
	pending := PendingAction{Key: "render", Generation: 1, Request: durableKey("pending"), CommandDigest: model.ObjectID(strings.Repeat("a", 64)), InputFence: anchor, Owner: spawn.Execution.ID, Status: ActionPending}
	originalPending, err := s.PutPending(context.Background(), pending)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := s.RebindPending(context.Background(), pending.Key, 1, durableKey("rebind"))
	if err != nil {
		t.Fatal(err)
	}
	rebase := GoalRebase{Task: create.Task.ID, NewGoal: "revised", ExpectedEpoch: 1}
	rebased, err := s.RebaseGoal(context.Background(), rebase)
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := s.CancelTask(context.Background(), create.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != TaskCanceled {
		t.Fatal("cancel didn't apply")
	}
	verifyReceipts := func(t *testing.T, s *DurableLedger) {
		t.Helper()
		got, e := s.CreateTask(context.Background(), create)
		requireEqual(t, got, e, originalTask)
		execution, e := s.Spawn(context.Background(), spawn)
		requireEqual(t, execution, e, originalExecution)
		step, e := s.StartStep(context.Background(), step1)
		requireEqual(t, step, e, originalStep)
		step, e = s.CompleteStep(context.Background(), spawn.Execution.ID, 1, 1)
		requireEqual(t, step, e, completed)
		if _, e := s.RecordInvocation(context.Background(), invocation); !errors.Is(e, ErrCanceled) {
			t.Fatalf("canceled standalone adapter admitted an invocation: %v", e)
		}
		action, e := s.PutPending(context.Background(), pending)
		requireEqual(t, action, e, originalPending)
		action, e = s.RebindPending(context.Background(), pending.Key, 1, durableKey("rebind"))
		requireEqual(t, action, e, rebound)
		task, e := s.RebaseGoal(context.Background(), rebase)
		requireEqual(t, task, e, rebased)
		task, e = s.CancelTask(context.Background(), create.Task.ID)
		requireEqual(t, task, e, canceled)
		current, e := s.GetTask(create.Task.ID)
		requireEqual(t, current, e, canceled)
	}
	verifyReceipts(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	reopened, report, err := OpenDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if report.ScannedTransactions != 9 {
		t.Fatalf("retries wrote extra transactions: %+v", report)
	}
	verifyReceipts(t, reopened)
	records, err := reopened.store.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Request.Space != create.Task.Space || record.Request.Principal != create.Task.Owner {
			t.Fatalf("wrong synthesized scope: %+v", record.Request)
		}
		if _, err = decodeDurableCommand(record.Payload); err != nil {
			t.Fatal(err)
		}
	}
}

var errDurableInjected = errors.New("injected durable IO failure")

type durableFaultIO struct {
	storage.DurableIO
	mutations    atomic.Int64
	segmentSync  atomic.Bool
	rootSync     atomic.Bool
	segmentClose atomic.Bool
}

func (d *durableFaultIO) CreateExclusive(name string) (storage.DurableFile, error) {
	d.mutations.Add(1)
	f, err := d.DurableIO.CreateExclusive(name)
	if f != nil {
		f = &durableFaultFile{DurableFile: f, name: name, owner: d}
	}
	return f, err
}
func (d *durableFaultIO) OpenAppend(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.OpenAppend(name)
	if f != nil {
		f = &durableFaultFile{DurableFile: f, name: name, owner: d}
	}
	return f, err
}
func (d *durableFaultIO) Mkdir(name string) error { d.mutations.Add(1); return d.DurableIO.Mkdir(name) }
func (d *durableFaultIO) SyncDir(name string) error {
	d.mutations.Add(1)
	err := d.DurableIO.SyncDir(name)
	if err == nil && name == "." && d.rootSync.Swap(false) {
		return errDurableInjected
	}
	return err
}
func (d *durableFaultIO) InstallNew(from, to string) error {
	d.mutations.Add(1)
	return d.DurableIO.InstallNew(from, to)
}
func (d *durableFaultIO) ReplaceAtomic(from, to string) error {
	d.mutations.Add(1)
	return d.DurableIO.ReplaceAtomic(from, to)
}
func (d *durableFaultIO) Remove(name string) error {
	d.mutations.Add(1)
	return d.DurableIO.Remove(name)
}

type durableFaultFile struct {
	storage.DurableFile
	name  string
	owner *durableFaultIO
}

func (f *durableFaultFile) Write(p []byte) (int, error) {
	f.owner.mutations.Add(1)
	return f.DurableFile.Write(p)
}
func (f *durableFaultFile) Truncate(n int64) error {
	f.owner.mutations.Add(1)
	return f.DurableFile.Truncate(n)
}
func (f *durableFaultFile) Sync() error {
	f.owner.mutations.Add(1)
	if strings.HasPrefix(f.name, "segments/") && f.owner.segmentSync.Swap(false) {
		return errDurableInjected
	}
	return f.DurableFile.Sync()
}
func (f *durableFaultFile) Close() error {
	if strings.HasPrefix(f.name, "segments/") && f.owner.segmentClose.Swap(false) {
		return errDurableInjected
	}
	return f.DurableFile.Close()
}

func TestDurableScopeAndIdempotencyRejectionsHaveNoIO(t *testing.T) {
	_, d := durableNative(t)
	defer d.Close()
	io := &durableFaultIO{DurableIO: d}
	s, err := CreateDurable(io, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	create := durableTask()
	original, err := s.CreateTask(context.Background(), create)
	if err != nil {
		t.Fatal(err)
	}
	before := io.mutations.Load()
	bad := durableExecution(create.Task.ID)
	bad.Request.Principal = "other"
	if _, err = s.Spawn(context.Background(), bad); !errors.Is(err, ErrDurableScope) {
		t.Fatalf("unauthorized request: %v", err)
	}
	changed := create
	changed.Task.Goal = "different command"
	if _, err = s.CreateTask(context.Background(), changed); !errors.Is(err, journalstore.ErrIdempotencyConflict) {
		t.Fatalf("changed key body accepted: %v", err)
	}
	if io.mutations.Load() != before {
		t.Fatal("rejected request performed persistent IO")
	}
	got, err := s.GetTask(original.ID)
	requireEqual(t, got, err, original)
	badCreate := create
	badCreate.Request = durableKey("second-create")
	badCreate.Request.Space = "other-space"
	if _, err = s.CreateTask(context.Background(), badCreate); !errors.Is(err, ErrDurableScope) {
		t.Fatal(err)
	}
	if io.mutations.Load() != before {
		t.Fatal("wrong scope consumed write quota")
	}
}
func TestDurableUnknownFencesProjectionAndReopenResolvesOriginalKey(t *testing.T) {
	for _, phase := range []string{"segment-sync", "current-directory-sync"} {
		t.Run(phase, func(t *testing.T) {
			path, d := durableNative(t)
			io := &durableFaultIO{DurableIO: d}
			s, err := CreateDurable(io, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			create := durableTask()
			task, err := s.CreateTask(context.Background(), create)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "segment-sync" {
				io.segmentSync.Store(true)
			} else {
				io.rootSync.Store(true)
			}
			if result, err := s.CancelTask(context.Background(), task.ID); !errors.Is(err, storage.ErrCommitUnknown) || result != (Task{}) {
				t.Fatalf("unknown leaked result %+v %v", result, err)
			}
			if got, err := s.GetTask(task.ID); !errors.Is(err, storage.ErrStoreFenced) || got != (Task{}) {
				t.Fatalf("fenced read leaked prior task %+v %v", got, err)
			}
			if got, err := s.CreateTask(context.Background(), create); !errors.Is(err, storage.ErrStoreFenced) || got != (Task{}) {
				t.Fatalf("fenced retry returned old receipt %+v %v", got, err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = storage.OpenDirectory(path)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			reopened, _, err := OpenDurable(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.GetTask(task.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := TaskOpen
			if phase == "current-directory-sync" {
				want = TaskCanceled
			}
			if got.Status != want {
				t.Fatalf("recovered %s, want %s", got.Status, want)
			}
			prior, err := reopened.CreateTask(context.Background(), create)
			requireEqual(t, prior, err, task)
		})
	}
}
func TestDurableCloseFailureIsStickyAndRetryRetainsOwner(t *testing.T) {
	path, d := durableNative(t)
	defer d.Close()
	io := &durableFaultIO{DurableIO: d}
	s, err := CreateDurable(io, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created, err := s.CreateTask(context.Background(), durableTask())
	if err != nil {
		t.Fatal(err)
	}
	io.segmentClose.Store(true)
	if err = s.Close(); !errors.Is(err, errDurableInjected) {
		t.Fatal(err)
	}
	if got, err := s.GetTask(created.ID); !errors.Is(err, journalstore.ErrClosing) || got != (Task{}) {
		t.Fatalf("Closing returned state: %+v %v", got, err)
	}
	other, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	lease, err := other.AcquireOwnerLock()
	if !errors.Is(err, storage.ErrOwnerBusy) {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatalf("owner released before file retired: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err = other.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	if got, err := s.GetTask(created.ID); !errors.Is(err, journalstore.ErrClosed) || got != (Task{}) {
		t.Fatalf("Closed returned state: %+v %v", got, err)
	}
}
func TestDurableSemanticFailurePrecedesTailRepair(t *testing.T) {
	path, d := durableNative(t)
	raw, err := encodeDurableCommand(durableCommand{operation: durableCreateTask, task: durableTask().Task})
	if err != nil {
		t.Fatal(err)
	}
	// The lower layer can authenticate this record physically, but it cannot
	// decide the profile's owner/space condition. OpenDurable must reject it before
	// any requested tail quarantine or witness sync.
	lower, err := journalstore.Create(d, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := durableKey("bad-owner")
	key.Principal = "intruder"
	if _, err = lower.Append(context.Background(), journalstore.Record{Kind: durableRecordKind, Payload: raw, Request: key}); err != nil {
		t.Fatal(err)
	}
	if err = lower.Close(); err != nil {
		t.Fatal(err)
	}
	names, err := os.ReadDir(filepath.Join(path, "segments"))
	if err != nil || len(names) != 1 {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(path, "segments", names[0].Name())
	tail := []byte("diagnostic unpublished tail")
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(tail); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	io := &durableFaultIO{DurableIO: d}
	opened, report, err := OpenDurable(io, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
	if opened != nil || !errors.Is(err, ErrDurableScope) || report.Verified || report.PublicationSynced {
		t.Fatalf("bad semantic replay escaped: %+v %v", report, err)
	}
	if io.mutations.Load() != 0 {
		t.Fatal("semantic rejection modified data or synced witness")
	}
	after, err := os.ReadFile(segmentPath)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatal("rejected replay changed tail")
	}
	lease, err := d.AcquireOwnerLock()
	if err != nil {
		t.Fatalf("failed open leaked owner: %v", err)
	}
	_ = lease.Close()
	_ = d.Close()
}
func TestDurableCommandDecoderRejectsSnapshotUnknownAndNoncanonicalBytes(t *testing.T) {
	command := durableCommand{operation: durableCancelTask, taskID: "task"}
	raw, err := encodeDurableCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		raw  []byte
	}{{"snapshot", []byte{1, 0}}, {"trailing", append(bytes.Clone(raw), 0)}, {"oversized", make([]byte, MaxRecordBytes+1)}} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeDurableCommand(test.raw); err == nil {
				t.Fatal("accepted invalid command")
			}
		})
	}
	unknown := append([]byte(nil), durableMagic...)
	unknown = codec.AppendUint(unknown, durableSchema)
	unknown = codec.AppendUint(unknown, 999)
	unknown, _ = codec.AppendBytes(unknown, nil)
	if _, err = decodeDurableCommand(unknown); !errors.Is(err, storage.ErrUnsupported) {
		t.Fatal(err)
	}
}
func TestDurableConcurrentRetriesShareOneOriginalResult(t *testing.T) {
	_, d := durableNative(t)
	defer d.Close()
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const n = 16
	var wg sync.WaitGroup
	results := make(chan Task, n)
	failures := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := s.CreateTask(context.Background(), durableTask())
			results <- task
			failures <- err
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	for task := range results {
		if task != durableTask().Task {
			t.Fatal(task)
		}
	}
	records, err := s.store.ReadRecords()
	if err != nil || len(records) != 1 {
		t.Fatalf("duplicate writes: %d %v", len(records), err)
	}
}
