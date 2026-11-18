package engine

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

type traceProfileCountingReader struct {
	source state.ObjectReader
	reads  map[model.ObjectID]int
	deny   model.ObjectID
}

func (r *traceProfileCountingReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	r.reads[id]++
	if id == r.deny {
		return nil, errors.New("unexpected warm profile object read")
	}
	return r.source.ReadObject(ctx, id)
}

func TestTaskTraceProfileHistoricalReadsReuseObject(t *testing.T) {
	ctx := context.Background()
	e, _ := coordinatorStore(t, Options{AgentProfile: true, EventProfile: true, Batch: BatchOptions{MaxCount: 1}})
	task := profileCoordinatorTask("trace-profile-task")
	first := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx,
		profileCoordinatorRequest(t, "trace-profile-create", agentprofile.Command{CreateTask: &task}))))
	anchor := model.SnapshotAnchor{Space: task.Space, Fence: first.Fence}
	req := api.TaskTraceRequest{Space: task.Space, Principal: task.Owner, Subject: api.TraceSubject{Kind: "task", ID: string(task.ID)}, Anchor: &anchor}
	id, found, err := state.Get(ctx, e.index, systemSpace, e.roots.LiveControl, agentPartitionKey(task.Space, task.Owner))
	if err != nil || !found {
		t.Fatalf("profile ID: %v %v", found, err)
	}
	read := func(req api.TaskTraceRequest, deny model.ObjectID) (taskTraceRead, *traceProfileCountingReader, error) {
		t.Helper()
		scoped, release, err := e.beginReadCut(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		cut := e.readCut(scoped)
		counter := &traceProfileCountingReader{source: cut.reader, reads: map[model.ObjectID]int{}, deny: deny}
		cut.reader = counter
		result, err := e.readTaskTrace(scoped, req)
		return result, counter, err
	}
	cold, counter, err := read(req, "")
	if err != nil || counter.reads[id] != 1 {
		t.Fatalf("cold profile object reads=%d err=%v", counter.reads[id], err)
	}
	cacheKey := taskTraceProfileKey{task.Space, task.Owner, id}
	cached, ok := e.traceProfiles.get(cacheKey)
	if !ok {
		t.Fatal("cold profile not retained")
	}
	// An unrelated domain commit changes the fence/roots but not the profile.
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx,
		coordinatorRequest(t, "trace-profile-domain", "unrelated", "value", api.ValueCondition{}))))
	latestReq := req
	latestReq.Anchor = nil
	latest, counter, err := read(latestReq, id)
	if err != nil || counter.reads[id] != 0 || counter.reads[cold.entry.roots.LiveControl] == 0 {
		t.Fatalf("warm read skipped historical partition or read profile: reads=%v err=%v", counter.reads, err)
	}
	if latest.provenance.roots != latest.entry.roots || latest.provenance.roots == cold.provenance.roots ||
		cached.provenance.roots != (model.RootSet{}) || len(e.traceProfiles.entries) != 1 {
		t.Fatal("same profile object retained stale roots or missed cache")
	}
	old, counter, err := read(req, id)
	if err != nil || !reflect.DeepEqual(old.keys, cold.keys) || old.provenance.roots != cold.provenance.roots || counter.reads[id] != 0 {
		t.Fatalf("old anchor changed after unrelated commit: %v", err)
	}
	execution := agentprofile.Execution{ID: "trace-profile-root", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx,
		profileCoordinatorRequest(t, "trace-profile-spawn", agentprofile.Command{Spawn: &execution}))))
	newID, _, err := state.Get(ctx, e.index, systemSpace, e.roots.LiveControl, agentPartitionKey(task.Space, task.Owner))
	if err != nil || newID == id {
		t.Fatal("profile update did not replace object")
	}
	newRead, counter, err := read(latestReq, id)
	if err != nil || len(newRead.keys) != 2 || counter.reads[newID] != 1 || len(e.traceProfiles.entries) != 2 {
		t.Fatalf("new profile was not independently built: keys=%v reads=%v err=%v", newRead.keys, counter.reads, err)
	}
	old, counter, err = read(req, id)
	if err != nil || len(old.keys) != 1 || counter.reads[id] != 0 {
		t.Fatalf("latest profile leaked into historical scope: %+v %v", old.keys, err)
	}
	for _, kind := range []string{"owner", "space", "fence"} {
		bad := req
		switch kind {
		case "owner":
			bad.Principal = "other"
		case "space":
			bad.Space = "other"
		case "fence":
			badAnchor := anchor
			badAnchor.Fence.CommitDigest = model.CommitDigest(strings.Repeat("f", 64))
			bad.Anchor = &badAnchor
		}
		if _, _, err := read(bad, id); err == nil {
			t.Fatalf("warm cache bypassed %s validation", kind)
		}
	}
	t.Log("profile object reads: cold=1, warm=0, unrelated latest=0, new profile=1; historical LiveControl lookup retained")
}

func traceProfileStoreObject(t *testing.T, reader *closureObjectReader, object codec.Object) model.ObjectID {
	t.Helper()
	id, err := codec.ObjectID(object)
	if err != nil {
		t.Fatal(err)
	}
	reader.objects[id], err = codec.EncodeObject(object)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func traceProfileStoreRoot(t *testing.T, reader *closureObjectReader, root model.ObjectID, space model.SpaceID, owner model.PrincipalID, id model.ObjectID) model.ObjectID {
	t.Helper()
	delta, err := state.Apply(context.Background(), reader, systemSpace, root, []state.Mutation{{Key: agentPartitionKey(space, owner), Value: id}})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range delta.Objects {
		traceProfileStoreObject(t, reader, object)
	}
	return delta.Root
}

func traceProfileRecordObject(t *testing.T, reader *closureObjectReader, space model.SpaceID, owner model.PrincipalID, snapshot agentprofile.Snapshot) codec.Object {
	t.Helper()
	builder := newBuilder(context.Background(), newObjectIndex(), DefaultMaxObjects)
	image, err := agentprofile.EncodeRecordImage(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := storeAgentRecords(context.Background(), builder, space, owner, "", image)
	if err != nil {
		t.Fatal(err)
	}
	for objectID, raw := range builder.staged {
		reader.objects[objectID] = bytes.Clone(raw)
	}
	object, err := codec.DecodeObject(builder.staged[id])
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func TestTaskTraceProfileLoadValidatesOwnerAndSession(t *testing.T) {
	ctx := context.Background()
	reader := &closureObjectReader{objects: map[model.ObjectID][]byte{}}
	cache := &taskTraceProfileCache{}
	var root model.ObjectID
	for _, owner := range []model.PrincipalID{"alice", "bob"} {
		snapshot := agentprofile.Snapshot{Lifecycle: agentprofile.LifecycleSnapshot{Queries: []agentprofile.Query{
			{ID: "same-query", Space: "space", Owner: owner, Session: string(owner), Version: 1, Status: "open"},
		}}}
		object := traceProfileRecordObject(t, reader, "space", owner, snapshot)
		id := traceProfileStoreObject(t, reader, object)
		root = traceProfileStoreRoot(t, reader, root, "space", owner, id)
	}
	for pass := 0; pass < 2; pass++ {
		for _, owner := range []model.PrincipalID{"alice", "bob"} {
			p, err := cache.load(ctx, reader, root, "space", owner)
			if err != nil {
				t.Fatal(err)
			}
			req := api.TaskTraceRequest{Space: "space", Principal: owner, Subject: api.TraceSubject{Kind: "query", ID: "same-query"}, Session: string(owner)}
			if _, err := p.scope(req); err != nil {
				t.Fatal(err)
			}
			req.Session = "wrong"
			if _, err := p.scope(req); !errors.Is(err, ErrNodeNotFound) {
				t.Fatal("session isolation lost")
			}
		}
	}
	// Even if the object is already cached for its rightful owner, a corrupt
	// partition mapping cannot reuse it under a different owner or space.
	aliceID, _, _ := state.Get(ctx, reader, systemSpace, root, agentPartitionKey("space", "alice"))
	for _, scope := range []taskTraceProfileKey{{space: "space", principal: "mallory"}, {space: "other-space", principal: "alice"}} {
		badRoot := traceProfileStoreRoot(t, reader, root, scope.space, scope.principal, aliceID)
		if _, err := cache.load(ctx, reader, badRoot, scope.space, scope.principal); !errors.Is(err, ErrInvalidStateData) {
			t.Fatalf("mismatched object scope accepted: %+v %v", scope, err)
		}
	}
	if len(cache.entries) != 2 {
		t.Fatal("invalid profile was cached")
	}
	// A hit still reads and validates the historical partition path.
	delete(reader.objects, root)
	if _, err := cache.load(ctx, reader, root, "space", "alice"); err == nil {
		t.Fatal("warm cache bypassed historical LiveControl lookup")
	}
}

func TestTaskTraceProfileStopDuringLoad(t *testing.T) {
	reader := &closureObjectReader{objects: map[model.ObjectID][]byte{}}
	object := traceProfileRecordObject(t, reader, "space", "owner", agentprofile.Snapshot{})
	id := traceProfileStoreObject(t, reader, object)
	root := traceProfileStoreRoot(t, reader, "", "space", "owner", id)
	cache := &taskTraceProfileCache{}
	entered, resume := make(chan struct{}), make(chan struct{})
	blocked := traceProfileReaderFunc(func(ctx context.Context, object model.ObjectID) ([]byte, error) {
		if object == id {
			close(entered)
			<-resume
		}
		return reader.ReadObject(ctx, object)
	})
	done := make(chan error, 1)
	go func() {
		_, err := cache.load(context.Background(), blocked, root, "space", "owner")
		done <- err
	}()
	<-entered
	cache.stop() // Must not wait for profile IO or allow its late insertion.
	close(resume)
	if err := <-done; err != nil || len(cache.entries) != 0 || cache.bytes != 0 {
		t.Fatalf("inflight load repopulated stopped cache: %v", err)
	}
}

type traceProfileReaderFunc func(context.Context, model.ObjectID) ([]byte, error)

func (f traceProfileReaderFunc) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	return f(ctx, id)
}

func TestTaskTraceProfileEngineStopHooks(t *testing.T) {
	for _, stop := range []string{"close", "read-failure", "writer-failure"} {
		t.Run(stop, func(t *testing.T) {
			e, _ := coordinatorStore(t, Options{})
			p := newTaskTraceProfile(agentprofile.Snapshot{})
			key := taskTraceProfileKey{"s", "p", "object"}
			e.traceProfiles.put(key, p)
			switch stop {
			case "close":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := e.CloseContext(ctx); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "read-failure":
				e.mu.Lock()
				_ = e.readFailure(ErrStoredObject)
				e.mu.Unlock()
			case "writer-failure":
				e.writerMu.Lock()
				_ = e.fenceWriter(ErrStoredObject)
				e.writerMu.Unlock()
			}
			e.traceProfiles.put(key, p)
			if _, ok := e.traceProfiles.get(key); ok || !e.traceProfiles.stopped || e.traceProfiles.bytes != 0 {
				t.Fatal("engine stop retained or accepted cache entries")
			}
		})
	}
}
