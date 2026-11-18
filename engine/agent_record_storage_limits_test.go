package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func recordLimitImage(t *testing.T, space model.SpaceID, count int) agentprofile.RecordImage {
	t.Helper()
	snapshot := agentprofile.Snapshot{}
	for i := 0; i < count; i++ {
		snapshot.Tasks = append(snapshot.Tasks, agentprofile.Task{
			ID: model.EntityID(fmt.Sprintf("task-%05d", i)), Space: space, Owner: "p",
			Goal: "g", GoalEpoch: 1, Status: agentprofile.TaskOpen,
		})
	}
	image, err := agentprofile.EncodeRecordImage(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

type recordScanCounter struct {
	state.ObjectReader
	nodes, bytes int
}

func (r *recordScanCounter) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	raw, err := r.ObjectReader.ReadObject(ctx, id)
	if err == nil {
		r.nodes++
		r.bytes += len(raw)
	}
	return raw, err
}

func recordLimitAdd(t *testing.T, b *objectBuilder, obj codec.Object, err error) model.ObjectID {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.add(obj, storage.KindObject)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestAgentRecordMaximumLegalScan(t *testing.T) {
	ctx := context.Background()
	space := model.SpaceID(strings.Repeat("s", model.MaxIdentityBytes))
	image := recordLimitImage(t, space, agentprofile.MaxStateRecords)
	b := newBuilder(ctx, newObjectIndex(), DefaultMaxObjects)
	id, catalog, err := storeAgentRecords(ctx, b, space, "p", "", image)
	if err != nil {
		t.Fatal(err)
	}
	counter := &recordScanCounter{ObjectReader: b}
	entries, err := agentRecordEntries(ctx, counter, space, catalog.root)
	if err != nil || len(entries) != agentprofile.MaxStateRecords {
		t.Fatalf("maximum scan: entries=%d err=%v", len(entries), err)
	}
	for _, entry := range entries {
		if len(entry.Key) != 40 {
			t.Fatalf("record key length=%d", len(entry.Key))
		}
	}
	// A deliberately loose bound from the actual radix/envelope encodings:
	// <=360 bytes per node (including a 256-byte space and 40-byte prefix),
	// 66 per value (payload + ref), 68 per edge (label/digest + ref).
	n := agentprofile.MaxStateRecords
	nodes := 2*n - 1
	readBytes := 360*nodes + 66*n + 68*(nodes-1)
	if nodes > state.MaxReadNodes || n*(40+64) > agentRecordScanBytes || readBytes > agentRecordScanReadBytes ||
		counter.nodes > nodes || counter.bytes > readBytes {
		t.Fatalf("scan closure: nodes=%d/%d bytes=%d/%d", counter.nodes, nodes, counter.bytes, readBytes)
	}
	obj, err := readObject(ctx, b, id)
	if err != nil {
		t.Fatal(err)
	}
	loaded, owner, err := loadAgentRecordCatalog(ctx, b, obj)
	if err != nil || owner != "p" || !reflect.DeepEqual(loaded.objects, catalog.objects) {
		t.Fatalf("maximum catalog: owner=%s err=%v", owner, err)
	}
	want, _ := image.Records()
	got, _ := loaded.image.Records()
	if !reflect.DeepEqual(got, want) {
		t.Fatal("maximum image changed")
	}
	// Exercise the uncached old-root enumeration as well.
	again, _, err := storeAgentRecords(ctx, b, space, "p", id, image)
	if err != nil || again != id {
		t.Fatalf("maximum old root: id=%s err=%v", again, err)
	}
	t.Logf("legal maximum: records=%d radix reads=%d bytes=%d; bounds nodes=%d bytes=%d",
		n, counter.nodes, counter.bytes, nodes, readBytes)
	// Persist the actual writer output and rebuild the object index on reopen.
	// This is physical journal + record catalog recovery, not Engine command
	// replay: the linear fixture deliberately avoids 16384 public commands.
	disk, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	journal, err := storage.CreateJournal(disk, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	publication, err := journal.Published()
	if err != nil {
		t.Fatal(err)
	}
	tx := storage.Transaction{TxID: workImageTx,
		Request: model.RequestKey{Space: space, Principal: "p", RequestID: "maximum"},
		LSN:     1, PreviousDigest: publication.Position.Digest, RequestDigest: asDigest(id),
		PostRootSet: asDigest(id), Receipt: asDigest(id)}
	for _, object := range b.objects {
		tx.Records = append(tx.Records, storage.Record{Kind: storage.KindObject, Payload: object.encoded})
	}
	if _, err = journal.Publish(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	index := newObjectIndex()
	reopened, report, err := storage.OpenJournalWithReplay(disk, storage.JournalOptions{}, func(tx storage.ScannedTransaction, source io.ReaderAt) error {
		index.source = source
		for i, record := range tx.Records {
			obj, err := codec.DecodeObject(record.Payload)
			if err != nil {
				return err
			}
			id, err := codec.ObjectID(obj)
			if err != nil {
				return err
			}
			index.put(id, tx.RecordLocations[i])
		}
		_, _, err := loadAgentRecordCatalog(ctx, index, obj)
		return err
	})
	if err != nil || !report.Verified {
		t.Fatalf("maximum reopen: verified=%t err=%v", report.Verified, err)
	}
	defer reopened.Close()
	index.source = reopened
	restored, _, err := loadAgentRecordCatalog(ctx, index, obj)
	if err != nil || !reflect.DeepEqual(restored.objects, catalog.objects) {
		t.Fatalf("maximum disk read: %v", err)
	}
}

// Build real, hash-correct, locally canonical radix objects. Shared suffixes
// keep the fixture small but Entries must visit every absolute key. At depth 15
// there are 32768 40-byte keys, impossible for an Agent writer (max 16384).
func recordInflatedRadix(t *testing.T, b *objectBuilder, space model.SpaceID) codec.Object {
	t.Helper()
	image := recordLimitImage(t, space, 1)
	record, _ := image.RecordAt(0)
	child, err := makeAgentRecord(space, "p", record.Raw)
	value := recordLimitAdd(t, b, child, err)
	node := func(prefix []byte, value model.ObjectID, children ...model.ObjectID) model.ObjectID {
		payload, _ := codec.AppendBytes(nil, prefix)
		refs := append([]model.ObjectID(nil), children...)
		if value == "" {
			payload = codec.AppendUint(payload, 0)
		} else {
			payload = codec.AppendUint(payload, 1)
			digest, _ := hex.DecodeString(string(value))
			payload, _ = codec.AppendBytes(payload, digest)
			refs = append(refs, value)
		}
		payload = codec.AppendUint(payload, uint64(len(children)))
		for label, id := range children {
			payload = codec.AppendUint(payload, uint64(label))
			digest, _ := hex.DecodeString(string(id))
			payload, _ = codec.AppendBytes(payload, digest)
		}
		return recordLimitAdd(t, b, codec.Object{Type: state.NodeType, Space: space,
			Version: state.NodeVersion, Refs: canonicalRefs(refs...), Payload: payload}, nil)
	}
	var pair [2]model.ObjectID
	for label := range pair {
		prefix := make([]byte, 26)
		prefix[0] = byte(label)
		pair[label] = node(prefix, value)
	}
	for depth := 14; depth > 0; depth-- {
		pair = [2]model.ObjectID{node([]byte{0}, "", pair[:]...), node([]byte{1}, "", pair[:]...)}
	}
	root := node(nil, "", pair[:]...)
	obj, err := makeAgentRecordRoot(space, agentRecordRoot{"p", root, agentprofile.MaxStateRecords, image.Size()})
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func TestAgentRecordStoredRadixLimitClassification(t *testing.T) {
	ctx := context.Background()
	b := newBuilder(ctx, newObjectIndex(), DefaultMaxObjects)
	obj := recordInflatedRadix(t, b, "s")
	id := recordLimitAdd(t, b, obj, nil)
	root, _ := readAgentRecordRoot(obj)
	// Reproduce the scanner's actual ErrLimit, not an injected sentinel.
	entries, err := state.Entries(ctx, b, "s", root.root, state.ScanLimits{
		MaxEntries: agentprofile.MaxStateRecords, MaxNodes: state.MaxReadNodes,
		MaxBytes: 4 << 20, MaxReadBytes: 32 << 20,
	})
	if entries != nil || !errors.Is(err, state.ErrLimit) {
		t.Fatalf("counterexample did not hit scan limit: %v", err)
	}
	image := recordLimitImage(t, "s", 1)
	for _, path := range []string{"load", "old-root", "registry"} {
		t.Run(path, func(t *testing.T) {
			var err error
			switch path {
			case "load":
				_, _, err = loadAgentRecordCatalog(ctx, b, obj)
			case "old-root":
				_, _, err = storeAgentRecords(ctx, b, "s", "p", id, image)
			case "registry":
				err = validateRecord(ctx, obj, b)
			}
			if !errors.Is(err, state.ErrLimit) || !errors.Is(err, ErrStoredObject) ||
				isLimit(err) || ClassifyError(err) != api.ErrorCorruption ||
				!errors.Is(semanticError(err), storage.ErrCorruption) {
				t.Fatalf("stored radix limit: class=%s err=%v", ClassifyError(err), err)
			}
		})
	}
}

func TestAgentRecordReaderFailureOrigins(t *testing.T) {
	ctx := context.Background()
	b := newBuilder(ctx, newObjectIndex(), DefaultMaxObjects)
	id, catalog, err := storeAgentRecords(ctx, b, "s", "p", "", recordLimitImage(t, "s", 1))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := readObject(ctx, b, id)
	if err != nil {
		t.Fatal(err)
	}
	// These limits come from a reader's own work budget on a VALID image.
	// They must remain resource failures even though the same sentinels can
	// originate inside the fixed radix scan on invalid stored structures.
	for _, cause := range []error{state.ErrLimit, storage.ErrLimit, context.Canceled,
		context.DeadlineExceeded, errors.Join(ErrObjectIO, syscall.EIO),
		errors.Join(ErrObjectIO, storage.ErrLimit)} {
		for _, target := range []model.ObjectID{catalog.root, catalog.objects[string(mustRecordKey(t, catalog.image))]} {
			reader := &workImageReader{ObjectReader: b, reads: map[model.ObjectID]int{}, fail: target, err: cause}
			got, owner, err := loadAgentRecordCatalog(ctx, reader, obj)
			if err != cause || owner != "" || got.objects != nil || errors.Is(err, ErrStoredObject) {
				t.Fatalf("source error changed: cause=%v got=%v", cause, err)
			}
			if isLimit(err) != (cause == state.ErrLimit || cause == storage.ErrLimit) {
				t.Fatalf("source limit misclassified: %v", err)
			}
			if errors.Is(semanticError(err), storage.ErrCorruption) {
				t.Fatalf("source error became replay corruption: %v", err)
			}
		}
	}
}

func mustRecordKey(t *testing.T, image agentprofile.RecordImage) []byte {
	t.Helper()
	record, err := image.RecordAt(0)
	if err != nil {
		t.Fatal(err)
	}
	return record.Key
}

type recordReadAtFault struct {
	io.ReaderAt
	offset int64
	cause  error
}

func (r recordReadAtFault) ReadAt(p []byte, offset int64) (int, error) {
	if offset == r.offset {
		return 0, r.cause
	}
	return r.ReaderAt.ReadAt(p, offset)
}

func TestAgentRecordOldRootFailureOrigins(t *testing.T) {
	ctx := context.Background()
	image := recordLimitImage(t, "s", 2)
	b := newBuilder(ctx, newObjectIndex(), DefaultMaxObjects)
	id, catalog, err := storeAgentRecords(ctx, b, "s", "p", "", image)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := readObject(ctx, b, id)
	if err != nil {
		t.Fatal(err)
	}
	index := newObjectIndex()
	var data []byte
	for _, object := range b.objects {
		index.put(object.id, storage.RecordLocation{PayloadOffset: int64(len(data)), PayloadLength: uint32(len(object.encoded))})
		data = append(data, object.encoded...)
	}
	for _, target := range []model.ObjectID{id, catalog.root} {
		for _, cause := range []error{syscall.EIO, storage.ErrLimit, io.ErrUnexpectedEOF} {
			index.source = recordReadAtFault{ReaderAt: bytes.NewReader(data), offset: index.entries[target].PayloadOffset, cause: cause}
			next, got, err := storeAgentRecords(ctx, newBuilder(ctx, index, DefaultMaxObjects), "s", "p", id, image)
			stored := cause == io.ErrUnexpectedEOF
			if next != "" || got.objects != nil || !errors.Is(err, cause) ||
				errors.Is(err, ErrStoredObject) != stored || errors.Is(err, ErrObjectIO) == stored || isLimit(err) {
				t.Fatalf("old-root IO: cause=%v got=%v", cause, err)
			}
			if errors.Is(semanticError(err), storage.ErrCorruption) != stored {
				t.Fatalf("old-root replay IO: %v", err)
			}
		}
	}
	index.source = bytes.NewReader(data)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	deadline, stop := context.WithDeadline(ctx, time.Unix(0, 0))
	defer stop()
	for _, ctx := range []context.Context{canceled, deadline} {
		_, _, err := storeAgentRecords(ctx, newBuilder(ctx, index, DefaultMaxObjects), "s", "p", id, image)
		if err != ctx.Err() || errors.Is(err, ErrStoredObject) {
			t.Fatalf("old-root cancellation: %v", err)
		}
	}
	for _, kind := range []string{"count", "key-length", "root-version", "root-type", "owner"} {
		t.Run(kind, func(t *testing.T) {
			bad := obj
			r, _ := readAgentRecordRoot(obj)
			switch kind {
			case "count":
				r.count++
				bad, err = makeAgentRecordRoot("s", r)
			case "key-length":
				delta, applyErr := state.Apply(ctx, b, "s", "", []state.Mutation{{Key: []byte("short"), Value: catalog.objects[string(mustRecordKey(t, image))]}})
				if applyErr != nil {
					t.Fatal(applyErr)
				}
				for _, node := range delta.Objects {
					recordLimitAdd(t, b, node, nil)
				}
				r.root, r.count = delta.Root, 1
				bad, err = makeAgentRecordRoot("s", r)
			case "root-version":
				bad.Version++
			case "root-type":
				bad.Type = "ate.unknown"
			case "owner":
				r.owner = "other"
				bad, err = makeAgentRecordRoot("s", r)
			}
			badID := recordLimitAdd(t, b, bad, err)
			_, _, loadErr := loadAgentRecordCatalog(ctx, b, bad)
			_, _, storeErr := storeAgentRecords(ctx, b, "s", "p", badID, image)
			for _, err := range []error{loadErr, storeErr} {
				if !errors.Is(err, ErrStoredObject) || isLimit(err) || ClassifyError(err) != api.ErrorCorruption {
					t.Fatalf("%s classification: %v", kind, err)
				}
			}
		})
	}
}

func TestAgentRecordStoredRadixLimitFences(t *testing.T) {
	for _, path := range []string{"read", "prepare"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			e, disk := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
			// Install a hash-correct but invalid stored-state overlay. The actual
			// journal prefix stays valid, so reopen can prove no writes occurred.
			b := newBuilder(ctx, e.index, DefaultMaxObjects)
			obj := recordInflatedRadix(t, b, "s")
			id := recordLimitAdd(t, b, obj, nil)
			delta, err := state.Apply(ctx, b, systemSpace, "", []state.Mutation{{Key: agentPartitionKey("s", "p"), Value: id}})
			if err != nil {
				t.Fatal(err)
			}
			for _, node := range delta.Objects {
				recordLimitAdd(t, b, node, nil)
			}
			roots := e.roots
			roots.LiveControl = delta.Root
			rootObj, err := makeRootSet(roots)
			rootID := recordLimitAdd(t, b, rootObj, err)
			// Give both objectIndex and the public pinned reader real locations
			// in the injected source, so neither fails before reaching the scan.
			index := newObjectIndex()
			var data []byte
			for _, object := range b.objects {
				index.put(object.id, storage.RecordLocation{PayloadOffset: int64(len(data)), PayloadLength: uint32(len(object.encoded))})
				data = append(data, object.encoded...)
			}
			index.source = bytes.NewReader(data)
			e.mu.Lock()
			e.index, e.roots = index, roots
			e.rootSet = rootID
			count, publication := e.index.count(), e.publication
			e.mu.Unlock()
			writes, syncs, currents, _ := disk.counts()
			if path == "read" {
				result, readErr := e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "s", Principal: "p"})
				err = readErr
				if !reflect.DeepEqual(result, api.AgentStateResult{}) {
					t.Fatal("failed scan leaked partial state")
				}
			} else {
				task := agentprofile.Task{ID: "new", Space: "s", Owner: "p", Goal: "g", GoalEpoch: 1, Status: agentprofile.TaskOpen}
				result, applyErr := e.ApplyAgent(ctx, api.ApplyAgentRequest{
					Key:     model.RequestKey{Space: "s", Principal: "p", RequestID: "bad-radix"},
					Command: agentprofile.Command{CreateTask: &task}})
				err = applyErr
				if result.Code != api.CodeStoreFenced || result.RootSet != "" || result.ResultDigest != "" {
					t.Fatalf("bad radix command result: %+v", result)
				}
			}
			if !errors.Is(err, state.ErrLimit) || !errors.Is(err, ErrStoredObject) ||
				!errors.Is(err, storage.ErrCorruption) || !errors.Is(err, storage.ErrStoreFenced) ||
				ClassifyError(err) != api.ErrorStoreFenced {
				t.Fatalf("bad radix did not fence: %v", err)
			}
			e.mu.Lock()
			unchanged := e.roots == roots && e.rootSet == rootID && e.index.count() == count && e.publication == publication
			e.mu.Unlock()
			if !unchanged {
				t.Fatal("bad radix installed a candidate")
			}
			if w, s, c, _ := disk.counts(); w != writes || s != syncs || c != currents {
				t.Fatal("bad radix performed journal IO")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, report, err := OpenExisting(disk, e.options)
			if err != nil || !report.Verified {
				t.Fatalf("unchanged prefix reopen: %v", err)
			}
			defer reopened.Close()
			lookup, err := reopened.LookupRequest(ctx, api.LookupRequestRequest{Space: "s", Principal: "p", RequestID: "bad-radix"})
			if err != nil || lookup.Code != api.CodeNotFoundAtFence {
				t.Fatalf("bad radix left receipt: %+v %v", lookup, err)
			}
		})
	}
}

func TestAgentRecordMultiBatchResourceFailureIsAtomic(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	task := profileCoordinatorTask("multi-batch")
	create := profileCoordinatorRequest(t, "create", agentprofile.Command{CreateTask: &task})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, create)))
	// Real public writes make this fixture independently replayable. CancelTask
	// changes every execution: 260 records => 520 sorted insert/delete mutations,
	// spanning three radix batches in a single storeAgentRecords invocation.
	for i := 0; i < 259; i++ {
		execution := agentprofile.Execution{ID: model.ExecutionID(fmt.Sprintf("exec-%03d", i)),
			Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
		req := profileCoordinatorRequest(t, fmt.Sprintf("spawn-%03d", i), agentprofile.Command{Spawn: &execution})
		coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, req)))
	}
	req := profileCoordinatorRequest(t, "cancel", agentprofile.Command{CancelTask: &agentprofile.CancelTaskCommand{Task: task.ID}})
	digest, err := CommandDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	before, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := e.index.count()
	oracle, err := e.prepareAgent(ctx, req, digest, workImageTx, before.Fence.LSN+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	profileAt := -1
	for i, object := range oracle.builder.objects {
		obj, err := codec.DecodeObject(object.encoded)
		if err != nil {
			t.Fatal(err)
		}
		if obj.Type == agentRecordRootType {
			profileAt = i
			break
		}
	}
	if profileAt < 2 {
		t.Fatal("missing final record root")
	}
	lastNode := oracle.builder.objects[profileAt-1]
	node, err := codec.DecodeObject(lastNode.encoded)
	if err != nil || node.Type != state.NodeType {
		t.Fatal("failure point is not the final radix batch")
	}
	// Reject the final node of the third batch, after earlier batches were
	// staged. The quota is a candidate budget, not a stored format violation.
	e.mu.Lock()
	e.options.MaxObjects = count + uint64(profileAt-1)
	e.mu.Unlock()
	writes, syncs, currents, _ := disk.counts()
	for attempt := 0; attempt < 2; attempt++ {
		result, err := e.Commit(ctx, req)
		if result.Code != api.CodeResourceLimit || !errors.Is(err, storage.ErrLimit) ||
			errors.Is(err, ErrStoredObject) || errors.Is(err, ErrObjectIO) ||
			ClassifyError(err) != api.ErrorResourceLimit {
			t.Fatalf("fresh candidate classification: %+v %v", result, err)
		}
		if result.RootSet != "" || result.StateRoot != "" || result.ResultDigest != "" || result.Fence != (model.CommitFence{}) {
			t.Fatal("rejected candidate escaped")
		}
		after, err := e.Snapshot(ctx)
		if err != nil || after != before {
			t.Fatalf("failed batch changed roots/LSN: %v", err)
		}
		if lookup := coordinatorLookup(t, e, req); lookup.Code != api.CodeNotFoundAtFence {
			t.Fatalf("failed batch left receipt: %+v", lookup)
		}
	}
	e.mu.Lock()
	unchanged := e.index.count() == count && e.fenced == nil
	e.options.MaxObjects = DefaultMaxObjects
	e.mu.Unlock()
	if !unchanged {
		t.Fatal("failed batch changed object catalog or fenced")
	}
	if w, s, c, _ := disk.counts(); w != writes || s != syncs || c != currents {
		t.Fatal("failed batch performed journal IO")
	}
	// The direct storage path also must not replace a caller's Work catalog.
	slot := &workAgentImage{}
	workCtx := context.WithValue(ctx, workAgentImageKey{}, slot)
	ledger, oldID, found, err := loadAgentLedger(workCtx, e.index, before.Roots.LiveControl, req.Space, req.Principal)
	if err != nil || !found {
		t.Fatal("missing old catalog", err)
	}
	saved := *slot
	_, image, err := ledger.ApplyCommandRecords(req.Key(), agentprofile.Command{CancelTask: &agentprofile.CancelTaskCommand{Task: task.ID}})
	if err != nil {
		t.Fatal(err)
	}
	// prepareAgent stages the command object before storeAgentRecords.
	limit := count + uint64(profileAt-2)
	builder := newBuilder(workCtx, e.index, limit)
	id, catalog, err := storeAgentRecords(workCtx, builder, req.Space, req.Principal, oldID, image)
	if !errors.Is(err, storage.ErrLimit) || id != "" || catalog.objects != nil ||
		uint64(len(builder.objects))+count != limit || !reflect.DeepEqual(*slot, saved) {
		t.Fatalf("late storage failure leaked candidate: id=%s err=%v", id, err)
	}
	if !reflect.DeepEqual(builder.objects, oracle.builder.objects[1:profileAt-1]) {
		t.Fatal("failure was not at the expected final batch node")
	}
	// Cancel at the same late staging boundary, without a timer or goroutine.
	cancelCtx, cancel := context.WithCancel(workCtx)
	defer cancel()
	cancelBuilder := newBuilder(cancelCtx, e.index, DefaultMaxObjects)
	target := oracle.builder.objects[profileAt-2].id
	cancelCtx = recordStageCancelContext{Context: cancelCtx, check: func() {
		if _, staged := cancelBuilder.staged[target]; staged {
			cancel()
		}
	}}
	cancelBuilder.ctx = cancelCtx
	id, catalog, err = storeAgentRecords(cancelCtx, cancelBuilder, req.Space, req.Principal, oldID, image)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrStoredObject) || isLimit(err) ||
		id != "" || catalog.objects != nil || !reflect.DeepEqual(*slot, saved) ||
		!reflect.DeepEqual(cancelBuilder.objects, builder.objects) || e.index.count() != count {
		t.Fatalf("late cancellation changed state/cache or classification: %v", err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenExisting(disk, e.options)
	if err != nil || !report.Verified {
		t.Fatalf("failed multi-batch reopen: %v", err)
	}
	defer reopened.Close()
	stateBefore, err := reopened.GetAgentState(ctx, api.ReadAgentStateRequest{Space: req.Space, Principal: req.Principal})
	if err != nil || len(stateBefore.Snapshot.Executions) != 259 || stateBefore.Snapshot.Tasks[0].Status != agentprofile.TaskOpen {
		t.Fatalf("failed batches changed recovered state: %v", err)
	}
	if lookup := coordinatorLookup(t, reopened, req); lookup.Code != api.CodeNotFoundAtFence {
		t.Fatal("failed batches recovered a receipt")
	}
	ack := coordinatorAck(t, coordinatorWait(t, coordinatorCommit(reopened, ctx, req)))
	if ack.Fence.LSN != before.Fence.LSN+1 {
		t.Fatal("retry did not publish exactly once")
	}
	stateAfter, err := reopened.GetAgentState(ctx, api.ReadAgentStateRequest{Space: req.Space, Principal: req.Principal})
	if err != nil || stateAfter.Snapshot.Tasks[0].Status != agentprofile.TaskCanceled {
		t.Fatalf("retry did not publish full cancellation: %v", err)
	}
	for _, execution := range stateAfter.Snapshot.Executions {
		if execution.Status != agentprofile.ExecutionCanceled {
			t.Fatal("retry published partial cancellation")
		}
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	replayed, report, err := OpenExisting(disk, e.options)
	if err != nil || !report.Verified {
		t.Fatalf("successful multi-batch reopen: %v", err)
	}
	defer replayed.Close()
	recovered, err := replayed.GetAgentState(ctx, api.ReadAgentStateRequest{Space: req.Space, Principal: req.Principal})
	// Ledger.Snapshot collects executions from a map. Compare all fields after
	// ordering by identity; iteration order is not a record storage invariant.
	byID := func(a, b agentprofile.Execution) int { return strings.Compare(string(a.ID), string(b.ID)) }
	slices.SortFunc(recovered.Snapshot.Executions, byID)
	slices.SortFunc(stateAfter.Snapshot.Executions, byID)
	if err != nil || !reflect.DeepEqual(recovered.Snapshot, stateAfter.Snapshot) {
		t.Fatalf("successful multi-batch replay changed image: %v", err)
	}
	if lookup := coordinatorLookup(t, replayed, req); lookup.Code != api.CodeDurableAck || lookup.Result != ack {
		t.Fatal("successful multi-batch replay changed receipt")
	}
}

type recordStageCancelContext struct {
	context.Context
	check func()
}

func (ctx recordStageCancelContext) Err() error {
	ctx.check()
	return ctx.Context.Err()
}

func TestAgentRecordReaderBudgetDoesNotFence(t *testing.T) {
	ctx := context.Background()
	e, _ := coordinatorStore(t, Options{AgentProfile: true})
	task := profileCoordinatorTask("reader-budget")
	req := profileCoordinatorRequest(t, "create", agentprofile.Command{CreateTask: &task})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, req)))
	id, found, err := state.Get(ctx, e.index, systemSpace, e.roots.LiveControl, agentPartitionKey(req.Space, req.Principal))
	if err != nil || !found {
		t.Fatal(err)
	}
	obj, err := readObject(ctx, e.index, id)
	if err != nil {
		t.Fatal(err)
	}
	root, err := readAgentRecordRoot(obj)
	if err != nil {
		t.Fatal(err)
	}
	for _, cause := range []error{state.ErrLimit, storage.ErrLimit} {
		scoped, closeCut, err := e.beginReadCut(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cut := e.readCut(scoped)
		cut.reader = &workImageReader{ObjectReader: cut.reader, reads: map[model.ObjectID]int{}, fail: root.root, err: cause}
		result, err := e.GetAgentState(scoped, api.ReadAgentStateRequest{Space: req.Space, Principal: req.Principal})
		closeCut()
		if err != cause || ClassifyError(err) != api.ErrorResourceLimit || !reflect.DeepEqual(result, api.AgentStateResult{}) {
			t.Fatalf("valid state reader budget: %v", err)
		}
		e.mu.Lock()
		fenced := e.fenced
		e.mu.Unlock()
		if fenced != nil {
			t.Fatalf("valid state reader budget fenced engine: %v", fenced)
		}
	}
	result, err := e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: req.Space, Principal: req.Principal})
	if err != nil || !result.Found || len(result.Snapshot.Tasks) != 1 {
		t.Fatalf("reader budget prevented retry: %v", err)
	}
}
