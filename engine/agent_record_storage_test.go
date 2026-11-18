package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type recordFaultReader struct {
	state.ObjectReader
	target  model.ObjectID
	missing bool
	mutate  bool
}

func (r recordFaultReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if id == r.target && r.missing {
		return nil, ErrMissingObject
	}
	raw, err := r.ObjectReader.ReadObject(ctx, id)
	if err != nil || id != r.target || !r.mutate {
		return raw, err
	}
	raw = bytes.Clone(raw)
	raw[len(raw)-1] ^= 1
	return raw, nil
}

func TestAgentRecordPublicResourceLimitIsAtomic(t *testing.T) {
	ctx := context.Background()
	e, disk := coordinatorStore(t, Options{AgentProfile: true, Batch: BatchOptions{MaxCount: 1}})
	snapshot := agentprofile.Snapshot{}
	for i := 0; i < agentprofile.MaxStateRecords; i++ {
		snapshot.Tasks = append(snapshot.Tasks, agentprofile.Task{
			ID: model.EntityID(fmt.Sprintf("task-%05d", i)), Space: "s", Owner: "p", Goal: "g", GoalEpoch: 1, Status: agentprofile.TaskOpen,
		})
	}
	// Seed a fully authenticated record profile directly into the private
	// object catalog to avoid quadratic 16K-command fixture setup. The request
	// below exercises real public admission, reduction and outcome delivery.
	e.mu.Lock()
	e.index = newStagingIndex(e.index)
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	image, err := agentprofile.EncodeRecordImage(snapshot)
	if err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	id, _, err := storeAgentRecords(ctx, b, "s", "p", "", image)
	if err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	delta, err := state.Apply(ctx, b, systemSpace, "", []state.Mutation{{Key: agentPartitionKey("s", "p"), Value: id}})
	if err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	for _, node := range delta.Objects {
		if _, err := b.add(node, storage.KindObject); err != nil {
			e.mu.Unlock()
			t.Fatal(err)
		}
	}
	roots := e.roots
	roots.LiveControl = delta.Root
	rootObject, err := makeRootSet(roots)
	if err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	rootID, err := b.add(rootObject, storage.KindObject)
	if err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	if err := absorbWorkPlan(ctx, e, newBuilder(ctx, newObjectIndex(), DefaultMaxObjects), &commitPlan{builder: b, roots: roots}); err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	e.rootSet = rootID
	count, published := e.index.count(), e.publication
	e.mu.Unlock()
	before, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writes, syncs, currents, _ := disk.counts()
	task := snapshot.Tasks[0]
	task.ID = "one-too-many"
	request := api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", Principal: "p", RequestID: "limit"}, Command: agentprofile.Command{CreateTask: &task}}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := e.ApplyAgent(ctx, request)
		if result.Code != api.CodeResourceLimit || !errors.Is(err, agentprofile.ErrStateLimit) {
			t.Fatalf("public state limit: code=%s err=%v", result.Code, err)
		}
		if result.Fence != (model.CommitFence{}) || result.RootSet != "" || result.StateRoot != "" || result.ResultDigest != "" {
			t.Fatal("resource rejection exposed a candidate publication")
		}
		got, err := e.Snapshot(ctx)
		if err != nil || got != before {
			t.Fatalf("limit changed roots or LSN: %v", err)
		}
		lookup, err := e.LookupRequest(ctx, api.LookupRequestRequest{Space: "s", Principal: "p", RequestID: "limit"})
		if err != nil || lookup.Code != api.CodeNotFoundAtFence || lookup.Result != (api.CommitResult{}) {
			t.Fatalf("limit retained a receipt: %+v %v", lookup, err)
		}
	}
	e.mu.Lock()
	unchanged := e.publication == published && e.index.count() == count && e.roots == roots && e.fenced == nil
	e.mu.Unlock()
	if !unchanged {
		t.Fatal("limit installed state or fenced engine")
	}
	if w, s, c, _ := disk.counts(); w != writes || s != syncs || c != currents {
		t.Fatal("limit performed journal IO")
	}
}

func TestAgentRecordStoredLimitRemainsCorruption(t *testing.T) {
	if !isLimit(agentprofile.ErrStateLimit) {
		t.Fatal("state limit missing from classifier")
	}
	for _, marker := range []error{ErrStoredObject, ErrObjectIO} {
		if isLimit(errors.Join(marker, agentprofile.ErrStateLimit)) {
			t.Fatal("stored error downgraded to resource rejection")
		}
	}
	// The byte-string envelope is within 4 MiB, but the record's allocation
	// budget rejects the decoded input. A durable reader must wrap the sentinel.
	q := agentprofile.Query{ID: "q", Space: "s", Owner: "p", Version: 1, Input: make([]byte, (agentprofile.MaxStateRecordBytes/2)+1)}
	body, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	raw := append([]byte("ATE-AGENT-RECORD\x00\x01\x06"), body...)
	obj, err := makeAgentRecord("s", "p", raw)
	if err != nil {
		t.Fatal(err)
	}
	err = validateRecord(context.Background(), obj, nil)
	if !errors.Is(err, ErrStoredObject) || !errors.Is(err, agentprofile.ErrStateLimit) || isLimit(err) {
		t.Fatalf("stored limit classification: %v", err)
	}
	if !errors.Is(semanticError(err), storage.ErrCorruption) {
		t.Fatal("stored limit not fail-closed")
	}
	// Correct object hashes, radix keys, scope and byte totals must not allow
	// loading to bypass the child schema or its allocation budget.
	validBody, _ := json.Marshal(agentprofile.Query{ID: "q", Space: "s", Owner: "p", Version: 1})
	noncanonical := append([]byte("ATE-AGENT-RECORD\x00\x01\x06"),
		bytes.Replace(validBody, []byte(`{"ID"`), []byte(`{"extra":1,"ID"`), 1)...)
	for name, data := range map[string][]byte{"allocation-limit": raw, "unknown-field": noncanonical} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			b := newBuilder(ctx, newObjectIndex(), DefaultMaxObjects)
			child, err := makeAgentRecord("s", "p", data)
			if err != nil {
				t.Fatal(err)
			}
			id, err := b.add(child, storage.KindObject)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			key := append(sum[:], make([]byte, 8)...)
			delta, err := state.Apply(ctx, b, "s", "", []state.Mutation{{Key: key, Value: id}})
			if err != nil {
				t.Fatal(err)
			}
			for _, node := range delta.Objects {
				if _, err := b.add(node, storage.KindObject); err != nil {
					t.Fatal(err)
				}
			}
			root, err := makeAgentRecordRoot("s", agentRecordRoot{"p", delta.Root, 1, uint64(len(data))})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = loadAgentRecordImage(ctx, b, root)
			if !errors.Is(err, ErrStoredObject) || isLimit(err) {
				t.Fatalf("child authentication not fail-closed: %v", err)
			}
			if name == "allocation-limit" && !errors.Is(err, agentprofile.ErrStateLimit) {
				t.Fatalf("lost state budget error: %v", err)
			}
		})
	}
}

func TestAgentRecordStorageCorruption(t *testing.T) {
	ctx := context.Background()
	e, req, _ := workImageFixture(t, 2, false, false)
	unit, _ := api.DecodeWorkUnit(req.Body)
	req.Method, req.Body = unit.Steps[0].Method, unit.Steps[0].Body
	digest, _ := CommandDigest(req)
	plan, err := e.prepareAgent(ctx, req, digest, workImageTx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := plan.builder
	id, found, err := state.Get(ctx, b, systemSpace, plan.roots.LiveControl, agentPartitionKey(req.Space, req.Principal))
	if err != nil || !found {
		t.Fatal("missing profile", err)
	}
	obj, err := readObject(ctx, b, id)
	if err != nil {
		t.Fatal(err)
	}
	root, err := readAgentRecordRoot(obj)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := agentRecordEntries(ctx, b, obj.Space, root.root)
	if err != nil {
		t.Fatal(err)
	}
	reject := func(t *testing.T, bad codec.Object) {
		t.Helper()
		if _, _, err := loadAgentRecordImage(ctx, b, bad); err == nil {
			t.Fatal("accepted corrupt profile")
		}
		if err := validateRecord(ctx, bad, b); err == nil {
			t.Fatal("registry accepted corrupt profile")
		}
	}
	t.Run("cancellation-is-not-corruption", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err := loadAgentRecordImage(canceled, b, obj)
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrStoredObject) {
			t.Fatalf("cancellation: %v", err)
		}
	})
	t.Run("root-limits", func(t *testing.T) {
		for _, bad := range []agentRecordRoot{
			{owner: root.owner, root: root.root, count: agentprofile.MaxStateRecords + 1, bytes: root.bytes},
			{owner: root.owner, root: root.root, count: root.count, bytes: agentprofile.MaxStateBytes + 1},
		} {
			if _, err := makeAgentRecordRoot(obj.Space, bad); err == nil {
				t.Fatal("unbounded root accepted")
			}
		}
	})
	t.Run("root-version", func(t *testing.T) {
		bad := obj
		bad.Version++
		reject(t, bad)
	})
	t.Run("count", func(t *testing.T) {
		bad := root
		bad.count++
		object, err := makeAgentRecordRoot(obj.Space, bad)
		if err != nil {
			t.Fatal(err)
		}
		reject(t, object)
	})
	t.Run("byte-sum", func(t *testing.T) {
		bad := root
		bad.bytes--
		object, err := makeAgentRecordRoot(obj.Space, bad)
		if err != nil {
			t.Fatal(err)
		}
		reject(t, object)
	})
	t.Run("owner", func(t *testing.T) {
		bad := root
		bad.owner = "foreign"
		object, err := makeAgentRecordRoot(obj.Space, bad)
		if err != nil {
			t.Fatal(err)
		}
		reject(t, object)
	})
	t.Run("references", func(t *testing.T) {
		bad := obj
		bad.Refs = nil
		reject(t, bad)
	})
	t.Run("missing-record", func(t *testing.T) {
		entry := entries[0]
		reader := recordFaultReader{ObjectReader: b, target: entry.Value, missing: true}
		if _, _, err := loadAgentRecordImage(ctx, reader, obj); !errors.Is(err, ErrMissingObject) {
			t.Fatalf("missing record: %v", err)
		}
	})
	t.Run("record-hash", func(t *testing.T) {
		entry := entries[0]
		reader := recordFaultReader{ObjectReader: b, target: entry.Value, mutate: true}
		if _, _, err := loadAgentRecordImage(ctx, reader, obj); err == nil {
			t.Fatal("accepted corrupted record bytes")
		}
		if err := validateRecord(ctx, obj, reader); err == nil {
			t.Fatal("registry accepted corrupted record bytes")
		}
	})
	t.Run("canonical-but-broken-relations", func(t *testing.T) {
		var removed state.Entry
		var raw []byte
		for _, entry := range entries {
			child, err := readObject(ctx, b, entry.Value)
			if err != nil {
				t.Fatal(err)
			}
			data, _, err := readAgentRecord(child)
			if err != nil {
				t.Fatal(err)
			}
			// Schema 1, tag 1 = task; remove one required parent record.
			if bytes.HasPrefix(data, []byte("ATE-AGENT-RECORD\x00\x01\x01")) {
				removed, raw = entry, data
				break
			}
		}
		if raw == nil {
			t.Fatal("missing task fixture")
		}
		delta, err := state.Apply(ctx, b, obj.Space, root.root, []state.Mutation{{Key: removed.Key, Delete: true}})
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range delta.Objects {
			if _, err := b.add(node, storage.KindObject); err != nil {
				t.Fatal(err)
			}
		}
		bad := root
		bad.root, bad.count, bad.bytes = delta.Root, root.count-1, root.bytes-uint64(len(raw))
		object, err := makeAgentRecordRoot(obj.Space, bad)
		if err != nil {
			t.Fatal(err)
		}
		reject(t, object)
	})
	t.Run("forged-content-key", func(t *testing.T) {
		entry := entries[0]
		key := bytes.Clone(entry.Key)
		key[0] ^= 0xff
		delta, err := state.Apply(ctx, b, obj.Space, root.root, []state.Mutation{{Key: entry.Key, Delete: true}, {Key: key, Value: entry.Value}})
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range delta.Objects {
			if _, err := b.add(node, storage.KindObject); err != nil {
				t.Fatal(err)
			}
		}
		bad := root
		bad.root = delta.Root
		object, err := makeAgentRecordRoot(obj.Space, bad)
		if err != nil {
			t.Fatal(err)
		}
		reject(t, object)
	})
}

func TestAgentRecordStorageReusesUnchangedLeaves(t *testing.T) {
	ctx := context.Background()
	e, req, _ := workImageFixture(t, 8, false, false)
	unit, _ := api.DecodeWorkUnit(req.Body)
	req.Method, req.Body = unit.Steps[0].Method, unit.Steps[0].Body
	digest, _ := CommandDigest(req)
	first, err := e.prepareAgent(ctx, req, digest, workImageTx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := e.workView()
	if err := absorbWorkPlan(ctx, view, newBuilder(ctx, e.index, e.options.MaxObjects), first); err != nil {
		t.Fatal(err)
	}
	body, err := agentprofile.EncodeCommand(agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: "task-0000", ExpectedEpoch: 1, NewGoal: "changed"}})
	if err != nil {
		t.Fatal(err)
	}
	req = api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: "update", Method: api.MethodApplyAgent, Body: body}
	digest, _ = CommandDigest(req)
	second, err := view.prepareAgent(ctx, req, digest, model.TxID("33333333333333333333333333333333"), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	records, roots, size := 0, 0, 0
	for _, staged := range second.builder.objects {
		object, err := codec.DecodeObject(staged.encoded)
		if err != nil {
			t.Fatal(err)
		}
		if object.Type == agentRecordType {
			records++
		}
		if object.Type == agentRecordRootType {
			roots++
		}
		size += len(staged.encoded)
	}
	if records != 1 || roots != 1 || size > 64<<10 {
		t.Fatalf("not a record delta: records=%d roots=%d bytes=%d", records, roots, size)
	}
}
