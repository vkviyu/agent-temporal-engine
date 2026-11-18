package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const workImageTx model.TxID = "11111111111111111111111111111111"

// This is the prepareWork orchestration without installing its context slot.
// All boundary checks, private reducers, absorption and finalization are real.
// Keep the cache choice out of production.
func prepareWorkWithoutImage(ctx context.Context, e *Engine, req api.CommitRequest, digest model.ObjectID, tx model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	unit, view, builder, commandID, err := e.prepareWorkInput(ctx, req)
	if err != nil {
		return nil, err
	}
	if unit.Kind == "query_intake" {
		err = view.prepareQueryIntake(ctx, req, unit, digest, tx, lsn, builder)
	} else {
		err = e.prepareWorkSteps(ctx, req, unit, view, builder, digest, tx, lsn)
	}
	if err != nil {
		return nil, err
	}
	return e.finishWorkPlan(ctx, req, unit, view, builder, commandID, digest, tx, lsn, recorded)
}

func workImageControl(tb testing.TB, c agentprofile.ControlCommand) api.WorkStep {
	tb.Helper()
	step, err := api.AgentWorkStep(agentprofile.Command{Control: &c})
	if err != nil {
		tb.Fatal(err)
	}
	return step
}

func workImageRequest(tb testing.TB, unit api.WorkUnit) api.CommitRequest {
	tb.Helper()
	raw, err := api.EncodeWorkUnit(unit)
	if err != nil {
		tb.Fatal(err)
	}
	return api.CommitRequest{Space: "image-space", Principal: "owner", RequestID: "work", Method: api.MethodApplyWork, Body: raw}
}

// Seed canonical objects and an authenticated historical root directly, keeping
// fixture setup linear. This models the in-memory object index used by private
// preparation; it does not measure journal IO or claim to test publication.
func workImageFixture(tb testing.TB, groups int, mixed, events bool) (*Engine, api.CommitRequest, int) {
	tb.Helper()
	ctx := context.Background()
	e := newEngine(Options{AgentProfile: true, EventProfile: events, MaxObjects: DefaultMaxObjects})
	e.index = newStagingIndex(nil)
	e.publication = storage.Publication{
		Store:    "22222222222222222222222222222222",
		Position: storage.Position{LSN: 1, Digest: asDigest(model.ObjectID(strings.Repeat("a", 64)))},
	}
	fence := e.publication.Fence()
	snapshot := agentprofile.Snapshot{}
	for i := 0; i < groups; i++ {
		task := agentprofile.Task{ID: model.EntityID(fmt.Sprintf("task-%04d", i)), Space: "image-space", Owner: "owner", Goal: "finish the recorded work", GoalEpoch: 1, Status: agentprofile.TaskOpen}
		x := agentprofile.Execution{ID: model.ExecutionID(fmt.Sprintf("exec-%04d", i)), Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionRunning}
		snapshot.Tasks = append(snapshot.Tasks, task)
		snapshot.Executions = append(snapshot.Executions, x)
		snapshot.Steps = append(snapshot.Steps, agentprofile.Step{Execution: x.ID, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: fence})
		snapshot.Lifecycle.Queries = append(snapshot.Lifecycle.Queries, agentprofile.Query{
			ID: fmt.Sprintf("query-%04d", i), Session: "session", Space: task.Space, Owner: task.Owner,
			Version: 1, Status: "open", InputFence: fence,
			Input: bytes.Repeat([]byte("input "), 128), Answer: bytes.Repeat([]byte("answer "), 128),
		})
	}
	builder := newBuilder(ctx, e.index, e.options.MaxObjects)
	add := func(object codec.Object, err error) model.ObjectID {
		tb.Helper()
		if err != nil {
			tb.Fatal(err)
		}
		id, err := builder.add(object, storage.KindObject)
		if err != nil {
			tb.Fatal(err)
		}
		return id
	}
	apply := func(space model.SpaceID, root model.ObjectID, mutations ...state.Mutation) model.ObjectID {
		tb.Helper()
		delta, err := state.Apply(ctx, builder, space, root, mutations)
		if err != nil {
			tb.Fatal(err)
		}
		for _, object := range delta.Objects {
			add(object, nil)
		}
		return delta.Root
	}
	image, err := agentprofile.EncodeRecordImage(snapshot)
	if err != nil {
		tb.Fatal(err)
	}
	profile, _, err := storeAgentRecords(ctx, builder, "image-space", "owner", "", image)
	if err != nil {
		tb.Fatal(err)
	}
	profileBytes := len(builder.staged[profile])
	e.roots.LiveControl = apply(systemSpace, "", state.Mutation{Key: agentPartitionKey("image-space", "owner"), Value: profile})
	payload := add(makeValue("image-space", api.Value{Kind: api.ValueBytes, Data: []byte("observed tool output")}))
	domain := apply("image-space", "", state.Mutation{Key: []byte("source"), Value: payload})
	e.roots.DomainStates = apply(systemSpace, "", state.Mutation{Key: []byte("image-space"), Value: domain})
	e.rootSet = add(makeRootSet(e.roots))
	if err := absorbWorkPlan(ctx, e, newBuilder(ctx, newObjectIndex(), DefaultMaxObjects), &commitPlan{builder: builder, roots: e.roots}); err != nil {
		tb.Fatal(err)
	}
	e.publication.PostRootSet = asDigest(e.rootSet)
	e.history[1] = commitEnvelope{fence: fence, rootSet: e.rootSet, space: "image-space", principal: "owner"}

	authority := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	unit := api.WorkUnit{Kind: "checkpoint"}
	for i := 0; i < 2; i++ {
		x := snapshot.Executions[i]
		unit.Steps = append(unit.Steps, workImageControl(tb, agentprofile.ControlCommand{
			GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Binding: authority, Status: "active", Evidence: payload}},
		}))
		if mixed && i == 0 {
			observation := knowledge.Observation{Key: model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "image-space", Fence: fence}}, Payload: payload, Sequence: 1, Label: "tool result"}
			observation.ID = knowledge.ObservationID(observation)
			step, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &observation})
			if err != nil {
				tb.Fatal(err)
			}
			unit.Steps = append(unit.Steps, step)
		}
		key := fmt.Sprintf("next-%d", i)
		body, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte(key), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("resume")}}}})
		if err != nil {
			tb.Fatal(err)
		}
		action := agentprofile.PendingAction{Key: key, Owner: x.ID, Generation: 1, Status: agentprofile.ActionPending,
			Request:    model.RequestKey{Space: "image-space", Principal: "owner", RequestID: model.RequestID(key)},
			InputFence: fence, CommandDigest: agentprofile.ExactRequestDigest(body)}
		unit.Steps = append(unit.Steps, workImageControl(tb, agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{
			Intent: agentprofile.ActionIntent{Action: action, Method: api.MethodMutateState, ExactCommand: body, Authority: authority},
			Continuation: agentprofile.Continuation{Execution: x.ID, Version: 1, Incarnation: 1, PendingKey: key, PendingGeneration: 1, InputFence: fence, Phase: "pending-render",
				Position: &agentprofile.ContinuationPosition{Space: "image-space", Principal: "owner", Step: 1, AdapterVersion: "image-test/v1", NextActions: []agentprofile.PendingActionRef{{Key: key, Generation: 1}}}},
			Authority: authority,
		}}))
	}
	return e, workImageRequest(tb, unit), profileBytes
}

func workImagePrepare(tb testing.TB, ctx context.Context, e *Engine, req api.CommitRequest, cached bool) (*commitPlan, error) {
	tb.Helper()
	digest, err := CommandDigest(req)
	if err != nil {
		tb.Fatal(err)
	}
	if cached {
		return e.prepareWork(ctx, req, digest, workImageTx, 2, nil)
	}
	return prepareWorkWithoutImage(ctx, e, req, digest, workImageTx, 2, nil)
}

func assertWorkImagePlansEqual(tb testing.TB, got, want *commitPlan) {
	tb.Helper()
	if got == nil || want == nil || len(got.builder.objects) == 0 || len(got.builder.objects) != len(want.builder.objects) {
		tb.Fatal("missing plan or different object counts")
	}
	for i, object := range got.builder.objects {
		expected := want.builder.objects[i]
		if object.id != expected.id || object.kind != expected.kind || !bytes.Equal(object.encoded, expected.encoded) {
			tb.Fatalf("ordered object %d differs: %s vs %s", i, object.id, expected.id)
		}
	}
	if got.roots != want.roots || got.rootSet != want.rootSet || got.stateRoot != want.stateRoot || got.receipt != want.receipt {
		tb.Fatal("root or receipt identities differ")
	}
	if !reflect.DeepEqual(got.transaction, want.transaction) {
		tb.Fatal("transactions differ")
	}
	if !reflect.DeepEqual(got.event, want.event) || !reflect.DeepEqual(got.profileEvents, want.profileEvents) || !reflect.DeepEqual(got.profileEventAliases, want.profileEventAliases) {
		tb.Fatal("event projections differ")
	}
}

func TestWorkAgentImagePreparationEquivalent(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, events := range []bool{false, true} {
			t.Run(fmt.Sprintf("mixed=%t/events=%t", mixed, events), func(t *testing.T) {
				e, req, _ := workImageFixture(t, 8, mixed, events)
				cached, err := workImagePrepare(t, context.Background(), e, req, true)
				if err != nil {
					t.Fatal(err)
				}
				uncached, err := workImagePrepare(t, context.Background(), e, req, false)
				if err != nil {
					t.Fatal(err)
				}
				assertWorkImagePlansEqual(t, cached, uncached)
				if cached.receipt == "" || cached.rootSet == "" || cached.roots.LiveControl == e.roots.LiveControl ||
					(cached.roots.ExecutionKnowledge != e.roots.ExecutionKnowledge) != mixed {
					t.Fatal("fixture did not exercise complete Work finalization")
				}
				ledger, _, found, err := loadAgentLedger(context.Background(), cached.builder, cached.roots.LiveControl, req.Space, req.Principal)
				if err != nil || !found {
					t.Fatalf("final ledger: found=%t err=%v", found, err)
				}
				s := ledger.Snapshot()
				if len(s.Actions) != 2 || len(s.Lifecycle.Continuations) != 2 || len(s.Lifecycle.Recovery.Authorities) != 2 {
					t.Fatal("sequential grants/checkpoints did not accumulate")
				}
				if mixed {
					c, err := ledger.GetContinuation("exec-0000")
					if err != nil || c.Position.KnowledgeSource == nil || c.Position.KnowledgeSource.ProducerTx != workImageTx {
						t.Fatalf("mixed Work lost private knowledge binding: %+v %v", c, err)
					}
				}
				object, err := readObject(context.Background(), cached.builder, cached.receipt)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := readReceipt(object)
				if err != nil || receipt.Key != req.Key() || receipt.TxID != workImageTx || receipt.LSN != 2 || receipt.StateRoot != cached.stateRoot {
					t.Fatalf("invalid final receipt: %+v %v", receipt, err)
				}
				// Replaying the recorded operations must take the same path too.
				object, err = readObject(context.Background(), cached.builder, receipt.Operations)
				if err != nil {
					t.Fatal(err)
				}
				recorded, err := readOperations(object)
				if err != nil {
					t.Fatal(err)
				}
				digest, _ := CommandDigest(req)
				replayed, err := e.prepareWork(context.Background(), req, digest, workImageTx, 2, &recorded)
				if err != nil {
					t.Fatal(err)
				}
				assertWorkImagePlansEqual(t, replayed, uncached)
			})
		}
	}
}

// Counts profile reads separately from radix lookups: a cache hit must still
// authenticate the root-to-profile mapping, but must not reread the snapshot.
type workImageReader struct {
	state.ObjectReader
	reads map[model.ObjectID]int
	fail  model.ObjectID
	err   error
}

func (r *workImageReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	r.reads[id]++
	if id == r.fail {
		return nil, r.err
	}
	return r.ObjectReader.ReadObject(ctx, id)
}

func workImageLoad(tb testing.TB, ctx context.Context, reader state.ObjectReader, root model.ObjectID) (*agentprofile.Ledger, model.ObjectID) {
	tb.Helper()
	ledger, id, found, err := loadAgentLedger(ctx, reader, root, "image-space", "owner")
	if err != nil || !found {
		tb.Fatalf("load: found=%t err=%v", found, err)
	}
	return ledger, id
}

func workImageLedgerBytes(tb testing.TB, ledger *agentprofile.Ledger) []byte {
	tb.Helper()
	image, err := agentprofile.EncodeRecordImage(ledger.Snapshot())
	if err != nil {
		tb.Fatal(err)
	}
	records, err := image.Records()
	if err != nil {
		tb.Fatal(err)
	}
	var raw []byte
	for _, record := range records {
		raw = append(raw, record.Key...)
		raw = append(raw, record.Raw...)
	}
	return raw
}

func TestWorkAgentImageRestoresIndependentLedgers(t *testing.T) {
	e, _, _ := workImageFixture(t, 4, false, false)
	slot := &workAgentImage{}
	ctx := context.WithValue(context.Background(), workAgentImageKey{}, slot)
	reader := &workImageReader{ObjectReader: e.index, reads: map[model.ObjectID]int{}}
	first, id := workImageLoad(t, ctx, reader, e.roots.LiveControl)
	want := workImageLedgerBytes(t, first)
	second, _ := workImageLoad(t, ctx, reader, e.roots.LiveControl)
	if first == second || reader.reads[id] != 1 || reader.reads[e.roots.LiveControl] != 2 {
		t.Fatal("loads did not use independent cached restores with root lookups")
	}
	_, err := first.ApplyCommand(model.RequestKey{Space: "image-space", Principal: "owner", RequestID: "mutate"},
		agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: "task-0000", ExpectedEpoch: 1, NewGoal: "changed"}})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(workImageLedgerBytes(t, first), want) {
		t.Fatal("mutation did not change first ledger")
	}
	owned := second.Snapshot()
	clear(owned.Lifecycle.Queries[0].Input)
	third, _ := workImageLoad(t, ctx, reader, e.roots.LiveControl)
	for _, ledger := range []*agentprofile.Ledger{second, third} {
		if !bytes.Equal(workImageLedgerBytes(t, ledger), want) {
			t.Fatal("restored ledger or image shares mutable state")
		}
	}
	if reader.reads[id] != 1 {
		t.Fatal("warm restores reread profile")
	}
}

func TestWorkAgentImageResolvesRootBeforeReuse(t *testing.T) {
	e, req, _ := workImageFixture(t, 4, false, false)
	slot := &workAgentImage{}
	ctx := context.WithValue(context.Background(), workAgentImageKey{}, slot)
	view := e.workView()
	oldRoot := view.roots.LiveControl
	before, oldID := workImageLoad(t, ctx, view.index, oldRoot)
	wantOld := workImageLedgerBytes(t, before)
	unit, err := api.DecodeWorkUnit(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	step := unit.Steps[0]
	sub := api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID, Method: step.Method, Body: step.Body}
	digest, _ := CommandDigest(sub)
	plan, err := view.prepareAgent(ctx, sub, digest, workImageTx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate := *slot
	if candidate.id == oldID || view.roots.LiveControl != oldRoot || view.index.contains(candidate.id) {
		t.Fatal("candidate was not retained privately before absorption")
	}
	reader := &workImageReader{ObjectReader: view.index, reads: map[model.ObjectID]int{}}
	got, id := workImageLoad(t, ctx, reader, oldRoot)
	if id != oldID || reader.reads[oldID] != 1 || !bytes.Equal(workImageLedgerBytes(t, got), wantOld) {
		t.Fatal("unabsorbed candidate masqueraded as installed state")
	}
	*slot = candidate
	if err := absorbWorkPlan(ctx, view, newBuilder(ctx, e.index, e.options.MaxObjects), plan); err != nil {
		t.Fatal(err)
	}
	got, id = workImageLoad(t, ctx, reader, view.roots.LiveControl)
	if id != candidate.id || reader.reads[id] != 0 || len(got.Snapshot().Lifecycle.Recovery.Authorities) != 1 {
		t.Fatal("absorbed candidate did not hit its retained image")
	}
	// The old root remains readable after absorption, and evicts the latest image.
	got, id = workImageLoad(t, ctx, reader, oldRoot)
	if id != oldID || reader.reads[oldID] != 2 || !bytes.Equal(workImageLedgerBytes(t, got), wantOld) {
		t.Fatal("historical root incorrectly used latest profile")
	}
	workImageLoad(t, ctx, reader, view.roots.LiveControl)
	if reader.reads[candidate.id] != 1 || slot.id != candidate.id {
		t.Fatal("single slot did not replace the historical image")
	}
	// An absent partition/root cannot borrow the latest image.
	for _, scope := range []struct {
		root  model.ObjectID
		space model.SpaceID
		owner model.PrincipalID
	}{{"", req.Space, req.Principal}, {view.roots.LiveControl, "other", req.Principal}, {view.roots.LiveControl, req.Space, "other"}} {
		ledger, id, found, err := loadAgentLedger(ctx, reader, scope.root, scope.space, scope.owner)
		if err != nil || found || id != "" || len(ledger.Snapshot().Tasks) != 0 {
			t.Fatalf("missing partition borrowed image: %+v found=%t err=%v", scope, found, err)
		}
	}
	// Even with a matching ID, both scope fields are part of the cache key.
	for _, change := range []func(*workAgentImage){
		func(s *workAgentImage) { s.space = "other" },
		func(s *workAgentImage) { s.owner = "other" },
	} {
		*slot = candidate
		change(slot)
		n := reader.reads[candidate.id]
		workImageLoad(t, ctx, reader, view.roots.LiveControl)
		if reader.reads[candidate.id] != n+1 || slot.space != req.Space || slot.owner != req.Principal {
			t.Fatal("scope mismatch reused image")
		}
	}
	*slot = candidate
	reader.fail, reader.err = view.roots.LiveControl, errors.New("root read failed")
	if _, _, _, err := loadAgentLedger(ctx, reader, view.roots.LiveControl, req.Space, req.Principal); !errors.Is(err, reader.err) {
		t.Fatalf("cache bypassed failed root lookup: %v", err)
	}
	reader.fail = oldID
	if _, _, _, err := loadAgentLedger(ctx, reader, oldRoot, req.Space, req.Principal); !errors.Is(err, reader.err) || slot.id != candidate.id {
		t.Fatalf("failed historical load changed cache: %v", err)
	}
}

func TestWorkAgentImageFailureIsolation(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			e, req, _ := workImageFixture(t, 8, true, true)
			beforeRoots, beforeRootSet, beforePublication := e.roots, e.rootSet, e.publication
			beforeObjects := make(map[model.ObjectID][]byte, len(e.index.staged))
			for id, raw := range e.index.staged {
				beforeObjects[id] = bytes.Clone(raw)
			}
			beforeEntries, beforeHistory := maps.Clone(e.index.entries), maps.Clone(e.history)
			oracle, err := workImagePrepare(t, context.Background(), e, req, false)
			if err != nil {
				t.Fatal(err)
			}
			unit, err := api.DecodeWorkUnit(req.Body)
			if err != nil {
				t.Fatal(err)
			}
			// A valid boundary with a reducer failure after every successful step.
			unit.Steps = append(unit.Steps, workImageControl(t, agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "exec-0000", ExpectedIncarnation: 99}}))
			bad := workImageRequest(t, unit)
			outer := &workAgentImage{}
			outerCtx := context.WithValue(context.Background(), workAgentImageKey{}, outer)
			workImageLoad(t, outerCtx, e.index, e.roots.LiveControl)
			outerBefore := *outer
			// Uncached comparison deliberately has no image slot at any level.
			ctx := context.Background()
			if cached {
				ctx = outerCtx
			}
			if plan, err := workImagePrepare(t, ctx, e, bad, cached); !errors.Is(err, agentprofile.ErrConflict) || plan != nil {
				t.Fatalf("expected late reducer failure: plan=%v err=%v", plan != nil, err)
			}
			if !reflect.DeepEqual(*outer, outerBefore) {
				t.Fatal("failed Work overwrote caller's image slot")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if plan, err := workImagePrepare(t, cancelled, e, req, cached); !errors.Is(err, context.Canceled) || plan != nil {
				t.Fatalf("canceled Work: plan=%v err=%v", plan != nil, err)
			}
			if e.roots != beforeRoots || e.rootSet != beforeRootSet || e.publication != beforePublication ||
				!reflect.DeepEqual(e.index.staged, beforeObjects) || !reflect.DeepEqual(e.index.entries, beforeEntries) ||
				!reflect.DeepEqual(e.history, beforeHistory) || len(e.events) != 0 || len(e.envelopes) != 0 {
				t.Fatal("private preparation changed parent state")
			}
			retry, err := workImagePrepare(t, ctx, e, req, cached)
			if err != nil {
				t.Fatal(err)
			}
			assertWorkImagePlansEqual(t, retry, oracle)
			if !reflect.DeepEqual(*outer, outerBefore) {
				t.Fatal("successful Work overwrote caller's image slot")
			}
		})
	}
}

func TestWorkAgentImageRejectedAgentPlanDoesNotRetainCandidate(t *testing.T) {
	e, req, _ := workImageFixture(t, 4, false, false)
	ctx := context.WithValue(context.Background(), workAgentImageKey{}, &workAgentImage{})
	view := e.workView()
	before, oldID := workImageLoad(t, ctx, view.index, view.roots.LiveControl)
	want := workImageLedgerBytes(t, before)
	unit, err := api.DecodeWorkUnit(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	sub := api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID, Method: unit.Steps[0].Method, Body: unit.Steps[0].Body}
	digest, _ := CommandDigest(sub)
	oracle, err := view.prepareAgent(context.Background(), sub, digest, workImageTx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Alter genuine recorded operations to reject after reduction, snapshot
	// encoding and radix construction, but before retaining the candidate.
	var recorded operationsRecord
	for _, object := range oracle.builder.objects {
		if object.kind == storage.KindStateOps {
			decoded, err := codec.DecodeObject(object.encoded)
			if err != nil {
				t.Fatal(err)
			}
			recorded, err = readOperations(decoded)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if recorded.BeforeRoot != view.roots.LiveControl || recorded.AfterRoot == recorded.BeforeRoot {
		t.Fatal("fixture lacks a real profile transition")
	}
	recorded.BeforeRoot = ""
	root, count := view.roots, view.index.count()
	plan, err := view.prepareAgent(ctx, sub, digest, workImageTx, 2, &recorded)
	if !errors.Is(err, ErrInvalidStateData) || plan != nil {
		t.Fatalf("expected recorded operations mismatch: plan=%v err=%v", plan != nil, err)
	}
	slot := ctx.Value(workAgentImageKey{}).(*workAgentImage)
	if slot.id != oldID || view.roots != root || view.index.count() != count {
		t.Fatal("rejected candidate replaced retained image or installed state")
	}
	restored, _ := workImageLoad(t, ctx, view.index, root.LiveControl)
	if !bytes.Equal(workImageLedgerBytes(t, restored), want) {
		t.Fatal("rejected reducer mutated the retained image")
	}
	retry, err := view.prepareAgent(ctx, sub, digest, workImageTx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertWorkImagePlansEqual(t, retry, oracle)
	if slot.id == oldID {
		t.Fatal("successful retry did not retain its candidate")
	}
}

func recordWorkSeed(tb testing.TB) (*Engine, context.Context, api.CommitRequest, *workAgentImage, model.ObjectID, model.ObjectID, *commitPlan) {
	tb.Helper()
	e, req, _ := workImageFixture(tb, 4, false, false)
	unit, err := api.DecodeWorkUnit(req.Body)
	if err != nil {
		tb.Fatal(err)
	}
	sub := api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID, Method: unit.Steps[0].Method, Body: unit.Steps[0].Body}
	view := e.workView()
	oldRoot := view.roots.LiveControl
	oldID, found, err := state.Get(context.Background(), view.index, systemSpace, oldRoot, agentPartitionKey(req.Space, req.Principal))
	if err != nil || !found {
		tb.Fatalf("old profile: found=%t err=%v", found, err)
	}
	slot := &workAgentImage{}
	ctx := context.WithValue(context.Background(), workAgentImageKey{}, slot)
	digest, err := CommandDigest(sub)
	if err != nil {
		tb.Fatal(err)
	}
	plan, err := view.prepareAgent(ctx, sub, digest, workImageTx, 2, nil)
	if err != nil {
		tb.Fatal(err)
	}
	if slot.records.image.Size() == 0 {
		tb.Fatal("record catalog was not retained")
	}
	return view, ctx, sub, slot, oldRoot, oldID, plan
}

func TestWorkAgentRecordCatalogSkipsOnlyValidCacheReads(t *testing.T) {
	view, ctx, _, slot, oldRoot, oldID, plan := recordWorkSeed(t)
	candidate := *slot
	reader := &workImageReader{ObjectReader: view.index, reads: map[model.ObjectID]int{}}
	if _, _, _, err := loadAgentLedger(ctx, reader, oldRoot, "image-space", "owner"); err != nil {
		t.Fatal(err)
	}
	if reader.reads[oldID] == 0 {
		t.Fatal("historical root incorrectly hit candidate record catalog")
	}
	*slot = candidate
	if err := absorbWorkPlan(ctx, view, newBuilder(ctx, view.index, view.options.MaxObjects), plan); err != nil {
		t.Fatal(err)
	}
	id, found, err := state.Get(ctx, view.index, systemSpace, view.roots.LiveControl, agentPartitionKey("image-space", "owner"))
	if err != nil || !found || id != slot.id {
		t.Fatalf("candidate profile mapping: id=%s found=%t err=%v", id, found, err)
	}
	catalog := slot.records
	reader.reads = map[model.ObjectID]int{}
	if _, _, _, err := loadAgentLedger(ctx, reader, view.roots.LiveControl, "image-space", "owner"); err != nil {
		t.Fatal(err)
	}
	if reader.reads[id] != 0 || reader.reads[catalog.root] != 0 {
		t.Fatalf("valid record cache reread profile or radix root: %v", reader.reads)
	}
	for key, object := range catalog.objects {
		if reader.reads[object] != 0 {
			t.Fatalf("valid record cache reread record key %x object %s", key, object)
		}
	}
	if len(reader.reads) == 0 {
		t.Fatal("cache skipped LiveControl root resolution")
	}

	reader.reads = map[model.ObjectID]int{}
	slot.id = oldID
	if _, _, _, err := loadAgentLedger(ctx, reader, view.roots.LiveControl, "image-space", "owner"); err != nil {
		t.Fatal(err)
	}
	if reader.reads[id] == 0 || reader.reads[catalog.root] == 0 {
		t.Fatalf("invalid profile ID reused record catalog: %v", reader.reads)
	}
}

func TestWorkAgentRecordCatalogRestoresIndependentLedgers(t *testing.T) {
	view, ctx, _, slot, _, _, plan := recordWorkSeed(t)
	if err := absorbWorkPlan(ctx, view, newBuilder(ctx, view.index, view.options.MaxObjects), plan); err != nil {
		t.Fatal(err)
	}
	reader := &workImageReader{ObjectReader: view.index, reads: map[model.ObjectID]int{}}
	first, id, found, err := loadAgentLedger(ctx, reader, view.roots.LiveControl, "image-space", "owner")
	if err != nil || !found || id != slot.id {
		t.Fatalf("first record restore: id=%s found=%t err=%v", id, found, err)
	}
	want := workImageLedgerBytes(t, first)
	second, _, found, err := loadAgentLedger(ctx, reader, view.roots.LiveControl, "image-space", "owner")
	if err != nil || !found || first == second {
		t.Fatalf("second record restore: found=%t same=%t err=%v", found, first == second, err)
	}
	if _, err := first.ApplyCommand(model.RequestKey{Space: "image-space", Principal: "owner", RequestID: "mutate"},
		agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: "task-0000", ExpectedEpoch: 1, NewGoal: "changed"}}); err != nil {
		t.Fatal(err)
	}
	got := workImageLedgerBytes(t, second)
	if !bytes.Equal(got, want) {
		t.Fatal("record restores share mutable ledger state")
	}
	if reader.reads[id] != 0 || reader.reads[slot.records.root] != 0 {
		t.Fatalf("warm record restore reread immutable records: %v", reader.reads)
	}
}

func TestWorkAgentRecordCatalogPreparationEquivalent(t *testing.T) {
	view, ctx, base, _, _, _, plan := recordWorkSeed(t)
	if err := absorbWorkPlan(ctx, view, newBuilder(ctx, view.index, view.options.MaxObjects), plan); err != nil {
		t.Fatal(err)
	}
	body, err := agentprofile.EncodeCommand(agentprofile.Command{
		RebaseGoal: &agentprofile.GoalRebase{Task: "task-0000", ExpectedEpoch: 1, NewGoal: "catalog"},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := api.CommitRequest{Space: base.Space, Principal: base.Principal, RequestID: "catalog-update", Method: api.MethodApplyAgent, Body: body}
	digest, err := CommandDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := view.prepareAgent(ctx, req, digest, model.TxID("44444444444444444444444444444444"), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	uncached, err := view.prepareAgent(context.Background(), req, digest, model.TxID("44444444444444444444444444444444"), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertWorkImagePlansEqual(t, cached, uncached)

	var recorded operationsRecord
	for _, object := range cached.builder.objects {
		if object.kind != storage.KindStateOps {
			continue
		}
		decoded, err := codec.DecodeObject(object.encoded)
		if err != nil {
			t.Fatal(err)
		}
		recorded, err = readOperations(decoded)
		if err != nil {
			t.Fatal(err)
		}
	}
	if recorded.AfterRoot == "" {
		t.Fatal("missing recorded operations")
	}
	replayed, err := view.prepareAgent(ctx, req, digest, model.TxID("44444444444444444444444444444444"), 2, &recorded)
	if err != nil {
		t.Fatal(err)
	}
	assertWorkImagePlansEqual(t, replayed, uncached)
}
