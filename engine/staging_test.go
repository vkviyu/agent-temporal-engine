package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func stagingEngine(t *testing.T) *Engine {
	t.Helper()
	o, err := (Options{}).checked()
	if err != nil {
		t.Fatal(err)
	}
	e := newEngine(o)
	e.publication = storage.Publication{Store: "staging-test", Generation: 1,
		Position:     storage.Position{Digest: storage.Digest{1}},
		PublishedEnd: storage.SegmentHeaderSize, Profile: storage.FileSyncExperimental}
	return e
}

func stagingRequest(t *testing.T, requestID, value string) api.CommitRequest {
	t.Helper()
	body, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{
		Key: []byte("key"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return api.CommitRequest{Space: "staging-space", Principal: "host", RequestID: model.RequestID(requestID),
		Method: api.MethodMutateState, Body: body}
}

func stagingPrepare(t *testing.T, e *Engine, request api.CommitRequest, txDigit string) (*commitPlan, *storage.PreparedTransaction) {
	t.Helper()
	digest, err := CommandDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := e.prepare(context.Background(), request, digest, model.TxID(strings.Repeat(txDigit, 32)), e.publication.Position.LSN+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := storage.PrepareTransaction(plan.transaction, e.options.Journal.Limits)
	if err != nil {
		t.Fatal(err)
	}
	return plan, p
}

// Install a published prefix in a bounded ReaderAt without opening a journal.
// Later staging must read this catalog and must never append to its source.
func stagingPublishedSeed(t *testing.T, e *Engine) (api.CommitRequest, *stagingReader) {
	t.Helper()
	request := stagingRequest(t, "published", "old")
	plan, prepared := stagingPrepare(t, e, request, "1")
	var data []byte
	for _, object := range plan.builder.objects {
		e.index.entries[object.id] = storage.RecordLocation{PayloadOffset: int64(len(data)), PayloadLength: uint32(len(object.encoded))}
		data = append(data, object.encoded...)
	}
	reader := &stagingReader{reader: bytes.NewReader(data)}
	e.index.source = reader
	e.roots, e.rootSet = plan.roots, plan.rootSet
	e.publication.Position, e.publication.PostRootSet = prepared.Position(), asDigest(plan.rootSet)
	e.publication.PublishedEnd += prepared.Size()
	e.envelopes[plan.transaction.TxID] = commitEnvelope{fence: e.publication.Fence(), rootSet: plan.rootSet}
	return request, reader
}

type stagingReader struct {
	reader *bytes.Reader
	closed bool
	reads  int
}

func (r *stagingReader) ReadAt(p []byte, offset int64) (int, error) {
	r.reads++
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	return r.reader.ReadAt(p, offset)
}

func stagingStateValue(t *testing.T, e *Engine) (model.ObjectID, api.Value) {
	t.Helper()
	ctx := context.Background()
	root, err := domainRoot(ctx, e.index, e.roots, "staging-space")
	if err != nil {
		t.Fatal(err)
	}
	id, found, err := state.Get(ctx, e.index, "staging-space", root, []byte("key"))
	if err != nil || !found {
		t.Fatalf("state missing: %v %v", found, err)
	}
	object, err := readObject(ctx, e.index, id)
	if err != nil {
		t.Fatal(err)
	}
	value, err := readValue(object)
	if err != nil {
		t.Fatal(err)
	}
	return id, value
}

func TestStagingViewReadsPredecessorAndKeepsPublishedStatePrivate(t *testing.T) {
	actual := stagingEngine(t)
	publishedRequest, reader := stagingPublishedSeed(t, actual)
	beforePublication, beforeRoots, beforeRootSet := actual.publication, actual.roots, actual.rootSet
	beforeEntries, beforeEnvelopes := len(actual.index.entries), len(actual.envelopes)
	beforeSourceSize := reader.reader.Len()
	view := actual.stagingView()
	if view.index.parent != actual.index || len(view.index.entries) != 0 || len(view.index.staged) != 0 || len(view.envelopes) != 0 {
		t.Fatal("view copied published history or lacks a private overlay")
	}
	if view.journal != actual.journal || !reflect.DeepEqual(view.options, actual.options) {
		t.Fatal("view lost its base configuration")
	}
	firstRequest := stagingRequest(t, "first", "new")
	first, preparedFirst := stagingPrepare(t, view, firstRequest, "2")
	if err := view.acceptStaged(first, preparedFirst); err != nil {
		t.Fatal(err)
	}
	valueID, value := stagingStateValue(t, view)
	if string(value.Data) != "new" {
		t.Fatalf("predecessor state unreadable: %+v", value)
	}
	firstProof, found, err := view.lookup(context.Background(), firstRequest.Key())
	if err != nil || !found || firstProof.Fence != view.publication.Fence() || firstProof.RootSet != first.rootSet {
		t.Fatalf("staged receipt proof: %+v %v %v", firstProof, found, err)
	}
	if _, found, err := view.lookup(context.Background(), publishedRequest.Key()); err != nil || !found {
		t.Fatalf("published receipt proof unavailable to view: %v %v", found, err)
	}
	secondRequest := stagingRequest(t, "second", "new")
	secondRequest.ExpectedFence = new(model.CommitFence)
	*secondRequest.ExpectedFence = view.publication.Fence()
	body, err := EncodeStateBatch(api.StateBatch{ExpectedRoot: &first.stateRoot, Operations: []api.StateMutation{{
		Key: []byte("key"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("new")},
		Condition: api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte("new")}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	secondRequest.Body = body
	second, preparedSecond := stagingPrepare(t, view, secondRequest, "3")
	if second.stateRoot != first.stateRoot {
		t.Fatal("second member did not apply to predecessor root")
	}
	for _, object := range second.builder.objects {
		if object.id == valueID {
			t.Fatal("same value was encoded again in the second member")
		}
	}
	if err := view.acceptStaged(second, preparedSecond); err != nil {
		t.Fatal(err)
	}
	if proof, found, err := view.lookup(context.Background(), firstRequest.Key()); err != nil || !found || proof != firstProof {
		t.Fatalf("first proof changed to the final batch position: %+v %v %v", proof, found, err)
	}
	if view.publication.Position != preparedSecond.Position() || view.publication.PublishedEnd != beforePublication.PublishedEnd+preparedFirst.Size()+preparedSecond.Size() || view.publication.Generation != beforePublication.Generation {
		t.Fatal("staged publication did not use prepared physical metadata")
	}
	// Returned bytes and the retained plan cannot mutate the overlay's objects.
	encoded, err := view.index.ReadObject(context.Background(), valueID)
	if err != nil {
		t.Fatal(err)
	}
	encoded[0] ^= 0xff
	for _, object := range first.builder.objects {
		if object.id == valueID {
			object.encoded[0] ^= 0xff
		}
	}
	if _, err := view.index.ReadObject(context.Background(), valueID); err != nil {
		t.Fatalf("owned staging bytes were mutated externally: %v", err)
	}
	if _, err := actual.index.ReadObject(context.Background(), valueID); !errors.Is(err, ErrMissingObject) {
		t.Fatalf("unpublished value escaped the private view: %v", err)
	}
	if actual.publication != beforePublication || actual.roots != beforeRoots || actual.rootSet != beforeRootSet || len(actual.index.entries) != beforeEntries || len(actual.envelopes) != beforeEnvelopes || actual.index.staged != nil {
		t.Fatal("discarding the view would change published engine state")
	}
	if _, value := stagingStateValue(t, actual); string(value.Data) != "old" || reader.reader.Len() != beforeSourceSize || reader.reads == 0 {
		t.Fatal("view changed its published object source")
	}
}

func TestStagingObjectBudgetIncludesPublishedAndEarlierMembers(t *testing.T) {
	actual := stagingEngine(t)
	stagingPublishedSeed(t, actual)
	view := actual.stagingView()
	plan, prepared := stagingPrepare(t, view, stagingRequest(t, "first", "new"), "2")
	if err := view.acceptStaged(plan, prepared); err != nil {
		t.Fatal(err)
	}
	count := actual.index.count() + uint64(len(plan.builder.objects))
	if view.index.count() != count {
		t.Fatalf("wrong shared object budget: got %d want %d", view.index.count(), count)
	}
	builder := newBuilder(context.Background(), view.index, count)
	existing, err := makeValue("staging-space", api.Value{Kind: api.ValueBytes, Data: []byte("new")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.add(existing, storage.KindObject); err != nil || len(builder.objects) != 0 {
		t.Fatalf("deduplication consumed a full object budget: %v", err)
	}
	fresh, err := makeValue("staging-space", api.Value{Kind: api.ValueBytes, Data: []byte("over-budget")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.add(fresh, storage.KindObject); !errors.Is(err, storage.ErrLimit) {
		t.Fatalf("earlier member objects escaped the quota: %v", err)
	}
	if actual.index.count() == view.index.count() {
		t.Fatal("staged catalog was installed in the published index")
	}
}

func TestStagingIndexVerifiesBytesAndUsesParentReaderLifetime(t *testing.T) {
	actual := stagingEngine(t)
	_, source := stagingPublishedSeed(t, actual)
	oldValue, _ := stagingStateValue(t, actual)
	view := actual.stagingView()
	plan, prepared := stagingPrepare(t, view, stagingRequest(t, "first", "new"), "2")
	if err := view.acceptStaged(plan, prepared); err != nil {
		t.Fatal(err)
	}
	newValue, _ := stagingStateValue(t, view)
	source.closed = true
	if _, err := view.index.ReadObject(context.Background(), oldValue); !errors.Is(err, ErrObjectIO) || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("view retained an independent published reader: %v", err)
	}
	if _, err := view.index.ReadObject(context.Background(), newValue); err != nil {
		t.Fatalf("staged read unnecessarily touched a closed journal reader: %v", err)
	}
	view.index.staged[newValue][0] ^= 0xff
	if _, err := view.index.ReadObject(context.Background(), newValue); !errors.Is(err, ErrStoredObject) {
		t.Fatalf("staged canonical identity was not checked: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := view.index.ReadObject(ctx, newValue); !errors.Is(err, context.Canceled) {
		t.Fatalf("staged read ignored cancellation: %v", err)
	}
}

func TestCheckPreparedUsesExactSizeAndNormalizedJournalLimits(t *testing.T) {
	e := stagingEngine(t)
	plan, prepared := stagingPrepare(t, e, stagingRequest(t, "quota", "value"), "1")
	initial := e.publication
	e.options.Journal.MaxPublishedBytes = initial.PublishedEnd + prepared.Size()
	e.options.Journal.MaxPublishedTransactions = 1
	if err := e.checkPrepared(prepared); err != nil {
		t.Fatalf("exact budget rejected: %v", err)
	}
	e.options.Journal.MaxPublishedBytes--
	if err := e.checkPrepared(prepared); !errors.Is(err, storage.ErrLimit) {
		t.Fatalf("one-byte encoded overflow accepted: %v", err)
	}
	e.options.Journal = storage.JournalOptions{}
	e.publication.PublishedEnd = storage.DefaultMaxPublishedBytes - prepared.Size()
	if err := e.checkPrepared(prepared); err != nil {
		t.Fatalf("default byte budget was not normalized: %v", err)
	}
	e.publication.PublishedEnd++
	if err := e.checkPrepared(prepared); !errors.Is(err, storage.ErrLimit) {
		t.Fatalf("default byte budget overflow accepted: %v", err)
	}
	e.publication = initial
	plan.transaction.LSN = storage.DefaultMaxPublishedTransactions + 1
	overTransactions, err := storage.PrepareTransaction(plan.transaction, storage.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.checkPrepared(overTransactions); !errors.Is(err, storage.ErrLimit) {
		t.Fatalf("default transaction budget overflow accepted: %v", err)
	}
	for _, mutate := range []func(*storage.Publication){
		func(p *storage.Publication) { p.Generation = math.MaxUint64 },
		func(p *storage.Publication) { p.Position.LSN = math.MaxUint64 },
		func(p *storage.Publication) { p.Position.LSN = storage.DefaultMaxPublishedTransactions },
		func(p *storage.Publication) { p.PublishedEnd = storage.DefaultMaxPublishedBytes + 1 },
	} {
		e.publication = initial
		mutate(&e.publication)
		if err := e.checkPrepared(prepared); !errors.Is(err, storage.ErrLimit) {
			t.Fatalf("overflowing publication accepted: %+v %v", e.publication, err)
		}
	}
	e.publication = initial
	e.publication.Generation = math.MaxUint64 - 1
	view := e.stagingView()
	first, firstPrepared := stagingPrepare(t, view, stagingRequest(t, "quota", "value"), "1")
	if err := view.acceptStaged(first, firstPrepared); err != nil {
		t.Fatal(err)
	}
	second, secondPrepared := stagingPrepare(t, view, stagingRequest(t, "second", "second"), "2")
	if err := view.acceptStaged(second, secondPrepared); err != nil {
		t.Fatalf("members incorrectly consumed multiple physical generations: %v", err)
	}
}

func TestStagingDefaultConstructorDoesNotShareMutableCatalogs(t *testing.T) {
	e := stagingEngine(t)
	view := e.stagingView()
	if e.index.parent != nil || e.index.staged != nil || view.index.parent != e.index || view.index.source != nil || view.options.MaxObjects != DefaultMaxObjects {
		t.Fatal("default catalogs or options are invalid")
	}
	plan, prepared := stagingPrepare(t, view, stagingRequest(t, "first", "value"), "1")
	if err := view.acceptStaged(plan, prepared); err != nil {
		t.Fatal(err)
	}
	if e.index.count() != 0 || len(e.envelopes) != 0 || !reflect.DeepEqual(e.roots, model.RootSet{}) {
		t.Fatal("staging changed a default engine")
	}
	for _, object := range plan.builder.objects {
		if _, err := codec.VerifyObject(object.id, view.index.staged[object.id]); err != nil {
			t.Fatal(err)
		}
	}
}
