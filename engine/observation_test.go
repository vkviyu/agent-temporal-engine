package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func r13Drain(c *observation.Collector) []observation.Event {
	events := make([]observation.Event, observation.MaxCapacity)
	return events[:c.Drain(events)]
}

func r13Event(t *testing.T, events []observation.Event, kind observation.Kind) observation.Event {
	t.Helper()
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	t.Fatalf("missing kind %v in %+v", kind, events)
	return observation.Event{}
}

func TestR13CommitStagesReadAndPrivateMaintenance(t *testing.T) {
	c, _ := observation.New(256)
	e, _ := coordinatorStore(t, Options{Observation: c})
	r13Drain(c)
	req := coordinatorRequest(t, "r13", "key", "value", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, t.Context(), req)))
	events := r13Drain(c)
	var stages []observation.Kind
	for _, event := range events {
		if event.Kind == observation.AdmissionWait || event.Kind == observation.Prepare || event.Kind == observation.P1 || event.Kind == observation.Commit {
			stages = append(stages, event.Kind)
			if event.Duration <= 0 || event.Outcome != observation.Success {
				t.Fatal(event)
			}
		}
	}
	want := []observation.Kind{observation.AdmissionWait, observation.Prepare, observation.P1, observation.Commit}
	if fmt.Sprint(stages) != fmt.Sprint(want) {
		t.Fatal(stages)
	}
	prep, p1 := r13Event(t, events, observation.Prepare), r13Event(t, events, observation.P1)
	if prep.Count != 1 || prep.Bytes == 0 || prep.Bytes != p1.Bytes {
		t.Fatal(prep, p1)
	}
	view, err := e.OpenView(t.Context(), api.OpenViewRequest{Space: req.Space, Principal: req.Principal})
	if err != nil {
		t.Fatal(err)
	}
	r13Drain(c)
	entries, err := e.GetViewEntries(t.Context(), api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 {
		t.Fatal(entries, err)
	}
	events = r13Drain(c)
	scan := r13Event(t, events, observation.Scan)
	read := r13Event(t, events, observation.Read)
	if scan.Count == 0 || scan.Bytes == 0 || read.Count < scan.Count || read.Bytes < scan.Bytes {
		t.Fatal(scan, read)
	}
	if err := e.CloseView(t.Context(), api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CreateRecoveryCheckpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CompactAuditFull(t.Context()); err != nil {
		t.Fatal(err)
	}
	events = r13Drain(c)
	r13Event(t, events, observation.Checkpoint)
	compact := r13Event(t, events, observation.Compaction)
	if compact.Bytes == 0 {
		t.Fatal(compact)
	}
	for _, event := range events {
		if event.Kind == observation.P1 || event.Kind == observation.Prepare || event.Kind == observation.Commit {
			t.Fatal("audit replay manufactured external publication", event)
		}
	}
}

func TestR13AdmissionWaitIncludesBlockedPublisher(t *testing.T) {
	c, _ := observation.New(256)
	e, d := coordinatorStore(t, Options{Observation: c, Batch: BatchOptions{MaxCount: 1}})
	gate := newCoordinatorGate()
	d.arm("sync", gate)
	first := coordinatorCommit(e, t.Context(), coordinatorRequest(t, "first", "a", "v", api.ValueCondition{}))
	coordinatorWait(t, gate.entered)
	r13Drain(c)
	req := coordinatorRequest(t, "queued", "b", "v", api.ValueCondition{})
	second := coordinatorCommit(e, t.Context(), req)
	coordinatorQueued(t, e, req)
	e.admission.mu.Lock()
	queuedAt := e.admission.flights[req.Key()].queuedAt
	e.admission.mu.Unlock()
	releasedAt := time.Now()
	gate.unblock()
	coordinatorAck(t, coordinatorWait(t, first))
	coordinatorAck(t, coordinatorWait(t, second))
	event := r13Event(t, r13Drain(c), observation.AdmissionWait)
	if event.Duration < releasedAt.Sub(queuedAt) {
		t.Fatal("queue wait did not include real publisher stall", event)
	}
}

func TestR13StoppedConsumerDoesNotBlockCommitReadClose(t *testing.T) {
	c, _ := observation.New(1)
	e, _ := coordinatorStore(t, Options{Observation: c}) // Create fills the mailbox.
	done := make(chan error, 1)
	req := coordinatorRequest(t, "no-consumer", "k", "v", api.ValueCondition{})
	go func() {
		result, err := e.Commit(context.Background(), req)
		if err == nil && result.Code != api.CodeDurableAck {
			err = fmt.Errorf("code %v", result.Code)
		}
		if err == nil {
			_, err = e.GetState(context.Background(), api.ReadStateRequest{Space: req.Space, Key: []byte("k")})
		}
		if err == nil {
			err = e.Close()
		}
		done <- err
	}()
	if err := coordinatorWait(t, done); err != nil {
		t.Fatal(err)
	}
	full, _ := c.Drops()
	if full == 0 {
		t.Fatal("fixture never filled mailbox")
	}
}

func TestR13PublicationUnknownAndCorruptionFence(t *testing.T) {
	t.Run("unknown", func(t *testing.T) {
		c, _ := observation.New(128)
		e, d := coordinatorStore(t, Options{Observation: c})
		g := newCoordinatorGate()
		g.err = errors.New("private IO details must not enter events")
		d.arm("sync", g)
		result := coordinatorCommit(e, t.Context(), coordinatorRequest(t, "unknown", "k", "v", api.ValueCondition{}))
		coordinatorWait(t, g.entered)
		g.unblock()
		out := coordinatorWait(t, result)
		if !errors.Is(out.err, storage.ErrCommitUnknown) {
			t.Fatal(out)
		}
		events := r13Drain(c)
		for _, kind := range []observation.Kind{observation.P1, observation.Fence, observation.Commit} {
			if event := r13Event(t, events, kind); event.Outcome != observation.Unknown {
				t.Fatal(event)
			}
		}
	})
	t.Run("corruption", func(t *testing.T) {
		c, _ := observation.New(128)
		e, _ := coordinatorStore(t, Options{Observation: c})
		req := coordinatorRequest(t, "corrupt", "k", "v", api.ValueCondition{})
		coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, t.Context(), req)))
		// Break the published object locator; the real public read must fence.
		e.index.mu.Lock()
		delete(e.index.entries, e.roots.DomainStates)
		e.index.mu.Unlock()
		r13Drain(c)
		_, err := e.GetState(t.Context(), api.ReadStateRequest{Space: req.Space, Key: []byte("k")})
		if !errors.Is(err, storage.ErrStoreFenced) {
			t.Fatal(err)
		}
		event := r13Event(t, r13Drain(c), observation.Fence)
		if event.Outcome != observation.Corruption {
			t.Fatal(event)
		}
		_, err = e.Commit(t.Context(), coordinatorRequest(t, "after-fence", "k", "v", api.ValueCondition{}))
		if !errors.Is(err, storage.ErrStoreFenced) {
			t.Fatal(err)
		}
	})
}

func TestR13CheckpointHitSuffixAndFullReplay(t *testing.T) {
	c, _ := observation.New(256)
	e, d := coordinatorStore(t, Options{Observation: c})
	req := coordinatorRequest(t, "before", "k", "v", api.ValueCondition{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, t.Context(), req)))
	if _, err := e.CreateRecoveryCheckpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, t.Context(), coordinatorRequest(t, "suffix", "k", "v2", api.ValueCondition{}))))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		r13Drain(c)
		opened, report, err := OpenExisting(d, Options{Observation: c, DisableRecoveryCheckpoints: full})
		if err != nil {
			t.Fatal(err)
		}
		event := r13Event(t, r13Drain(c), observation.Open)
		certified, replayed := uint64(1), uint64(1)
		if full {
			certified, replayed = 0, 2
		}
		if event.Outcome != observation.Success || event.Count != 2 || event.Bytes != report.PublishedBytes ||
			event.Certified != certified || event.Replayed != replayed || event.CheckpointHit == full {
			t.Fatal(event, report)
		}
		if err := opened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Close first selects on coordinator completion, then on outstanding pins.
type r13PinWaitingContext struct {
	context.Context
	waiting chan struct{}
	calls   int
}

func (c *r13PinWaitingContext) Done() <-chan struct{} {
	c.calls++
	if c.calls == 2 {
		close(c.waiting)
	}
	return c.Context.Done()
}

func TestR13PinWaitCanceledAndDrained(t *testing.T) {
	c, _ := observation.New(128)
	e, _ := coordinatorStore(t, Options{Observation: c})
	_, release, err := e.beginReadCut(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	waiting := &r13PinWaitingContext{Context: ctx, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- e.CloseContext(waiting) }()
	coordinatorWait(t, waiting.waiting)
	cancel()
	if err := coordinatorWait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	event := r13Event(t, r13Drain(c), observation.PinWait)
	if event.Outcome != observation.Canceled || event.Count != 1 || event.Duration <= 0 {
		t.Fatal(event)
	}
	// Force the second close into the actual view wait before releasing the pin.
	waiting = &r13PinWaitingContext{Context: context.Background(), waiting: make(chan struct{})}
	go func() { done <- e.CloseContext(waiting) }()
	coordinatorWait(t, waiting.waiting)
	release()
	if err := coordinatorWait(t, done); err != nil {
		t.Fatal(err)
	}
	event = r13Event(t, r13Drain(c), observation.PinWait)
	if event.Outcome != observation.Success || event.Count != 1 {
		t.Fatal(event)
	}
}

func TestR13CacheAccounting(t *testing.T) {
	c, _ := observation.New(256)
	e := newEngine(Options{Observation: c})
	e.traceEvidence.get("missing")
	e.traceProfiles.get(taskTraceProfileKey{})
	for i := 0; i < 33; i++ {
		key := fmt.Sprint(i)
		e.traceEvidence.put(key, &traceEvidence{ids: []model.EventID{"id"}})
		e.traceProfiles.put(taskTraceProfileKey{object: model.ObjectID(key)}, &taskTraceProfile{bytes: 128})
	}
	e.traceEvidence.get("32")
	e.traceProfiles.get(taskTraceProfileKey{object: "32"})
	events := r13Drain(c)
	for _, kind := range []observation.CacheKind{observation.TraceEvidence, observation.TraceProfile} {
		counts := map[observation.Outcome]int{}
		var last observation.Event
		for _, event := range events {
			if event.Cache != kind {
				continue
			}
			counts[event.Outcome]++
			last = event
		}
		if counts[observation.Miss] != 1 || counts[observation.Eviction] != 1 || counts[observation.Hit] != 1 ||
			counts[observation.Insert] != 33 || last.Count != 32 {
			t.Fatal(kind, counts, last)
		}
		want := e.traceEvidence.bytes
		if kind == observation.TraceProfile {
			want = e.traceProfiles.bytes
		}
		if last.RetainedEstimate != uint64(want) {
			t.Fatal(last, want)
		}
	}
}

func TestR13DisabledEngineAndFailedOpen(t *testing.T) {
	for _, c := range []*observation.Collector{nil, {}} {
		e, _ := coordinatorStore(t, Options{Observation: c})
		req := coordinatorRequest(t, "disabled", "k", "v", api.ValueCondition{})
		coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, t.Context(), req)))
		if _, err := e.GetState(t.Context(), api.ReadStateRequest{Space: req.Space, Key: []byte("k")}); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if len(r13Drain(c)) != 0 {
			t.Fatal("disabled engine emitted")
		}
	}
	c, _ := observation.New(8)
	opened, _, err := OpenExisting(nil, Options{Observation: c, MaxObjects: MaxObjects + 1})
	if opened != nil || !errors.Is(err, storage.ErrLimit) {
		t.Fatal(opened, err)
	}
	event := r13Event(t, r13Drain(c), observation.Open)
	if event.Outcome != observation.Limit {
		t.Fatal(event)
	}
}

func TestR13EmptyCheckpointHit(t *testing.T) {
	c, _ := observation.New(128)
	e, d := coordinatorStore(t, Options{Observation: c})
	if _, err := e.CreateRecoveryCheckpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	r13Drain(c)
	opened, _, err := OpenExisting(d, Options{Observation: c})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	event := r13Event(t, r13Drain(c), observation.Open)
	if !event.CheckpointHit || event.Certified != 0 || event.Count != 0 || event.Replayed != 0 {
		t.Fatal(event)
	}
}

func BenchmarkR13PublicRead(b *testing.B) {
	for _, mode := range []string{"disabled", "enabled", "full"} {
		b.Run(mode, func(b *testing.B) {
			var c *observation.Collector
			if mode != "disabled" {
				c, _ = observation.New(64)
			}
			d, err := storage.OpenDirectory(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer d.Close()
			e, err := Create(d, Options{Observation: c})
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()
			_, err = e.Mutate(context.Background(), api.MutateRequest{
				Key: model.RequestKey{Space: "s", Principal: "p", RequestID: "r"},
				Batch: api.StateBatch{Operations: []api.StateMutation{{
					Key: []byte("k"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("v")},
				}}},
			})
			if err != nil {
				b.Fatal(err)
			}
			if mode == "full" {
				for i := 0; i < 64; i++ {
					c.Emit(observation.Event{})
				}
			}
			var dst [64]observation.Event
			req := api.ReadStateRequest{Space: "s", Key: []byte("k")}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.GetState(context.Background(), req); err != nil {
					b.Fatal(err)
				}
				if mode == "enabled" && i%32 == 31 {
					c.Drain(dst[:])
				}
			}
		})
	}
}
