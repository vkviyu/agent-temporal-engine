package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestTaskTraceProfileScopeMatchesLedger(t *testing.T) {
	// Only task/query existence comes from the ledger in the old helper. Keep
	// its validated records independent of this deliberately intricate topology.
	base := agentprofile.Snapshot{
		Tasks: []agentprofile.Task{profileCoordinatorTask("parent"), profileCoordinatorTask("child"), profileCoordinatorTask("sibling")},
		Lifecycle: agentprofile.LifecycleSnapshot{Queries: []agentprofile.Query{
			{ID: "q", Session: "session", Space: "coordinator-space", Owner: "host", Version: 1, Status: "open"},
		}},
	}
	image, err := agentprofile.EncodeRecordImage(base)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := image.Restore()
	if err != nil {
		t.Fatal(err)
	}
	base.Executions = []agentprofile.Execution{
		{ID: "root", Task: "parent"},
		{ID: "same-task", Parent: "root", Task: "parent"},
		{ID: "cross-task", Parent: "same-task", Task: "child"},
		{ID: "grandchild", Parent: "cross-task", Task: "child"},
		{ID: "other-child-root", Task: "child"},
		{ID: "unrelated", Parent: "other-child-root", Task: "sibling"},
	}
	profile := newTaskTraceProfile(base)
	for _, subject := range []api.TraceSubject{
		{Kind: "task", ID: "parent"}, {Kind: "task", ID: "child"}, {Kind: "task", ID: "sibling"},
		{Kind: "task", ID: "missing"}, {Kind: "query", ID: "q"}, {Kind: "query", ID: "missing"},
	} {
		for _, session := range []string{"", "session", "wrong-session"} {
			req := api.TaskTraceRequest{Space: "coordinator-space", Principal: "host", Subject: subject, Session: session}
			want, wantErr := taskTraceScope(req, ledger, base)
			got, gotErr := profile.scope(req)
			if !errors.Is(gotErr, wantErr) || !reflect.DeepEqual(got, want) {
				t.Fatalf("%+v: got=%+v %v want=%+v %v", req, got, gotErr, want, wantErr)
			}
		}
	}
	keys, _ := profile.scope(api.TaskTraceRequest{Subject: api.TraceSubject{Kind: "task", ID: "parent"}})
	ids := map[string]bool{}
	for _, key := range keys {
		ids[key.kind+":"+key.id] = true
	}
	if len(ids) != 6 || !ids["task:child"] || !ids["execution:grandchild"] ||
		ids["execution:other-child-root"] || ids["task:sibling"] {
		t.Fatalf("subtree expanded to unrelated task executions: %+v", keys)
	}
	// Repeated scopes must not append into cached task execution buffers.
	if len(profile.taskExecutions["parent"]) != 2 || len(profile.taskExecutions["child"]) != 3 {
		t.Fatal("scope mutated cached topology")
	}
}

func TestTaskTraceProfileMinimalOwnedProvenance(t *testing.T) {
	large := strings.Repeat("payload", 1<<18)
	outcome := agentprofile.TaskOutcome{Task: "task", Execution: "execution", Result: "result-object", Validation: "validation", ValidationRequirement: "requirement"}
	spec := agentprofile.ActivitySpec{
		Activity: "activity", Request: "request", ContextPacket: "packet", FinalAnswerInput: "answer-input",
		InputFence: model.CommitFence{Store: "store", LSN: 42, CommitDigest: "digest"},
		Contract:   agentprofile.EffectContract{Definition: "definition"},
		Recipient:  large, ResourceScope: large, IdempotencyKey: large,
	}
	snapshot := agentprofile.Snapshot{Lifecycle: agentprofile.LifecycleSnapshot{
		Queries:  []agentprofile.Query{{ID: "q", Session: "session", Input: []byte(large)}},
		Results:  []agentprofile.ActivityResult{{ID: "result", Activity: "activity", Payload: []byte(large)}},
		Attempts: []agentprofile.Attempt{{ID: "attempt", Activity: "activity"}},
		Recovery: &agentprofile.RecoverySnapshot{
			ActivitySpecs: []agentprofile.ActivitySpec{spec},
			Outcomes:      []agentprofile.TaskOutcome{outcome},
			Answers:       []agentprofile.QueryAnswer{{Query: "q", Slot: "slot", Body: []byte(large), GeneratedFrom: "result", InputEvidence: "input", Outcomes: []agentprofile.TaskOutcome{outcome}}},
		},
	}}
	p := newTaskTraceProfile(snapshot)
	a, b := p.provenanceAt(model.RootSet{DomainStates: "old"}), p.provenanceAt(model.RootSet{DomainStates: "new"})
	if a.roots.DomainStates != "old" || b.roots.DomainStates != "new" || p.provenance.roots != (model.RootSet{}) {
		t.Fatal("request roots leaked into cached provenance")
	}
	got := a.specs["activity"]
	want := agentprofile.ActivitySpec{Request: spec.Request, ContextPacket: spec.ContextPacket, FinalAnswerInput: spec.FinalAnswerInput, InputFence: spec.InputFence, Contract: spec.Contract}
	if !reflect.DeepEqual(got, want) || a.results["result"] != "activity" || a.attempts["attempt"] != "activity" {
		t.Fatal("activity provenance fields changed")
	}
	answer := a.answers[recoveryNodeID("q", "slot")]
	if answer.Body != nil || answer.Query != "" || answer.GeneratedFrom != "result" || answer.InputEvidence != "input" ||
		!reflect.DeepEqual(answer.Outcomes, []agentprofile.TaskOutcome{traceProfileOutcome(outcome)}) ||
		a.outcomes["task"] != traceProfileOutcome(outcome) {
		t.Fatalf("answer/outcome projection: %+v", answer)
	}
	snapshot.Lifecycle.Recovery.Answers[0].Outcomes[0].Validation = "mutated"
	snapshot.Lifecycle.Recovery.ActivitySpecs[0].Request = "mutated"
	snapshot.Lifecycle.Queries[0].Session = "mutated"
	if p.provenance.answers[recoveryNodeID("q", "slot")].Outcomes[0].Validation != "validation" ||
		p.provenance.specs["activity"].Request != "request" || p.queries["q"] != "session" {
		t.Fatal("projection aliases source snapshot")
	}
	if p.bytes > 32<<10 {
		t.Fatalf("large payloads retained: charged=%d", p.bytes)
	}
	// Disclosure behavior for an activity with no packet is identical after
	// trimming (the final-answer/packet paths are covered by integration tests).
	spec.ContextPacket, spec.FinalAnswerInput = "", ""
	trimmed := newTraceProvenance(model.RootSet{}, agentprofile.Snapshot{Lifecycle: agentprofile.LifecycleSnapshot{
		Recovery: &agentprofile.RecoverySnapshot{ActivitySpecs: []agentprofile.ActivitySpec{spec}},
	}})
	e := &Engine{}
	wantIDs, err := e.activityInputDisclosureIDs(context.Background(), "s", "p", spec)
	gotIDs, gotErr := e.activityInputDisclosureIDs(context.Background(), "s", "p", trimmed.specs["activity"])
	if err != nil || gotErr != nil || !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("trimmed disclosure changed: %v %v %v %v", gotIDs, wantIDs, gotErr, err)
	}
}

func TestTaskTraceProfileCacheCapacityAndIsolation(t *testing.T) {
	p := newTaskTraceProfile(agentprofile.Snapshot{})
	c := &taskTraceProfileCache{}
	key := func(i int) taskTraceProfileKey {
		return taskTraceProfileKey{"space", "owner", model.ObjectID(fmt.Sprint(i))}
	}
	for i := 0; i < taskTraceProfileEntries; i++ {
		c.put(key(i), p)
	}
	c.get(key(0))
	c.put(key(taskTraceProfileEntries), p)
	if _, ok := c.get(key(1)); ok || len(c.entries) != taskTraceProfileEntries {
		t.Fatal("entry cap/LRU not enforced")
	}
	if _, ok := c.get(key(0)); !ok {
		t.Fatal("recently used entry evicted")
	}
	for _, other := range []taskTraceProfileKey{{"other", "owner", "0"}, {"space", "other", "0"}, {"space", "owner", "unknown"}} {
		if _, ok := c.get(other); ok {
			t.Fatalf("cache crossed identity: %+v", other)
		}
	}
	// Real retained strings drive byte eviction, not an injected size field.
	heavy := newTaskTraceProfile(agentprofile.Snapshot{Lifecycle: agentprofile.LifecycleSnapshot{
		Queries: []agentprofile.Query{{ID: "q", Session: strings.Repeat("s", 6<<20)}},
	}})
	bytesCache := &taskTraceProfileCache{}
	bytesCache.put(key(0), heavy)
	bytesCache.put(key(1), heavy)
	bytesCache.get(key(0))
	bytesCache.put(key(2), heavy)
	if _, ok := bytesCache.get(key(1)); ok || len(bytesCache.entries) != 2 || bytesCache.bytes > taskTraceProfileBytes {
		t.Fatalf("byte LRU not enforced: entries=%d bytes=%d", len(bytesCache.entries), bytesCache.bytes)
	}
	oversized := newTaskTraceProfile(agentprofile.Snapshot{Lifecycle: agentprofile.LifecycleSnapshot{
		Queries: []agentprofile.Query{{ID: "q", Session: strings.Repeat("s", taskTraceProfileBytes)}},
	}})
	before := bytesCache.bytes
	bytesCache.put(key(3), oversized)
	if _, ok := bytesCache.get(key(3)); ok || bytesCache.bytes != before {
		t.Fatal("oversized projection cached or displaced useful entries")
	}
	if _, err := oversized.scope(api.TaskTraceRequest{Subject: api.TraceSubject{Kind: "query", ID: "q"}}); err != nil {
		t.Fatal("oversized uncached projection cannot serve read")
	}
}

func TestTaskTraceProfileAccountingIncludesNestedStorage(t *testing.T) {
	p := newTaskTraceProfile(agentprofile.Snapshot{})
	base := traceProfileRetainedBytes(reflect.ValueOf(p).Elem())
	p.provenance.answers["answer"] = agentprofile.QueryAnswer{
		GeneratedFrom: strings.Repeat("r", 100), InputEvidence: model.ObjectID(strings.Repeat("i", 200)),
		Outcomes: make([]agentprofile.TaskOutcome, 1, 100),
	}
	answer := p.provenance.answers["answer"]
	answer.Outcomes[0].Validation = model.ObjectID(strings.Repeat("v", 1000))
	with := traceProfileRetainedBytes(reflect.ValueOf(p).Elem())
	if with-base < 1300+100*int(reflect.TypeOf(agentprofile.TaskOutcome{}).Size()) {
		t.Fatal("accounting omitted nested strings or spare slice capacity")
	}
}

func TestTaskTraceProfileConcurrentStop(t *testing.T) {
	p := newTaskTraceProfile(agentprofile.Snapshot{Tasks: []agentprofile.Task{{ID: "task"}}})
	c := &taskTraceProfileCache{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				key := taskTraceProfileKey{"s", "p", model.ObjectID(fmt.Sprint(i, "/", j%40))}
				c.put(key, p)
				if got, ok := c.get(key); ok {
					_, _ = got.scope(api.TaskTraceRequest{Subject: api.TraceSubject{Kind: "task", ID: "task"}})
					_ = got.provenanceAt(model.RootSet{LiveControl: model.ObjectID(fmt.Sprint(i))})
				}
				if j == 50 {
					c.stop()
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	c.put(taskTraceProfileKey{"s", "p", "late"}, p)
	if !c.stopped || len(c.entries) != 0 || c.bytes != 0 {
		t.Fatal("stop allowed late insert")
	}
}
