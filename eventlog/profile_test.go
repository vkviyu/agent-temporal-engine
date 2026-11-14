package eventlog

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestProfileOccurrenceAndCoreManifestRoundTrip(t *testing.T) {
	x := sampleEvent()
	x.Kind = KindAgentTransition
	x.Stream = model.EventStreamKey{Kind: "execution", Owner: "worker", Incarnation: 2}
	x.PreviousInStream = model.EventID(strings.Repeat("c", 64))
	x.Attribution = &model.EventAttribution{Subject: model.EntityKey{Profile: "agent", Kind: "Attempt", ID: "attempt"}, InitiatingActor: "actor", Execution: "worker", Query: "query", Session: "session", Step: 7, Operation: "Control.DispatchAttempt", Ordinal: 1}
	x.CausalParents = []model.EventID{model.EventID(strings.Repeat("d", 64))}
	o, err := Object(x)
	if err != nil || o.Type != ProfileType || o.Version != 1 {
		t.Fatal(o, err)
	}
	decoded, err := Decode(o)
	if err != nil {
		t.Fatal(err)
	}
	decoded.ID = ""
	if !reflect.DeepEqual(decoded, x) {
		t.Fatalf("profile metadata changed: %+v", decoded)
	}
	id, _ := codec.ObjectID(o)
	core := sampleEvent()
	core.SchemaVersion = 2
	core.ProfileRegistryVersion = ProfileRegistryVersion
	core.ProfileEvents = []model.EventID{model.EventID(id)}
	core.PreviousInStream = x.PreviousInStream
	co, err := Object(core)
	if err != nil || co.Type != Type || co.Version != 2 {
		t.Fatal(co, err)
	}
	got, err := Decode(co)
	if err != nil || got.ProfileRegistryVersion != ProfileRegistryVersion || !reflect.DeepEqual(got.ProfileEvents, core.ProfileEvents) || got.PreviousInStream != core.PreviousInStream {
		t.Fatalf("core manifest: %+v %v", got, err)
	}
	for _, mutate := range []func(*model.Event){
		func(e *model.Event) { e.ProfileRegistryVersion = 99 },
		func(e *model.Event) { e.ProfileEvents = nil },
		func(e *model.Event) { e.ProfileEvents = append(e.ProfileEvents, e.ProfileEvents[0]) },
		func(e *model.Event) { e.CausalParents = []model.EventID{e.PreviousInStream} },
	} {
		bad := core
		bad.ProfileEvents = append([]model.EventID(nil), core.ProfileEvents...)
		mutate(&bad)
		if _, err := Object(bad); err == nil {
			t.Fatal("invalid manifest accepted")
		}
	}
	for _, mutate := range []func(*model.Event){
		func(e *model.Event) { e.Attribution.Execution = "other" },
		func(e *model.Event) { e.Attribution.Operation = "arbitrary callback" },
		func(e *model.Event) { e.Attribution.Ordinal = 0 },
		func(e *model.Event) { e.Stream.Incarnation = 0 },
	} {
		bad := x
		a := *x.Attribution
		bad.Attribution = &a
		mutate(&bad)
		if _, err := Object(bad); err == nil {
			t.Fatal("invalid attribution accepted")
		}
	}
}

func TestQueryOccurrenceDoesNotInventExecutionOrActor(t *testing.T) {
	e := sampleEvent()
	e.Kind = KindAgentTransition
	e.Stream = model.EventStreamKey{Kind: "query", Owner: "query"}
	e.Attribution = &model.EventAttribution{Subject: model.EntityKey{Profile: "agent", Kind: "Query", ID: "query"}, Query: "query", Session: "session", Operation: "Work.query_intake", Ordinal: 1}
	if _, err := Object(e); err != nil {
		t.Fatal(err)
	}
	e.Attribution.Execution = "fake-execution"
	if _, err := Object(e); err == nil {
		t.Fatal("query stream accepted execution attribution")
	}
}
