package ate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/observation"
)

func TestR13FacadeCollectorSurvivesOpenFailure(t *testing.T) {
	c, _ := observation.New(16)
	options := ate.Options{Observation: c}
	store, _, err := ate.OpenExisting(filepath.Join(t.TempDir(), "missing"), options)
	if err == nil || store != nil {
		t.Fatal("missing directory opened")
	}
	var events [16]observation.Event
	if c.Drain(events[:]) != 1 || events[0].Kind != observation.Open || events[0].Outcome != observation.Failure {
		t.Fatal(events)
	}
	path := t.TempDir()
	store, err = ate.Create(path, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Drain(events[:])
	// A valid directory with no store fails below the facade, retaining one
	// engine Open event in the same collector and no extra facade Open event.
	store, _, err = ate.OpenExisting(t.TempDir(), options)
	if err == nil || store != nil {
		t.Fatal("empty directory opened")
	}
	if c.Drain(events[:]) != 1 || events[0].Kind != observation.Open || events[0].Outcome == observation.Success {
		t.Fatal(events)
	}
	store, _, err = ate.OpenExisting(path, options)
	if err != nil {
		t.Fatal(err)
	}
	if n := c.Drain(events[:]); n != 1 || events[0].Kind != observation.Open || events[0].Outcome != observation.Success {
		t.Fatal(n, events)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Drain(events[:])
	if err := os.WriteFile(filepath.Join(path, "CURRENT"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	store, _, err = ate.OpenExisting(path, options)
	if store != nil || err == nil {
		t.Fatal("corrupt CURRENT opened")
	}
	if n := c.Drain(events[:]); n != 1 || events[0].Kind != observation.Open || events[0].Outcome != observation.Corruption {
		t.Fatal(n, events)
	}
}
