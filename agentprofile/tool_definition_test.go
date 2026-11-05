package agentprofile

import (
	"errors"
	"reflect"
	"testing"
)

func TestToolDefinitionCanonicalCapabilities(t *testing.T) {
	definition := ToolDefinition{
		Name: "read", Version: "read/v1", SchemaEncoding: "json-schema/v1", Schema: []byte(`{"type":"object"}`),
		DispatchMode: "external", DispatchClass: "pure",
		Contract: EffectContract{Version: "effect/v1", Recipient: "executor", VerifiedPure: true, DifferentRequest: "unsupported", ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "none"},
	}
	raw, err := definition.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeToolDefinition(raw)
	if err != nil || !reflect.DeepEqual(decoded, definition) {
		t.Fatalf("definition did not round trip: %+v %v", decoded, err)
	}
	if _, err := DecodeToolDefinition(append(raw, 0)); !errors.Is(err, ErrCommandShape) {
		t.Fatalf("accepted a trailing byte: %v", err)
	}
	for name, change := range map[string]func(*ToolDefinition){
		"self-reference":  func(d *ToolDefinition) { d.Contract = d.BindEffect(digest('a')) },
		"unverified-pure": func(d *ToolDefinition) { d.Contract.VerifiedPure = false },
		"effectful-delegate": func(d *ToolDefinition) {
			d.DispatchMode, d.DispatchClass = "delegate", "non_idempotent"
			d.Contract.VerifiedPure, d.Contract.AllowsMutation = false, true
		},
		"no-schema": func(d *ToolDefinition) { d.Schema = nil },
	} {
		bad := definition
		change(&bad)
		if _, err := bad.Canonical(); !errors.Is(err, ErrCommandShape) {
			t.Fatalf("accepted %s: %v", name, err)
		}
	}
}
