package agentprofile

import (
	"bytes"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const toolDefinitionDomain = "ATE-TOOL-DEFINITION\x00"

// ToolDefinition records the trusted host's versioned tool interface and
// effect declaration. Publish its canonical bytes as a ValueBytes object.
// Schema contains the exact adapter schema bytes in SchemaEncoding. The
// embedded Contract has no Definition field value because this record's own
// ObjectID is only known after publication.
type ToolDefinition struct {
	Name           string
	Version        string
	SchemaEncoding string
	Schema         []byte
	DispatchMode   string // external or delegate
	DispatchClass  string
	Contract       EffectContract
}

func (t ToolDefinition) Canonical() ([]byte, error) {
	if !commandText(t.Name) || !commandText(t.Version) || !commandText(t.SchemaEncoding) || len(t.Schema) == 0 || len(t.Schema) > MaxCommandBytes ||
		(t.DispatchMode != "external" && t.DispatchMode != "delegate") || !validEffect(t.DispatchClass) ||
		(t.DispatchMode == "delegate" && t.DispatchClass != "pure") ||
		!commandText(t.Contract.Recipient) || t.Contract.Definition != "" ||
		!validEffectCapabilities(t.DispatchClass, t.Contract.Recipient, t.Contract) {
		return nil, ErrCommandShape
	}
	raw, err := appendControlValue(codec.AppendUint([]byte(toolDefinitionDomain), 1), reflect.ValueOf(t), 0)
	if err != nil || len(raw) > MaxCommandBytes {
		return nil, ErrCommandShape
	}
	return raw, nil
}

func DecodeToolDefinition(raw []byte) (ToolDefinition, error) {
	if len(raw) > MaxCommandBytes || !bytes.HasPrefix(raw, []byte(toolDefinitionDomain)) {
		return ToolDefinition{}, ErrCommandShape
	}
	d := codec.NewDecoder(raw[len(toolDefinitionDomain):])
	version, err := d.Uint()
	if err != nil || version != 1 {
		return ToolDefinition{}, ErrCommandVersion
	}
	var t ToolDefinition
	if readControlValue(d, reflect.ValueOf(&t).Elem(), 0) != nil || d.Done() != nil {
		return ToolDefinition{}, ErrCommandShape
	}
	canonical, err := t.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return ToolDefinition{}, ErrCommandShape
	}
	return t, nil
}

// BindEffect fixes the published definition on a copy of the effect contract.
func (t ToolDefinition) BindEffect(definition model.ObjectID) EffectContract {
	contract := t.Contract
	contract.Definition = definition
	return contract
}
