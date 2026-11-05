package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestUnknownRequiredSemanticFeaturesAreUnsupported(t *testing.T) {
	method := semanticError(ErrUnsupportedMethod)
	if !errors.Is(method, ErrUnsupportedSchema) || !errors.Is(method, ErrUnsupportedMethod) || errors.Is(method, storage.ErrCorruption) {
		t.Fatal(method)
	}
	for _, kind := range []string{state.NodeType, valueType, commandType, operationsType, receiptType, rootSetType} {
		err := semanticError(validateRecord(context.Background(), codec.Object{Type: kind, Space: "project", Version: 2}, nil))
		if !errors.Is(err, ErrUnsupportedSchema) || errors.Is(err, storage.ErrCorruption) {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}
