package engine

import (
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
)

// objectContentProjection is shared by ReadContent and emission authorization.
// Callers authenticate historical membership and current policy separately.
func objectContentProjection(object codec.Object, projection string) ([]byte, error) {
	switch projection {
	case "", "payload":
		return object.Payload, nil
	case "value_bytes":
		if object.Type != valueType {
			return nil, ErrUnsupportedMethod
		}
		value, err := readValue(object)
		if err != nil {
			return nil, err
		}
		if value.Kind != api.ValueBytes {
			return nil, ErrUnsupportedMethod
		}
		return value.Data, nil
	default:
		return nil, api.ErrInvalidRequest
	}
}
