package investigation

import (
	"bytes"
	"encoding/json"
)

const (
	MaxUpdateBytes     = 128 << 10
	MaxCheckpointBytes = 256 << 10

	updatePrefix     = "ATE-INVESTIGATION-UPDATE\x00v1\x00"
	checkpointPrefix = "ATE-INVESTIGATION-CHECKPOINT\x00v1\x00"
)

// EncodeUpdate returns the canonical command bytes used by durable receipts.
func EncodeUpdate(update Update) ([]byte, error) {
	if err := update.Validate(); err != nil {
		return nil, err
	}
	if update.OpenQuestions != nil && *update.OpenQuestions == nil {
		empty := []string{}
		update.OpenQuestions = &empty
	}
	return encode(updatePrefix, MaxUpdateBytes, update)
}

func DecodeUpdate(raw []byte) (Update, error) {
	return decode(raw, updatePrefix, MaxUpdateBytes, EncodeUpdate)
}

func EncodeCheckpoint(checkpoint Checkpoint) ([]byte, error) {
	if err := checkpoint.Validate(); err != nil {
		return nil, err
	}
	return encode(checkpointPrefix, MaxCheckpointBytes, checkpoint)
}

func DecodeCheckpoint(raw []byte) (Checkpoint, error) {
	return decode(raw, checkpointPrefix, MaxCheckpointBytes, EncodeCheckpoint)
}

func encode(prefix string, limit int, value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalid
	}
	if len(raw) > limit-len(prefix) {
		return nil, ErrLimit
	}
	return append([]byte(prefix), raw...), nil
}

func decode[T any](raw []byte, prefix string, limit int, encodeValue func(T) ([]byte, error)) (T, error) {
	var value, zero T
	if len(raw) > limit {
		return zero, ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(prefix)) {
		schema, _, _ := bytes.Cut([]byte(prefix), []byte{0})
		if bytes.HasPrefix(raw, append(bytes.Clone(schema), 0)) {
			return zero, ErrUnsupported
		}
		return zero, ErrInvalid
	}
	if err := json.Unmarshal(raw[len(prefix):], &value); err != nil {
		return zero, ErrInvalid
	}
	canonical, err := encodeValue(value)
	if err != nil {
		return zero, err
	}
	if !bytes.Equal(canonical, raw) {
		return zero, ErrInvalid
	}
	return value, nil
}
