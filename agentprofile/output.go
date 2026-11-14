package agentprofile

import (
	"bytes"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// OutputChunk is an immutable durable prefix of one response from one Attempt.
// Its ID addresses this embedded record, not a separately readable core object.
type OutputChunk struct {
	ID                model.ObjectID
	Activity, Attempt string
	RequestDigest     model.ObjectID
	Incarnation       uint64
	Response          string
	Sequence          uint64
	Payload           []byte
}

func OutputChunkID(chunk OutputChunk) model.ObjectID {
	chunk.ID = ""
	raw, err := appendControlValue([]byte("ATE-OUTPUT-CHUNK\x00\x01"), reflect.ValueOf(chunk), 0)
	if err != nil {
		return ""
	}
	return ExactRequestDigest(raw)
}

func validOutputChunk(chunk OutputChunk) bool {
	return commandText(chunk.Activity) && commandText(chunk.Attempt) && validDigest(chunk.RequestDigest) &&
		chunk.Incarnation > 0 && commandText(chunk.Response) && chunk.Sequence > 0 && chunk.Sequence <= MaxLifecycleRecords &&
		len(chunk.Payload) > 0 && len(chunk.Payload) <= MaxCommandBytes && validDigest(chunk.ID) && chunk.ID == OutputChunkID(chunk)
}

func validResultCapture(result ActivityResult) bool {
	switch result.Capture {
	case "full", "truncated", "partial", "unknown":
	default:
		return false
	}
	if len(result.OutputChunks) > MaxLifecycleRecords || (result.Response != "" && !commandText(result.Response)) || (len(result.OutputChunks) > 0 && result.Response == "") {
		return false
	}
	for _, id := range result.OutputChunks {
		if !validDigest(id) {
			return false
		}
	}
	return true
}

func acceptableResultCapture(spec ActivitySpec, result ActivityResult) bool {
	return result.Capture == "full" || result.Capture == "truncated" && !modelRole(spec.Role)
}

func outputForAttempt(chunks []OutputChunk, attempt string) []OutputChunk {
	var selected []OutputChunk
	for _, chunk := range chunks {
		if chunk.Attempt == attempt {
			selected = append(selected, chunk)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Sequence < selected[j].Sequence })
	return selected
}

func validateOutputSequence(chunks []OutputChunk) error {
	total := 0
	for i, chunk := range chunks {
		if !validOutputChunk(chunk) || chunk.Sequence != uint64(i+1) || chunk.Response != chunks[0].Response ||
			chunk.Activity != chunks[0].Activity || chunk.RequestDigest != chunks[0].RequestDigest || chunk.Incarnation != chunks[0].Incarnation ||
			len(chunk.Payload) > MaxCommandBytes-total {
			return ErrConflict
		}
		total += len(chunk.Payload)
	}
	return nil
}

func validateResultChunks(result ActivityResult, chunks []OutputChunk) error {
	if !validResultCapture(result) {
		return ErrInvalid
	}
	selected := outputForAttempt(chunks, result.Attempt)
	if err := validateOutputSequence(selected); err != nil {
		return err
	}
	if len(selected) != len(result.OutputChunks) {
		return ErrConflict
	}
	if result.Response == "" {
		return nil
	}
	var payload []byte
	for i, chunk := range selected {
		if chunk.ID != result.OutputChunks[i] || chunk.Response != result.Response || chunk.Activity != result.Activity || chunk.RequestDigest != result.RequestDigest || chunk.Incarnation != result.Incarnation {
			return ErrConflict
		}
		payload = append(payload, chunk.Payload...)
	}
	if !bytes.Equal(payload, result.Payload) {
		return ErrConflict
	}
	return nil
}

func (l *Ledger) captureOutputChunk(chunk OutputChunk) error {
	_, attempt, err := l.attemptControl(chunk.Attempt)
	if err != nil {
		return err
	}
	incarnation := attempt.Authority.Incarnation
	spec, err := l.activitySpec(chunk.Activity)
	if err != nil {
		return err
	}
	if spec.Query != "" {
		incarnation = 1
	}
	if attempt.Activity != chunk.Activity || attempt.RequestDigest != chunk.RequestDigest || incarnation != chunk.Incarnation || !validDigest(attempt.DispatchEvidence) {
		return ErrConflict
	}
	s := l.recovery()
	for _, old := range s.OutputChunks {
		if old.ID == chunk.ID {
			return nil
		}
	}
	for _, result := range l.lifecycle.Results {
		if result.Attempt == chunk.Attempt {
			return ErrUniqueSlot
		}
	}
	selected := outputForAttempt(s.OutputChunks, chunk.Attempt)
	selected = append(selected, chunk)
	if err := validateOutputSequence(selected); err != nil {
		return err
	}
	chunk.Payload = bytes.Clone(chunk.Payload)
	s.OutputChunks = append(s.OutputChunks, chunk)
	return nil
}

func validateOutputState(snapshot Snapshot) error {
	s := snapshot.Lifecycle.Recovery
	if s == nil {
		for _, result := range snapshot.Lifecycle.Results {
			if err := validateResultChunks(result, nil); err != nil {
				return err
			}
		}
		return nil
	}
	attempts := make(map[string]AttemptControl, len(s.AttemptControls))
	for _, attempt := range s.AttemptControls {
		attempts[attempt.Attempt] = attempt
	}
	byAttempt := make(map[string][]OutputChunk)
	for _, chunk := range s.OutputChunks {
		attempt, found := attempts[chunk.Attempt]
		incarnation := attempt.Authority.Incarnation
		if zeroBinding(attempt.Authority) {
			incarnation = 1
		}
		if !found || chunk.Activity != attempt.Activity || chunk.RequestDigest != attempt.RequestDigest || chunk.Incarnation != incarnation || !validDigest(attempt.DispatchEvidence) {
			return ErrInvalid
		}
		byAttempt[chunk.Attempt] = append(byAttempt[chunk.Attempt], chunk)
	}
	for attempt := range byAttempt {
		chunks := outputForAttempt(byAttempt[attempt], attempt)
		if err := validateOutputSequence(chunks); err != nil {
			return err
		}
	}
	for _, result := range snapshot.Lifecycle.Results {
		if err := validateResultChunks(result, byAttempt[result.Attempt]); err != nil {
			return err
		}
	}
	return nil
}
