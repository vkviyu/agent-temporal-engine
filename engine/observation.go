package engine

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func observationOutcome(err error) observation.Outcome {
	switch {
	case err == nil:
		return observation.Success
	case errors.Is(err, storage.ErrCommitUnknown):
		return observation.Unknown
	case errors.Is(err, storage.ErrCorruption), errors.Is(err, ErrSemanticCorruption), errors.Is(err, ErrStoredObject):
		return observation.Corruption
	case errors.Is(err, storage.ErrStoreFenced):
		return observation.Fenced
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return observation.Canceled
	case isLimit(err):
		return observation.Limit
	case errors.Is(err, storage.ErrClosed), errors.Is(err, storage.ErrClosing):
		return observation.Closed
	default:
		return observation.Failure
	}
}

func commitObservation(result api.CommitResult, err error) observation.Outcome {
	switch result.Code {
	case api.CodeCommitUnknown:
		return observation.Unknown
	case api.CodeConflict:
		return observation.Conflict
	case api.CodeRejected:
		return observation.Rejected
	case api.CodeResourceLimit:
		return observation.Limit
	default:
		return observationOutcome(err)
	}
}
