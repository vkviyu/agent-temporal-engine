package engine

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// ClassifyError is the core's transport-independent error adapter. It never
// infers a category from text or changes a command's separate ResultCode.
func ClassifyError(err error) api.ErrorClass {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, storage.ErrCommitUnknown):
		return api.ErrorCommitUnknown
	case errors.Is(err, storage.ErrStoreFenced):
		return api.ErrorStoreFenced
	case errors.Is(err, storage.ErrCorruption) || errors.Is(err, ErrSemanticCorruption) || errors.Is(err, ErrStoredObject):
		return api.ErrorCorruption
	case errors.Is(err, storage.ErrClosing):
		return api.ErrorClosing
	case errors.Is(err, storage.ErrClosed):
		return api.ErrorClosed
	case errors.Is(err, ErrWaitInterrupted):
		return api.ErrorWaitInterrupted
	case errors.Is(err, context.Canceled):
		return api.ErrorCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return api.ErrorDeadline
	case errors.Is(err, ErrViewExpired):
		return api.ErrorViewExpired
	case errors.Is(err, ErrInvalidView):
		return api.ErrorInvalidView
	case errors.Is(err, ErrNodeNotFound):
		return api.ErrorNotFound
	case errors.Is(err, model.ErrAnchorMismatch) || errors.Is(err, model.ErrInvalidFence):
		return api.ErrorAnchorMismatch
	case isLimit(err):
		return api.ErrorResourceLimit
	case errors.Is(err, annotation.ErrUnsupported) || errors.Is(err, investigation.ErrUnsupported) || errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, ErrUnsupportedSchema) || errors.Is(err, knowledge.ErrUnsupportedSchema) || errors.Is(err, storage.ErrUnsupported) || errors.Is(err, agentprofile.ErrCommandVersion):
		return api.ErrorUnsupported
	case errors.Is(err, api.ErrReadEmissionDenied) || errors.Is(err, agentprofile.ErrDurableScope) || errors.Is(err, agentprofile.ErrCommandScope) || errors.Is(err, memory.ErrCommandScope) || errors.Is(err, memory.ErrHistoryAccessDenied) || errors.Is(err, memory.ErrUseDenied):
		return api.ErrorUnauthorized
	case errors.Is(err, agentprofile.ErrCommandShape):
		return api.ErrorInvalidRequest
	case transferCommandRejection(err):
		return api.ErrorInvalidRequest
	case errors.Is(err, annotation.ErrInvalid) || errors.Is(err, investigation.ErrInvalid) || errors.Is(err, knowledge.ErrInvalid) || errors.Is(err, knowledge.ErrDuplicateItem) || errors.Is(err, graph.ErrCommandShape) || errors.Is(err, memory.ErrCommandShape):
		return api.ErrorInvalidRequest
	case errors.Is(err, api.ErrReadAuthorizationChanged) || errors.Is(err, ErrWorkBoundary) || summaryCommandRejection(err) || annotationCommandRejection(err):
		return api.ErrorConflict
	case emissionCommandRejection(err):
		return api.ErrorConflict
	case knowledgeCommandRejection(err) || graphCommandRejection(err) || memoryCommandRejection(err):
		return api.ErrorConflict
	case agentCommandRejection(err):
		return api.ErrorConflict
	case errors.Is(err, api.ErrInvalidRequest) || errors.Is(err, model.ErrInvalidNodeRef) || errors.Is(err, model.ErrInvalidRequestKey) || errors.Is(err, state.ErrInvalidKey) || errors.Is(err, ErrInvalidStateData) || errors.Is(err, codec.ErrLengthOverflow) || errors.Is(err, storage.ErrMalformed):
		return api.ErrorInvalidRequest
	default:
		return api.ErrorInternal
	}
}
