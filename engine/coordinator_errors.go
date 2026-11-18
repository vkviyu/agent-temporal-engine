package engine

import (
	"errors"

	"github.com/vkviyu/agent-temporal-engine/api"
)

// classifyPrepareError maps command preparation failures to request outcomes.
// A false result leaves the failure with the coordinator's abort/fence path.
// Delivery still depends on publication of any private predecessor.
// Do not use this for transaction encoding, staged installation or publication.
func classifyPrepareError(req api.CommitRequest, err error) (api.CommitResult, error, bool) {
	result := api.CommitResult{RequestID: req.RequestID}
	switch {
	case errors.Is(err, errCAS):
		result.Code = api.CodeConflict
		result.Conflict = api.ConflictPrecondition
		return result, nil, true
	case isLimit(err):
		result.Code = api.CodeResourceLimit
	case req.Method == api.MethodApplyAgent && agentCommandRejection(err):
		result.Code = api.CodeRejected
	case errors.Is(err, ErrActionReservation):
		result.Code = api.CodeRejected
	case prepareCommandRejection(req.Method, err):
		result.Code = api.CodeRejected
	default:
		return api.CommitResult{}, err, false
	}
	return result, err, true
}

// Domain predicates retain their own stored-data exclusions and shared errors.
// Matching a different command's error alone does not authorize rejection.
func prepareCommandRejection(method string, err error) bool {
	switch method {
	case api.MethodApplyWork:
		return workCommandRejection(err)
	case api.MethodApplyAnnotation:
		return annotationCommandRejection(err)
	case api.MethodUpdateInvestigation:
		return investigationCommandRejection(err)
	case api.MethodApplyTaskTrace:
		return taskTraceRejection(err)
	case api.MethodApplySummary:
		return summaryCommandRejection(err)
	case api.MethodImportTransfer:
		return transferCommandRejection(err)
	case api.MethodApplyEmission:
		return emissionCommandRejection(err)
	case api.MethodApplyKnowledge:
		return knowledgeCommandRejection(err)
	case api.MethodApplyGraph:
		return graphCommandRejection(err)
	case api.MethodApplyMemory:
		return memoryCommandRejection(err)
	default:
		return false
	}
}
