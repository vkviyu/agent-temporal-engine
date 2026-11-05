package engine

import (
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// domainRootSlot is the only shared method metadata needed by the core root
// planner. Reducer dispatch and error classification remain explicit in their
// existing files; this table only prevents root ownership rules from drifting.
type domainRootSlot uint8

const (
	rootSlotDomain domainRootSlot = iota
	rootSlotBranch
	rootSlotKnowledge
)

func domainMethodRootSlot(method string) domainRootSlot {
	switch method {
	case api.MethodApplyGraph:
		return rootSlotBranch
	case api.MethodApplyMemory, api.MethodApplyKnowledge, api.MethodApplySummary, api.MethodApplyAnnotation, api.MethodImportTransfer:
		return rootSlotKnowledge
	default:
		return rootSlotDomain
	}
}

func domainRootForMethod(roots model.RootSet, method string) model.ObjectID {
	switch domainMethodRootSlot(method) {
	case rootSlotBranch:
		return roots.BranchRefs
	case rootSlotKnowledge:
		return roots.ExecutionKnowledge
	default:
		return roots.DomainStates
	}
}

func setDomainRootForMethod(roots *model.RootSet, method string, root model.ObjectID) {
	switch domainMethodRootSlot(method) {
	case rootSlotBranch:
		roots.BranchRefs = root
	case rootSlotKnowledge:
		roots.ExecutionKnowledge = root
	default:
		roots.DomainStates = root
	}
}

func isRegisteredDomainMethod(method string) bool {
	switch method {
	case api.MethodMutateState, api.MethodApplyGraph, api.MethodApplyMemory, api.MethodApplyKnowledge, api.MethodApplySummary, api.MethodApplyAnnotation:
		return true
	default:
		return false
	}
}
