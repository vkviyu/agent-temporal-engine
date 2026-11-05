package agentprofile

import (
	"bytes"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func commandInvocationContract(i Invocation) bool {
	return i.AcceptedResult != "" && model.ValidDigest(string(i.ToolDefinition)) &&
		i.ToolVersion != "" && (i.DispatchClass == "pure" || i.DispatchClass == "idempotent" || i.DispatchClass == "non_idempotent")
}

// validateInvocationSource binds every V1 tool decision to the one accepted
// primary model result for its execution/step.
func validateInvocationSource(i Invocation, lifecycle LifecycleSnapshot) error {
	if !commandInvocationContract(i) {
		return ErrConflict
	}
	if _, err := EncodeInvocation(i); err != nil {
		return err
	}
	if lifecycle.Recovery == nil {
		return ErrConflict
	}
	for _, result := range lifecycle.Results {
		if result.ID != i.AcceptedResult || result.Outcome != "success" {
			continue
		}
		for _, spec := range lifecycle.Recovery.ActivitySpecs {
			if spec.Activity != result.Activity || spec.Execution != i.Execution || spec.Step != i.Step || spec.Role != "primary_decision" {
				continue
			}
			for _, control := range lifecycle.Recovery.ActivityControls {
				if control.Activity == spec.Activity && control.AcceptedResult == i.AcceptedResult {
					return nil
				}
			}
		}
	}
	return ErrConflict
}

func sameInvocationContract(a, b Invocation) bool {
	return a.AcceptedResult == b.AcceptedResult && a.ToolDefinition == b.ToolDefinition &&
		a.ToolVersion == b.ToolVersion && a.DispatchClass == b.DispatchClass &&
		a.Tool == b.Tool && bytes.Equal(a.Args, b.Args)
}
