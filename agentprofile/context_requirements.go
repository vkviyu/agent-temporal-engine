package agentprofile

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ContextRequirementsVersion defines deterministic structural discovery, not
// semantic relevance or a tokenizer. Hosts publish these exact images as raw
// content and adopt them before using them in an execution model packet.
const ContextRequirementsVersion = "agent_mandatory_context/v1"

type ContextRequirementImage struct {
	Kind  string
	ID    string
	Bytes []byte
}

// EncodeContextRequirementImage exposes the canonical bytes that a host must
// publish and adopt when it records one structural context obligation. The
// record is encoded with the same closed control codec used by ContextRequirements.
func EncodeContextRequirementImage(kind, id string, record any) ([]byte, error) {
	if !commandText(kind) || !commandText(id) {
		return nil, ErrInvalid
	}
	raw := codec.AppendUint([]byte("ATE-AGENT-CONTEXT-REQUIREMENT\x00"), 1)
	raw, _ = codec.AppendString(raw, kind)
	raw, _ = codec.AppendString(raw, id)
	encoded, err := appendControlValue(raw, reflect.ValueOf(record), 0)
	if err != nil || len(encoded) > MaxCommandBytes {
		return nil, ErrInvalid
	}
	return encoded, nil
}

// ContextRequirements derives requirements from one authenticated historical
// state image. It never consults a current clock, a newer ledger, another
// execution's private results, or model-generated classifications. Goal images
// include the initial goal and every inherited ancestor goal. The remaining
// records describe obligations owned by this execution, without inferring
// semantic facts from arbitrary text.
func ContextRequirements(snapshot Snapshot, execution model.ExecutionID) ([]ContextRequirementImage, error) {
	// Requirements operate on semantic records, including record-root images
	// larger than the legacy recovery envelope. Keep all relationship checks.
	if err := snapshot.validate(true); err != nil {
		return nil, err
	}
	tasks := map[model.EntityID]Task{}
	executions := map[model.ExecutionID]Execution{}
	for _, task := range snapshot.Tasks {
		tasks[task.ID] = task
	}
	for _, x := range snapshot.Executions {
		executions[x.ID] = x
	}
	x, ok := executions[execution]
	if !ok {
		return nil, ErrUnknown
	}
	out := []ContextRequirementImage{}
	appendRecord := func(kind, id string, record any) error {
		raw, err := EncodeContextRequirementImage(kind, id, record)
		if err != nil {
			return ErrInvalid
		}
		out = append(out, ContextRequirementImage{Kind: kind, ID: id, Bytes: raw})
		return nil
	}
	seen := map[model.ExecutionID]bool{}
	seenTasks := map[model.EntityID]bool{}
	for current := x; ; {
		if seen[current.ID] {
			return nil, ErrConflict
		}
		seen[current.ID] = true
		task, exists := tasks[current.Task]
		if !exists {
			return nil, ErrUnknown
		}
		if !seenTasks[task.ID] {
			raw, err := GoalKnowledge(task)
			if err != nil {
				return nil, err
			}
			out = append(out, ContextRequirementImage{Kind: "goal", ID: string(task.ID), Bytes: raw})
			seenTasks[task.ID] = true
		}
		if current.Parent == "" {
			break
		}
		var existsParent bool
		current, existsParent = executions[current.Parent]
		if !existsParent {
			return nil, ErrUnknown
		}
	}
	for _, invocation := range snapshot.Invocations {
		if invocation.Execution == execution && (invocation.Status == InvocationPending || invocation.Status == InvocationUnknown || invocation.Status == InvocationAccepted) {
			if err := appendRecord("invocation", invocation.ID, invocation); err != nil {
				return nil, err
			}
		}
	}
	for _, action := range snapshot.Actions {
		if action.Owner == execution && (action.Status == ActionPending || action.Status == ActionUnknown) {
			if err := appendRecord("action", fmt.Sprintf("%s/%d", action.Key, action.Generation), action); err != nil {
				return nil, err
			}
		}
	}
	consumed := map[string]bool{}
	for _, consumption := range snapshot.Lifecycle.Consumptions {
		if consumption.Recipient == execution {
			consumed[consumption.Message] = true
		}
	}
	for _, message := range snapshot.Lifecycle.Messages {
		if message.Recipient == execution && !consumed[message.ID] {
			if err := appendRecord("message", message.ID, message); err != nil {
				return nil, err
			}
		}
	}
	if recovery := snapshot.Lifecycle.Recovery; recovery != nil {
		specs := map[string]ActivitySpec{}
		for _, spec := range recovery.ActivitySpecs {
			specs[spec.Activity] = spec
		}
		for _, attempt := range recovery.AttemptControls {
			spec := specs[attempt.Activity]
			if spec.Execution != execution || (attempt.Status != "dispatch_recorded" && attempt.Status != "effect_unknown") {
				continue
			}
			record := struct {
				Spec           ActivitySpec
				Attempt        AttemptControl
				Reconciliation AttemptReconciliation
			}{Spec: spec, Attempt: attempt}
			for _, r := range recovery.Reconciliations {
				if r.Attempt == attempt.Attempt && r.Version > record.Reconciliation.Version {
					record.Reconciliation = r
				}
			}
			if err := appendRecord("unknown_attempt", attempt.Attempt, record); err != nil {
				return nil, err
			}
		}
		latest := map[string]ValidationRequirement{}
		for _, requirement := range recovery.ValidationRequirements {
			if requirement.Spec.Execution != execution || requirement.Spec.GoalEpoch != tasks[x.Task].GoalEpoch {
				continue
			}
			key := string(requirement.Spec.Task)
			if requirement.Version > latest[key].Version {
				latest[key] = requirement
			}
		}
		for _, requirement := range latest {
			if err := appendRecord("validation", string(requirement.ID), requirement); err != nil {
				return nil, err
			}
		}
		for _, join := range recovery.Joins {
			if join.Parent == execution && join.Status == "waiting" {
				if err := appendRecord("join", join.ID, join); err != nil {
					return nil, err
				}
			}
		}
	}
	if len(out) > MaxStateRecords {
		return nil, ErrInvalid
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}
