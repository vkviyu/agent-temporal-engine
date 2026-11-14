package agentprofile

import (
	"bytes"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// validateFinalAnswer binds a final response to the accepted Query model
// response and the exact completed outcomes of every work slot it started.
func validateFinalAnswer(snapshot Snapshot, query, generatedFrom string, body []byte, outcomes []TaskOutcome, inputEvidence model.ObjectID) error {
	r := snapshot.Lifecycle.Recovery
	if r == nil || generatedFrom == "" {
		return ErrConflict
	}
	var result ActivityResult
	for _, candidate := range snapshot.Lifecycle.Results {
		if candidate.ID == generatedFrom {
			result = candidate
			break
		}
	}
	if result.ID == "" || result.Outcome != "success" || result.Capture != "full" || !bytes.Equal(result.Payload, body) {
		return ErrConflict
	}
	accepted := false
	for _, control := range r.ActivityControls {
		accepted = accepted || (control.Activity == result.Activity && control.AcceptedResult == result.ID)
	}
	modelSource := false
	for _, spec := range r.ActivitySpecs {
		modelSource = modelSource || (spec.Activity == result.Activity && spec.Query == query && spec.Execution == "" && spec.Role == "query_decision" && spec.FinalAnswerInput == inputEvidence)
	}
	if !accepted || !modelSource || (len(outcomes) > 0 && inputEvidence == "") {
		return ErrConflict
	}
	return validateAnswerOutcomes(snapshot, query, outcomes)
}

func validateAnswerOutcomes(snapshot Snapshot, query string, outcomes []TaskOutcome) error {
	r := snapshot.Lifecycle.Recovery
	if r == nil {
		return ErrConflict
	}
	byTask := make(map[model.EntityID]TaskOutcome, len(outcomes))
	for _, outcome := range outcomes {
		if _, duplicate := byTask[outcome.Task]; duplicate {
			return ErrConflict
		}
		byTask[outcome.Task] = outcome
	}
	started := 0
	for _, start := range r.Starts {
		if start.Query != query {
			continue
		}
		started++
		outcome, ok := byTask[start.Task]
		if !ok || outcome.Execution != start.Execution {
			return ErrConflict
		}
		found := false
		for _, recorded := range r.Outcomes {
			found = found || recorded == outcome
		}
		if !found {
			return ErrConflict
		}
	}
	if started != len(outcomes) {
		return ErrConflict
	}
	return nil
}

// FinalAnswerInput is the host renderer's immutable attestation that the exact
// recorded provider request was formed from these completed task outcomes.
// The engine authenticates its links and source cut. The named host rendering
// policy remains responsible for how evidence appears in the provider request.
type FinalAnswerInput struct {
	SchemaVersion     uint64
	Query             string
	Outcomes          []TaskOutcome
	ContextPacket     model.ObjectID
	Request           model.ObjectID
	RequestDigest     model.ObjectID
	HostPolicyVersion string
}

// EncodeFinalAnswerInput requires Outcomes in strictly increasing Task order.
// Their complete immutable values, including the validation binding, are part
// of the renderer attestation's canonical bytes.
func EncodeFinalAnswerInput(input FinalAnswerInput) ([]byte, error) {
	if input.SchemaVersion != 1 || !commandText(input.Query) || len(input.Outcomes) == 0 || len(input.Outcomes) > MaxLifecycleRecords || !validDigest(input.ContextPacket) || !validDigest(input.Request) || !validDigest(input.RequestDigest) || !commandText(input.HostPolicyVersion) {
		return nil, ErrCommandShape
	}
	for i, outcome := range input.Outcomes {
		if !commandText(string(outcome.Task)) || !commandText(string(outcome.Execution)) || outcome.GoalEpoch == 0 || outcome.Sequence == 0 || !validDigest(outcome.Result) || !validDigest(outcome.Validation) || (outcome.ValidationRequirement != "" && !validDigest(outcome.ValidationRequirement)) || (i > 0 && input.Outcomes[i-1].Task >= outcome.Task) {
			return nil, ErrCommandShape
		}
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > MaxCommandBytes {
		return nil, ErrCommandShape
	}
	return raw, nil
}

func DecodeFinalAnswerInput(raw []byte) (FinalAnswerInput, error) {
	var input FinalAnswerInput
	if len(raw) > MaxCommandBytes || json.Unmarshal(raw, &input) != nil {
		return input, ErrCommandShape
	}
	canonical, err := EncodeFinalAnswerInput(input)
	if err != nil || !bytes.Equal(raw, canonical) {
		return input, ErrCommandShape
	}
	return input, nil
}

// ValidateFinalAnswerInput checks the host attestation against the profile at
// the model Activity's published input cut, before that Activity is dispatched.
func ValidateFinalAnswerInput(snapshot Snapshot, spec ActivitySpec, input FinalAnswerInput) error {
	if spec.Query != input.Query || spec.Execution != "" || spec.Role != "query_decision" || spec.ContextPacket != input.ContextPacket || spec.Request != input.Request || spec.RequestDigest != input.RequestDigest {
		return ErrConflict
	}
	return validateAnswerOutcomes(snapshot, input.Query, input.Outcomes)
}
