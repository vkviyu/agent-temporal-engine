package agentprofile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const validationRequirementDomain = "ATE-VALIDATION-REQUIREMENT\x00"
const validationEnvelopeDomain = "ATE-VALIDATION-ENVELOPE\x00"

var ErrValidation = errors.New("validation evidence does not satisfy the current task requirement")

// ValidationSpec binds a host's acceptance rule to exact immutable inputs.
// Goal is a GoalKnowledgeDigest, not a raw codec object address. Command is the
// stored validation command (including arguments), Environment is its recorded
// resource/toolchain descriptor, and Result is the exact artifact accepted.
// The engine verifies these identities; the trusted host remains responsible
// for the truth of the external observation and for running its stated policy.
type ValidationSpec struct {
	Space         model.SpaceID
	Task          model.EntityID
	Execution     model.ExecutionID
	GoalEpoch     uint64
	Goal          model.ObjectID
	Result        model.ObjectID
	Command       model.ObjectID
	Environment   model.ObjectID
	ToolVersion   string
	PolicyVersion string
}

// ValidationRequirement is a controller-authorized, immutable acceptance
// contract. Versions are contiguous for (Task, Execution, GoalEpoch); replacing
// the required environment/command/rule needs an explicit new version, which
// makes an earlier envelope ineligible without changing its audit bytes.
type ValidationRequirement struct {
	ID      model.ObjectID
	Version uint64
	Spec    ValidationSpec
	Source  model.CommitFence
}

// ValidationEnvelope is immutable evidence for one exact requirement version.
// Passed is an attestation by the trusted host, not an engine execution of the
// command. A model's untyped text or a digest with no matching requirement is
// insufficient for a new task completion.
type ValidationEnvelope struct {
	Requirement model.ObjectID
	Spec        ValidationSpec
	Passed      bool
	Evidence    []model.ObjectID
}

func validationText(s string) bool {
	return s != "" && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s)
}

func (s ValidationSpec) Validate() error {
	if !validationText(string(s.Space)) || !validationText(string(s.Task)) || !validationText(string(s.Execution)) || s.GoalEpoch == 0 || !validationText(s.ToolVersion) || !validationText(s.PolicyVersion) {
		return ErrValidation
	}
	for _, id := range []model.ObjectID{s.Goal, s.Result, s.Command, s.Environment} {
		if !model.ValidDigest(string(id)) {
			return ErrValidation
		}
	}
	return nil
}

func GoalKnowledgeDigest(task Task) (model.ObjectID, error) {
	raw, err := GoalKnowledge(task)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return model.ObjectID(hex.EncodeToString(sum[:])), nil
}

func requirementBody(r ValidationRequirement) ([]byte, error) {
	if r.Spec.Validate() != nil || r.Version == 0 || r.Source.Validate() != nil {
		return nil, ErrValidation
	}
	b := codec.AppendUint([]byte(validationRequirementDomain), 1)
	b = codec.AppendUint(b, r.Version)
	b, err := appendControlValue(b, reflect.ValueOf(r.Spec), 0)
	if err != nil {
		return nil, ErrValidation
	}
	return appendControlValue(b, reflect.ValueOf(r.Source), 0)
}

func ValidationRequirementID(r ValidationRequirement) model.ObjectID {
	raw, err := requirementBody(r)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return model.ObjectID(hex.EncodeToString(sum[:]))
}

func (r ValidationRequirement) Canonical() ([]byte, error) {
	b, err := requirementBody(r)
	if err != nil || r.ID == "" || r.ID != ValidationRequirementID(r) {
		return nil, ErrValidation
	}
	return codec.AppendString(b, string(r.ID))
}

func DecodeValidationRequirement(raw []byte) (ValidationRequirement, error) {
	if len(raw) > MaxCommandBytes || !bytes.HasPrefix(raw, []byte(validationRequirementDomain)) {
		return ValidationRequirement{}, ErrValidation
	}
	d := codec.NewDecoder(raw[len(validationRequirementDomain):])
	version, err := d.Uint()
	if err != nil || version != 1 {
		return ValidationRequirement{}, ErrValidation
	}
	var r ValidationRequirement
	if r.Version, err = d.Uint(); err != nil {
		return ValidationRequirement{}, ErrValidation
	}
	if readControlValue(d, reflect.ValueOf(&r.Spec).Elem(), 0) != nil || readControlValue(d, reflect.ValueOf(&r.Source).Elem(), 0) != nil {
		return ValidationRequirement{}, ErrValidation
	}
	id, err := d.String(64)
	if err != nil || d.Done() != nil {
		return ValidationRequirement{}, ErrValidation
	}
	r.ID = model.ObjectID(id)
	canonical, err := r.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return ValidationRequirement{}, ErrValidation
	}
	return r, nil
}

func (v ValidationEnvelope) Canonical() ([]byte, error) {
	if !model.ValidDigest(string(v.Requirement)) || v.Spec.Validate() != nil || len(v.Evidence) == 0 || len(v.Evidence) > MaxLifecycleRecords {
		return nil, ErrValidation
	}
	seen := make(map[model.ObjectID]bool, len(v.Evidence))
	for _, id := range v.Evidence {
		if !model.ValidDigest(string(id)) || seen[id] {
			return nil, ErrValidation
		}
		seen[id] = true
	}
	b := codec.AppendUint([]byte(validationEnvelopeDomain), 1)
	b, err := appendControlValue(b, reflect.ValueOf(v), 0)
	if err != nil || len(b) > MaxCommandBytes {
		return nil, ErrValidation
	}
	return b, nil
}

func IsValidationEnvelope(raw []byte) bool {
	return bytes.HasPrefix(raw, []byte(validationEnvelopeDomain))
}

func DecodeValidationEnvelope(raw []byte) (ValidationEnvelope, error) {
	if len(raw) > MaxCommandBytes || !IsValidationEnvelope(raw) {
		return ValidationEnvelope{}, ErrValidation
	}
	d := codec.NewDecoder(raw[len(validationEnvelopeDomain):])
	version, err := d.Uint()
	if err != nil || version != 1 {
		return ValidationEnvelope{}, ErrValidation
	}
	var v ValidationEnvelope
	if readControlValue(d, reflect.ValueOf(&v).Elem(), 0) != nil || d.Done() != nil {
		return ValidationEnvelope{}, ErrValidation
	}
	canonical, err := v.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return ValidationEnvelope{}, ErrValidation
	}
	return v, nil
}

func (v ValidationEnvelope) Satisfies(r ValidationRequirement) error {
	if _, err := r.Canonical(); err != nil {
		return err
	}
	if _, err := v.Canonical(); err != nil {
		return err
	}
	if !v.Passed || v.Requirement != r.ID || v.Spec != r.Spec {
		return ErrValidation
	}
	return nil
}

func LatestValidationRequirement(s Snapshot, task model.EntityID, execution model.ExecutionID, epoch uint64) (ValidationRequirement, error) {
	var latest ValidationRequirement
	if s.Lifecycle.Recovery != nil {
		for _, r := range s.Lifecycle.Recovery.ValidationRequirements {
			if r.Spec.Task == task && r.Spec.Execution == execution && r.Spec.GoalEpoch == epoch && r.Version > latest.Version {
				latest = r
			}
		}
	}
	if latest.Version == 0 {
		return ValidationRequirement{}, ErrValidation
	}
	return latest, nil
}

func validateValidationRequirements(snapshot Snapshot) error {
	r := snapshot.Lifecycle.Recovery
	if r == nil {
		return nil
	}
	tasks := make(map[model.EntityID]Task, len(snapshot.Tasks))
	executions := make(map[model.ExecutionID]Execution, len(snapshot.Executions))
	for _, task := range snapshot.Tasks {
		tasks[task.ID] = task
	}
	for _, execution := range snapshot.Executions {
		executions[execution.ID] = execution
	}
	type scope struct {
		task      model.EntityID
		execution model.ExecutionID
		epoch     uint64
	}
	versions := map[scope]map[uint64]bool{}
	byID := map[model.ObjectID]ValidationRequirement{}
	for _, record := range r.ValidationRequirements {
		if _, err := record.Canonical(); err != nil {
			return ErrValidation
		}
		task, ok := tasks[record.Spec.Task]
		execution, executionOK := executions[record.Spec.Execution]
		if !ok || !executionOK || task.Space != record.Spec.Space || execution.Task != task.ID || record.Spec.GoalEpoch > task.GoalEpoch {
			return ErrValidation
		}
		if _, duplicate := byID[record.ID]; duplicate {
			return ErrValidation
		}
		byID[record.ID] = record
		key := scope{record.Spec.Task, record.Spec.Execution, record.Spec.GoalEpoch}
		if versions[key] == nil {
			versions[key] = map[uint64]bool{}
		}
		if record.Version > MaxLifecycleRecords || versions[key][record.Version] {
			return ErrValidation
		}
		versions[key][record.Version] = true
	}
	for _, sequence := range versions {
		for n := uint64(1); n <= uint64(len(sequence)); n++ {
			if !sequence[n] {
				return ErrValidation
			}
		}
	}
	for _, outcome := range r.Outcomes {
		if outcome.ValidationRequirement == "" {
			continue // historical untyped completion
		}
		required, ok := byID[outcome.ValidationRequirement]
		if !ok || required.Spec.Task != outcome.Task || required.Spec.Execution != outcome.Execution || required.Spec.GoalEpoch != outcome.GoalEpoch || required.Spec.Result != outcome.Result {
			return ErrValidation
		}
	}
	return nil
}
