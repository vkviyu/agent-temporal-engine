package agentprofile

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func validationFixture(t *testing.T) (*Ledger, AuthorityBinding, ValidationRequirement) {
	t.Helper()
	l, authority := recoveryFixture(t)
	task, _ := l.GetTask("task")
	goal, err := GoalKnowledgeDigest(task)
	if err != nil {
		t.Fatal(err)
	}
	r := ValidationRequirement{Version: 1, Source: recoveryFence(), Spec: ValidationSpec{Space: "space", Task: "task", Execution: "exec", GoalEpoch: 1, Goal: goal, Result: digest('a'), Command: digest('b'), Environment: digest('c'), ToolVersion: "checker/v1", PolicyVersion: "acceptance/v1"}}
	r.ID = ValidationRequirementID(r)
	return l, authority, r
}

func TestValidationEnvelopeMustMatchEveryCurrentRequirementBinding(t *testing.T) {
	_, _, requirement := validationFixture(t)
	valid := ValidationEnvelope{Requirement: requirement.ID, Spec: requirement.Spec, Passed: true, Evidence: []model.ObjectID{digest('d')}}
	if err := valid.Satisfies(requirement); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ValidationEnvelope){
		func(v *ValidationEnvelope) { v.Spec.Result = digest('f') },
		func(v *ValidationEnvelope) { v.Spec.GoalEpoch++ },
		func(v *ValidationEnvelope) { v.Spec.Goal = digest('f') },
		func(v *ValidationEnvelope) { v.Spec.Command = digest('f') },
		func(v *ValidationEnvelope) { v.Spec.Environment = digest('f') },
		func(v *ValidationEnvelope) { v.Spec.ToolVersion = "checker/v2" },
		func(v *ValidationEnvelope) { v.Spec.PolicyVersion = "weaker/v1" },
		func(v *ValidationEnvelope) { v.Passed = false },
	} {
		changed := valid
		change(&changed)
		if err := changed.Satisfies(requirement); !errors.Is(err, ErrValidation) {
			t.Fatalf("unbound acceptance evidence satisfied requirement: %+v %v", changed, err)
		}
	}
}

func TestValidationCanonicalBytesAndClosedDecoders(t *testing.T) {
	_, _, r := validationFixture(t)
	raw, err := r.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := DecodeValidationRequirement(raw)
	if err != nil || restored != r {
		t.Fatalf("requirement roundtrip: %+v %v", restored, err)
	}
	v := ValidationEnvelope{Requirement: r.ID, Spec: r.Spec, Passed: true, Evidence: []model.ObjectID{digest('d')}}
	envelope, err := v.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeValidationEnvelope(envelope)
	if err != nil || decoded.Satisfies(r) != nil {
		t.Fatal("envelope roundtrip changed contract", err)
	}
	for n := 0; n < len(raw); n++ {
		if _, err := DecodeValidationRequirement(raw[:n]); err == nil {
			t.Fatalf("truncated requirement accepted at %d", n)
		}
	}
	for n := 0; n < len(envelope); n++ {
		if _, err := DecodeValidationEnvelope(envelope[:n]); err == nil {
			t.Fatalf("truncated envelope accepted at %d", n)
		}
	}
	if _, err := DecodeValidationEnvelope(append(bytes.Clone(envelope), 0)); err == nil {
		t.Fatal("trailing envelope bytes accepted")
	}
}

func TestValidationRequirementUpdatesAreCASAndOldEnvelopeCannotPass(t *testing.T) {
	l, authority, first := validationFixture(t)
	bind := ControlCommand{BindValidation: &BindValidationCommand{Requirement: first, Authority: authority}}
	recoveryControl(t, l, "require-first", bind)
	old := ValidationEnvelope{Requirement: first.ID, Spec: first.Spec, Passed: true, Evidence: []model.ObjectID{digest('d')}}
	second := first
	second.Version++
	second.Spec.Environment = digest('e')
	second.ID = ValidationRequirementID(second)
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{BindValidation: &BindValidationCommand{Requirement: second, Authority: authority}}}, ErrCommandShape)
	recoveryControl(t, l, "require-second", ControlCommand{BindValidation: &BindValidationCommand{Requirement: second, ExpectedVersion: 1, Authority: authority}})
	latest, err := LatestValidationRequirement(l.Snapshot(), first.Spec.Task, first.Spec.Execution, first.Spec.GoalEpoch)
	if err != nil || latest != second || old.Satisfies(latest) == nil {
		t.Fatal("old environment validation remained current", err)
	}
	image, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	l, err = RestoreSnapshot(image)
	if err != nil {
		t.Fatal(err)
	}
	latest, err = LatestValidationRequirement(l.Snapshot(), first.Spec.Task, first.Spec.Execution, first.Spec.GoalEpoch)
	if err != nil || latest != second {
		t.Fatal("requirements lost across snapshot restore", err)
	}
}
