package agentprofile

import (
	"bytes"
	"math"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (l *Ledger) applyCreateActivity(s *RecoverySnapshot, x *CreateActivityCommand) (Result, error) {
	v := x.Spec
	if err := l.checkActivity(v, x.Authority); err != nil {
		return Result{}, err
	}
	for _, old := range s.ActivitySpecs {
		if keyRequestConflict(old, v) {
			return Result{}, ErrConflict
		}
		if old.Activity == v.Activity {
			if old == v {
				return Result{}, nil
			}
			return Result{}, ErrConflict
		}
		if v.Invocation != "" && old.Execution == v.Execution && old.Query == v.Query && old.Invocation == v.Invocation {
			return Result{}, ErrUniqueSlot
		}
		if v.Role == "primary_decision" && old.Role == v.Role && old.Execution == v.Execution && old.Step == v.Step {
			return Result{}, ErrUniqueSlot
		}
	}
	if v.Execution != "" && v.Step > 0 {
		step, ok := l.steps[stepKey(v.Execution, v.Step)]
		if !ok || step.Status != StepOpen || step.GoalEpoch != v.GoalEpoch {
			return Result{}, ErrConflict
		}
	}
	if v.Invocation != "" {
		inv, ok := l.invocations[v.Invocation]
		if !ok || inv.Execution != v.Execution || inv.Step != v.Step || inv.Status != InvocationPending {
			return Result{}, ErrConflict
		}
	}
	if _, err := l.GetActivity(v.Activity); err == nil {
		return Result{}, ErrConflict
	}
	inc := uint64(1)
	if v.Execution != "" {
		inc = x.Authority.Incarnation
	}
	s.ActivitySpecs = append(s.ActivitySpecs, v)
	s.ActivityControls = append(s.ActivityControls, ActivityControl{Activity: v.Activity, Version: 1, Incarnation: inc})
	l.lifecycle.Activities = append(l.lifecycle.Activities, Activity{ID: v.Activity, Execution: v.Execution, Kind: v.Role, Invocation: v.Invocation, Step: v.Step, InputFence: v.InputFence, Status: "created", Version: 1})
	return Result{}, nil
}

func (l *Ledger) applyClaimAttempt(s *RecoverySnapshot, x *ClaimAttemptCommand) (Result, error) {
	spec, err := l.activitySpec(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if err = l.checkActivity(spec, x.Authority); err != nil {
		return Result{}, err
	}
	_, ctl, err := l.activityControl(x.Activity)
	if err != nil {
		return Result{}, err
	}
	inc := uint64(1)
	if spec.Execution != "" {
		inc = x.Authority.Incarnation
	}
	if ctl.AcceptedResult != "" || ctl.Incarnation != inc {
		return Result{}, ErrConflict
	}
	var last uint64
	for _, a := range s.AttemptControls {
		if a.Attempt == x.Attempt {
			if a.Activity == x.Activity && a.Sequence == x.Sequence && a.Authority == x.Authority && a.ClaimUse == x.Use {
				return Result{}, nil
			}
			return Result{}, ErrConflict
		}
		if a.Activity == x.Activity && a.Sequence > last {
			last = a.Sequence
		}
	}
	if x.Sequence != last+1 || last == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	if last > 0 && (!l.safeRetry(spec, x.Authority, ctl.Version, x.Sequence, "", x.Use) ||
		(x.Use.Authorization != "" && (ctl.RetryAuthorization != x.Use.Authorization || x.Use.ObservedAt < ctl.RetryObservedAt))) {
		return Result{}, ErrActionUnknown
	}
	for _, a := range s.AttemptControls {
		if a.Activity == x.Activity && (a.Status == "created" || a.Status == "dispatch_recorded") {
			return Result{}, ErrConflict
		}
	}
	s.AttemptControls = append(s.AttemptControls, AttemptControl{Attempt: x.Attempt, Activity: x.Activity, Sequence: x.Sequence, Authority: x.Authority, RequestDigest: spec.RequestDigest, Status: "created", ClaimUse: x.Use})
	l.lifecycle.Attempts = append(l.lifecycle.Attempts, Attempt{ID: x.Attempt, Activity: x.Activity, Sequence: x.Sequence, Incarnation: inc, Status: "created"})
	return Result{}, nil
}

func (l *Ledger) applyDispatchAttempt(s *RecoverySnapshot, x *DispatchAttemptCommand) (Result, error) {
	i, a, err := l.attemptControl(x.Attempt)
	if err != nil {
		return Result{}, err
	}
	spec, err := l.activitySpec(a.Activity)
	if err != nil {
		return Result{}, err
	}
	if err = l.checkActivity(spec, x.Authority); err != nil {
		return Result{}, err
	}
	if a.Authority != x.Authority {
		return Result{}, ErrConflict
	}
	if a.Status == "dispatch_recorded" && a.DispatchEvidence == x.Evidence && a.DispatchUse == x.Use {
		return Result{}, nil
	}
	if a.Status != "created" {
		return Result{}, ErrConflict
	}
	_, ctl, err := l.activityControl(spec.Activity)
	if err != nil {
		return Result{}, err
	}
	if a.Sequence > 1 && (x.Use.ObservedAt < a.ClaimUse.ObservedAt || !l.safeRetry(spec, x.Authority, ctl.Version, a.Sequence, a.Attempt, x.Use) || (x.Use.Authorization != "" && (ctl.RetryAuthorization != x.Use.Authorization || a.ClaimUse.Authorization != x.Use.Authorization))) {
		return Result{}, ErrActionUnknown
	}
	a.Status = "dispatch_recorded"
	a.DispatchUse = x.Use
	a.DispatchEvidence = x.Evidence
	s.AttemptControls[i] = a
	l.setAttemptStatus(a.Attempt, a.Status)
	return Result{}, nil
}

func (l *Ledger) applyCaptureResult(s *RecoverySnapshot, v ActivityResult) (Result, error) {
	i, a, err := l.attemptControl(v.Attempt)
	if err != nil {
		return Result{}, err
	}
	spec, err := l.activitySpec(v.Activity)
	if err != nil {
		return Result{}, err
	}
	inc := uint64(1)
	if spec.Execution != "" {
		inc = a.Authority.Incarnation
	}
	if a.Activity != v.Activity || a.RequestDigest != v.RequestDigest || inc != v.Incarnation || a.Status == "created" {
		return Result{}, ErrConflict
	}
	if err := validateResultChunks(v, s.OutputChunks); err != nil {
		return Result{}, err
	}
	for _, old := range l.lifecycle.Results {
		if old.ID == v.ID {
			if reflect.DeepEqual(old, v) {
				return Result{}, nil
			}
			return Result{}, ErrConflict
		}
		if old.Attempt == v.Attempt {
			return Result{}, ErrUniqueSlot
		}
	}
	quarantined := l.checkActivity(spec, a.Authority) != nil || (a.Status != "dispatch_recorded" && a.Status != "effect_unknown")
	if !quarantined {
		_, ctl, e := l.activityControl(spec.Activity)
		if e != nil {
			return Result{}, e
		}
		quarantined = ctl.Incarnation != inc
	}
	v.Payload = bytes.Clone(v.Payload)
	v.OutputChunks = append([]model.ObjectID(nil), v.OutputChunks...)
	l.lifecycle.Results = append(l.lifecycle.Results, v)
	s.Captures = append(s.Captures, ResultCapture{Result: v.ID, Quarantined: quarantined})
	if !quarantined {
		a.Status = "result_recorded"
		if v.Outcome == "unknown" {
			a.Status = "effect_unknown"
		}
		s.AttemptControls[i] = a
		l.setAttemptStatus(a.Attempt, a.Status)
	}
	return Result{}, nil
}

func (l *Ledger) applyAcceptActivity(s *RecoverySnapshot, x *AcceptActivityCommand) (Result, error) {
	spec, err := l.activitySpec(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if err = l.checkActivity(spec, x.Authority); err != nil {
		return Result{}, err
	}
	i, ctl, err := l.activityControl(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if ctl.AcceptedResult != "" {
		if ctl.AcceptedResult == x.Result {
			return Result{}, nil
		}
		return Result{}, ErrUniqueSlot
	}
	if ctl.Version != x.ExpectedVersion || ctl.Version == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	v, err := l.GetActivityResult(x.Result)
	if err != nil {
		return Result{}, err
	}
	_, a, err := l.attemptControl(v.Attempt)
	if err != nil {
		return Result{}, err
	}
	if v.Activity != x.Activity || a.Activity != x.Activity || a.Authority != x.Authority || v.RequestDigest != spec.RequestDigest || v.Outcome == "unknown" || !acceptableResultCapture(spec, v) || a.Status != "result_recorded" {
		return Result{}, ErrConflict
	}
	for _, capture := range s.Captures {
		if capture.Result == v.ID && capture.Quarantined {
			return Result{}, ErrConflict
		}
	}
	ctl.AcceptedResult = v.ID
	ctl.Version++
	s.ActivityControls[i] = ctl
	s.Acceptances = append(s.Acceptances, ActivityAcceptance{Activity: x.Activity, Result: x.Result, Version: ctl.Version, Authority: x.Authority, Evidence: x.Evidence})
	l.setActivityStatus(x.Activity, "accepted", ctl.Version)
	if spec.Invocation != "" {
		inv := l.invocations[spec.Invocation]
		inv.Status = InvocationAccepted
		if v.Outcome == "failure" {
			inv.Status = InvocationRejected
		}
		inv.Result = bytes.Clone(v.Payload)
		l.invocations[inv.ID] = inv
	}
	return Result{}, nil
}

func (l *Ledger) applySettleActivity(s *RecoverySnapshot, x *SettleActivityCommand) (Result, error) {
	if _, err := l.activitySpec(x.Activity); err != nil {
		return Result{}, err
	}
	var last uint64
	lastStatus := ""
	for _, v := range s.Settlements {
		if v.Activity == x.Activity && v.Version > last {
			last = v.Version
			lastStatus = v.Status
		}
	}
	if last != x.ExpectedVersion || last == math.MaxUint64 || lastStatus == "settled" {
		return Result{}, ErrConflict
	}
	s.Settlements = append(s.Settlements, ActivitySettlement{Activity: x.Activity, Version: last + 1, Status: x.Status, Evidence: x.Evidence})
	if x.Status == "settled" {
		spec, _ := l.activitySpec(x.Activity)
		_, ctl, _ := l.activityControl(x.Activity)
		if spec.Invocation != "" && ctl.AcceptedResult != "" {
			inv := l.invocations[spec.Invocation]
			inv.Status = InvocationSettled
			l.invocations[inv.ID] = inv
		}
	}
	return Result{}, nil
}

func (l *Ledger) applyReconcileAttempt(s *RecoverySnapshot, x *ReconcileAttemptCommand) (Result, error) {
	i, a, err := l.attemptControl(x.Attempt)
	if err != nil {
		return Result{}, err
	}
	if a.Status == "created" || a.Status == "fenced_before_dispatch" {
		return Result{}, ErrConflict
	}
	var last uint64
	terminal := false
	for _, v := range s.Reconciliations {
		if v.Attempt == x.Attempt && v.Version > last {
			last = v.Version
			terminal = v.Outcome != "unresolved"
		}
	}
	if last != x.ExpectedVersion || last == math.MaxUint64 || terminal {
		return Result{}, ErrConflict
	}
	s.Reconciliations = append(s.Reconciliations, AttemptReconciliation{Attempt: x.Attempt, Version: last + 1, Outcome: x.Outcome, Evidence: x.Evidence, FutureEffectFenced: x.FutureEffectFenced})
	if x.Outcome == "no_effect" {
		a.Status = "no_effect"
	} else if x.Outcome == "unresolved" {
		a.Status = "effect_unknown"
	} else {
		a.Status = x.Outcome
	}
	s.AttemptControls[i] = a
	l.setAttemptStatus(a.Attempt, a.Status)
	return Result{}, nil
}

func (l *Ledger) applyRetryActivity(s *RecoverySnapshot, x *RetryActivityCommand) (Result, error) {
	spec, err := l.activitySpec(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if err = l.checkActivity(spec, x.Authority); err != nil {
		return Result{}, err
	}
	i, ctl, err := l.activityControl(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if ctl.Version != x.ExpectedVersion || ctl.AcceptedResult != "" || ctl.Version == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	_, last := l.retryAttemptIDs(spec.Activity, "")
	if !l.safeRetry(spec, x.Authority, ctl.Version+1, last+1, "", x.Use) {
		return Result{}, ErrActionUnknown
	}
	ctl.Version++
	ctl.RetryAuthorization = x.Use.Authorization
	ctl.RetryObservedAt = x.Use.ObservedAt
	if spec.Execution != "" {
		ctl.Incarnation = x.Authority.Incarnation
	}
	s.ActivityControls[i] = ctl
	return Result{}, nil
}

func (l *Ledger) applyImportActivityResult(s *RecoverySnapshot, x *AcceptActivityCommand) (Result, error) {
	spec, err := l.activitySpec(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if spec.Execution == "" {
		return Result{}, ErrConflict
	}
	if err = l.checkActivity(spec, x.Authority); err != nil {
		return Result{}, err
	}
	i, ctl, err := l.activityControl(x.Activity)
	if err != nil {
		return Result{}, err
	}
	if ctl.AcceptedResult != "" || ctl.Version != x.ExpectedVersion || ctl.Version == math.MaxUint64 {
		return Result{}, ErrConflict
	}
	result, err := l.GetActivityResult(x.Result)
	if err != nil {
		return Result{}, err
	}
	_, attempt, err := l.attemptControl(result.Attempt)
	if err != nil {
		return Result{}, err
	}
	if result.Activity != spec.Activity || result.RequestDigest != spec.RequestDigest || result.Outcome == "unknown" || !acceptableResultCapture(spec, result) || attempt.Authority.Incarnation >= x.Authority.Incarnation || attempt.Authority == x.Authority {
		return Result{}, ErrConflict
	}
	var reconciliation AttemptReconciliation
	for _, r := range s.Reconciliations {
		if r.Attempt == attempt.Attempt && r.Version > reconciliation.Version {
			reconciliation = r
		}
	}
	if reconciliation.Outcome != "occurred" {
		return Result{}, ErrActionUnknown
	}
	captured := false
	for _, capture := range s.Captures {
		captured = captured || capture.Result == result.ID
	}
	if !captured {
		return Result{}, ErrUnknown
	}
	ctl.AcceptedResult = result.ID
	ctl.Incarnation = x.Authority.Incarnation
	ctl.Version++
	s.ActivityControls[i] = ctl
	accepted := ActivityAcceptance{Activity: x.Activity, Result: result.ID, Version: ctl.Version, Authority: x.Authority, Evidence: x.Evidence}
	s.Acceptances = append(s.Acceptances, accepted)
	s.Imports = append(s.Imports, accepted)
	l.setActivityStatus(spec.Activity, "accepted", ctl.Version)
	if spec.Invocation != "" {
		inv := l.invocations[spec.Invocation]
		inv.Status = InvocationAccepted
		if result.Outcome == "failure" {
			inv.Status = InvocationRejected
		}
		inv.Result = bytes.Clone(result.Payload)
		l.invocations[inv.ID] = inv
	}
	return Result{}, nil
}
