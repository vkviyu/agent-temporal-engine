package agentprofile

import (
	"bytes"
	"math"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (l *Ledger) applyControl(key model.RequestKey, c ControlCommand) (Result, error) {
	s := l.recovery()
	switch {
	case c.AnswerQuery != nil:
		x := c.AnswerQuery
		i, q, err := l.query(x.Query)
		if err != nil {
			return Result{}, err
		}
		for _, a := range s.Answers {
			if a.Query == x.Query && a.Slot == x.Slot {
				if a.Kind == x.Kind && a.Final == x.Final && bytes.Equal(a.Body, x.Body) {
					return Result{}, nil
				}
				return Result{}, ErrUniqueSlot
			}
		}
		if q.Version != x.ExpectedVersion || q.Status != "open" || q.Version == math.MaxUint64 {
			return Result{}, ErrConflict
		}
		q.Version++
		if x.Final {
			q.Status = "answered"
			q.Answer = bytes.Clone(x.Body)
		}
		l.lifecycle.Queries[i] = q
		s.Answers = append(s.Answers, QueryAnswer{Query: x.Query, Slot: x.Slot, Kind: x.Kind, Version: q.Version, Body: bytes.Clone(x.Body), Final: x.Final})
		return Result{}, nil
	case c.StartWork != nil:
		x := c.StartWork
		raw, err := encodeControlCommand(c)
		if err != nil {
			return Result{}, err
		}
		intentDigest := commandHash(raw)
		for _, a := range s.Starts {
			if a.Query == x.Query && a.Slot == x.Slot {
				if a.Task != x.Task.ID || a.Execution != x.Execution.ID || a.IntentDigest == "" || a.IntentDigest != intentDigest {
					return Result{}, ErrUniqueSlot
				}
				t := l.tasks[a.Task]
				e := l.executions[a.Execution]
				return Result{Task: &t, Execution: &e}, nil
			}
		}
		_, q, err := l.query(x.Query)
		if err != nil {
			return Result{}, err
		}
		if q.Status != "open" || q.Version != x.ExpectedQueryVersion {
			return Result{}, ErrConflict
		}
		if _, ok := l.tasks[x.Task.ID]; ok {
			return Result{}, ErrConflict
		}
		if _, ok := l.executions[x.Execution.ID]; ok {
			return Result{}, ErrConflict
		}
		l.tasks[x.Task.ID] = x.Task
		l.executions[x.Execution.ID] = x.Execution
		s.Starts = append(s.Starts, WorkStart{Query: x.Query, Slot: x.Slot, Task: x.Task.ID, Execution: x.Execution.ID, IntentDigest: intentDigest})
		t, e := x.Task, x.Execution
		return Result{Task: &t, Execution: &e}, nil
	case c.GrantAuthority != nil:
		x := c.GrantAuthority
		a := x.Authority
		e, ok := l.executions[a.Execution]
		if !ok {
			return Result{}, ErrUnknown
		}
		t := l.tasks[e.Task]
		if t.Status != TaskOpen || e.Status == ExecutionCanceled || e.Status == ExecutionCompleted || a.Binding.Incarnation != e.Incarnation {
			return Result{}, ErrCanceled
		}
		if _, old, ok := l.authority(a.Execution); ok {
			if old.Binding == a.Binding && old.Evidence == a.Evidence && old.Status == "active" {
				return Result{Execution: &e}, nil
			}
			return Result{}, ErrConflict
		}
		if x.ExpectedVersion != 0 {
			return Result{}, ErrConflict
		}
		a.Version = 1
		s.Authorities = append(s.Authorities, a)
		return Result{Execution: &e}, nil
	case c.FenceAuthority != nil:
		x := c.FenceAuthority
		_, a, ok := l.authority(x.Execution)
		if !ok {
			return Result{}, ErrUnknown
		}
		if a.Status != "active" && a.Status != "fenced" {
			return Result{}, ErrConflict
		}
		// A parent fence propagates to descendants. A worker may still issue
		// its own fence command after that propagation; treat the already
		// fenced version as an idempotent acknowledgement so Resume can use the
		// version produced by the inherited barrier.
		if a.Status == "fenced" && (a.Version == x.ExpectedVersion || (x.ExpectedVersion != math.MaxUint64 && a.Version == x.ExpectedVersion+1)) {
			e := l.executions[x.Execution]
			return Result{Execution: &e}, nil
		}
		if a.Version != x.ExpectedVersion || a.Version == math.MaxUint64 {
			return Result{}, ErrConflict
		}
		l.fenceExecutionTree(x.Execution, x.Evidence)
		e := l.executions[x.Execution]
		return Result{Execution: &e}, nil
	case c.Resume != nil:
		x := c.Resume
		for _, continuation := range l.lifecycle.Continuations {
			if continuation.Execution == x.Execution && continuation.Position != nil && continuation.Position.AdapterVersion != x.AdapterVersion {
				// Adapter migration requires a separate typed contract.
				return Result{}, ErrConflict
			}
		}
		i, a, ok := l.authority(x.Execution)
		if !ok {
			return Result{}, ErrUnknown
		}
		e := l.executions[x.Execution]
		t := l.tasks[e.Task]
		if a.Status != "fenced" || a.Version != x.ExpectedAuthorityVersion || a.Version == math.MaxUint64 || e.Incarnation != x.ExpectedIncarnation || x.Next.Incarnation != e.Incarnation+1 || t.Status != TaskOpen || e.Status == ExecutionCanceled || e.Status == ExecutionCompleted || x.Next.OwnerEpoch <= a.Binding.OwnerEpoch {
			return Result{}, ErrConflict
		}
		e.Incarnation++
		e.Status = ExecutionReady
		l.executions[e.ID] = e
		a.Version++
		a.Status = "active"
		a.Binding = x.Next
		a.Evidence = x.ResourceFenceEvidence
		s.Authorities[i] = a
		s.Resumes = append(s.Resumes, *x)
		return Result{Execution: &e}, nil
	case c.PutIdempotencyGuarantee != nil:
		return l.putIdempotencyGuarantee(c.PutIdempotencyGuarantee.Guarantee)
	case c.AuthorizeRetry != nil:
		return l.authorizeRetry(c.AuthorizeRetry.Authorization)
	case c.CreateActivity != nil:
		x := c.CreateActivity
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
	case c.ClaimAttempt != nil:
		x := c.ClaimAttempt
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
	case c.DispatchAttempt != nil:
		x := c.DispatchAttempt
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
	case c.CaptureResult != nil:
		v := c.CaptureResult.Result
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
	case c.AcceptActivity != nil:
		x := c.AcceptActivity
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
		if v.Activity != x.Activity || a.Activity != x.Activity || a.Authority != x.Authority || v.RequestDigest != spec.RequestDigest || v.Outcome == "unknown" || a.Status != "result_recorded" {
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
	case c.SettleActivity != nil:
		x := c.SettleActivity
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
	case c.ReconcileAttempt != nil:
		x := c.ReconcileAttempt
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
	case c.RetryActivity != nil:
		x := c.RetryActivity
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
	case c.SaveContinuation != nil:
		x := c.SaveContinuation
		v := cloneContinuation(x.Value)
		if _, _, err := l.checkExecution(v.Execution, x.Authority, true, true); err != nil {
			return Result{}, err
		}
		if v.Incarnation != x.Authority.Incarnation {
			return Result{}, ErrConflict
		}
		a, ok := l.actions[v.PendingKey]
		if !ok || a.Owner != v.Execution || a.Generation != v.PendingGeneration || a.InputFence != v.InputFence {
			return Result{}, ErrConflict
		}
		if err := validateContinuationRelations(v, l.Snapshot()); err != nil {
			return Result{}, err
		}
		if v.Position != nil {
			var latestStep uint64
			for _, step := range l.steps {
				if step.Execution == v.Execution && step.Sequence > latestStep {
					latestStep = step.Sequence
				}
			}
			if v.Position.Step != latestStep {
				return Result{}, ErrConflict
			}
			for _, ref := range v.Position.NextActions {
				action, found := l.actions[ref.Key]
				if !found || action.Owner != v.Execution || action.Generation != ref.Generation {
					return Result{}, ErrConflict
				}
			}
		}
		i, found := findLifecycle(l.lifecycle.Continuations, func(c Continuation) bool { return c.Execution == v.Execution })
		var old uint64
		if found {
			old = l.lifecycle.Continuations[i].Version
			if l.lifecycle.Continuations[i].Position != nil && v.Position == nil {
				return Result{}, ErrConflict
			}
			if oldPosition := l.lifecycle.Continuations[i].Position; oldPosition != nil {
				if oldPosition.AdapterVersion != v.Position.AdapterVersion || (!continuationKnowledgeEmpty(oldPosition) && continuationKnowledgeEmpty(v.Position)) {
					return Result{}, ErrConflict
				}
			}
		}
		if old != x.ExpectedVersion {
			return Result{}, ErrConflict
		}
		if found {
			l.lifecycle.Continuations[i] = v
		} else {
			l.lifecycle.Continuations = append(l.lifecycle.Continuations, v)
		}
		return Result{}, nil
	case c.ObserveGoal != nil:
		x := c.ObserveGoal
		e, t, err := l.checkExecution(x.Execution, x.Authority, false, true)
		if err != nil {
			return Result{}, err
		}
		if t.GoalEpoch != x.Epoch {
			return Result{}, ErrGoalEpoch
		}
		for i, v := range s.Goals {
			if v.Execution == x.Execution && v.Epoch == x.Epoch {
				if v.Observation == "" {
					v.Observation, v.Evidence = x.Observation, x.Evidence
					s.Goals[i] = v
					e.AcceptedGoalEpoch = x.Epoch
					l.executions[e.ID] = e
					return Result{Execution: &e}, nil
				}
				if v.Evidence != x.Evidence || v.Observation != x.Observation {
					return Result{}, ErrConflict
				}
				return Result{Execution: &e}, nil
			}
		}
		e.AcceptedGoalEpoch = x.Epoch
		l.executions[e.ID] = e
		s.Goals = append(s.Goals, GoalObservation{Execution: e.ID, Epoch: x.Epoch, Evidence: x.Evidence, Observation: x.Observation})
		return Result{Execution: &e}, nil
	case c.CompleteTask != nil:
		x := c.CompleteTask
		e, t, err := l.checkExecution(x.Execution, x.Authority, true, true)
		if err != nil {
			return Result{}, err
		}
		if e.Task != x.Task || t.GoalEpoch != x.Epoch {
			return Result{}, ErrGoalEpoch
		}
		for _, v := range s.Outcomes {
			if v.Task == x.Task {
				return Result{}, ErrUniqueSlot
			}
		}
		for _, child := range l.executions {
			if child.Parent == e.ID && child.Status != ExecutionCompleted && child.Status != ExecutionCanceled {
				return Result{}, ErrConflict
			}
			if child.Task == t.ID && child.ID != e.ID && child.Status != ExecutionCompleted && child.Status != ExecutionCanceled {
				return Result{}, ErrConflict
			}
		}
		for _, step := range l.steps {
			if step.Execution == e.ID && step.Status != StepCompleted && step.Status != StepCanceled {
				return Result{}, ErrConflict
			}
		}
		if l.hasUnknownEffectsTree(e.ID) {
			return Result{}, ErrActionUnknown
		}
		if err := l.requireActivityAcceptance(e.ID, 0); err != nil {
			return Result{}, err
		}
		t.Status = TaskCompleted
		e.Status = ExecutionCompleted
		l.tasks[t.ID] = t
		l.executions[e.ID] = e
		var sequence uint64
		for _, v := range s.Outcomes {
			if v.Sequence > sequence {
				sequence = v.Sequence
			}
		}
		if sequence == math.MaxUint64 {
			return Result{}, ErrConflict
		}
		outcome := TaskOutcome{Task: t.ID, Execution: e.ID, GoalEpoch: x.Epoch, Result: x.Result, Validation: x.Validation, Sequence: sequence + 1}
		if requirement, err := LatestValidationRequirement(l.Snapshot(), t.ID, e.ID, x.Epoch); err == nil {
			outcome.ValidationRequirement = requirement.ID
		}
		s.Outcomes = append(s.Outcomes, outcome)
		return Result{Task: &t, Execution: &e}, nil
	case c.CancelExecution != nil:
		x := c.CancelExecution
		e, ok := l.executions[x.Execution]
		if !ok {
			return Result{}, ErrUnknown
		}
		if e.Incarnation != x.ExpectedIncarnation {
			return Result{}, ErrConflict
		}
		l.cancelExecutionTree(e.ID)
		e = l.executions[e.ID]
		return Result{Execution: &e}, nil
	case c.SpawnChild != nil:
		x := c.SpawnChild
		parent, t, err := l.checkExecution(x.Parent, x.Authority, true, true)
		if err != nil {
			return Result{}, err
		}
		if x.InheritedEpoch != t.GoalEpoch {
			return Result{}, ErrGoalEpoch
		}
		raw, err := encodeControlCommand(c)
		if err != nil {
			return Result{}, err
		}
		intentDigest := commandHash(raw)
		for _, link := range s.TaskLinks {
			if link.ParentExecution == x.Parent && link.Invocation == x.Invocation {
				if link.Task != x.Task.ID || (link.IntentDigest == "" || link.IntentDigest != intentDigest) {
					return Result{}, ErrUniqueSlot
				}
				e, ok := l.executions[x.Execution.ID]
				if !ok || e.Task != link.Task {
					return Result{}, ErrUniqueSlot
				}
				task := l.tasks[link.Task]
				return Result{Task: &task, Execution: &e}, nil
			}
		}
		inv, ok := l.invocations[x.Invocation]
		if !ok || inv.Execution != parent.ID || inv.Status != InvocationPending {
			return Result{}, ErrConflict
		}
		if _, ok = l.tasks[x.Task.ID]; ok {
			return Result{}, ErrConflict
		}
		if _, ok = l.executions[x.Execution.ID]; ok {
			return Result{}, ErrConflict
		}
		l.tasks[x.Task.ID] = x.Task
		l.executions[x.Execution.ID] = x.Execution
		s.TaskLinks = append(s.TaskLinks, TaskLink{Task: x.Task.ID, ParentExecution: parent.ID, Invocation: inv.ID, InheritedTask: t.ID, InheritedEpoch: t.GoalEpoch, Compatibility: x.Compatibility, IntentDigest: intentDigest})
		task, e := x.Task, x.Execution
		// The accepted result of the Spawn command is the child identity. The
		// parent remains responsible for joining or canceling the child tree.
		inv.Status = InvocationAccepted
		inv.Result = []byte(e.ID)
		l.invocations[inv.ID] = inv
		return Result{Task: &task, Execution: &e, Invocation: &inv}, nil
	case c.PutAction != nil:
		x := c.PutAction
		v := x.Intent
		if _, _, err := l.checkExecution(v.Action.Owner, x.Authority, true, true); err != nil {
			return Result{}, err
		}
		if v.Action.CommandDigest != commandHash(v.ExactCommand) {
			return Result{}, ErrConflict
		}
		for _, id := range v.InputObjects {
			if !validDigest(id) {
				return Result{}, ErrInvalid
			}
		}
		if _, ok := l.actions[v.Action.Key]; ok {
			return Result{}, ErrUniqueSlot
		}
		if l.actionRequestReserved(v.Action.Request) {
			return Result{}, ErrUniqueSlot
		}
		v.ExactCommand = bytes.Clone(v.ExactCommand)
		v.InputObjects = append([]model.ObjectID(nil), v.InputObjects...)
		v.ExpectedFence = cloneExpectedFence(v.ExpectedFence)
		l.actions[v.Action.Key] = v.Action
		s.Actions = append(s.Actions, v)
		a := v.Action
		return Result{Pending: &a}, nil
	case c.ResolveAction != nil:
		v := c.ResolveAction.Resolution
		a, ok := l.actions[v.Key]
		if !ok {
			return Result{}, ErrUnknown
		}
		if a.Generation != v.Generation {
			return Result{}, ErrConflict
		}
		for _, r := range s.Resolutions {
			if r.Key == v.Key && r.Generation == v.Generation && r.Outcome == "applied" {
				if r == v {
					return Result{Pending: &a}, nil
				}
				return Result{}, ErrConflict
			}
		}
		if v.Outcome == "applied" {
			a.Status = ActionApplied
		} else if v.Outcome == "unknown" {
			a.Status = ActionUnknown
		} else {
			a.Status = ActionFenced
		}
		l.actions[a.Key] = a
		s.Resolutions = append(s.Resolutions, v)
		return Result{Pending: &a}, nil
	case c.RebindAction != nil:
		x := c.RebindAction
		a, ok := l.actions[x.Key]
		if !ok {
			return Result{}, ErrUnknown
		}
		if _, _, err := l.checkExecution(a.Owner, x.Authority, true, true); err != nil {
			return Result{}, err
		}
		if a.Generation != x.ExpectedGeneration || a.Status != ActionFenced || x.NewRequest == a.Request || x.CommandDigest != commandHash(x.ExactCommand) {
			return Result{}, ErrConflict
		}
		if l.actionRequestReserved(x.NewRequest) {
			return Result{}, ErrUniqueSlot
		}
		absent := false
		for _, v := range s.Resolutions {
			if v.Key == a.Key && v.Generation == a.Generation && v.Outcome == "absent" {
				absent = true
			}
		}
		if !absent {
			return Result{}, ErrActionUnknown
		}
		i, ok := findLifecycle(s.Actions, func(v ActionIntent) bool { return v.Action.Key == a.Key && v.Action.Generation == a.Generation })
		if !ok {
			return Result{}, ErrUnknown
		}
		a.Generation++
		a.Request = x.NewRequest
		a.CommandDigest = x.CommandDigest
		a.Status = ActionRebound
		a.Supersedes = a.Key
		l.actions[a.Key] = a
		v := s.Actions[i]
		_, authority, ok := l.authority(a.Owner)
		if !ok || v.Authority == x.Authority || v.Authority.Incarnation >= x.Authority.Incarnation || authority.Evidence != x.FenceEvidence {
			return Result{}, ErrConflict
		}
		v.Action = a
		v.Method = x.Method
		v.ExactCommand = bytes.Clone(x.ExactCommand)
		v.Authority = x.Authority
		v.ExpectedFence = cloneExpectedFence(x.ExpectedFence)
		s.Actions = append(s.Actions, v)
		return Result{Pending: &a}, nil
	case c.BeginStep != nil:
		x := c.BeginStep
		if _, _, err := l.checkExecution(x.Step.Execution, x.Authority, true, true); err != nil {
			return Result{}, err
		}
		v, err := l.StartStep(StepRequest{Step: x.Step, Request: key})
		if err != nil {
			return Result{}, err
		}
		return Result{Step: &v}, nil
	case c.RecordInvocation != nil:
		x := c.RecordInvocation
		if _, _, err := l.checkExecution(x.Invocation.Execution, x.Authority, true, true); err != nil {
			return Result{}, err
		}
		step, ok := l.steps[stepKey(x.Invocation.Execution, x.Invocation.Step)]
		if !ok || (step.Status != StepOpen && step.Status != StepAccepted) {
			return Result{}, ErrConflict
		}
		v, err := l.RecordInvocation(InvocationRequest{Invocation: x.Invocation, Request: key})
		if err != nil {
			return Result{}, err
		}
		return Result{Invocation: &v}, nil
	case c.FinishStep != nil:
		x := c.FinishStep
		if _, _, err := l.checkExecution(x.Execution, x.Authority, true, true); err != nil {
			return Result{}, err
		}
		if err := l.requireActivityAcceptance(x.Execution, x.Sequence); err != nil {
			return Result{}, err
		}
		for _, a := range s.ActivitySpecs {
			if a.Execution == x.Execution && a.Step == x.Sequence && a.Role == "primary_decision" {
				_, ctl, err := l.activityControl(a.Activity)
				if err != nil || ctl.AcceptedResult == "" {
					return Result{}, ErrConflict
				}
			}
		}
		v, err := l.CompleteStep(x.Execution, x.Sequence, x.Epoch)
		if err != nil {
			return Result{}, err
		}
		return Result{Step: &v}, nil
	case c.JoinExecutions != nil:
		return l.applyExecutionJoin(*c.JoinExecutions)
	case c.RebaseInherited != nil:
		x := c.RebaseInherited
		e, _, err := l.checkExecution(x.Execution, x.Authority, false, false)
		if err != nil {
			return Result{}, err
		}
		i, ok := findLifecycle(s.TaskLinks, func(v TaskLink) bool { return v.Task == e.Task && v.InheritedTask == x.SourceTask })
		if !ok {
			return Result{}, ErrUnknown
		}
		v := s.TaskLinks[i]
		source, ok := l.tasks[x.SourceTask]
		if !ok || source.Status != TaskOpen || source.GoalEpoch != x.Epoch || v.InheritedEpoch != x.ExpectedEpoch {
			return Result{}, ErrGoalEpoch
		}
		if x.Epoch == x.ExpectedEpoch && v.GoalObservation != "" {
			if v.GoalObservation == x.Observation && v.Compatibility == x.Compatibility {
				return Result{Execution: &e}, nil
			}
			return Result{}, ErrConflict
		}
		v.InheritedEpoch = x.Epoch
		v.Compatibility = x.Compatibility
		v.GoalObservation = x.Observation
		s.TaskLinks[i] = v
		return Result{Execution: &e}, nil
	case c.Checkpoint != nil:
		x := c.Checkpoint
		if _, err := l.applyControl(key, ControlCommand{PutAction: &PutActionCommand{Intent: x.Intent, Authority: x.Authority}}); err != nil {
			return Result{}, err
		}
		if _, err := l.applyControl(key, ControlCommand{SaveContinuation: &SaveContinuationCommand{Value: x.Continuation, ExpectedVersion: x.ExpectedVersion, Authority: x.Authority}}); err != nil {
			return Result{}, err
		}
		a := l.actions[x.Intent.Action.Key]
		return Result{Pending: &a}, nil
	case c.SendMessage != nil:
		x := c.SendMessage
		_, sender, err := l.checkExecution(x.Message.Sender, x.Authority, true, true)
		if err != nil {
			return Result{}, err
		}
		recipient, ok := l.executions[x.Message.Recipient]
		if !ok {
			return Result{}, ErrUnknown
		}
		task := l.tasks[recipient.Task]
		if task.Owner != sender.Owner || task.Space != sender.Space || task.GoalEpoch != x.Message.GoalEpoch || task.Status != TaskOpen || recipient.Status == ExecutionCanceled || recipient.Status == ExecutionCompleted {
			return Result{}, ErrConflict
		}
		return l.applyLifecycle(LifecycleCommand{PutMessage: &x.Message})
	case c.ConsumeMessage != nil:
		x := c.ConsumeMessage
		_, task, err := l.checkExecution(x.Consumption.Recipient, x.Authority, true, true)
		if err != nil {
			return Result{}, err
		}
		message, err := l.GetMessage(x.Consumption.Message)
		if err != nil {
			return Result{}, err
		}
		if message.Recipient != x.Consumption.Recipient || message.GoalEpoch != task.GoalEpoch || x.Consumption.GoalEpoch != message.GoalEpoch || x.Consumption.Incarnation != x.Authority.Incarnation {
			return Result{}, ErrConflict
		}
		return l.applyLifecycle(LifecycleCommand{ConsumeMessage: &x.Consumption})
	case c.ImportActivityResult != nil:
		x := c.ImportActivityResult
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
		if result.Activity != spec.Activity || result.RequestDigest != spec.RequestDigest || result.Outcome == "unknown" || attempt.Authority.Incarnation >= x.Authority.Incarnation || attempt.Authority == x.Authority {
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
	case c.BindValidation != nil:
		x := c.BindValidation
		r := x.Requirement
		e, task, err := l.checkExecution(r.Spec.Execution, x.Authority, true, true)
		if err != nil {
			return Result{}, err
		}
		goal, err := GoalKnowledgeDigest(task)
		if err != nil || r.Spec.Task != task.ID || e.Task != task.ID || r.Spec.Space != task.Space || r.Spec.GoalEpoch != task.GoalEpoch || r.Spec.Goal != goal {
			return Result{}, ErrValidation
		}
		var previous uint64
		for _, old := range s.ValidationRequirements {
			if old.Spec.Task == task.ID && old.Spec.Execution == e.ID && old.Spec.GoalEpoch == task.GoalEpoch && old.Version > previous {
				previous = old.Version
			}
		}
		if previous != x.ExpectedVersion || previous == math.MaxUint64 || r.Version != previous+1 {
			return Result{}, ErrConflict
		}
		s.ValidationRequirements = append(s.ValidationRequirements, r)
		return Result{}, nil
	}
	return Result{}, ErrCommandShape
}
