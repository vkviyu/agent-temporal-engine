package engine

import (
	"reflect"
	"strconv"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type profileEventDraft struct {
	attribution     model.EventAttribution
	stream          model.EventStreamKey
	causes          []profileEventCause
	actorAuthorized bool
	activity        string
}
type profileEventCause struct{ operation, subject string }

func eventSubject(kind, id string) model.EntityKey {
	return model.EntityKey{Profile: "agent", Kind: model.EntityKind(kind), ID: model.EntityID(id)}
}
func selectedProfileOperation(command any) string {
	v := reflect.ValueOf(command)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() == reflect.Pointer && !f.IsNil() {
			name := v.Type().Field(i).Name
			if name == "Control" || name == "Lifecycle" {
				return name + "." + selectedProfileOperation(f.Interface())
			}
			return name
		}
	}
	return ""
}

func agentEventDraft(command agentprofile.Command, snapshot agentprofile.Snapshot) (profileEventDraft, error) {
	d := profileEventDraft{attribution: model.EventAttribution{Operation: selectedProfileOperation(command)}}
	// Owning an execution does not prove that its actor initiated a host
	// controller/collector operation (grant, cancel, raw result capture, etc.).
	// Only a transition with an actual checked execution AuthorityBinding can
	// attribute initiation to that actor. RecordedBy always remains the caller.
	if command.Control != nil {
		v := reflect.ValueOf(*command.Control)
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Kind() == reflect.Pointer && !f.IsNil() {
				authority := f.Elem().FieldByName("Authority")
				if authority.IsValid() && authority.Type() == reflect.TypeOf(agentprofile.AuthorityBinding{}) {
					binding := authority.Interface().(agentprofile.AuthorityBinding)
					d.actorAuthorized = binding.Incarnation > 0
				}
				break
			}
		}
	}
	a := &d.attribution
	set := func(kind, id string, execution model.ExecutionID) {
		a.Subject = eventSubject(kind, id)
		a.Execution = execution
	}
	activity := func(id string) {
		d.activity = id
		if snapshot.Lifecycle.Recovery != nil {
			for _, s := range snapshot.Lifecycle.Recovery.ActivitySpecs {
				if s.Activity == id {
					a.Execution = s.Execution
					a.Query = s.Query
					a.Step = s.Step
					a.Invocation = s.Invocation
					return
				}
			}
		}
		for _, s := range snapshot.Lifecycle.Activities {
			if s.ID == id {
				a.Execution = s.Execution
				a.Step = s.Step
				a.Invocation = s.Invocation
				return
			}
		}
	}
	attempt := func(id string) {
		for _, s := range snapshot.Lifecycle.Attempts {
			if s.ID == id {
				activity(s.Activity)
				return
			}
		}
	}
	action := func(id string) {
		for _, s := range snapshot.Actions {
			if s.Key == id {
				a.Execution = s.Owner
				return
			}
		}
	}
	invocation := func(id string) {
		for _, s := range snapshot.Invocations {
			if s.ID == id {
				a.Execution = s.Execution
				a.Step = s.Step
				a.Invocation = s.ID
				return
			}
		}
	}
	cause := func(op, id string) { d.causes = append(d.causes, profileEventCause{op, id}) }
	switch {
	case command.CreateTask != nil:
		set("Task", string(command.CreateTask.ID), "")
	case command.Spawn != nil:
		set("Execution", string(command.Spawn.ID), command.Spawn.Parent)
	case command.StartStep != nil:
		v := command.StartStep
		set("Step", agentStepID(v.Execution, v.Sequence), v.Execution)
		a.Step = v.Sequence
	case command.RecordInvocation != nil:
		v := command.RecordInvocation
		set("Invocation", v.ID, v.Execution)
		a.Step = v.Step
		a.Invocation = v.ID
	case command.CompleteStep != nil:
		v := command.CompleteStep
		set("Step", agentStepID(v.Execution, v.Sequence), v.Execution)
		a.Step = v.Sequence
	case command.CancelTask != nil:
		set("Task", string(command.CancelTask.Task), "")
	case command.RebaseGoal != nil:
		set("Task", string(command.RebaseGoal.Task), "")
	case command.PutPending != nil:
		set("PendingAction", command.PutPending.Key, command.PutPending.Owner)
	case command.RebindPending != nil:
		set("PendingAction", command.RebindPending.Key, "")
		action(command.RebindPending.Key)
	case command.SettleInvocation != nil:
		set("Invocation", command.SettleInvocation.ID, "")
		invocation(command.SettleInvocation.ID)
	case command.Lifecycle != nil:
		c := command.Lifecycle
		switch {
		case c.PutQuery != nil:
			set("Query", c.PutQuery.ID, "")
			a.Query = c.PutQuery.ID
		case c.PutActivity != nil:
			set("Activity", c.PutActivity.ID, c.PutActivity.Execution)
			a.Step = c.PutActivity.Step
			a.Invocation = c.PutActivity.Invocation
		case c.PutAttempt != nil:
			set("Attempt", c.PutAttempt.ID, "")
			activity(c.PutAttempt.Activity)
		case c.PutResult != nil:
			set("ActivityResult", c.PutResult.ID, "")
			activity(c.PutResult.Activity)
		case c.PutMessage != nil:
			set("Message", c.PutMessage.ID, c.PutMessage.Sender)
		case c.ConsumeMessage != nil:
			v := c.ConsumeMessage
			set("MessageConsumption", recoveryNodeID(v.Message, string(v.Recipient)), v.Recipient)
			cause("Control.SendMessage", v.Message)
		case c.PutContinuation != nil:
			set("Continuation", string(c.PutContinuation.Execution), c.PutContinuation.Execution)
		}
	case command.Control != nil:
		c := command.Control
		switch {
		case c.AnswerQuery != nil:
			set("QueryAnswer", recoveryNodeID(c.AnswerQuery.Query, c.AnswerQuery.Slot), "")
			a.Query = c.AnswerQuery.Query
		case c.StartWork != nil:
			set("WorkStart", recoveryNodeID(c.StartWork.Query, c.StartWork.Slot), "")
			a.Query = c.StartWork.Query
		case c.GrantAuthority != nil:
			set("ExecutionAuthority", string(c.GrantAuthority.Authority.Execution), c.GrantAuthority.Authority.Execution)
		case c.FenceAuthority != nil:
			set("ExecutionAuthority", string(c.FenceAuthority.Execution), c.FenceAuthority.Execution)
		case c.Resume != nil:
			set("ExecutionAuthority", string(c.Resume.Execution), c.Resume.Execution)
		case c.CreateActivity != nil:
			v := c.CreateActivity.Spec
			d.activity = v.Activity
			set("Activity", v.Activity, v.Execution)
			a.Query = v.Query
			a.Step = v.Step
			a.Invocation = v.Invocation
			if v.Invocation != "" {
				cause("Control.RecordInvocation", v.Invocation)
			}
		case c.ClaimAttempt != nil:
			v := c.ClaimAttempt
			set("Attempt", v.Attempt, "")
			activity(v.Activity)
			cause("Control.CreateActivity", v.Activity)
		case c.DispatchAttempt != nil:
			v := c.DispatchAttempt
			set("Attempt", v.Attempt, "")
			attempt(v.Attempt)
			cause("Control.ClaimAttempt", v.Attempt)
		case c.CaptureResult != nil:
			v := c.CaptureResult.Result
			set("ActivityResult", v.ID, "")
			activity(v.Activity)
			cause("Control.DispatchAttempt", v.Attempt)
		case c.AcceptActivity != nil:
			v := c.AcceptActivity
			set("ActivityAcceptance", v.Activity, "")
			activity(v.Activity)
			cause("Control.CaptureResult", v.Result)
		case c.SettleActivity != nil:
			v := c.SettleActivity
			set("ActivitySettlement", recoveryNodeID(v.Activity, strconv.FormatUint(v.ExpectedVersion+1, 10)), "")
			activity(v.Activity)
			cause("Control.AcceptActivity", v.Activity)
		case c.ReconcileAttempt != nil:
			v := c.ReconcileAttempt
			set("AttemptReconciliation", recoveryNodeID(v.Attempt, strconv.FormatUint(v.ExpectedVersion+1, 10)), "")
			attempt(v.Attempt)
			cause("Control.DispatchAttempt", v.Attempt)
		case c.RetryActivity != nil:
			set("Activity", c.RetryActivity.Activity, "")
			activity(c.RetryActivity.Activity)
		case c.SaveContinuation != nil:
			v := c.SaveContinuation.Value
			set("Continuation", string(v.Execution), v.Execution)
		case c.ObserveGoal != nil:
			set("GoalObservation", recoveryNodeID(string(c.ObserveGoal.Execution), strconv.FormatUint(c.ObserveGoal.Epoch, 10)), c.ObserveGoal.Execution)
		case c.CompleteTask != nil:
			set("TaskOutcome", string(c.CompleteTask.Task), c.CompleteTask.Execution)
		case c.CancelExecution != nil:
			set("Execution", string(c.CancelExecution.Execution), c.CancelExecution.Execution)
		case c.SpawnChild != nil:
			v := c.SpawnChild
			set("Execution", string(v.Execution.ID), v.Parent)
			a.Invocation = v.Invocation
			cause("Control.RecordInvocation", v.Invocation)
		case c.PutAction != nil:
			v := c.PutAction.Intent.Action
			set("PendingAction", v.Key, v.Owner)
		case c.ResolveAction != nil:
			set("PendingAction", c.ResolveAction.Resolution.Key, "")
			action(c.ResolveAction.Resolution.Key)
		case c.RebindAction != nil:
			set("PendingAction", c.RebindAction.Key, "")
			action(c.RebindAction.Key)
		case c.BeginStep != nil:
			v := c.BeginStep.Step
			set("Step", agentStepID(v.Execution, v.Sequence), v.Execution)
			a.Step = v.Sequence
		case c.RecordInvocation != nil:
			v := c.RecordInvocation.Invocation
			set("Invocation", v.ID, v.Execution)
			a.Step = v.Step
			a.Invocation = v.ID
			if v.AcceptedResult != "" {
				for _, result := range snapshot.Lifecycle.Results {
					if result.ID == v.AcceptedResult {
						cause("Control.AcceptActivity", result.Activity)
						break
					}
				}
			}
		case c.FinishStep != nil:
			v := c.FinishStep
			set("Step", agentStepID(v.Execution, v.Sequence), v.Execution)
			a.Step = v.Sequence
		case c.JoinExecutions != nil:
			v := c.JoinExecutions.Join
			set("ExecutionJoin", v.ID, v.Parent)
		case c.RebaseInherited != nil:
			for _, execution := range snapshot.Executions {
				if execution.ID == c.RebaseInherited.Execution {
					set("TaskLink", string(execution.Task), execution.ID)
					break
				}
			}
		case c.Checkpoint != nil:
			v := c.Checkpoint.Continuation
			set("Continuation", string(v.Execution), v.Execution)
		case c.SendMessage != nil:
			set("Message", c.SendMessage.Message.ID, c.SendMessage.Message.Sender)
		case c.ConsumeMessage != nil:
			v := c.ConsumeMessage.Consumption
			set("MessageConsumption", recoveryNodeID(v.Message, string(v.Recipient)), v.Recipient)
			cause("Control.SendMessage", v.Message)
		case c.ImportActivityResult != nil:
			v := c.ImportActivityResult
			set("ActivityAcceptance", v.Activity, "")
			activity(v.Activity)
			cause("Control.CaptureResult", v.Result)
		case c.BindValidation != nil:
			v := c.BindValidation.Requirement
			set("ValidationRequirement", string(v.ID), v.Spec.Execution)
		}
	}
	if !eventlog.RegisteredAgentOperation(a.Operation) || !a.Subject.Valid() {
		return d, ErrUnsupportedSchema
	}
	bindProfileOwner(&d, snapshot)
	return d, nil
}

func bindProfileOwner(d *profileEventDraft, snapshot agentprofile.Snapshot) {
	a := &d.attribution
	if a.Execution != "" {
		for _, x := range snapshot.Executions {
			if x.ID == a.Execution {
				if d.actorAuthorized {
					a.InitiatingActor = x.Actor
				}
				d.stream = model.EventStreamKey{Kind: "execution", Owner: string(x.ID), Incarnation: x.Incarnation}
				break
			}
		}
		if snapshot.Lifecycle.Recovery != nil {
			current := a.Execution
			seen := map[model.ExecutionID]bool{}
			for current != "" && !seen[current] {
				seen[current] = true
				for _, start := range snapshot.Lifecycle.Recovery.Starts {
					if start.Execution == current {
						a.Query = start.Query
					}
				}
				var parent model.ExecutionID
				for _, x := range snapshot.Executions {
					if x.ID == current {
						parent = x.Parent
						break
					}
				}
				current = parent
			}
		}
	}
	for _, q := range snapshot.Lifecycle.Queries {
		if q.ID == a.Query {
			a.Session = q.Session
			break
		}
	}
	if d.stream.Kind == "" && a.Query != "" {
		d.stream = model.EventStreamKey{Kind: "query", Owner: a.Query}
	}
	if d.stream.Kind == "" && a.Subject.Kind == "Task" {
		d.stream = model.EventStreamKey{Kind: "task", Owner: string(a.Subject.ID)}
	}
}

// profileEventDrafts describes accepted transitions, not every state field that
// a reducer happens to update. Work preserves one ordered fact per registered
// subtransition, plus its outer protected boundary, under one publication.
func knowledgeWorkEventDraft(c knowledge.Command, snapshot agentprofile.Snapshot) profileEventDraft {
	d := profileEventDraft{attribution: model.EventAttribution{Operation: "Knowledge." + selectedProfileOperation(c)}}
	a := &d.attribution
	switch {
	case c.Observation != nil:
		a.Subject = model.EntityKey{Profile: "knowledge", Kind: "Observation", ID: model.EntityID(c.Observation.ID)}
		a.Execution = c.Observation.Key.Execution
	case c.Packet != nil:
		a.Subject = model.EntityKey{Profile: "knowledge", Kind: "ContextPacket", ID: model.EntityID(c.Packet.ID)}
		if c.Packet.Key != nil {
			a.Execution = c.Packet.Key.Execution
		}
	case c.Import != nil:
		a.Subject = eventSubject("Execution", string(c.Import.Target.Execution))
		a.Execution = c.Import.Target.Execution
	}
	bindProfileOwner(&d, snapshot)
	return d
}
