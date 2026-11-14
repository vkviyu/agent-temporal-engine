package engine

import (
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func (d *profileEventDraft) causedBy(operation, subject string) {
	if subject != "" {
		d.causes = append(d.causes, profileEventCause{operation, subject})
	}
}

func bindEventTask(d *profileEventDraft, snapshot agentprofile.Snapshot) {
	a := &d.attribution
	if a.Subject.Profile == "agent" && (a.Subject.Kind == "Task" || a.Subject.Kind == "TaskOutcome") {
		a.Task = a.Subject.ID
		return
	}
	if a.Execution != "" {
		for _, execution := range snapshot.Executions {
			if execution.ID == a.Execution {
				a.Task = execution.Task
				return
			}
		}
	}
}

func linkControlEvent(d *profileEventDraft, command agentprofile.ControlCommand, snapshot agentprofile.Snapshot) {
	switch {
	case command.AnswerQuery != nil:
		answer := command.AnswerQuery
		d.causedBy("Work.query_intake", answer.Query)
		d.causedBy("Lifecycle.PutQuery", answer.Query)
		d.causedBy("Control.CaptureResult", answer.GeneratedFrom)
		for _, result := range snapshot.Lifecycle.Results {
			if result.ID == answer.GeneratedFrom {
				d.causedBy("Control.AcceptActivity", result.Activity)
				break
			}
		}
		for _, outcome := range answer.Outcomes {
			d.causedBy("Control.CompleteTask", string(outcome.Task))
		}
	case command.StartWork != nil:
		start := command.StartWork
		d.attribution.Task = start.Task.ID
		d.causedBy("Work.query_intake", start.Query)
		d.causedBy("Lifecycle.PutQuery", start.Query)
	case command.CreateActivity != nil:
		spec := command.CreateActivity.Spec
		d.causedBy("Knowledge.Packet", string(spec.ContextPacket))
		if spec.Step > 0 {
			d.causedBy("Control.BeginStep", agentStepID(spec.Execution, spec.Step))
			d.causedBy("StartStep", agentStepID(spec.Execution, spec.Step))
		}
		d.causedBy("Control.RecordInvocation", spec.Invocation)
		if spec.Query != "" {
			d.causedBy("Work.query_intake", spec.Query)
			d.causedBy("Lifecycle.PutQuery", spec.Query)
		}
		if recovery := snapshot.Lifecycle.Recovery; recovery != nil && spec.Execution != "" {
			for _, start := range recovery.Starts {
				if start.Execution == spec.Execution {
					d.causedBy("Control.StartWork", recoveryNodeID(start.Query, start.Slot))
				}
			}
			d.causedBy("Control.SpawnChild", string(spec.Execution))
		}
	case command.SpawnChild != nil:
		d.attribution.Task = command.SpawnChild.Task.ID
	case command.JoinExecutions != nil:
		recovery := snapshot.Lifecycle.Recovery
		if recovery == nil {
			return
		}
		id := command.JoinExecutions.Join.ID
		for _, join := range recovery.Joins {
			if join.ID != id || join.Status != "completed" {
				continue
			}
			winners := make(map[model.ExecutionID]bool, len(join.Winners))
			for _, winner := range join.Winners {
				winners[winner] = true
			}
			for _, outcome := range recovery.Outcomes {
				if winners[outcome.Execution] {
					d.causedBy("Control.CompleteTask", string(outcome.Task))
				}
			}
			return
		}
	case command.CompleteTask != nil:
		recovery := snapshot.Lifecycle.Recovery
		if recovery == nil {
			return
		}
		for _, outcome := range recovery.Outcomes {
			if outcome.Task == command.CompleteTask.Task {
				d.causedBy("Control.BindValidation", string(outcome.ValidationRequirement))
				linkCompletedSteps(d, snapshot, outcome.Execution)
				for _, activity := range recovery.ActivitySpecs {
					if activity.Execution == outcome.Execution && activity.Step == 0 {
						linkActivityAcceptance(d, activity.Activity)
					}
				}
				return
			}
		}
	case command.BeginStep != nil:
		step := command.BeginStep.Step
		// StartStep requires the immediately preceding step to be completed.
		if step.Sequence > 1 {
			linkStepCompletion(d, step.Execution, step.Sequence-1)
		}
	case command.RecordInvocation != nil:
		invocation := command.RecordInvocation.Invocation
		d.causedBy("Control.BeginStep", agentStepID(invocation.Execution, invocation.Step))
		d.causedBy("StartStep", agentStepID(invocation.Execution, invocation.Step))
	case command.FinishStep != nil:
		step := command.FinishStep
		d.causedBy("Control.BeginStep", agentStepID(step.Execution, step.Sequence))
		d.causedBy("StartStep", agentStepID(step.Execution, step.Sequence))
		recovery := snapshot.Lifecycle.Recovery
		if recovery == nil {
			return
		}
		for _, activity := range recovery.ActivitySpecs {
			if activity.Execution == step.Execution && activity.Step == step.Sequence {
				linkActivityAcceptance(d, activity.Activity)
			}
		}
	}
}

// Every successive step requires its predecessor's completion, so the last
// completed step is the terminal node of the required step chain.
func linkCompletedSteps(d *profileEventDraft, snapshot agentprofile.Snapshot, execution model.ExecutionID) {
	var sequence uint64
	for _, step := range snapshot.Steps {
		if step.Execution == execution && step.Status == agentprofile.StepCompleted && step.Sequence > sequence {
			sequence = step.Sequence
		}
	}
	if sequence != 0 {
		linkStepCompletion(d, execution, sequence)
	}
}

func linkStepCompletion(d *profileEventDraft, execution model.ExecutionID, sequence uint64) {
	id := agentStepID(execution, sequence)
	d.causedBy("Control.FinishStep", id)
	d.causedBy("CompleteStep", id)
}

func linkActivityAcceptance(d *profileEventDraft, activity string) {
	d.causedBy("Control.AcceptActivity", activity)
	d.causedBy("Control.ImportActivityResult", activity)
}

func linkKnowledgeEvent(d *profileEventDraft, command knowledge.Command) {
	switch {
	case command.Observation != nil:
		if command.Observation.PayloadKind == "summary" {
			d.causedBy("Summary.Apply", string(command.Observation.Payload))
		}
	case command.Packet != nil:
		for _, item := range command.Packet.Items {
			d.causedBy("Knowledge.Observation", string(item.Observation))
		}
	case command.Import != nil:
		for _, id := range command.Import.Observations {
			d.causedBy("Knowledge.Observation", string(id))
		}
	}
}
