package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func recoveryCompleteTask(t *testing.T, s *integrationStore, id string, complete agentprofile.CompleteTaskCommand) api.CommitResult {
	t.Helper()
	state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	var task agentprofile.Task
	for _, current := range state.Snapshot.Tasks {
		if current.ID == complete.Task {
			task = current
		}
	}
	goal, err := agentprofile.GoalKnowledgeDigest(task)
	if err != nil {
		t.Fatal(err)
	}
	put := func(name, body string) (model.ObjectID, api.CommitResult) {
		ack := integrationMutate(t, s.e, integrationRequest(name, integrationPut([]byte(name), api.Value{Kind: api.ValueBytes, Data: []byte(body)})))
		object, _ := recoveryValueAt(t, s, ack.Fence, name)
		return object, ack
	}
	command, _ := put(id+"-command", "host-recovery-checker/v1: inspect the reconciled exact artifact")
	environment, source := put(id+"-environment", "host-recovery/v1; resource effect reconciled from exact file content")
	required := agentprofile.ValidationRequirement{Version: 1, Source: source.Fence, Spec: agentprofile.ValidationSpec{Space: "host-space", Task: complete.Task, Execution: complete.Execution, GoalEpoch: complete.Epoch, Goal: goal, Result: complete.Result, Command: command, Environment: environment, ToolVersion: "host-recovery-checker/v1", PolicyVersion: "file-reconciliation/v1"}}
	required.ID = agentprofile.ValidationRequirementID(required)
	recoveryCoreControl(t, s, id+"-bind", agentprofile.ControlCommand{BindValidation: &agentprofile.BindValidationCommand{Requirement: required, Authority: complete.Authority}})
	evidence, _ := put(id+"-evidence", fmt.Sprintf("validated reconciled artifact %s against the original file effect", complete.Result))
	envelope := agentprofile.ValidationEnvelope{Requirement: required.ID, Spec: required.Spec, Passed: true, Evidence: []model.ObjectID{evidence}}
	raw, err := envelope.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	complete.Validation, _ = put(id+"-envelope", string(raw))
	return recoveryCoreControl(t, s, id, agentprofile.ControlCommand{CompleteTask: &complete})
}
