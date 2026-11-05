package host_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// completeTask records the deterministic host's actual artifact inspection,
// execution environment and acceptance rule as a typed requirement/evidence
// pair. The engine checks this binding; it does not claim to run this checker
// itself or infer real-world correctness from the word "passed".
func (h *host) completeTask(id string, completion agentprofile.CompleteTaskCommand) api.CommitResult {
	h.t.Helper()
	ctx := context.Background()
	state, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatal(err)
	}
	var task agentprofile.Task
	for _, current := range state.Snapshot.Tasks {
		if current.ID == completion.Task {
			task = current
		}
	}
	goal, err := agentprofile.GoalKnowledgeDigest(task)
	if err != nil {
		h.t.Fatal(err)
	}
	view, err := h.peer.OpenView(ctx, api.OpenViewRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatal(err)
	}
	var artifact []byte
	for {
		part, err := h.peer.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: completion.Result}, Projection: "value_bytes", Offset: uint64(len(artifact)), Length: 4096})
		if err != nil {
			h.t.Fatal(err)
		}
		artifact = append(artifact, part.Data...)
		if part.EOF {
			break
		}
	}
	if err := h.peer.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		h.t.Fatal(err)
	}
	sum := sha256.Sum256(artifact)
	digest := hex.EncodeToString(sum[:])
	commandRaw, err := json.Marshal(struct {
		Operation string
		Artifact  model.ObjectID
		SHA256    string
	}{"inspect-exact-artifact-sha256/v1", completion.Result, digest})
	if err != nil {
		h.t.Fatal(err)
	}
	command, _ := h.put(id+"-validation-command", id+"-validation-command", commandRaw)
	environmentRaw, err := json.Marshal(struct{ OS, Arch, Runtime, Host string }{runtime.GOOS, runtime.GOARCH, runtime.Version(), "host-acceptance/v1"})
	if err != nil {
		h.t.Fatal(err)
	}
	environment, source := h.put(id+"-validation-environment", id+"-validation-environment", environmentRaw)
	required := agentprofile.ValidationRequirement{Version: 1, Source: source.Fence, Spec: agentprofile.ValidationSpec{Space: "project", Task: completion.Task, Execution: completion.Execution, GoalEpoch: task.GoalEpoch, Goal: goal, Result: completion.Result, Command: command, Environment: environment, ToolVersion: "sha256-inspection/v1", PolicyVersion: "host-acceptance/v1"}}
	required.ID = agentprofile.ValidationRequirementID(required)
	h.control(id+"-bind-validation", agentprofile.ControlCommand{BindValidation: &agentprofile.BindValidationCommand{Requirement: required, Authority: completion.Authority}})
	evidence, _ := h.put(id+"-validation-observation", id+"-validation-observation", []byte("inspected exact artifact bytes; sha256="+digest))
	envelope := agentprofile.ValidationEnvelope{Requirement: required.ID, Spec: required.Spec, Passed: true, Evidence: []model.ObjectID{evidence}}
	raw, err := envelope.Canonical()
	if err != nil {
		h.t.Fatal(err)
	}
	completion.Validation, _ = h.put(id+"-validation-envelope", id+"-validation-envelope", raw)
	return h.control(id, agentprofile.ControlCommand{CompleteTask: &completion})
}
