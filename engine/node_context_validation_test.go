package engine

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestTypedNodeArtifactsBindPublishedValidationRequirement(t *testing.T) {
	e, _, authority, required, evidence := coreValidationFixture(t)
	ctx := context.Background()
	envelope := agentprofile.ValidationEnvelope{Requirement: required.ID, Spec: required.Spec, Passed: true, Evidence: []model.ObjectID{evidence}}
	raw, err := envelope.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	validation, _ := validationPut(t, e, "node-validation-envelope", raw)
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, validationCompletionRequest(t, "node-complete-bound-proof", required, authority, validation))))
	// Remove live keys. Artifact expansion must follow the accepted Outcome's
	// explicit immutable references, rather than current domain membership.
	batch, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte("validated-artifact"), Kind: api.MutationDelete}, {Key: []byte("node-validation-envelope"), Kind: api.MutationDelete}}})
	if err != nil {
		t.Fatal(err)
	}
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, api.CommitRequest{Space: "coordinator-space", Principal: "host", RequestID: "node-remove-live-artifact", Method: api.MethodMutateState, Body: batch})))
	v, err := e.OpenView(ctx, api.OpenViewRequest{Space: "coordinator-space", Principal: "host"})
	if err != nil {
		t.Fatal(err)
	}
	defer e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	out, err := e.GetNodeContext(ctx, api.NodeContextRequest{View: v.Ref, Ref: agentEntity("TaskOutcome", string(required.Spec.Task)), Mode: "evidence_neighborhood", Sections: []string{"artifacts"}, Limits: api.QueryLimits{MaxDepth: 3}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[model.NodeRef]bool{agentEntity("ValidationRequirement", string(required.ID)): false, {Kind: model.NodeObject, Object: required.Spec.Result}: false, {Kind: model.NodeObject, Object: validation}: false, {Kind: model.NodeObject, Object: required.Spec.Command}: false, {Kind: model.NodeObject, Object: required.Spec.Environment}: false}
	for _, node := range out.Sections["artifacts"] {
		if _, ok := want[node.Ref]; ok {
			want[node.Ref] = true
		}
	}
	for ref, seen := range want {
		if !seen {
			t.Fatalf("missing exact accepted artifact relation %+v in %+v", ref, out)
		}
	}
	d, err := e.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: agentEntity("ValidationRequirement", string(required.ID))})
	if err != nil || d.Summary.Kind != "ValidationRequirement" {
		t.Fatalf("validation requirement not inspectable %+v %v", d, err)
	}
	for _, source := range d.Sources {
		if source.Kind == model.NodeObject && source.Object == required.Spec.Goal {
			t.Fatal("logical goal digest became raw object grant")
		}
	}
}
