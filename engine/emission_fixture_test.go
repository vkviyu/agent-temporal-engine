package engine_test

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// recoveryEmissionDispatch exercises the public V1 grant/Work protocol. It does
// not invoke a private reducer or turn a direct DispatchAttempt into authority.
func recoveryEmissionDispatch(t *testing.T, s *integrationStore, id string, c agentprofile.ControlCommand) api.CommitResult {
	t.Helper()
	ctx := context.Background()
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	var spec agentprofile.ActivitySpec
	var activity string
	if state.Snapshot.Lifecycle.Recovery != nil {
		for _, a := range state.Snapshot.Lifecycle.Recovery.AttemptControls {
			if a.Attempt == c.DispatchAttempt.Attempt {
				activity = a.Activity
			}
		}
		for _, a := range state.Snapshot.Lifecycle.Recovery.ActivitySpecs {
			if a.Activity == activity {
				spec = a
			}
		}
	}
	if spec.Activity == "" {
		t.Fatal("dispatch fixture has no claimed Activity")
	}
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: spec.InputFence}
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: v.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: spec.Request}, Projection: "value_bytes"})
	_ = s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	if err != nil {
		t.Fatal(err)
	}
	grant := api.EmissionSpec{ID: id + "-grant", Query: spec.Query, Execution: spec.Execution, GoalEpoch: spec.GoalEpoch, Authority: c.DispatchAttempt.Authority, Content: spec.Request, Anchor: anchor, Projection: "value_bytes", Length: part.TotalBytes, Destination: spec.Recipient, Activity: spec.Activity, Attempt: c.DispatchAttempt.Attempt, RequestDigest: spec.RequestDigest}
	if spec.ContextPacket != "" {
		grant.Content, grant.ContentKind = spec.ContextPacket, "packet"
	}
	for _, x := range state.Snapshot.Executions {
		if x.ID == spec.Execution {
			grant.Task = x.Task
		}
	}
	if state.Snapshot.Lifecycle.Recovery != nil {
		for _, a := range state.Snapshot.Lifecycle.Recovery.Authorities {
			if a.Execution == spec.Execution {
				grant.AuthorityVersion = a.Version
			}
		}
	}
	out, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey(id + "-issue"), Command: api.EmissionCommand{Issue: &grant}})
	if err != nil || out.Code != api.CodeDurableAck {
		t.Fatalf("dispatch fixture grant %+v: %+v %v", grant, out, err)
	}
	claim, err := api.EmissionWorkStep(api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: grant.Authority})
	if err != nil {
		t.Fatal(err)
	}
	return workApply(t, s, api.ApplyWorkRequest{Key: agentKey(id), Unit: api.WorkUnit{Kind: "emission_dispatch", Query: spec.Query, Steps: []api.WorkStep{claim, workControl(t, c)}}})
}
