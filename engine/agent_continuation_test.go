package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestContinuationPositionRetainsPublishedKnowledgeCutAcrossRestart(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	o := knowledgeObservation(payload, source, "one", 1)
	_, adopted := applyObservation(t, s.e, "position-observation", o)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: adopted.Fence, Knowledge: &o.Key}
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
	if err != nil || known.Revision == nil {
		t.Fatalf("knowledge revision: %+v %v", known, err)
	}
	authority := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "position-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: authority, Status: "active", Evidence: payload}}})
	body, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{integrationPut([]byte("position-after"), integrationBytes('b'))}})
	if err != nil {
		t.Fatal(err)
	}
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "position-next", Generation: 1, Request: agentKey("position-execute"), CommandDigest: agentprofile.ExactRequestDigest(body), InputFence: adopted.Fence, Owner: "one", Status: agentprofile.ActionPending}, Method: api.MethodMutateState, ExactCommand: body, Authority: authority, InputObjects: []model.ObjectID{payload}}
	c := agentprofile.Continuation{Execution: "one", Version: 1, Incarnation: 1, PendingKey: intent.Action.Key, PendingGeneration: 1, InputFence: adopted.Fence, Phase: "dispatch", Position: &agentprofile.ContinuationPosition{
		Space: "host-space", Principal: "host-principal", KnowledgeFence: adopted.Fence, KnowledgeCursor: model.Cursor{Store: adopted.Fence.Store, Space: "host-space", Revision: known.Revision.ID}, KnowledgeKey: o.Key, AdapterVersion: "host/v1", NextActions: []agentprofile.PendingActionRef{{Key: intent.Action.Key, Generation: 1}},
	}}
	// A syntactically valid digest is not an authenticated knowledge location.
	forged := c
	position := *c.Position
	forged.Position = &position
	forged.Position.KnowledgeCursor.Revision = model.RevisionID(integrationID('e'))
	recoveryCoreReject(t, s, "position-forged-cursor", agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: forged, Authority: authority}})
	ack := recoveryCoreControl(t, s, "position-checkpoint", agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: c, Authority: authority}})
	applyObservation(t, s.e, "position-newer-observation", knowledgeObservation(payload, source, "one", 2))
	s.reopen()
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Lifecycle.Continuations) != 1 || !reflect.DeepEqual(state.Snapshot.Lifecycle.Continuations[0], c) {
		t.Fatalf("restart changed exact recovery position: %+v %v", state.Snapshot.Lifecycle.Continuations, err)
	}
	if got := recoveryCoreControl(t, s, "position-checkpoint", agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: c, Authority: authority}}); got != ack {
		t.Fatal("checkpoint retry did not reuse the original receipt")
	}
	p := state.Snapshot.Lifecycle.Continuations[0].Position
	exact := model.SnapshotAnchor{Space: p.Space, Fence: p.KnowledgeFence, Cursor: &p.KnowledgeCursor, Knowledge: &p.KnowledgeKey}
	recovered, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: p.Space, Principal: p.Principal, Anchor: &exact})
	if err != nil || len(recovered.Snapshot.Observations) != 1 || recovered.Revision.ID != known.Revision.ID {
		t.Fatalf("recovery replaced original knowledge with latest: %+v %v", recovered, err)
	}
}
