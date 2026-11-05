package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func clientAgentRequest() api.ApplyAgentRequest {
	return api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", RequestID: "original"}, Command: agentprofile.Command{CreateTask: &agentprofile.Task{ID: "task", Space: "s", Owner: "host", Goal: "goal", Status: agentprofile.TaskOpen, GoalEpoch: 1}}}
}

func TestApplyAgentUnknownRetainsOriginalAuthenticatedRequest(t *testing.T) {
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer b.Close()
		raw, err := protocol.ReadFrame(b)
		if err != nil {
			return
		}
		hello, _ := protocol.DecodeRequest(raw)
		reply(t, b, hello, protocol.HelloResult{Principal: "host", Spaces: []model.SpaceID{"s"}, Capabilities: []string{"agent/v1"}})
		raw, err = protocol.ReadFrame(b)
		if err != nil {
			return
		}
		request, err := protocol.DecodeRequest(raw)
		if err != nil {
			t.Error(err)
			return
		}
		var wire protocol.ApplyAgentRequest
		if err = protocol.DecodeStrict(request.Body, &wire); err != nil {
			t.Error(err)
			return
		}
		if request.Method != "apply_agent" || wire.RequestID != "original" || wire.Space != "s" {
			t.Errorf("wire=%+v request=%+v", wire, request)
		}
		command, err := agentprofile.DecodeCommand(wire.Command)
		if err != nil || command.CreateTask == nil || command.CreateTask.Owner != "host" {
			t.Errorf("canonical command=%+v err=%v", command, err)
		}
		// Close without a response, after the command may have been admitted.
	}()
	c, err := New(context.Background(), a, Options{Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ApplyAgent(context.Background(), clientAgentRequest())
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) || unknown.Key != (model.RequestKey{Space: "s", Principal: "host", RequestID: "original"}) {
		t.Fatalf("identity lost: %v", err)
	}
	<-done
}

func TestAgentStateMalformedSuccessIsRejected(t *testing.T) {
	fence := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
	id := model.ObjectID(strings.Repeat("b", 64))
	valid := func() api.AgentStateResult {
		return api.AgentStateResult{Found: true, Anchor: model.SnapshotAnchor{Space: "s", Fence: fence}, Fence: fence, RootSet: id, LiveControlRoot: id,
			Snapshot: agentprofile.Snapshot{Tasks: []agentprofile.Task{{ID: "task", Space: "s", Owner: "host", Goal: "goal", GoalEpoch: 1, Status: agentprofile.TaskOpen}}}}
	}
	for _, test := range []struct {
		name   string
		change func(*api.AgentStateResult)
	}{
		{"different_fence", func(r *api.AgentStateResult) { r.Fence.LSN++ }},
		{"foreign_principal", func(r *api.AgentStateResult) { r.Snapshot.Tasks[0].Owner = "other" }},
		{"foreign_space", func(r *api.AgentStateResult) { r.Anchor.Space = "other" }},
		{"false_with_content", func(r *api.AgentStateResult) { r.Found = false }},
		{"malformed_snapshot", func(r *api.AgentStateResult) { r.Snapshot.Tasks[0].Status = "invented" }},
		{"missing_control_root", func(r *api.AgentStateResult) { r.LiveControlRoot = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, b := net.Pipe()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer b.Close()
				raw, e := protocol.ReadFrame(b)
				if e != nil {
					return
				}
				hello, _ := protocol.DecodeRequest(raw)
				reply(t, b, hello, protocol.HelloResult{Principal: "host", Spaces: []model.SpaceID{"s"}, Capabilities: []string{"agent/v1"}})
				raw, e = protocol.ReadFrame(b)
				if e != nil {
					return
				}
				request, _ := protocol.DecodeRequest(raw)
				response := valid()
				test.change(&response)
				reply(t, b, request, response)
			}()
			c, e := New(context.Background(), a, Options{Token: "secret"})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = c.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s"}); !errors.Is(e, protocol.ErrProtocol) {
				t.Fatalf("invalid success accepted: %v", e)
			}
			_ = c.Close()
			<-done
		})
	}
}
