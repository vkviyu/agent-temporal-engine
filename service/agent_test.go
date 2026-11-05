package service

import (
	"context"
	"net"
	"reflect"
	"runtime"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func nativeAgentStore(t *testing.T, enabled bool) (*ate.Store, string) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native core DirectoryIO is supported on Darwin/Linux")
	}
	dir := t.TempDir()
	s, err := ate.Create(dir, ate.Options{AgentProfile: enabled})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, dir
}

func hasAgentCapability(c *client.Client) bool {
	for _, capability := range c.Identity().Capabilities {
		if capability == "agent/v1" {
			return true
		}
	}
	return false
}

func agentCreateRequest(id string) api.ApplyAgentRequest {
	return api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", RequestID: model.RequestID(id)}, Command: agentprofile.Command{CreateTask: &agentprofile.Task{ID: "t", Space: "s", Owner: "owner", Goal: "finish", GoalEpoch: 1, Status: agentprofile.TaskOpen}}}
}

func agentCredentials() Options {
	return Options{Credentials: map[string]Credential{
		"owner-token": {Principal: "owner", Spaces: []model.SpaceID{"s"}},
		"other-token": {Principal: "other", Spaces: []model.SpaceID{"s"}},
	}}
}

func TestAgentServiceCoreReopenHistoricalStateAndPartitionIsolation(t *testing.T) {
	core, path := nativeAgentStore(t, true)
	s := newServer(t, core, agentCredentials())
	owner := pipeClient(t, s, "owner-token")
	other := pipeClient(t, s, "other-token")
	if !hasAgentCapability(owner) {
		t.Fatal("enabled core did not advertise agent/v1")
	}
	initial, err := owner.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s"})
	if err != nil || initial.Found || initial.Fence.Validate() != nil {
		t.Fatalf("initial state=%+v err=%v", initial, err)
	}
	r := agentCreateRequest("create")
	ack, err := owner.ApplyAgent(context.Background(), r)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("create ack=%+v err=%v", ack, err)
	}
	changed := agentCreateRequest("create")
	changed.Command.CreateTask.Goal = "different body under the same key"
	if conflict, err := owner.ApplyAgent(context.Background(), changed); err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
		t.Fatalf("different command reused key: %+v %v", conflict, err)
	}
	state, err := owner.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s"})
	if err != nil || !state.Found || len(state.Snapshot.Tasks) != 1 || state.Snapshot.Tasks[0].Owner != "owner" || state.Fence != ack.Fence {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	otherState, err := other.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s"})
	if err != nil || otherState.Found || len(otherState.Snapshot.Tasks) != 0 {
		t.Fatalf("other state=%+v err=%v", otherState, err)
	}
	if _, err := other.ApplyAgent(context.Background(), r); err == nil {
		t.Fatal("other principal created owner task")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
	if _, err := owner.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "other-space"}); err == nil {
		t.Fatal("foreign space readable")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
	if _, err := owner.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s", Principal: "other"}); err != client.ErrPrincipal {
		t.Fatalf("SDK principal mismatch: %v", err)
	}
	if _, err := owner.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", RequestID: "rebase"}, Command: agentprofile.Command{RebaseGoal: &agentprofile.GoalRebase{Task: "t", ExpectedEpoch: 1, NewGoal: "new goal"}}}); err != nil {
		t.Fatal(err)
	}
	historical, err := owner.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s", Anchor: &state.Anchor})
	if err != nil || historical.Snapshot.Tasks[0].Goal != "finish" || historical.Fence != ack.Fence {
		t.Fatalf("history=%+v err=%v", historical, err)
	}
	beforeCreate, err := owner.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s", Anchor: &initial.Anchor})
	if err != nil || beforeCreate.Found {
		t.Fatalf("genesis history=%+v err=%v", beforeCreate, err)
	}
	if _, err := owner.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", RequestID: "cancel"}, Command: agentprofile.Command{CancelTask: &agentprofile.CancelTaskCommand{Task: "t"}}}); err != nil {
		t.Fatal(err)
	}
	if retry, err := owner.ApplyAgent(context.Background(), r); err != nil || !reflect.DeepEqual(retry, ack) {
		t.Fatalf("retry ack=%+v want=%+v err=%v", retry, ack, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := ate.OpenExisting(path, ate.Options{AgentProfile: true})
	if err != nil || !report.Verified {
		t.Fatalf("reopen report=%+v err=%v", report, err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	reopenedServer := newServer(t, reopened, agentCredentials())
	reopenedClient := pipeClient(t, reopenedServer, "owner-token")
	if retry, err := reopenedClient.ApplyAgent(context.Background(), r); err != nil || !reflect.DeepEqual(retry, ack) {
		t.Fatalf("reopen retry ack=%+v want=%+v err=%v", retry, ack, err)
	}
	got, err := reopenedClient.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s"})
	if err != nil || got.Snapshot.Tasks[0].Status != agentprofile.TaskCanceled || got.Snapshot.Tasks[0].Goal != "new goal" {
		t.Fatalf("reopened state=%+v err=%v", got, err)
	}
}

func TestAgentServiceCapabilityAndCanonicalWireValidation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			core, _ := nativeAgentStore(t, enabled)
			s := newServer(t, core, agentCredentials())
			c := pipeClient(t, s, "owner-token")
			if hasAgentCapability(c) != enabled {
				t.Fatal("capability does not match enabled profile")
			}
			if !enabled {
				if _, err := c.ApplyAgent(context.Background(), agentCreateRequest("disabled")); err == nil {
					t.Fatal("disabled command accepted")
				} else {
					remoteCode(t, err, api.ErrorUnsupported)
				}
				if _, err := c.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s"}); err == nil {
					t.Fatal("disabled query accepted")
				} else {
					remoteCode(t, err, api.ErrorUnsupported)
				}
				return
			}
			a, b := net.Pipe()
			defer a.Close()
			if !s.acceptConn(b) {
				t.Fatal("connection refused")
			}
			if reply := exchange(t, a, "hello", "hello", protocol.HelloRequest{Token: "owner-token"}); reply.Error != nil {
				t.Fatal(reply.Error)
			}
			unknown := append([]byte(nil), []byte("ATE-AGENT-COMMAND\x00")...)
			unknown = codec.AppendUint(unknown, agentprofile.CommandSchema)
			unknown = codec.AppendUint(unknown, 999)
			unknown, _ = codec.AppendBytes(unknown, nil)
			tests := []struct {
				method string
				body   any
				want   api.ErrorClass
			}{
				{"apply_agent", protocol.ApplyAgentRequest{Space: "s", RequestID: "malformed", Command: []byte("malformed")}, api.ErrorInvalidRequest},
				{"apply_agent", protocol.ApplyAgentRequest{Space: "s", RequestID: "unknown", Command: unknown}, api.ErrorUnsupported},
				{"apply_agent", map[string]any{"space": "s", "request_id": "spoof", "command": []byte("x"), "principal": "other"}, api.ErrorInvalidRequest},
				{"get_agent_state", map[string]any{"space": "s", "principal": "other"}, api.ErrorInvalidRequest},
			}
			for _, test := range tests {
				response := exchange(t, a, test.method, test.method, test.body)
				if response.Error == nil || response.Error.Code != test.want {
					t.Fatalf("%s reply=%+v want=%s", test.method, response, test.want)
				}
			}
			if _, err := c.ApplyAgent(context.Background(), agentCreateRequest("after-invalid")); err != nil {
				t.Fatalf("malformed wire fenced core: %v", err)
			}
		})
	}
}
