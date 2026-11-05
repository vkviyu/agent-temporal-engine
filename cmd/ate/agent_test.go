package main

import (
	"bytes"
	"context"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestRecoverStateAgentProfileRequiresExplicitFlag(t *testing.T) {
	dir := t.TempDir()
	s, err := ate.Create(dir, ate.Options{AgentProfile: true})
	if err != nil {
		t.Fatal(err)
	}
	task := agentprofile.Task{ID: "task", Space: "s", Owner: "host", Goal: "recover", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	ack, err := s.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", Principal: "host", RequestID: "create"}, Command: agentprofile.Command{CreateTask: &task}})
	if err != nil || ack.Code != api.CodeDurableAck {
		_ = s.Close()
		t.Fatalf("setup: %+v %v", ack, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if status := recoverState([]string{dir}, &out, &errOut); status == 0 {
		t.Fatal("core-only recovery accepted a required profile")
	}
	out.Reset()
	errOut.Reset()
	if status := recoverState([]string{"--agent-profile", dir}, &out, &errOut); status != 0 || !bytes.Contains(out.Bytes(), []byte(`"semantic_replay_verified":true`)) {
		t.Fatalf("enabled recovery status=%d stdout=%s stderr=%s", status, out.String(), errOut.String())
	}
}
