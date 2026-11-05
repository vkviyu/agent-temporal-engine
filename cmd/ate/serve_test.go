package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestServeUsageAndMissingCredentialBeforeOpen(t *testing.T) {
	for _, args := range [][]string{{}, {"--socket", "/tmp/test.sock"}, {"--socket", "/tmp/test.sock", "--space", "s", "--token-env", "ATE_TEST_TOKEN_MISSING", "nonexistent"}} {
		var out, errOut bytes.Buffer
		t.Setenv("ATE_TEST_TOKEN_MISSING", "")
		if code := serve(context.Background(), args, &out, &errOut); code != 2 || out.Len() != 0 || errOut.Len() == 0 {
			t.Fatalf("config %v: %d %s %s", args, code, out.String(), errOut.String())
		}
	}
	var out, errOut bytes.Buffer
	if code := serve(context.Background(), []string{"-h"}, &out, &errOut); code != 0 || !strings.Contains(errOut.String(), "token-env") {
		t.Fatal(code, errOut.String())
	}
}

type serveReady struct{ once chan []byte }

func (w serveReady) Write(b []byte) (int, error) {
	owned := append([]byte(nil), b...)
	select {
	case w.once <- owned:
	default:
	}
	return len(b), nil
}
func TestServeCLIRealSocketOwnsStoreAndDrains(t *testing.T) {
	t.Run("core-only", func(t *testing.T) { serveCLIRealSocket(t, false) })
	t.Run("agent-profile", func(t *testing.T) { serveCLIRealSocket(t, true) })
}

func serveCLIRealSocket(t *testing.T, profile bool) {
	requireNativeJournal(t)
	dir, err := os.MkdirTemp("/tmp", "ate-cli-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "db")
	if err = os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := ate.Create(path, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	// Probe the platform sandbox before starting a server goroutine. Functional
	// service security always runs over net.Pipe; this test validates the binary path.
	sock := filepath.Join(dir, "a.sock")
	probe, err := os.CreateTemp(dir, "probe-")
	if err != nil {
		t.Fatal(err)
	}
	_ = probe.Close()
	_ = os.Remove(probe.Name())
	t.Setenv("ATE_TEST_SERVE_TOKEN", "cli-private-test-token")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan []byte, 1)
	done := make(chan int, 1)
	var errs bytes.Buffer
	args := []string{"--socket", sock, "--space", "s", "--token-env", "ATE_TEST_SERVE_TOKEN"}
	if profile {
		args = append(args, "--agent-profile")
	}
	args = append(args, path)
	go func() {
		done <- serve(ctx, args, serveReady{ready}, &errs)
	}()
	select {
	case code := <-done:
		if strings.Contains(errs.String(), "operation not permitted") {
			t.Skip("sandbox disallows Unix socket bind")
		}
		t.Fatalf("serve stopped: %d %s", code, errs.String())
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not start")
	}
	c, err := client.Dial(context.Background(), sock, client.Options{Token: "cli-private-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	hasProfile := false
	for _, capability := range c.Identity().Capabilities {
		hasProfile = hasProfile || capability == "agent/v1"
	}
	if hasProfile != profile {
		t.Fatal("CLI and service disagreed on profile capability")
	}
	if profile {
		ack, err := c.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: model.RequestKey{Space: "s", RequestID: "cli-task"}, Command: agentprofile.Command{CreateTask: &agentprofile.Task{
			ID: "task", Space: "s", Owner: "local-host", Goal: "survive service restart", GoalEpoch: 1, Status: agentprofile.TaskOpen,
		}}})
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatalf("CLI profile command: %+v %v", ack, err)
		}
	}
	ack, err := c.Mutate(context.Background(), api.MutateRequest{Key: model.RequestKey{Space: "s", RequestID: "cli-write"}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("k"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("v")}}}}})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatal(ack, err)
	}
	if _, err = c.Snapshot(context.Background()); err == nil {
		t.Fatal("ordinary credential obtained global snapshot")
	}
	if err = c.Close(); err != nil && !errors.Is(err, syscall.EPIPE) {
		t.Fatal(err)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shutdown: %d %s", code, errs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not retire")
	}
	reopened, _, err := ate.OpenExisting(path, ate.Options{AgentProfile: profile})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetState(context.Background(), api.ReadStateRequest{Space: "s", Key: []byte("k")})
	if err != nil || !got.Found || string(got.Value.Data) != "v" {
		t.Fatal(got, err)
	}
	if profile {
		state, err := reopened.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "s", Principal: "local-host"})
		if err != nil || !state.Found || len(state.Snapshot.Tasks) != 1 || state.Snapshot.Tasks[0].Goal != "survive service restart" {
			t.Fatalf("CLI profile recovery: %+v %v", state, err)
		}
	}
	if _, err = os.Lstat(sock); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("owned socket remains after shutdown", err)
	}
}
