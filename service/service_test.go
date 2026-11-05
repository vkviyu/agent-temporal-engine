package service

import (
	"context"
	"errors"
	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/model"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalServiceClientConformance(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ate-svc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	storePath := filepath.Join(dir, "store")
	if err := os.Mkdir(storePath, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := ate.Create(storePath, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runDir := filepath.Join(dir, "run")
	if err = os.Mkdir(runDir, 0700); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(runDir, "ate.sock")
	srv, err := New(store, Options{Credentials: map[string]Credential{"secret": {Principal: "host", Spaces: []model.SpaceID{"smoke"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = srv.Listen(sock); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skip("sandbox disallows Unix sockets")
		}
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	defer func() { _ = srv.Shutdown(context.Background()); <-done }()
	time.Sleep(10 * time.Millisecond)
	c, err := client.Dial(context.Background(), sock, client.Options{Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result, err := c.Mutate(context.Background(), api.MutateRequest{Key: model.RequestKey{Space: "smoke", Principal: "host", RequestID: "r1"}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("k"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("v")}}}}})
	if err != nil || result.Code != api.CodeDurableAck {
		t.Fatalf("mutate %v %+v", err, result)
	}
	got, err := c.GetState(context.Background(), api.ReadStateRequest{Space: "smoke", Key: []byte("k")})
	if err != nil || !got.Found || string(got.Value.Data) != "v" {
		t.Fatalf("state %v %+v", err, got)
	}
	view, err := c.OpenView(context.Background(), api.OpenViewRequest{Space: "smoke", Principal: "host", Mode: "state"})
	if err != nil {
		t.Fatal(err)
	}
	at, err := c.GetViewState(context.Background(), api.ReadViewStateRequest{View: view.Ref, Key: []byte("k")})
	if err != nil || !at.Found || string(at.Value.Data) != "v" {
		t.Fatalf("view %v %+v", err, at)
	}
	entries, err := c.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil || len(entries.Items) != 1 || string(entries.Items[0].Key) != "k" {
		t.Fatalf("view entries %v %+v", err, entries)
	}
	if err := c.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetViewState(context.Background(), api.ReadViewStateRequest{View: view.Ref, Key: []byte("k")}); err == nil {
		t.Fatal("closed view readable")
	}
	if _, err := c.Mutate(context.Background(), api.MutateRequest{Key: model.RequestKey{Space: "other", Principal: "host", RequestID: "r2"}}); err == nil {
		t.Fatal("unauthorized space accepted")
	}
}

func socketDirectory(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ate-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
func TestListenRefusesExistingSocketAndCleanupPreservesReplacement(t *testing.T) {
	dir := socketDirectory(t)
	path := filepath.Join(dir, "existing.sock")
	original, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("native Unix sockets denied; session security is covered by net.Pipe")
		}
		t.Fatal(err)
	}
	original.SetUnlinkOnClose(false)
	defer original.Close()
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	s := newServer(t, &testBackend{}, Options{})
	if err = s.Listen(path); !errors.Is(err, ErrSocketExists) {
		t.Fatalf("existing socket replaced: %v", err)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("Listen changed existing socket")
	}
	ownPath := filepath.Join(dir, "owned.sock")
	if err = s.Listen(ownPath); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(ownPath, ownPath+".moved"); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement must remain")
	if err = os.WriteFile(ownPath, replacement, 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(ownPath)
	if err != nil || string(got) != string(replacement) {
		t.Fatalf("cleanup removed replacement: %q %v", got, err)
	}
}
func TestServeContextStopsListenerAndSessions(t *testing.T) {
	dir := socketDirectory(t)
	path := filepath.Join(dir, "ctx.sock")
	s := newServer(t, &testBackend{}, Options{})
	if err := s.Listen(path); err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("native Unix sockets denied; session security is covered by net.Pipe")
		}
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	c, err := client.Dial(context.Background(), path, client.Options{Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve didn't exit on context cancellation")
	}
	finishCtx, finish := context.WithTimeout(context.Background(), 3*time.Second)
	defer finish()
	if err = s.Shutdown(finishCtx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned socket was not removed: %v", err)
	}
}
