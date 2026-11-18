package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type testBackend struct {
	mu        sync.Mutex
	opened    int
	closed    []model.ViewRef
	snapshot  atomic.Int64
	commit    func(context.Context, api.CommitRequest) (api.CommitResult, error)
	closeView func(context.Context, api.CloseViewRequest) error
}

func testFence() model.CommitFence {
	return model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
}
func (b *testBackend) Commit(ctx context.Context, r api.CommitRequest) (api.CommitResult, error) {
	if b.commit != nil {
		return b.commit(ctx, r)
	}
	return api.CommitResult{Code: api.CodeConflict, RequestID: r.RequestID, Conflict: api.ConflictPrecondition}, nil
}
func (b *testBackend) Mutate(context.Context, api.MutateRequest) (api.CommitResult, error) {
	return api.CommitResult{}, errors.New("unused mutate")
}
func (b *testBackend) LookupRequest(context.Context, api.LookupRequestRequest) (api.LookupRequestResult, error) {
	return api.LookupRequestResult{Code: api.CodeNotFoundAtFence, ObservedFence: testFence()}, nil
}
func (b *testBackend) GetState(ctx context.Context, _ api.ReadStateRequest) (api.StateValueResult, error) {
	return fakeReadResult(ctx, api.StateValueResult{Fence: testFence()})
}
func (b *testBackend) Snapshot(ctx context.Context) (api.StateSnapshot, error) {
	b.snapshot.Add(1)
	return fakeReadResult(ctx, api.StateSnapshot{Fence: testFence()})
}
func (b *testBackend) OpenView(ctx context.Context, r api.OpenViewRequest) (api.ViewDescriptor, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opened++
	ref := model.ViewRef(strings.Repeat("v", b.opened))
	result := api.ViewDescriptor{Ref: ref, Anchor: model.SnapshotAnchor{Space: r.Space, Fence: testFence()}, ViewDigest: model.ObjectID(strings.Repeat("a", 64))}
	if grant, ok := ctx.Value(fakeStateReadCapture{}).(*fakeStateReadGrant); ok {
		grant.body = mustJSON(result)
	}
	return result, nil
}
func (b *testBackend) CloseView(ctx context.Context, r api.CloseViewRequest) error {
	if b.closeView != nil {
		if err := b.closeView(ctx, r); err != nil {
			return err
		}
	}
	b.mu.Lock()
	b.closed = append(b.closed, r.Ref)
	b.mu.Unlock()
	return nil
}
func (b *testBackend) GetViewState(ctx context.Context, _ api.ReadViewStateRequest) (api.StateValueResult, error) {
	return fakeReadResult(ctx, api.StateValueResult{Fence: testFence()})
}

type fakeStateReadCapture struct{}

type fakeStateReadGrant struct {
	binding api.ReadDeliveryBinding
	body    []byte
}

func (g *fakeStateReadGrant) Consume(binding api.ReadDeliveryBinding) ([]byte, error) {
	if g.binding != binding || g.body == nil {
		return nil, api.ErrReadEmissionDenied
	}
	body := g.body
	g.body = nil
	return body, nil
}

func (b *testBackend) PrepareReadDelivery(ctx context.Context, binding api.ReadDeliveryBinding, read func(context.Context) error) (api.ReadDeliveryGrant, error) {
	grant := &fakeStateReadGrant{binding: binding}
	if err := read(context.WithValue(ctx, fakeStateReadCapture{}, grant)); err != nil {
		return nil, err
	}
	if grant.body == nil {
		return nil, api.ErrReadEmissionDenied
	}
	return grant, nil
}

func fakeReadResult[T any](ctx context.Context, result T) (T, error) {
	if grant, ok := ctx.Value(fakeStateReadCapture{}).(*fakeStateReadGrant); ok {
		grant.body = mustJSON(result)
		var zero T
		return zero, nil
	}
	return result, nil
}

// Deliberately classify everything as internal. Transport auth/parse failures
// must retain their own class instead of being delegated to this adapter.
func (b *testBackend) ClassifyError(error) api.ErrorClass { return api.ErrorInternal }
func newServer(t *testing.T, b Backend, o Options) *Server {
	t.Helper()
	if o.Credentials == nil {
		o.Credentials = map[string]Credential{"secret": {Principal: "host", Spaces: []model.SpaceID{"s"}}}
	}
	s, err := New(b, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}
func pipeClient(t *testing.T, s *Server, token string) *client.Client {
	t.Helper()
	a, b := net.Pipe()
	if !s.acceptConn(b) {
		t.Fatal("connection refused")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := client.New(ctx, a, client.Options{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func remoteCode(t *testing.T, err error, code api.ErrorClass) {
	t.Helper()
	var remote *protocol.Error
	if !errors.As(err, &remote) || remote.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}
func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}
func exchange(t *testing.T, c net.Conn, id, method string, body any) protocol.Response {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(protocol.Request{Version: protocol.Version, CorrelationID: id, Method: method, Body: b})
	if err = protocol.WriteFrame(c, raw); err != nil {
		t.Fatal(err)
	}
	raw, err = protocol.ReadFrame(c)
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.DecodeResponse(raw)
	if err != nil || response.CorrelationID != id {
		t.Fatalf("%+v %v", response, err)
	}
	return response
}

func TestConnectionOwnsViewsAndSnapshotNeedsOperator(t *testing.T) {
	b := &testBackend{}
	s := newServer(t, b, Options{Credentials: map[string]Credential{"secret": {Principal: "host", Spaces: []model.SpaceID{"s"}}, "operator": {Principal: "operator", Spaces: []model.SpaceID{"s"}, AllowGlobalSnapshot: true}}})
	c1 := pipeClient(t, s, "secret")
	c2 := pipeClient(t, s, "secret")
	op := pipeClient(t, s, "operator")
	view, err := c1.OpenView(context.Background(), api.OpenViewRequest{Space: "s"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c2.GetViewState(context.Background(), api.ReadViewStateRequest{View: view.Ref})
	remoteCode(t, err, api.ErrorUnauthorized)
	err = c2.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref})
	remoteCode(t, err, api.ErrorUnauthorized)
	b.mu.Lock()
	closed := len(b.closed)
	b.mu.Unlock()
	if closed != 0 {
		t.Fatal("foreign CloseView reached backend")
	}
	if _, err = c1.GetViewState(context.Background(), api.ReadViewStateRequest{View: view.Ref}); err != nil {
		t.Fatal(err)
	}
	if _, err = c1.Snapshot(context.Background()); err == nil {
		t.Fatal("scoped credential received global snapshot")
	} else {
		remoteCode(t, err, api.ErrorUnauthorized)
	}
	if b.snapshot.Load() != 0 {
		t.Fatal("unauthorized snapshot called backend")
	}
	if _, err = op.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.snapshot.Load() != 1 {
		t.Fatal("operator snapshot missing")
	}
}
func TestCredentialOwnershipAndRepeatedHelloRejected(t *testing.T) {
	b := &testBackend{}
	spaces := []model.SpaceID{"s"}
	creds := map[string]Credential{"secret": {Principal: "host", Spaces: spaces}}
	s := newServer(t, b, Options{Credentials: creds})
	spaces[0] = "other"
	delete(creds, "secret")
	a, z := net.Pipe()
	defer a.Close()
	if !s.acceptConn(z) {
		t.Fatal("refused")
	}
	hello := exchange(t, a, "first", "hello", protocol.HelloRequest{Token: "secret"})
	if hello.Error != nil {
		t.Fatal(hello.Error)
	}
	var identity protocol.HelloResult
	if err := protocol.DecodeStrict(hello.Body, &identity); err != nil {
		t.Fatal(err)
	}
	if identity.Principal != "host" || len(identity.Spaces) != 1 || identity.Spaces[0] != "s" {
		t.Fatalf("borrowed credentials changed: %+v", identity)
	}
	for _, capability := range identity.Capabilities {
		if capability == "query/explore-v1" {
			t.Fatal("advertised unimplemented explorer")
		}
	}
	second := exchange(t, a, "second", "hello", protocol.HelloRequest{Token: "secret"})
	if second.Error == nil || second.Error.Code != api.ErrorUnauthorized {
		t.Fatalf("repeated hello accepted: %+v", second)
	}
}
func TestAuthAndMalformedRequestsKeepLocalErrorClass(t *testing.T) {
	s := newServer(t, &testBackend{}, Options{})
	for _, test := range []struct {
		name, method string
		body         any
		want         api.ErrorClass
	}{{"missing hello", "snapshot", protocol.Empty{}, api.ErrorUnauthorized}, {"bad token", "hello", protocol.HelloRequest{Token: "wrong"}, api.ErrorUnauthorized}, {"principal injection", "hello", map[string]string{"token": "secret", "principal": "admin"}, api.ErrorInvalidRequest}} {
		t.Run(test.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			if !s.acceptConn(b) {
				t.Fatal("refused")
			}
			r := exchange(t, a, "id", test.method, test.body)
			if r.Error == nil || r.Error.Code != test.want {
				t.Fatalf("%+v", r)
			}
		})
	}
	c := pipeClient(t, s, "secret")
	_, err := c.GetState(context.Background(), api.ReadStateRequest{Space: "other"})
	remoteCode(t, err, api.ErrorUnauthorized)
}
func TestDisconnectCancelsWaitAndRetiresOwnedView(t *testing.T) {
	started := make(chan struct{})
	interrupted := make(chan struct{})
	retired := make(chan struct{})
	var once sync.Once
	b := &testBackend{commit: func(ctx context.Context, r api.CommitRequest) (api.CommitResult, error) {
		close(started)
		<-ctx.Done()
		close(interrupted)
		return api.CommitResult{Code: api.CodeWaitInterrupted, RequestID: r.RequestID}, ctx.Err()
	}, closeView: func(context.Context, api.CloseViewRequest) error { once.Do(func() { close(retired) }); return nil }}
	s := newServer(t, b, Options{})
	c := pipeClient(t, s, "secret")
	if _, err := c.OpenView(context.Background(), api.OpenViewRequest{Space: "s"}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := c.Commit(context.Background(), api.CommitRequest{Space: "s", RequestID: "original"})
		result <- err
	}()
	waitSignal(t, started)
	_ = c.Close()
	waitSignal(t, interrupted)
	waitSignal(t, retired)
	select {
	case err := <-result:
		var unknown *client.OutcomeUnknownError
		if !errors.As(err, &unknown) || unknown.Key.RequestID != "original" || unknown.Key.Principal != "host" {
			t.Fatalf("original identity lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client didn't return")
	}
}
func TestShutdownWaitCanBeCanceledAndRetried(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	b := &testBackend{commit: func(context.Context, api.CommitRequest) (api.CommitResult, error) {
		close(started)
		<-release
		return api.CommitResult{}, nil
	}}
	s := newServer(t, b, Options{})
	c := pipeClient(t, s, "secret")
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = c.Commit(context.Background(), api.CommitRequest{Space: "s", RequestID: "r"})
	}()
	waitSignal(t, started)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	a, z := net.Pipe()
	if s.acceptConn(z) {
		t.Fatal("shutdown admitted new session")
	}
	_ = a.Close()
	close(release)
	waitSignal(t, finished)
	ctx, finish := context.WithTimeout(context.Background(), 3*time.Second)
	defer finish()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestFrameReceiverLimitsBeforePayloadRead(t *testing.T) {
	for _, limit := range []uint32{128, protocol.MaxFrameBytes} {
		s := newServer(t, &testBackend{}, Options{MaxFrameBytes: limit})
		a, b := net.Pipe()
		if !s.acceptConn(b) {
			t.Fatal("refused")
		}
		n := limit + 1
		if limit > 64<<10 {
			n = (64 << 10) + 1
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], n)
		if _, err := a.Write(header[:]); err != nil {
			t.Fatal(err)
		}
		_ = a.SetReadDeadline(time.Now().Add(3 * time.Second))
		var one [1]byte
		if _, err := a.Read(one[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("oversize header didn't close without body: %v", err)
		}
		_ = a.Close()
	}
}
func TestCredentialValidationAndListenPreservesPaths(t *testing.T) {
	for _, credential := range []Credential{{Spaces: []model.SpaceID{"s"}}, {Principal: "p"}, {Principal: "p", Spaces: []model.SpaceID{"ate:system"}}, {Principal: "p", Spaces: []model.SpaceID{"s", "s"}}} {
		if _, err := New(&testBackend{}, Options{Credentials: map[string]Credential{"token": credential}}); err == nil {
			t.Fatalf("invalid credential accepted: %+v", credential)
		}
	}
	s := newServer(t, &testBackend{}, Options{})
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "must-remain")
	content := []byte("owned by caller")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(path); !errors.Is(err, ErrSocketExists) {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(content) {
		t.Fatal("Listen changed existing file")
	}
	if err := os.Chmod(dir, 0750); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0700)
	if err := s.Listen(filepath.Join(dir, "new.sock")); err == nil {
		t.Fatal("nonprivate parent accepted")
	}
	info, _ := os.Stat(dir)
	if info.Mode().Perm() != 0750 {
		t.Fatal("Listen changed caller directory permissions")
	}
}

type largeAgentBackend struct {
	testBackend
	state      api.AgentStateResult
	prepared   atomic.Int64
	consumed   atomic.Int64
	mutations  atomic.Int64
	agentReads atomic.Int64
}

func (b *largeAgentBackend) AgentProfileEnabled() bool { return true }
func (b *largeAgentBackend) ApplyAgent(context.Context, api.ApplyAgentRequest) (api.CommitResult, error) {
	b.mutations.Add(1)
	return api.CommitResult{}, errors.New("unexpected agent mutation")
}
func (b *largeAgentBackend) Commit(context.Context, api.CommitRequest) (api.CommitResult, error) {
	b.mutations.Add(1)
	return api.CommitResult{}, errors.New("unexpected commit")
}
func (b *largeAgentBackend) Mutate(context.Context, api.MutateRequest) (api.CommitResult, error) {
	b.mutations.Add(1)
	return api.CommitResult{}, errors.New("unexpected mutation")
}
func (b *largeAgentBackend) GetAgentState(ctx context.Context, r api.ReadAgentStateRequest) (api.AgentStateResult, error) {
	b.agentReads.Add(1)
	if r.Principal != "host" {
		return api.AgentStateResult{}, ErrUnauthorized
	}
	if r.Space == "small" {
		return fakeReadResult(ctx, api.AgentStateResult{Anchor: model.SnapshotAnchor{Space: r.Space, Fence: testFence()}, Fence: testFence()})
	}
	return fakeReadResult(ctx, b.state)
}

type countedReadGrant struct {
	api.ReadDeliveryGrant
	consumed *atomic.Int64
}

func (g countedReadGrant) Consume(binding api.ReadDeliveryBinding) ([]byte, error) {
	g.consumed.Add(1)
	return g.ReadDeliveryGrant.Consume(binding)
}
func (b *largeAgentBackend) PrepareReadDelivery(ctx context.Context, binding api.ReadDeliveryBinding, read func(context.Context) error) (api.ReadDeliveryGrant, error) {
	b.prepared.Add(1)
	grant, err := b.testBackend.PrepareReadDelivery(ctx, binding, read)
	if err != nil {
		return nil, err
	}
	return countedReadGrant{grant, &b.consumed}, nil
}

func TestOversizedAgentStateReturnsResourceLimitAndClientRemainsUsable(t *testing.T) {
	b := &largeAgentBackend{state: api.AgentStateResult{
		Found: true, Anchor: model.SnapshotAnchor{Space: "s", Fence: testFence()}, Fence: testFence(),
		RootSet: model.ObjectID(strings.Repeat("b", 64)), LiveControlRoot: model.ObjectID(strings.Repeat("c", 64)),
	}}
	for i := 0; i < 25; i++ {
		b.state.Snapshot.Lifecycle.Queries = append(b.state.Snapshot.Lifecycle.Queries, agentprofile.Query{
			ID: fmt.Sprintf("q-%03d", i), Space: "s", Owner: "host", Status: "open", Version: 1,
			Input: bytes.Repeat([]byte("x"), 512<<10),
		})
	}
	image, err := agentprofile.EncodeRecordImage(b.state.Snapshot)
	if err != nil || image.Size() <= protocol.MaxFrameBytes {
		t.Fatalf("invalid oversized record fixture: size=%d err=%v", image.Size(), err)
	}
	server := newServer(t, b, Options{Credentials: map[string]Credential{
		"secret": {Principal: "host", Spaces: []model.SpaceID{"s", "small"}},
	}})
	c := pipeClient(t, server, "secret")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := c.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "s"})
	// testBackend classifies every error as Internal: the local frame failure
	// must still arrive as the exact application ResourceLimit class.
	remoteCode(t, err, api.ErrorResourceLimit)
	if !reflect.DeepEqual(got, api.AgentStateResult{}) {
		t.Fatal("oversized response exposed partial agent state")
	}
	if b.prepared.Load() != 1 || b.consumed.Load() != 1 || b.agentReads.Load() != 1 || b.mutations.Load() != 0 {
		t.Fatalf("read bypassed authorization, retried or committed: prepares=%d consumes=%d reads=%d mutations=%d",
			b.prepared.Load(), b.consumed.Load(), b.agentReads.Load(), b.mutations.Load())
	}
	small, err := c.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "small"})
	if err != nil || small.Found || small.Anchor.Space != "small" {
		t.Fatalf("same client cannot read a bounded snapshot: found=%v err=%v", small.Found, err)
	}
	if value, err := c.GetState(ctx, api.ReadStateRequest{Space: "s"}); err != nil || value.Fence != testFence() {
		t.Fatalf("subsequent request failed: %+v %v", value, err)
	}
	if b.prepared.Load() != 3 || b.consumed.Load() != 3 || b.mutations.Load() != 0 {
		t.Fatal("delivery changed the backend's mutation state or lost grants")
	}
}

func TestReplyChecksFullEnvelopeBeforeWriting(t *testing.T) {
	response := protocol.Response{Version: protocol.Version, CorrelationID: "<correlation>", Body: json.RawMessage(`{"data":""}`)}
	empty, err := encodeResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	padding := protocol.MaxFrameBytes - len(empty)
	response.Body = json.RawMessage(`{"data":"` + strings.Repeat("x", padding) + `"}`)
	if raw, err := encodeResponse(response); err != nil || len(raw) != protocol.MaxFrameBytes {
		t.Fatalf("exact ceiling rejected: length=%d err=%v", len(raw), err)
	}
	response.Body = json.RawMessage(`{"data":"` + strings.Repeat("x", padding+1) + `"}`)
	if len(response.Body) >= protocol.MaxFrameBytes {
		t.Fatal("fixture must exceed the limit only after adding the envelope")
	}
	if _, err := encodeResponse(response); !errors.Is(err, protocol.ErrFrameLimit) {
		t.Fatalf("oversized envelope accepted: %v", err)
	}
	server := newServer(t, &testBackend{}, Options{})
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	ss := &session{server: server, conn: z}
	done := make(chan error, 1)
	go func() { done <- ss.reply(response) }()
	_ = a.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := protocol.ReadFrame(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := protocol.DecodeResponse(raw)
	if err != nil || got.CorrelationID != response.CorrelationID || string(got.Body) != "{}" || got.Error == nil || got.Error.Code != api.ErrorResourceLimit {
		t.Fatalf("expected only a complete ResourceLimit frame, got length=%d err=%v response=%+v", len(raw), err, got)
	}
	if err := <-done; err != nil {
		t.Fatalf("application limit returned a transport failure: %v", err)
	}
}
