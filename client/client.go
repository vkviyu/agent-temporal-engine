// Package client is the connection-affine Go SDK for ATE's local service.
package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

var ErrClosed = errors.New("ATE client connection is closed")
var ErrPrincipal = errors.New("request principal differs from authenticated principal")

// OutcomeUnknownError never implies the command was absent. Reconnect and look
// up Key with the same credentials; never silently retry under a new RequestID.
type OutcomeUnknownError struct {
	Key   model.RequestKey
	Cause error
}

func (e *OutcomeUnknownError) Error() string {
	return "ATE command response unavailable; reconcile original RequestKey: " + e.Cause.Error()
}
func (e *OutcomeUnknownError) Unwrap() error { return e.Cause }

type Options struct{ Token string }
type Client struct {
	conn     net.Conn
	gate     chan struct{}
	closed   atomic.Bool
	once     sync.Once
	sequence uint64
	identity protocol.HelloResult
}

func Dial(ctx context.Context, path string, o Options) (*Client, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	c, err := New(ctx, conn, o)
	if err != nil {
		conn.Close()
	}
	return c, err
}

// New takes ownership of conn, including a failed handshake.
func New(ctx context.Context, conn net.Conn, o Options) (*Client, error) {
	if conn == nil {
		return nil, protocol.ErrProtocol
	}
	c := &Client{conn: conn, gate: make(chan struct{}, 1)}
	c.gate <- struct{}{}
	if err := c.call(ctx, "hello", protocol.HelloRequest{Token: o.Token}, &c.identity, nil); err != nil {
		c.Close()
		return nil, err
	}
	if c.identity.Principal == "" {
		c.Close()
		return nil, protocol.ErrProtocol
	}
	return c, nil
}
func (c *Client) Identity() protocol.HelloResult {
	r := c.identity
	r.Spaces = append([]model.SpaceID(nil), r.Spaces...)
	r.Capabilities = append([]string(nil), r.Capabilities...)
	return r
}
func (c *Client) Close() error {
	var err error
	c.once.Do(func() { c.closed.Store(true); err = c.conn.Close() })
	return err
}
func (c *Client) principal(p model.PrincipalID) error {
	if p != "" && p != c.identity.Principal {
		return ErrPrincipal
	}
	return nil
}
func (c *Client) call(ctx context.Context, method string, body, out any, key *model.RequestKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { c.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.closed.Load() {
		return ErrClosed
	}
	p, err := json.Marshal(body)
	if err != nil {
		return err
	}
	c.sequence++
	id := strconv.FormatUint(c.sequence, 10)
	data, err := json.Marshal(protocol.Request{Version: protocol.Version, CorrelationID: id, Method: method, Body: p})
	if err != nil {
		return err
	}
	if len(data) > protocol.MaxFrameBytes {
		return protocol.ErrFrameLimit
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { c.Close(); close(canceled) })
	defer func() {
		if !stop() {
			<-canceled
		}
	}()
	transportFailure := func(err error) error {
		c.Close()
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if key != nil {
			return &OutcomeUnknownError{Key: *key, Cause: err}
		}
		return err
	}
	// From the first attempted frame write onward a command may have reached the
	// server, even if the caller observes only a short write or canceled wait.
	if err = protocol.WriteFrame(c.conn, data); err != nil {
		return transportFailure(err)
	}
	response, err := protocol.ReadFrame(c.conn)
	if err != nil {
		return transportFailure(err)
	}
	reply, err := protocol.DecodeResponse(response)
	if err != nil {
		return transportFailure(err)
	}
	if reply.CorrelationID != id {
		return transportFailure(protocol.ErrProtocol)
	}
	payload := bytes.TrimSpace(reply.Body)
	if _, records := out.(*api.AgentStateRecordsPage); records && len(reply.Body) > api.MaxAgentStatePageBytes {
		return transportFailure(protocol.ErrProtocol)
	}
	// A successful RPC must carry its typed object. JSON null must not turn
	// a missing ACK or View into a successful zero Go value.
	if bytes.Equal(payload, []byte("null")) {
		if reply.Error != nil {
			return reply.Error
		}
		return transportFailure(protocol.ErrProtocol)
	}
	if len(payload) == 0 || payload[0] != '{' {
		return transportFailure(protocol.ErrProtocol)
	}
	if out != nil {
		if err = protocol.DecodeStrict(payload, out); err != nil {
			return transportFailure(err)
		}
	} else {
		var empty protocol.Empty
		if err = protocol.DecodeStrict(payload, &empty); err != nil {
			return transportFailure(err)
		}
	}
	if reply.Error != nil {
		return reply.Error
	}
	if err = validateResult(out, key); err != nil {
		return transportFailure(err)
	}
	return nil
}
func (c *Client) Commit(ctx context.Context, r api.CommitRequest) (api.CommitResult, error) {
	var out api.CommitResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	key := r.Key()
	key.Principal = c.identity.Principal
	err := c.call(ctx, "commit", protocol.CommitRequest{Space: r.Space, RequestID: r.RequestID, Method: r.Method, Body: r.Body, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, err
}
func (c *Client) Mutate(ctx context.Context, r api.MutateRequest) (api.CommitResult, error) {
	var out api.CommitResult
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	err := c.call(ctx, "mutate", protocol.MutateRequest{Space: key.Space, RequestID: key.RequestID, Batch: r.Batch, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, err
}
func (c *Client) LookupRequest(ctx context.Context, r api.LookupRequestRequest) (api.LookupRequestResult, error) {
	var out api.LookupRequestResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	err := c.call(ctx, "lookup_request", protocol.LookupRequest{Space: r.Space, RequestID: r.RequestID}, &out, nil)
	return out, err
}
func (c *Client) GetState(ctx context.Context, r api.ReadStateRequest) (api.StateValueResult, error) {
	var out api.StateValueResult
	err := c.call(ctx, "get_state", protocol.ReadStateRequest{Space: r.Space, Key: r.Key}, &out, nil)
	return out, err
}
func (c *Client) Snapshot(ctx context.Context) (api.StateSnapshot, error) {
	var out api.StateSnapshot
	err := c.call(ctx, "snapshot", protocol.Empty{}, &out, nil)
	return out, err
}
func (c *Client) OpenView(ctx context.Context, r api.OpenViewRequest) (api.ViewDescriptor, error) {
	var out api.ViewDescriptor
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	err := c.call(ctx, "open_view", protocol.OpenViewRequest{Space: r.Space, Anchor: r.Anchor, Knowledge: r.Knowledge, Mode: r.Mode}, &out, nil)
	return out, err
}
func (c *Client) CloseView(ctx context.Context, r api.CloseViewRequest) error {
	return c.call(ctx, "close_view", protocol.CloseViewRequest{Ref: r.Ref}, nil, nil)
}
func (c *Client) GetViewState(ctx context.Context, r api.ReadViewStateRequest) (api.StateValueResult, error) {
	var out api.StateValueResult
	err := c.call(ctx, "get_view_state", protocol.ReadViewRequest{View: r.View, Key: r.Key}, &out, nil)
	return out, err
}
func (c *Client) GetViewEntries(ctx context.Context, r api.ReadViewEntriesRequest) (api.StateEntriesResult, error) {
	var out api.StateEntriesResult
	err := c.call(ctx, "get_view_entries", protocol.ReadViewEntriesRequest{View: r.View, Limits: r.Limits}, &out, nil)
	return out, err
}

var _ api.Committer = (*Client)(nil)
var _ api.ViewOpener = (*Client)(nil)
var _ api.ViewEntriesReader = (*Client)(nil)

func optionalID(id model.ObjectID) bool { return id == "" || model.ValidDigest(string(id)) }
func validateCommit(r api.CommitResult, key *model.RequestKey) error {
	if key != nil && r.RequestID != key.RequestID {
		return protocol.ErrProtocol
	}
	switch r.Code {
	case api.CodeDurableAck:
		raw, err := hex.DecodeString(string(r.TxID))
		if err != nil || len(raw) != 16 || string(r.TxID) != hex.EncodeToString(raw) || r.Fence.Validate() != nil || !model.ValidDigest(string(r.RequestDigest)) || !model.ValidDigest(string(r.ResultDigest)) || !model.ValidDigest(string(r.RootSet)) || !optionalID(r.StateRoot) {
			return protocol.ErrProtocol
		}
	case api.CodeConflict:
		if r.Conflict != api.ConflictIdempotency && r.Conflict != api.ConflictPrecondition {
			return protocol.ErrProtocol
		}
	case api.CodeNotSubmitted, api.CodeRejected, api.CodeResourceLimit, api.CodeWaitInterrupted, api.CodeCommitUnknown, api.CodeStoreFenced:
	default:
		return protocol.ErrProtocol
	}
	return nil
}
func validateResult(out any, key *model.RequestKey) error {
	switch r := out.(type) {
	case *protocol.HelloResult:
		if r.Principal == "" || len(r.Principal) > model.MaxIdentityBytes || len(r.Spaces) == 0 {
			return protocol.ErrProtocol
		}
	case *api.CommitResult:
		return validateCommit(*r, key)
	case *api.LookupRequestResult:
		switch r.Code {
		case api.CodeDurableAck:
			if err := validateCommit(r.Result, nil); err != nil {
				return err
			}
		case api.CodeNotFoundAtFence, api.CodeInFlight, api.CodeIndeterminate:
		default:
			return protocol.ErrProtocol
		}
		if r.Code != api.CodeIndeterminate && r.ObservedFence.Validate() != nil {
			return protocol.ErrProtocol
		}
	case *api.StateValueResult:
		if r.Fence.Validate() != nil || !optionalID(r.StateRoot) || !optionalID(r.RootSet) {
			return protocol.ErrProtocol
		}
		if r.Found {
			switch r.Value.Kind {
			case api.ValueNull:
				if len(r.Value.Data) != 0 {
					return protocol.ErrProtocol
				}
			case api.ValueBytes:
			default:
				return protocol.ErrProtocol
			}
		} else if r.Value.Kind != "" || len(r.Value.Data) != 0 {
			return protocol.ErrProtocol
		}
	case *api.StateSnapshot:
		if r.Fence.Validate() != nil || !optionalID(r.RootSet) {
			return protocol.ErrProtocol
		}
	case *api.ViewDescriptor:
		if r.Ref == "" || r.Anchor.Validate() != nil || !model.ValidDigest(string(r.ViewDigest)) {
			return protocol.ErrProtocol
		}
	case *api.StateEntriesResult:
		if r.Anchor.Validate() != nil || r.Fence.Validate() != nil || r.Anchor.Fence != r.Fence || !optionalID(r.StateRoot) || !optionalID(r.RootSet) {
			return protocol.ErrProtocol
		}
		for _, item := range r.Items {
			if len(item.Key) > api.MaxStateKeyBytes || !model.ValidDigest(string(item.Value)) {
				return protocol.ErrProtocol
			}
		}
	case *api.AgentStateResult:
		return validateAgentState(*r)
	case *api.AgentStateRecordsPage:
		return validateAgentStateRecords(*r)
	}
	return nil
}
