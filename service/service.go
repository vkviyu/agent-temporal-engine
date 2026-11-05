// Package service exposes the embedded ATE contracts over a bounded local Unix
// socket. The supplied backend remains the single storage authority.
package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type Backend interface {
	Commit(context.Context, api.CommitRequest) (api.CommitResult, error)
	Mutate(context.Context, api.MutateRequest) (api.CommitResult, error)
	LookupRequest(context.Context, api.LookupRequestRequest) (api.LookupRequestResult, error)
	GetState(context.Context, api.ReadStateRequest) (api.StateValueResult, error)
	Snapshot(context.Context) (api.StateSnapshot, error)
	OpenView(context.Context, api.OpenViewRequest) (api.ViewDescriptor, error)
	CloseView(context.Context, api.CloseViewRequest) error
	GetViewState(context.Context, api.ReadViewStateRequest) (api.StateValueResult, error)
}

type entriesBackend interface {
	GetViewEntries(context.Context, api.ReadViewEntriesRequest) (api.StateEntriesResult, error)
}

type Credential struct {
	Principal model.PrincipalID
	Spaces    []model.SpaceID
	// A global snapshot exposes all RootSet domains and a global publication
	// fence. A scoped credential must opt in explicitly as a trusted operator.
	AllowGlobalSnapshot bool
}
type Options struct {
	Credentials    map[string]Credential
	MaxConnections int
	RequestTimeout time.Duration
	MaxFrameBytes  uint32
}
type authentication struct {
	digest     [sha256.Size]byte
	credential Credential
}
type Server struct {
	backend     Backend
	options     Options
	credentials []authentication
	mu          sync.Mutex
	listener    net.Listener
	socketPath  string
	socketInfo  os.FileInfo
	sessions    map[*session]struct{}
	stopping    bool
	serving     bool
	changed     chan struct{}
	stopOnce    sync.Once
	closed      chan struct{}
	shutdownErr error
	life        context.Context
	cancel      context.CancelFunc
}
type session struct {
	id             string
	conn           net.Conn
	server         *Server
	principal      model.PrincipalID
	spaces         map[model.SpaceID]bool
	globalSnapshot bool
	views          map[model.ViewRef]struct{}
}

var (
	ErrUnauthorized     = errors.New("local client is not authorized")
	ErrSpaceDenied      = errors.New("local client is not allowed in this space")
	ErrAlreadyListening = errors.New("service already listening or serving")
	ErrSocketExists     = errors.New("socket path already exists; refusing replacement")
)

func apiError(c api.ErrorClass) error { return &protocol.Error{Code: c, Message: string(c)} }
func identity(s string) bool {
	return s != "" && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s)
}
func (o Options) checked() (Options, error) {
	if len(o.Credentials) == 0 || len(o.Credentials) > 64 {
		return o, ErrUnauthorized
	}
	if o.MaxConnections == 0 {
		o.MaxConnections = 16
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = 10 * time.Minute
	}
	if o.MaxFrameBytes == 0 {
		o.MaxFrameBytes = protocol.MaxFrameBytes
	}
	if o.MaxConnections < 1 || o.MaxConnections > 128 || o.RequestTimeout <= 0 || o.RequestTimeout > time.Hour || o.MaxFrameBytes > protocol.MaxFrameBytes {
		return o, apiError(api.ErrorResourceLimit)
	}
	owned := make(map[string]Credential, len(o.Credentials))
	for token, c := range o.Credentials {
		if len(token) < 1 || len(token) > 4096 || !identity(string(c.Principal)) || len(c.Spaces) == 0 || len(c.Spaces) > 1024 {
			return o, ErrUnauthorized
		}
		c.Spaces = append([]model.SpaceID(nil), c.Spaces...)
		sort.Slice(c.Spaces, func(i, j int) bool { return c.Spaces[i] < c.Spaces[j] })
		for i, space := range c.Spaces {
			if !identity(string(space)) || strings.HasPrefix(string(space), "ate:") || (i > 0 && c.Spaces[i-1] == space) {
				return o, ErrUnauthorized
			}
		}
		owned[token] = c
	}
	o.Credentials = owned
	return o, nil
}

type incoming struct {
	request protocol.Request
	err     error
}
