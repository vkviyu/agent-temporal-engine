package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/service"
)

type spaceFlags []model.SpaceID

func (s *spaceFlags) String() string     { return fmt.Sprint([]model.SpaceID(*s)) }
func (s *spaceFlags) Set(v string) error { *s = append(*s, model.SpaceID(v)); return nil }

// serve opens an existing core store and keeps one owner until the server has
// retired its sessions and the accepted core work has drained. Credentials are
// read from the named environment variable, never command arguments or output.
func serve(ctx context.Context, args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(errOut)
	socket := flags.String("socket", "", "absolute Unix socket path in an existing private directory")
	tokenEnv := flags.String("token-env", "ATE_AUTH_TOKEN", "environment variable holding the local credential")
	principal := flags.String("principal", "local-host", "stable trusted principal bound to this credential")
	timeout := flags.Duration("request-timeout", 10*time.Minute, "maximum wait for one request; accepted commands continue")
	connections := flags.Int("max-connections", 16, "bounded concurrent local connections")
	global := flags.Bool("allow-global-snapshot", false, "grant this trusted operator access to global RootSet diagnostics")
	agentProfile := flags.Bool("agent-profile", false, "enable the statically compiled agent/v1 profile and its replay")
	eventProfile := flags.Bool("event-profile", false, "enable canonical occurrence events and their replay; required for full audit navigation")
	var spaces spaceFlags
	flags.Var(&spaces, "space", "allowed space; repeat for each allowed space")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate serve --socket PATH --space SPACE [--token-env NAME] DIR\nServe an existing core store; socket parent must already exist and be private.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 || !filepath.IsAbs(*socket) || len(spaces) == 0 || *tokenEnv == "" {
		flags.Usage()
		return 2
	}
	token, ok := os.LookupEnv(*tokenEnv)
	if !ok || token == "" {
		fmt.Fprintf(errOut, "credential environment variable %q is missing or empty\n", *tokenEnv)
		return 2
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	store, _, err := ate.OpenExisting(flags.Arg(0), ate.Options{AgentProfile: *agentProfile, EventProfile: *eventProfile})
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	server, err := service.New(store, service.Options{Credentials: map[string]service.Credential{token: {Principal: model.PrincipalID(*principal), Spaces: spaces, AllowGlobalSnapshot: *global}}, RequestTimeout: *timeout, MaxConnections: *connections})
	// Both startup failures and normal shutdown retire sessions before the backend.
	finish := func(cause error) int {
		if server != nil {
			cause = errors.Join(cause, server.Shutdown(context.Background()))
		}
		cause = errors.Join(cause, store.Close())
		if cause != nil {
			fmt.Fprintln(errOut, cause)
			return 1
		}
		return 0
	}
	if err != nil {
		return finish(err)
	}
	if err = server.Listen(*socket); err != nil {
		return finish(err)
	}
	if err = json.NewEncoder(out).Encode(struct {
		Socket       string            `json:"socket"`
		Principal    model.PrincipalID `json:"principal"`
		Spaces       []model.SpaceID   `json:"spaces"`
		Capabilities []string          `json:"capabilities"`
	}{*socket, model.PrincipalID(*principal), spaces, server.Capabilities()}); err != nil {
		return finish(err)
	}
	return finish(server.Serve(ctx))
}
