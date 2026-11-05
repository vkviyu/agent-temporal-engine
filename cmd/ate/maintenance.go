package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// compact-state obtains the same exclusive owner as serve. The core first
// replays the complete typed history, so a physical-only tool cannot retire
// a layout whose domain references or recovery controls it did not understand.
func compactState(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("compact-state", flag.ContinueOnError)
	flags.SetOutput(errOut)
	agent := flags.Bool("agent-profile", false, "verify the statically compiled agent/v1 profile")
	events := flags.Bool("event-profile", false, "verify canonical occurrence streams")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate compact-state [--agent-profile] [--event-profile] DIR")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return 2
	}
	store, _, err := ate.OpenExisting(flags.Arg(0), ate.Options{AgentProfile: *agent, EventProfile: *events})
	if err != nil {
		var pending *storage.CleanupError
		if errors.As(err, &pending) {
			for i := 0; i < 3; i++ {
				if closeErr := pending.Close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				} else {
					break
				}
			}
		}
		fmt.Fprintln(errOut, err)
		return 1
	}
	report, err := store.CompactAuditFull(context.Background())
	for i := 0; i < 3; i++ {
		if closeErr := store.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		} else {
			break
		}
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	if err = json.NewEncoder(out).Encode(report); err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}
