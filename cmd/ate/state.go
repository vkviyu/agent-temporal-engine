package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// State commands run the trusted-host core. Agent profile authorization is a
// separate API milestone; the CLI deliberately exposes no generic state writer.
type stateRootsJSON struct {
	DomainStates       string `json:"domain_states"`
	ExecutionKnowledge string `json:"execution_knowledge"`
	BranchRefs         string `json:"branch_refs"`
	LiveControl        string `json:"live_control"`
	CommandReceipts    string `json:"command_receipts"`
}

type stateReport struct {
	journalReport
	RootSet string         `json:"root_set"`
	Roots   stateRootsJSON `json:"roots"`
}

func stateJSON(physical storage.RecoveryReport, snapshot *api.StateSnapshot, semantic bool) stateReport {
	report := stateReport{journalReport: journalJSON(physical)}
	report.Format = "ATE-CORE-STATE-EXPERIMENTAL-1"
	report.SemanticReplayVerified = semantic
	if snapshot != nil {
		report.RootSet = string(snapshot.RootSet)
		r := snapshot.Roots
		report.Roots = stateRootsJSON{string(r.DomainStates), string(r.ExecutionKnowledge), string(r.BranchRefs), string(r.LiveControl), string(r.CommandReceipts)}
	}
	return report
}

// finishStateCommand retires Store resources before emitting success. Native
// segment/owner handles may each need one subsequent Close to confirm retirement;
// the Store also owns its directory. Preserve every error even after retry.
// Failed constructors carry their complete cleanup through storage.CleanupError.
func finishStateCommand(store io.Closer, physical storage.RecoveryReport, snapshot *api.StateSnapshot, commandErr error, out, errOut io.Writer) int {
	var pending io.Closer = store
	if pending == nil {
		var cleanup *storage.CleanupError
		if errors.As(commandErr, &cleanup) {
			pending = cleanup
		}
	}
	if pending != nil {
		for attempt := 0; attempt < 3; attempt++ {
			if err := pending.Close(); err != nil {
				commandErr = errors.Join(commandErr, err)
				continue
			}
			break
		}
	}
	if commandErr == nil && (store == nil || snapshot == nil) {
		commandErr = errors.New("state verification returned no usable snapshot")
	}
	semantic := commandErr == nil && store != nil && snapshot != nil
	if physical.Verified {
		commandErr = errors.Join(commandErr, json.NewEncoder(out).Encode(stateJSON(physical, snapshot, semantic)))
	}
	if commandErr != nil {
		fmt.Fprintln(errOut, commandErr)
		return 1
	}
	return 0
}

func initState(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("init-state", flag.ContinueOnError)
	flags.SetOutput(errOut)
	eventProfile := flags.Bool("event-profile", false, "record canonical occurrence events for accepted transactions")
	agentProfile := flags.Bool("agent-profile", false, "enable official agent/v1 transitions")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate init-state DIR\nInitialize a verified core state store; DIR must already exist and be empty.")
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
	store, err := ate.Create(flags.Arg(0), ate.Options{AgentProfile: *agentProfile, EventProfile: *eventProfile})
	var snapshot *api.StateSnapshot
	var physical storage.RecoveryReport
	if err == nil {
		var current api.StateSnapshot
		current, err = store.Snapshot(context.Background())
		if err == nil {
			snapshot = &current
			var digest []byte
			digest, err = hex.DecodeString(string(current.Fence.CommitDigest))
			if err == nil && len(digest) != 32 {
				err = errors.New("invalid genesis fence digest")
			}
			if err == nil && current.Fence.LSN != 0 {
				err = errors.New("unexpected non-genesis initial state")
			}
			if err == nil {
				var position storage.Position
				copy(position.Digest[:], digest)
				// Create's trusted genesis has no transactions and a single
				// fixed-size segment header; no physical cursor is inferred.
				physical = storage.RecoveryReport{Verified: true, PublicationSynced: true, Publication: storage.Publication{Store: current.Fence.Store, Position: position, PublishedEnd: storage.SegmentHeaderSize, Profile: storage.FileSyncExperimental}, PublishedBytes: storage.SegmentHeaderSize, TailDisposition: "none"}
			}
		}
	}
	if store == nil {
		return finishStateCommand(nil, physical, snapshot, err, out, errOut)
	}
	return finishStateCommand(store, physical, snapshot, err, out, errOut)
}

func recoverState(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("recover-state", flag.ContinueOnError)
	flags.SetOutput(errOut)
	quarantine := flags.Bool("quarantine-tail", false, "preserve and sync unpublished tail evidence after semantic verification")
	agentProfile := flags.Bool("agent-profile", false, "enable semantic replay of the statically compiled agent/v1 profile")
	eventProfile := flags.Bool("event-profile", false, "verify canonical occurrence events and rebuild their streams")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate recover-state [--quarantine-tail] DIR\nVerify physical publication and replay core state before exposing a snapshot.")
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
	options := ate.Options{AgentProfile: *agentProfile, EventProfile: *eventProfile, Journal: storage.JournalOptions{TailPolicy: storage.TailReject}}
	if *quarantine {
		options.Journal.TailPolicy = storage.TailQuarantine
	}
	store, physical, err := ate.OpenExisting(flags.Arg(0), options)
	var snapshot *api.StateSnapshot
	if err == nil {
		var current api.StateSnapshot
		current, err = store.Snapshot(context.Background())
		if err == nil {
			snapshot = &current
		}
	}
	if store == nil {
		return finishStateCommand(nil, physical, snapshot, err, out, errOut)
	}
	return finishStateCommand(store, physical, snapshot, err, out, errOut)
}
