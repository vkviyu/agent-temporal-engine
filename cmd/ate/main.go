package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const version = "0.1.0-dev.m6"

const usage = `usage: ate <command>
commands:
  version          show the development version
  check-journal    check an experimental raw journal stream (not a store)
  init-journal     initialize a physical journal in an existing empty directory
  recover-journal  verify a physical journal; optionally quarantine its tail
  init-state       initialize a verified core state store
  recover-state    verify physical publication and replay core state
  compact-state    verify and rewrite a store while retaining all audit history
  serve            serve an existing store over authenticated local IPC
  help             show this help
`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return 0
	}
	switch args[0] {
	case "version":
		if len(args) != 1 {
			fmt.Fprintln(errOut, "version takes no arguments")
			return 2
		}
		fmt.Fprintln(out, "agent-temporal-engine", version)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(out, usage)
		return 0
	case "check-journal":
		return checkJournal(args[1:], out, errOut)
	case "init-journal":
		return initJournal(args[1:], out, errOut)
	case "recover-journal":
		return recoverJournal(args[1:], out, errOut)
	case "init-state":
		return initState(args[1:], out, errOut)
	case "serve":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return serve(ctx, args[1:], out, errOut)
	case "recover-state":
		return recoverState(args[1:], out, errOut)
	case "compact-state":
		return compactState(args[1:], out, errOut)
	default:
		fmt.Fprintf(errOut, "unknown command %q\n%s", args[0], usage)
		return 2
	}
}

func checkJournal(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("check-journal", flag.ContinueOnError)
	flags.SetOutput(errOut)
	previous := flags.String("previous-digest", "", "required starting commit/genesis SHA-256 in lowercase hex")
	lsn := flags.Uint64("previous-lsn", 0, "LSN immediately before this stream")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate check-journal --previous-digest HEX [--previous-lsn N] FILE")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 || !model.ValidDigest(*previous) {
		flags.Usage()
		return 2
	}
	var position storage.Position
	b, _ := hex.DecodeString(*previous)
	copy(position.Digest[:], b)
	position.LSN = *lsn
	file, err := os.Open(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	defer file.Close()
	scanner, err := storage.NewScanner(file, position, storage.Limits{})
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	count := uint64(0)
	for {
		_, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintln(errOut, err)
			return 1
		}
		count++
	}
	result := struct {
		Format              string `json:"format"`
		Transactions        uint64 `json:"transactions"`
		Bytes               int64  `json:"bytes"`
		LastLSN             uint64 `json:"last_lsn"`
		CommitDigest        string `json:"commit_digest"`
		PublicationVerified bool   `json:"publication_verified"`
	}{"ATE-JOURNAL-EXPERIMENTAL-1", count, scanner.LastCompleteOffset(), scanner.Position().LSN, scanner.Position().Digest.String(), false}
	if err := json.NewEncoder(out).Encode(result); err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}
