package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vkviyu/agent-temporal-engine/storage"
)

// journalReport describes physical publication evidence only. The command does
// not run the semantic engine or verify domain roots, authorization or receipts.
type journalReport struct {
	Format                 string   `json:"format"`
	PublicationVerified    bool     `json:"publication_verified"`
	PublicationSynced      bool     `json:"publication_synced"`
	SemanticReplayVerified bool     `json:"semantic_replay_verified"`
	Store                  string   `json:"store"`
	Generation             uint64   `json:"generation"`
	LastLSN                uint64   `json:"last_lsn"`
	CommitDigest           string   `json:"commit_digest"`
	PublishedBytes         uint64   `json:"published_bytes"`
	Profile                string   `json:"profile"`
	ScannedTransactions    uint64   `json:"scanned_transactions"`
	TailBytes              uint64   `json:"tail_bytes"`
	TailDisposition        string   `json:"tail_disposition"`
	QuarantineFile         string   `json:"quarantine_file,omitempty"`
	OrphanSegments         []string `json:"orphan_segments,omitempty"`
	QuarantineFiles        []string `json:"quarantine_files,omitempty"`
}

func journalJSON(report storage.RecoveryReport) journalReport {
	p := report.Publication
	return journalReport{
		Format: "ATE-STORE-EXPERIMENTAL-1", PublicationVerified: report.Verified, PublicationSynced: report.PublicationSynced,
		SemanticReplayVerified: false, Store: string(p.Store), Generation: p.Generation, LastLSN: p.Position.LSN,
		CommitDigest: p.Position.Digest.String(), PublishedBytes: report.PublishedBytes, Profile: string(p.Profile),
		ScannedTransactions: report.ScannedTransactions, TailBytes: report.TailBytes, TailDisposition: report.TailDisposition,
		QuarantineFile: report.QuarantineFile, OrphanSegments: report.OrphanSegments, QuarantineFiles: report.QuarantineFiles,
	}
}

// finishJournalCommand runs dependent cleanup in ownership order before output.
// Native file and owner handles retire on their first Close, including an error;
// three attempts let Journal confirm both resources closed after one error each.
// Failed constructors retain the same responsibility through CleanupError.
// Retain every error even when a retry finishes. If closure remains uncertain,
// keep DirectoryIO alive rather than closing the parent ahead of its children.
func finishJournalCommand(d *storage.DirectoryIO, j *storage.Journal, report storage.RecoveryReport, commandErr error, out, errOut io.Writer) int {
	var pending io.Closer
	if j != nil {
		pending = j
	} else {
		if cleanup, ok := errors.AsType[*storage.CleanupError](commandErr); ok {
			pending = cleanup
		}
	}
	resourcesClosed := pending == nil
	if pending != nil {
		for attempt := 0; attempt < 3; attempt++ {
			closeErr := pending.Close()
			if closeErr == nil {
				resourcesClosed = true
				break
			}
			commandErr = errors.Join(commandErr, closeErr)
		}
	}
	if resourcesClosed {
		commandErr = errors.Join(commandErr, d.Close())
	}
	if report.Verified {
		commandErr = errors.Join(commandErr, json.NewEncoder(out).Encode(journalJSON(report)))
	}
	if commandErr != nil {
		fmt.Fprintln(errOut, commandErr)
		return 1
	}
	return 0
}

func initJournal(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("init-journal", flag.ContinueOnError)
	flags.SetOutput(errOut)
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate init-journal DIR\nInitialize only a physical journal; DIR must already exist and be empty.")
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
	d, err := storage.OpenDirectory(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	j, err := storage.CreateJournal(d, storage.JournalOptions{})
	var report storage.RecoveryReport
	if err == nil {
		var p storage.Publication
		p, err = j.Published()
		if err == nil {
			report = storage.RecoveryReport{Verified: true, PublicationSynced: true, Publication: p, PublishedBytes: p.PublishedEnd, TailDisposition: "none"}
		}
	}
	return finishJournalCommand(d, j, report, err, out, errOut)
}

func recoverJournal(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("recover-journal", flag.ContinueOnError)
	flags.SetOutput(errOut)
	quarantine := flags.Bool("quarantine-tail", false, "preserve and sync unpublished tail evidence before truncating it")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "usage: ate recover-journal [--quarantine-tail] DIR\nVerify physical publication only; semantic replay is not performed.")
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
	d, err := storage.OpenDirectory(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	options := storage.JournalOptions{TailPolicy: storage.TailReject}
	if *quarantine {
		options.TailPolicy = storage.TailQuarantine
	}
	j, report, err := storage.OpenJournal(d, options)
	return finishJournalCommand(d, j, report, err, out, errOut)
}
