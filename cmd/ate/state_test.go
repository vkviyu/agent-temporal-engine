package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func decodeStateReport(t *testing.T, encoded []byte, semantic bool) stateReport {
	t.Helper()
	var report stateReport
	d := json.NewDecoder(bytes.NewReader(encoded))
	d.DisallowUnknownFields()
	if err := d.Decode(&report); err != nil {
		t.Fatalf("invalid state report %q: %v", encoded, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatalf("extra report output: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"format", "publication_verified", "publication_synced", "semantic_replay_verified", "store", "generation", "last_lsn", "commit_digest", "published_bytes", "profile", "scanned_transactions", "tail_bytes", "tail_disposition", "root_set", "roots"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing %s: %s", key, encoded)
		}
	}
	var roots map[string]json.RawMessage
	if err := json.Unmarshal(fields["roots"], &roots); err != nil {
		t.Fatal(err)
	}
	if len(roots) != 5 {
		t.Fatalf("invalid root fields: %s", fields["roots"])
	}
	for _, key := range []string{"domain_states", "execution_knowledge", "branch_refs", "live_control", "command_receipts"} {
		if _, ok := roots[key]; !ok {
			t.Fatalf("missing root %s", key)
		}
	}
	if report.Format != "ATE-CORE-STATE-EXPERIMENTAL-1" || !report.PublicationVerified || report.SemanticReplayVerified != semantic || report.Profile != string(storage.FileSyncExperimental) || len(report.Store) != 32 || !model.ValidDigest(report.CommitDigest) {
		t.Fatalf("invalid state evidence: %s", encoded)
	}
	return report
}

func initStateFixture(t *testing.T) (string, stateReport) {
	t.Helper()
	dir := t.TempDir()
	code, out, errOut := runJournalCommand(t, "init-state", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("init state: %d %s %s", code, out, errOut)
	}
	return dir, decodeStateReport(t, out, true)
}

func TestStateCommandUsage(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"init-state"}, 2}, {[]string{"init-state", "a", "b"}, 2}, {[]string{"init-state", "--unknown"}, 2}, {[]string{"init-state", "--quarantine-tail", "dir"}, 2}, {[]string{"init-state", "-h"}, 0}, {[]string{"init-state", "--help"}, 0},
		{[]string{"recover-state"}, 2}, {[]string{"recover-state", "--quarantine-tail"}, 2}, {[]string{"recover-state", "a", "b"}, 2}, {[]string{"recover-state", "--unknown", "dir"}, 2}, {[]string{"recover-state", "--quarantine-tail=bad", "dir"}, 2}, {[]string{"recover-state", "-h"}, 0}, {[]string{"recover-state", "--help"}, 0},
	} {
		code, out, errOut := runJournalCommand(t, tc.args...)
		if code != tc.code || len(out) != 0 || !strings.Contains(errOut, "usage:") {
			t.Fatalf("%v: %d %s %s", tc.args, code, out, errOut)
		}
	}
	if !strings.Contains(usage, "init-state") || !strings.Contains(usage, "recover-state") {
		t.Fatal("semantic commands missing")
	}
}

func TestStateCommandsVerifyGenesisAndPublicMutation(t *testing.T) {
	requireNativeJournal(t)
	dir, initial := initStateFixture(t)
	if !initial.PublicationSynced || initial.Generation != 0 || initial.LastLSN != 0 || initial.PublishedBytes != storage.SegmentHeaderSize || initial.RootSet != "" || initial.Roots != (stateRootsJSON{}) || initial.TailDisposition != "none" {
		t.Fatalf("invalid genesis: %#v", initial)
	}
	code, out, errOut := runJournalCommand(t, "recover-state", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("recover genesis: %d %s %s", code, out, errOut)
	}
	if recovered := decodeStateReport(t, out, true); !reflect.DeepEqual(recovered, initial) {
		t.Fatalf("genesis changed: %#v != %#v", recovered, initial)
	}
	store, _, err := ate.OpenExisting(dir, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := api.MutateRequest{Key: model.RequestKey{Space: "space", Principal: "host", RequestID: "write-1"}, Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte{0, 0xff}, Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("persist")}}}}}
	ack, err := store.Mutate(context.Background(), req)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatal(ack, err)
	}
	snapshot, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	code, out, errOut = runJournalCommand(t, "recover-state", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("recover state: %d %s %s", code, out, errOut)
	}
	report := decodeStateReport(t, out, true)
	if report.RootSet != string(snapshot.RootSet) || report.RootSet != string(ack.RootSet) || report.Roots.DomainStates != string(snapshot.Roots.DomainStates) || report.Roots.CommandReceipts != string(snapshot.Roots.CommandReceipts) || report.LastLSN != 1 || report.Generation != 1 || report.CommitDigest != string(ack.Fence.CommitDigest) || report.ScannedTransactions != 1 || report.PublishedBytes <= storage.SegmentHeaderSize {
		t.Fatalf("replay mismatch: %#v %#v", report, snapshot)
	}
	// Physical diagnostics retain their original meaning on the same store.
	code, out, errOut = runJournalCommand(t, "recover-journal", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("physical recover: %d %s %s", code, out, errOut)
	}
	physical := decodeJournalReport(t, out)
	if physical.SemanticReplayVerified || physical.LastLSN != 1 {
		t.Fatalf("physical command claimed semantic replay: %#v", physical)
	}
}

func TestStateRecoveryTailRequiresExplicitPolicy(t *testing.T) {
	requireNativeJournal(t)
	dir, _ := initStateFixture(t)
	segment := journalSegmentPath(t, dir)
	prefix := journalReadFile(t, segment)
	tail := []byte("unpublished state tail")
	journalAppendTail(t, dir, tail)
	code, out, errOut := runJournalCommand(t, "recover-state", dir)
	if code != 1 || !strings.Contains(errOut, storage.ErrRecoveryRequired.Error()) {
		t.Fatalf("default tail: %d %s %s", code, out, errOut)
	}
	report := decodeStateReport(t, out, false)
	if report.PublicationSynced || report.TailBytes != uint64(len(tail)) || report.TailDisposition != "preserved" {
		t.Fatalf("tail diagnostics: %#v", report)
	}
	if !bytes.Equal(journalReadFile(t, segment), append(bytes.Clone(prefix), tail...)) {
		t.Fatal("default semantic recovery truncated tail")
	}
	code, out, errOut = runJournalCommand(t, "recover-state", "--quarantine-tail", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("quarantine state: %d %s %s", code, out, errOut)
	}
	report = decodeStateReport(t, out, true)
	if !report.PublicationSynced || report.TailDisposition != "quarantined" || report.TailBytes != uint64(len(tail)) || report.QuarantineFile == "" {
		t.Fatal(report)
	}
	if !bytes.Equal(journalReadFile(t, segment), prefix) || !bytes.Equal(journalReadFile(t, filepath.Join(dir, filepath.FromSlash(report.QuarantineFile))), tail) {
		t.Fatal("quarantine altered evidence")
	}
}

func TestStateRecoveryRejectsPhysicallyValidSemanticCorruptionBeforeTailChange(t *testing.T) {
	requireNativeJournal(t)
	dir, _ := initStateFixture(t)
	// This helper publishes a fully chained physical transaction with invented
	// receipt/root digests and no registered semantic objects.
	publishJournalFixture(t, dir)
	tail := []byte("must survive semantic rejection")
	journalAppendTail(t, dir, tail)
	segment := journalSegmentPath(t, dir)
	before := journalReadFile(t, segment)
	current := journalReadFile(t, filepath.Join(dir, "CURRENT"))
	code, out, errOut := runJournalCommand(t, "recover-journal", dir)
	if code != 1 || !strings.Contains(errOut, storage.ErrRecoveryRequired.Error()) {
		t.Fatalf("physical fixture is invalid: %d %s %s", code, out, errOut)
	}
	if physical := decodeJournalReport(t, out); !physical.PublicationVerified || physical.ScannedTransactions != 1 {
		t.Fatal(physical)
	}
	code, out, errOut = runJournalCommand(t, "recover-state", "--quarantine-tail", dir)
	if code != 1 || errOut == "" || bytes.Contains(out, []byte(`"semantic_replay_verified":true`)) {
		t.Fatalf("semantic corruption accepted: %d %s %s", code, out, errOut)
	}
	if len(out) > 0 {
		decodeStateReport(t, out, false)
	}
	if !bytes.Equal(journalReadFile(t, segment), before) || !bytes.Equal(journalReadFile(t, filepath.Join(dir, "CURRENT")), current) {
		t.Fatal("semantic rejection modified publication or tail")
	}
	if _, err := os.Stat(filepath.Join(dir, "quarantine")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("semantic failure quarantined evidence: %v", err)
	}
}

func TestStateCommandsRejectInvalidPathsAndOwnership(t *testing.T) {
	requireNativeJournal(t)
	missing := filepath.Join(t.TempDir(), "missing")
	for _, command := range []string{"init-state", "recover-state"} {
		code, out, errOut := runJournalCommand(t, command, missing)
		if code != 1 || len(out) != 0 || errOut == "" {
			t.Fatalf("%s: %d %s %s", command, code, out, errOut)
		}
	}
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep")
	if err := os.WriteFile(keep, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := runJournalCommand(t, "init-state", dir)
	if code != 1 || len(out) != 0 || !strings.Contains(errOut, storage.ErrNotEmpty.Error()) {
		t.Fatalf("nonempty: %d %s %s", code, out, errOut)
	}
	if string(journalReadFile(t, keep)) != "keep" {
		t.Fatal("init altered an existing file")
	}
	dir, _ = initStateFixture(t)
	store, _, err := ate.OpenExisting(dir, ate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	code, out, errOut = runJournalCommand(t, "recover-state", dir)
	if code != 1 || len(out) != 0 || !strings.Contains(errOut, storage.ErrOwnerBusy.Error()) {
		t.Fatalf("owner competition: %d %s %s", code, out, errOut)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	code, out, errOut = runJournalCommand(t, "recover-state", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("owner leaked: %d %s %s", code, out, errOut)
	}
	decodeStateReport(t, out, true)
}

func TestStateCommandOutputFailureReleasesOwner(t *testing.T) {
	requireNativeJournal(t)
	dir := t.TempDir()
	var errOut bytes.Buffer
	if code := run([]string{"init-state", dir}, journalOutputError{}, &errOut); code != 1 || !strings.Contains(errOut.String(), "report output failed") {
		t.Fatalf("output failure: %d %s", code, &errOut)
	}
	code, out, errorsOut := runJournalCommand(t, "recover-state", dir)
	if code != 0 || errorsOut != "" {
		t.Fatalf("output failure leaked owner: %d %s %s", code, out, errorsOut)
	}
	decodeStateReport(t, out, true)
}

type stateCloseFault struct{ calls, failures int }

func (c *stateCloseFault) Close() error {
	c.calls++
	if c.calls <= c.failures {
		return errors.New("state close failed")
	}
	return nil
}

func TestStateCommandRetainsCloseErrorsAndNeverClaimsSuccess(t *testing.T) {
	physical := storage.RecoveryReport{Verified: true, PublicationSynced: true, Publication: storage.Publication{Store: "0123456789abcdef0123456789abcdef", Profile: storage.FileSyncExperimental, Position: storage.Position{Digest: storage.Digest{1}}}}
	snapshot := &api.StateSnapshot{}
	for _, failures := range []int{1, 2, 9} {
		t.Run(string(rune('0'+failures)), func(t *testing.T) {
			closer := &stateCloseFault{failures: failures}
			var out, errOut bytes.Buffer
			code := finishStateCommand(closer, physical, snapshot, nil, &out, &errOut)
			if code != 1 || !strings.Contains(errOut.String(), "state close failed") {
				t.Fatalf("close failure: %d %s %s", code, &out, &errOut)
			}
			decodeStateReport(t, out.Bytes(), false)
			want := failures + 1
			if want > 3 {
				want = 3
			}
			if closer.calls != want {
				t.Fatalf("Close called %d times; want %d", closer.calls, want)
			}
		})
	}
}

func TestStateCommandFinishesConstructorCleanup(t *testing.T) {
	// CloseOnError first attempts closure. The resulting public CleanupError
	// retains the child and can be discovered through an outer joined error.
	closer := &stateCloseFault{failures: 2}
	cause := errors.New("constructor failed")
	pending := storage.CloseOnError(cause, closer)
	var cleanup *storage.CleanupError
	if !errors.As(pending, &cleanup) {
		t.Fatal("fixture lost cleanup responsibility")
	}
	var out, errOut bytes.Buffer
	if code := finishStateCommand(nil, storage.RecoveryReport{}, nil, errors.Join(errors.New("outer error"), pending), &out, &errOut); code != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), cause.Error()) {
		t.Fatalf("constructor cleanup: %d %s %s", code, &out, &errOut)
	}
	if closer.calls != 3 {
		t.Fatalf("constructor resource not retired: %d", closer.calls)
	}
	if err := cleanup.Close(); err != nil || closer.calls != 3 {
		t.Fatalf("cleanup was still pending: %d %v", closer.calls, err)
	}
}
