package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func requireNativeJournal(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native journal directory requires Darwin or Linux")
	}
}

func runJournalCommand(t *testing.T, args ...string) (int, []byte, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return code, out.Bytes(), errOut.String()
}

func decodeJournalReport(t *testing.T, b []byte) journalReport {
	t.Helper()
	var got journalReport
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&got); err != nil {
		t.Fatalf("invalid report %q: %v", b, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatalf("unexpected data after report: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"format", "publication_verified", "publication_synced", "semantic_replay_verified", "store", "generation", "last_lsn", "commit_digest", "published_bytes", "profile", "scanned_transactions", "tail_bytes", "tail_disposition"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing %s in %s", key, b)
		}
	}
	if got.Format != "ATE-STORE-EXPERIMENTAL-1" || !got.PublicationVerified || got.SemanticReplayVerified || got.Profile != string(storage.FileSyncExperimental) || len(got.Store) != 32 || !model.ValidDigest(got.CommitDigest) {
		t.Fatalf("invalid physical evidence: %s", b)
	}
	return got
}

func initJournalFixture(t *testing.T) (string, journalReport) {
	t.Helper()
	dir := t.TempDir()
	code, out, errOut := runJournalCommand(t, "init-journal", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("init: %d %s %s", code, out, errOut)
	}
	return dir, decodeJournalReport(t, out)
}

func journalSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "segments", "SEGMENT.*"))
	if err != nil || len(names) != 1 {
		t.Fatalf("segment paths: %v %v", names, err)
	}
	return names[0]
}

func journalReadFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func journalAppendTail(t *testing.T, dir string, tail []byte) {
	t.Helper()
	f, err := os.OpenFile(journalSegmentPath(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, writeErr := f.Write(tail)
	if n != len(tail) && writeErr == nil {
		writeErr = io.ErrShortWrite
	}
	syncErr := f.Sync()
	err = errors.Join(writeErr, syncErr, f.Close())
	if err != nil {
		t.Fatal(err)
	}
}

func TestJournalCommandUsage(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"init-journal"}, 2}, {[]string{"init-journal", "one", "two"}, 2},
		{[]string{"init-journal", "--unknown"}, 2}, {[]string{"init-journal", "--quarantine-tail", "dir"}, 2},
		{[]string{"init-journal", "-h"}, 0}, {[]string{"init-journal", "--help"}, 0},
		{[]string{"recover-journal"}, 2}, {[]string{"recover-journal", "--quarantine-tail"}, 2},
		{[]string{"recover-journal", "one", "two"}, 2}, {[]string{"recover-journal", "--unknown", "dir"}, 2},
		{[]string{"recover-journal", "--quarantine-tail=invalid", "dir"}, 2},
		{[]string{"recover-journal", "-h"}, 0}, {[]string{"recover-journal", "--help"}, 0},
	} {
		code, out, errOut := runJournalCommand(t, tc.args...)
		if code != tc.code || len(out) != 0 || !strings.Contains(errOut, "usage:") {
			t.Fatalf("%v: %d %s %s", tc.args, code, out, errOut)
		}
	}
	code, out, errOut := runJournalCommand(t, "version")
	if code != 0 || string(out) != "agent-temporal-engine "+version+"\n" || errOut != "" {
		t.Fatalf("version: %d %s %s", code, out, errOut)
	}
}

func TestInitAndRecoverPhysicalJournal(t *testing.T) {
	requireNativeJournal(t)
	dir, initial := initJournalFixture(t)
	if !initial.PublicationSynced || initial.Generation != 0 || initial.LastLSN != 0 || initial.PublishedBytes != storage.SegmentHeaderSize || initial.ScannedTransactions != 0 || initial.TailBytes != 0 || initial.TailDisposition != "none" || initial.QuarantineFile != "" {
		t.Fatalf("genesis: %#v", initial)
	}
	code, out, errOut := runJournalCommand(t, "recover-journal", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("recover: %d %s %s", code, out, errOut)
	}
	if got := decodeJournalReport(t, out); !reflect.DeepEqual(got, initial) {
		t.Fatalf("recovery differs: %#v vs %#v", got, initial)
	}
	if bytes.Contains(out, []byte("quarantine_file")) {
		t.Fatalf("empty optional path emitted: %s", out)
	}
	// Commands release ownership before returning; reopening needs no restart.
	d, err := storage.OpenDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	j, report, err := storage.OpenJournal(d, storage.JournalOptions{})
	if err != nil || !report.Verified || !report.PublicationSynced {
		t.Fatalf("public reopen: %#v %v", report, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitJournalRequiresExistingEmptyDirectory(t *testing.T) {
	requireNativeJournal(t)
	dir := t.TempDir()
	name := filepath.Join(dir, "keep")
	if err := os.WriteFile(name, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := runJournalCommand(t, "init-journal", dir)
	if code != 1 || len(out) != 0 || !strings.Contains(errOut, storage.ErrNotEmpty.Error()) {
		t.Fatalf("nonempty: %d %s %s", code, out, errOut)
	}
	if string(journalReadFile(t, name)) != "preserve" {
		t.Fatal("existing data changed")
	}
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nonempty initialized: %v", err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	code, out, errOut = runJournalCommand(t, "init-journal", missing)
	if code != 1 || len(out) != 0 || errOut == "" {
		t.Fatalf("missing: %d %s %s", code, out, errOut)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created directory implicitly: %v", err)
	}
}

func TestRecoverJournalRequiresCurrentAndRejectsCorruption(t *testing.T) {
	requireNativeJournal(t)
	for _, mode := range []string{"missing-current", "corrupt-current", "unknown-flags"} {
		t.Run(mode, func(t *testing.T) {
			dir, _ := initJournalFixture(t)
			currentPath := filepath.Join(dir, "CURRENT")
			current := journalReadFile(t, currentPath)
			switch mode {
			case "missing-current":
				// A valid orphan manifest and CURRENT copy must never be a fallback.
				if err := os.Rename(currentPath, filepath.Join(dir, ".CURRENT.saved")); err != nil {
					t.Fatal(err)
				}
			case "corrupt-current":
				current[len(current)-1] ^= 1
				if err := os.WriteFile(currentPath, current, 0o600); err != nil {
					t.Fatal(err)
				}
			case "unknown-flags":
				current[9] = 1
				sum := sha256.Sum256(current[:len(current)-32])
				copy(current[len(current)-32:], sum[:])
				if err := os.WriteFile(currentPath, current, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			tail := []byte("never truncate an unverified store")
			journalAppendTail(t, dir, tail)
			before := journalReadFile(t, journalSegmentPath(t, dir))
			code, out, errOut := runJournalCommand(t, "recover-journal", "--quarantine-tail", dir)
			if code != 1 || len(out) != 0 || errOut == "" {
				t.Fatalf("%s: %d %s %s", mode, code, out, errOut)
			}
			if mode == "unknown-flags" && !strings.Contains(errOut, storage.ErrUnsupported.Error()) {
				t.Fatalf("flags classification: %s", errOut)
			}
			if !bytes.Equal(journalReadFile(t, journalSegmentPath(t, dir)), before) {
				t.Fatal("unverified segment changed")
			}
			if _, err := os.Stat(filepath.Join(dir, "quarantine")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unverified recovery mutated layout: %v", err)
			}
		})
	}
}

func publishJournalFixture(t *testing.T, dir string) storage.Publication {
	t.Helper()
	d, err := storage.OpenDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	j, _, err := storage.OpenJournal(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := j.Close(); err != nil {
			t.Error(err)
		}
	}()
	p, err := j.Published()
	if err != nil {
		t.Fatal(err)
	}
	tx := storage.Transaction{
		TxID: "11111111111111111111111111111111", Request: model.RequestKey{Space: "space", Principal: "principal", RequestID: "request"},
		RequestDigest: storage.Digest(sha256.Sum256([]byte("request"))), LSN: p.Position.LSN + 1, PreviousDigest: p.Position.Digest,
		Receipt: storage.Digest(sha256.Sum256([]byte("receipt"))), PostRootSet: storage.Digest(sha256.Sum256([]byte("root"))),
	}
	p, err = j.Publish(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRecoverJournalTailRequiresExplicitQuarantine(t *testing.T) {
	requireNativeJournal(t)
	dir, _ := initJournalFixture(t)
	p := publishJournalFixture(t, dir)
	segment := journalSegmentPath(t, dir)
	prefix := journalReadFile(t, segment)
	current := journalReadFile(t, filepath.Join(dir, "CURRENT"))
	tail := []byte{0, 0xff, 'A', 'T', 'E', 'F', 'R', 'M', '0', '1', 1, 0, 0, 0, 0}
	journalAppendTail(t, dir, tail)
	code, out, errOut := runJournalCommand(t, "recover-journal", dir)
	if code != 1 || !strings.Contains(errOut, storage.ErrRecoveryRequired.Error()) {
		t.Fatalf("default tail recovery: %d %s %s", code, out, errOut)
	}
	report := decodeJournalReport(t, out)
	if report.PublicationSynced || report.TailBytes != uint64(len(tail)) || report.TailDisposition != "preserved" || report.QuarantineFile != "" || report.ScannedTransactions != 1 || report.LastLSN != 1 || report.CommitDigest != p.Position.Digest.String() {
		t.Fatalf("tail diagnostic: %#v", report)
	}
	if !bytes.Equal(journalReadFile(t, segment), append(bytes.Clone(prefix), tail...)) {
		t.Fatal("default recovery truncated tail")
	}
	code, out, errOut = runJournalCommand(t, "recover-journal", "--quarantine-tail", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("quarantine: %d %s %s", code, out, errOut)
	}
	report = decodeJournalReport(t, out)
	if !report.PublicationSynced || report.TailBytes != uint64(len(tail)) || report.TailDisposition != "quarantined" || !strings.HasPrefix(report.QuarantineFile, "quarantine/TAIL.") || report.PublishedBytes != p.PublishedEnd || report.LastLSN != p.Position.LSN || report.Generation != p.Generation || report.ScannedTransactions != 1 {
		t.Fatalf("quarantine report: %#v", report)
	}
	if !bytes.Equal(journalReadFile(t, filepath.Join(dir, filepath.FromSlash(report.QuarantineFile))), tail) {
		t.Fatal("quarantine evidence differs")
	}
	if !bytes.Equal(journalReadFile(t, segment), prefix) {
		t.Fatal("published prefix changed")
	}
	if !bytes.Equal(journalReadFile(t, filepath.Join(dir, "CURRENT")), current) {
		t.Fatal("recovery changed CURRENT")
	}
	code, out, errOut = runJournalCommand(t, "recover-journal", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("post-quarantine: %d %s %s", code, out, errOut)
	}
	if report = decodeJournalReport(t, out); report.TailBytes != 0 || report.TailDisposition != "none" || report.LastLSN != p.Position.LSN || !report.PublicationSynced {
		t.Fatalf("post-quarantine report: %#v", report)
	}
}

type journalOutputError struct{}

func (journalOutputError) Write([]byte) (int, error) {
	return 0, errors.New("report output failed")
}

func TestJournalReportOutputFailureReturnsErrorAndReleasesOwner(t *testing.T) {
	requireNativeJournal(t)
	dir := t.TempDir()
	var errOut bytes.Buffer
	if code := run([]string{"init-journal", dir}, journalOutputError{}, &errOut); code != 1 || !strings.Contains(errOut.String(), "report output failed") {
		t.Fatalf("output failure: %d %s", code, &errOut)
	}
	code, out, errorsOut := runJournalCommand(t, "recover-journal", dir)
	if code != 0 || errorsOut != "" {
		t.Fatalf("owner leaked: %d %s %s", code, out, errorsOut)
	}
	decodeJournalReport(t, out)
}

// These wrappers use only public storage interfaces. They model a Close error
// either before retiring the handle or after the native Close already retired it.
type journalCloseFault struct {
	base                 *storage.DirectoryIO
	label                string
	remaining            int
	before               bool
	close                func() error
	retired              bool
	calls                int
	directoryClosedEarly bool
}

func (f *journalCloseFault) Close() error {
	f.calls++
	if _, err := f.base.List("."); err != nil {
		f.directoryClosedEarly = true
		return errors.Join(errors.New("directory closed before "+f.label), err)
	}
	if f.retired {
		return nil
	}
	if f.remaining > 0 {
		f.remaining--
		injected := errors.New(f.label + " close failed")
		if f.before {
			return injected
		}
		f.retired = true
		return errors.Join(f.close(), injected)
	}
	f.retired = true
	return f.close()
}

type journalCloseFaultFile struct {
	storage.DurableFile
	fault   *journalCloseFault
	syncErr error
}

func (f *journalCloseFaultFile) Close() error { return f.fault.Close() }

func (f *journalCloseFaultFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.DurableFile.Sync()
}

type journalCloseFaultIO struct {
	storage.DurableIO
	segment        *journalCloseFault
	owner          *journalCloseFault
	segmentSyncErr error
}

func (d *journalCloseFaultIO) CreateExclusive(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.CreateExclusive(name)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(name, "segments/SEGMENT.") {
		d.segment.close = f.Close
		return &journalCloseFaultFile{DurableFile: f, fault: d.segment, syncErr: d.segmentSyncErr}, nil
	}
	return f, nil
}

func (d *journalCloseFaultIO) AcquireOwnerLock() (storage.OwnerLock, error) {
	owner, err := d.DurableIO.AcquireOwnerLock()
	if err != nil {
		return nil, err
	}
	d.owner.close = owner.Close
	return d.owner, nil
}

func closeFaultJournal(t *testing.T, dir string, segmentErrors, ownerErrors int, before bool) (*storage.DirectoryIO, *storage.Journal, storage.RecoveryReport, *journalCloseFaultIO) {
	t.Helper()
	base, err := storage.OpenDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &journalCloseFaultIO{
		DurableIO: base,
		segment:   &journalCloseFault{base: base, label: "segment", remaining: segmentErrors, before: before},
		owner:     &journalCloseFault{base: base, label: "owner", remaining: ownerErrors, before: before},
	}
	j, err := storage.CreateJournal(wrapped, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		wrapped.segment.remaining = 0
		wrapped.owner.remaining = 0
		if err := j.Close(); err != nil {
			t.Error(err)
		}
		if err := base.Close(); err != nil {
			t.Error(err)
		}
	})
	p, err := j.Published()
	if err != nil {
		t.Fatal(err)
	}
	report := storage.RecoveryReport{Verified: true, PublicationSynced: true, Publication: p, PublishedBytes: p.PublishedEnd, TailDisposition: "none"}
	return base, j, report, wrapped
}

func TestJournalCommandRetriesCloseBeforeDirectory(t *testing.T) {
	requireNativeJournal(t)
	for _, tc := range []struct {
		name                       string
		segmentErrors, ownerErrors int
		segmentCalls, ownerCalls   int
		before                     bool
	}{
		{"segment-before", 1, 0, 2, 1, true},
		{"owner-after", 0, 1, 1, 2, false},
		{"both-after", 1, 1, 2, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base, j, report, wrapped := closeFaultJournal(t, dir, tc.segmentErrors, tc.ownerErrors, tc.before)
			var out, errOut bytes.Buffer
			if code := finishJournalCommand(base, j, report, nil, &out, &errOut); code != 1 {
				t.Fatalf("cleanup error hidden: %d %s %s", code, &out, &errOut)
			}
			if tc.segmentErrors > 0 && !strings.Contains(errOut.String(), "segment close failed") {
				t.Fatalf("segment error lost: %s", &errOut)
			}
			if tc.ownerErrors > 0 && !strings.Contains(errOut.String(), "owner close failed") {
				t.Fatalf("owner error lost: %s", &errOut)
			}
			if wrapped.segment.calls != tc.segmentCalls || wrapped.owner.calls != tc.ownerCalls || wrapped.segment.directoryClosedEarly || wrapped.owner.directoryClosedEarly {
				t.Fatalf("wrong closure order/count: segment=%+v owner=%+v", wrapped.segment, wrapped.owner)
			}
			if _, err := base.List("."); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("directory not closed after successful journal retry: %v", err)
			}
			decodeJournalReport(t, out.Bytes())
			reopened, err := storage.OpenDirectory(dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered, _, err := storage.OpenJournal(reopened, storage.JournalOptions{})
			if err != nil {
				t.Fatalf("owner not released: %v", err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJournalCommandKeepsDirectoryWhenCloseStillFails(t *testing.T) {
	requireNativeJournal(t)
	dir := t.TempDir()
	base, j, report, wrapped := closeFaultJournal(t, dir, 4, 0, true)
	var out, errOut bytes.Buffer
	if code := finishJournalCommand(base, j, report, nil, &out, &errOut); code != 1 || strings.Count(errOut.String(), "segment close failed") != 3 {
		t.Fatalf("bounded cleanup diagnostics: %d %s %s", code, &out, &errOut)
	}
	if wrapped.segment.calls != 3 || wrapped.owner.calls != 0 {
		t.Fatalf("unbounded or out-of-order cleanup: segment=%d owner=%d", wrapped.segment.calls, wrapped.owner.calls)
	}
	if _, err := base.List("."); err != nil {
		t.Fatalf("directory closed with live journal: %v", err)
	}
	if _, err := base.AcquireOwnerLock(); !errors.Is(err, storage.ErrOwnerBusy) {
		t.Fatalf("ownership released before segment closure: %v", err)
	}
	decodeJournalReport(t, out.Bytes())
}

func TestJournalCommandDrainsFailedConstructorCleanup(t *testing.T) {
	requireNativeJournal(t)
	for _, tc := range []struct {
		name          string
		segmentErrors int
		stillPending  bool
	}{
		{"retries-complete", 1, false},
		{"keeps-directory-for-pending", 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base, err := storage.OpenDirectory(dir)
			if err != nil {
				t.Fatal(err)
			}
			syncErr := errors.New("constructor segment sync failed")
			wrapped := &journalCloseFaultIO{
				DurableIO:      base,
				segment:        &journalCloseFault{base: base, label: "segment", remaining: tc.segmentErrors, before: true},
				owner:          &journalCloseFault{base: base, label: "owner", remaining: 1, before: true},
				segmentSyncErr: syncErr,
			}
			j, constructorErr := storage.CreateJournal(wrapped, storage.JournalOptions{})
			var pending *storage.CleanupError
			if j != nil || !errors.Is(constructorErr, syncErr) || !errors.As(constructorErr, &pending) {
				t.Fatalf("missing constructor cleanup handle: j=%v err=%v", j, constructorErr)
			}
			t.Cleanup(func() {
				wrapped.segment.remaining = 0
				wrapped.owner.remaining = 0
				if err := pending.Close(); err != nil {
					t.Error(err)
				}
				if err := base.Close(); err != nil {
					t.Error(err)
				}
			})
			var out, errOut bytes.Buffer
			if code := finishJournalCommand(base, j, storage.RecoveryReport{}, constructorErr, &out, &errOut); code != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), syncErr.Error()) {
				t.Fatalf("constructor failure hidden: %d %s %s", code, &out, &errOut)
			}
			if wrapped.segment.directoryClosedEarly || wrapped.owner.directoryClosedEarly {
				t.Fatal("directory closed before retained constructor resources")
			}
			if !strings.Contains(errOut.String(), "segment close failed") {
				t.Fatalf("constructor close diagnostic lost: %s", &errOut)
			}
			if tc.stillPending {
				// One constructor attempt plus three command attempts leave the
				// owner behind the segment, and the root must remain usable.
				if wrapped.segment.calls != 4 || wrapped.owner.calls != 0 {
					t.Fatalf("unbounded or reordered constructor cleanup: segment=%d owner=%d", wrapped.segment.calls, wrapped.owner.calls)
				}
				if _, err := base.List("."); err != nil {
					t.Fatalf("pending cleanup lost its directory: %v", err)
				}
				if _, err := base.AcquireOwnerLock(); !errors.Is(err, storage.ErrOwnerBusy) {
					t.Fatalf("pending constructor ownership released: %v", err)
				}
				wrapped.segment.remaining = 0
				wrapped.owner.remaining = 0
				if err := pending.Close(); err != nil {
					t.Fatalf("cannot resume cleanup after command failure: %v", err)
				}
				owner, err := base.AcquireOwnerLock()
				if err != nil {
					t.Fatalf("owner not released by resumed cleanup: %v", err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if wrapped.segment.calls != 2 || wrapped.owner.calls != 2 || !strings.Contains(errOut.String(), "owner close failed") {
					t.Fatalf("constructor cleanup did not drain with diagnostics: segment=%d owner=%d stderr=%s", wrapped.segment.calls, wrapped.owner.calls, &errOut)
				}
				if _, err := base.List("."); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("directory not closed after retained cleanup completed: %v", err)
				}
				reopened, err := storage.OpenDirectory(dir)
				if err != nil {
					t.Fatal(err)
				}
				owner, err := reopened.AcquireOwnerLock()
				if err != nil {
					t.Fatalf("failed constructor leaked its owner: %v", err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// A failed successor create may leave an empty managed file. Its zero length
// must not hide the recovery requirement or the evidence path in CLI output.
func TestRecoverJournalReportsAndQuarantinesZeroByteOrphan(t *testing.T) {
	requireNativeJournal(t)
	dir, initial := initJournalFixture(t)
	name := "SEGMENT.ffffffffffffffffffffffffffffffff"
	path := filepath.Join(dir, "segments", name)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := runJournalCommand(t, "recover-journal", dir)
	if code != 1 || errOut == "" {
		t.Fatalf("unpublished empty orphan accepted: %d %s %s", code, out, errOut)
	}
	report := decodeJournalReport(t, out)
	if !report.PublicationVerified || report.PublicationSynced || report.Generation != initial.Generation || report.LastLSN != 0 || report.TailBytes != 0 || report.TailDisposition != "preserved" || !reflect.DeepEqual(report.OrphanSegments, []string{"segments/" + name}) || len(report.QuarantineFiles) != 0 {
		t.Fatalf("missing orphan diagnostics: %+v", report)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("default recovery removed orphan: %v", err)
	}
	code, out, errOut = runJournalCommand(t, "recover-journal", "--quarantine-tail", dir)
	if code != 0 || errOut != "" {
		t.Fatalf("explicit repair: %d %s %s", code, out, errOut)
	}
	report = decodeJournalReport(t, out)
	if !report.PublicationSynced || report.Generation != initial.Generation || report.LastLSN != 0 || report.TailDisposition != "quarantined" || report.QuarantineFile != "" || len(report.OrphanSegments) != 1 || len(report.QuarantineFiles) != 1 {
		t.Fatalf("missing repair evidence: %+v", report)
	}
	evidence, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(report.QuarantineFiles[0])))
	if err != nil || len(evidence) != 0 {
		t.Fatalf("empty orphan evidence: %x %v", evidence, err)
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan not removed after preservation: %v", err)
	}
}
