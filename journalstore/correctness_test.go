package journalstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var errInjected = errors.New("injected journalstore boundary failure")

// tracedIO logs only persistent mutations. Acquiring/closing handles is allowed
// on rejected recovery, while sync/truncate/namespace edits are not.
type tracedIO struct {
	storage.DurableIO
	mutations []string
	failClose string
	failWrite string
}

func (d *tracedIO) CreateExclusive(name string) (storage.DurableFile, error) {
	d.mutations = append(d.mutations, "create "+name)
	f, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(f, name), err
}
func (d *tracedIO) OpenAppend(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.OpenAppend(name)
	return d.wrap(f, name), err
}
func (d *tracedIO) Mkdir(name string) error {
	d.mutations = append(d.mutations, "mkdir "+name)
	return d.DurableIO.Mkdir(name)
}
func (d *tracedIO) SyncDir(name string) error {
	d.mutations = append(d.mutations, "syncdir "+name)
	return d.DurableIO.SyncDir(name)
}
func (d *tracedIO) Remove(name string) error {
	d.mutations = append(d.mutations, "remove "+name)
	return d.DurableIO.Remove(name)
}
func (d *tracedIO) ReplaceAtomic(from, to string) error {
	d.mutations = append(d.mutations, "replace "+to)
	return d.DurableIO.ReplaceAtomic(from, to)
}
func (d *tracedIO) InstallNew(from, to string) error {
	d.mutations = append(d.mutations, "install "+to)
	return d.DurableIO.InstallNew(from, to)
}
func (d *tracedIO) wrap(f storage.DurableFile, name string) storage.DurableFile {
	if f == nil {
		return nil
	}
	return &tracedFile{DurableFile: f, owner: d, name: name}
}

type tracedFile struct {
	storage.DurableFile
	owner *tracedIO
	name  string
}

func (f *tracedFile) Write(b []byte) (int, error) {
	f.owner.mutations = append(f.owner.mutations, "write "+f.name)
	n, err := f.DurableFile.Write(b)
	if f.owner.failWrite == f.name {
		f.owner.failWrite = ""
		return n, errors.Join(err, errInjected)
	}
	return n, err
}
func (f *tracedFile) Sync() error {
	f.owner.mutations = append(f.owner.mutations, "sync "+f.name)
	return f.DurableFile.Sync()
}
func (f *tracedFile) Truncate(n int64) error {
	f.owner.mutations = append(f.owner.mutations, "truncate "+f.name)
	return f.DurableFile.Truncate(n)
}
func (f *tracedFile) Close() error {
	if f.owner.failClose == f.name {
		f.owner.failClose = ""
		return errInjected
	}
	return f.DurableFile.Close()
}

func nativeIO(t *testing.T) (string, *storage.DirectoryIO, *tracedIO) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native IO requires Darwin/Linux")
	}
	path := t.TempDir()
	d, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return path, d, &tracedIO{DurableIO: d}
}
func testRecord(id string) Record {
	return Record{Kind: storage.KindEvent, Payload: []byte("payload/" + id), Request: model.RequestKey{Space: "s", Principal: "p", RequestID: model.RequestID(id)}}
}
func activeSegment(t *testing.T, path string) string {
	t.Helper()
	files, err := os.ReadDir(filepath.Join(path, "segments"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatal("fixture expects one segment")
	}
	return "segments/" + files[0].Name()
}
func zeroResult(t *testing.T, r storage.PublishedTransaction) {
	t.Helper()
	if !reflect.DeepEqual(r, storage.PublishedTransaction{}) {
		t.Fatalf("failure exposed publication: %+v", r)
	}
}

func TestRequestReceiptIsExactBeforeAndAfterReopen(t *testing.T) {
	_, _, d := nativeIO(t)
	applied := 0
	s, err := Create(d, storage.JournalOptions{}, func(r Record) error { applied++; r.Payload[0] = 'X'; return nil })
	if err != nil {
		t.Fatal(err)
	}
	r := testRecord("one")
	ack, err := s.Append(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.Payload[0] = 'Y'
	retry := testRecord("one")
	again, err := s.Append(context.Background(), retry)
	if err != nil || !reflect.DeepEqual(again, ack) || applied != 1 {
		t.Fatalf("retry=%+v err=%v applies=%d", again, err, applied)
	}
	got := s.Records()
	if len(got) != 1 || !bytes.Equal(got[0].Payload, retry.Payload) {
		t.Fatalf("callback or caller changed accepted input: %+v", got)
	}
	d.mutations = nil
	for _, changed := range []Record{{Kind: storage.KindRevision, Payload: retry.Payload, Request: retry.Request}, {Kind: retry.Kind, Payload: []byte("changed"), Request: retry.Request}} {
		out, err := s.Append(context.Background(), changed)
		zeroResult(t, out)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatal(err)
		}
	}
	if len(d.mutations) != 0 {
		t.Fatalf("idempotency conflict wrote: %v", d.mutations)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := Open(d, storage.JournalOptions{}, nil)
	if err != nil || report.ScannedTransactions != 1 {
		t.Fatalf("reopen: %+v %v", report, err)
	}
	defer reopened.Close()
	found, ok, err := reopened.Lookup(retry.Request)
	if err != nil || !ok || !reflect.DeepEqual(found, ack) {
		t.Fatalf("original proof changed: %+v %v %v", found, ok, err)
	}
	found.RecordLocations[0].PayloadOffset = -1
	foundAgain, ok, err := reopened.Lookup(retry.Request)
	if err != nil || !ok || !reflect.DeepEqual(foundAgain, ack) {
		t.Fatal("receipt result aliases cache")
	}
	again, err = reopened.Append(context.Background(), retry)
	if err != nil || !reflect.DeepEqual(again, ack) {
		t.Fatalf("replayed retry: %+v %v", again, err)
	}
}

func TestAppendAfterCompactionReopensWithOriginalReceipts(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%v", prepared), func(t *testing.T) {
			_, _, d := nativeIO(t)
			ctx := context.Background()
			options := storage.JournalOptions{SegmentTargetBytes: storage.MinSegmentTargetBytes, MaxPublishedTransactions: 6}
			var projected []Record
			apply := func(r Record) error { projected = append(projected, r); return nil }
			prepare := func(r Record) (func(), error) {
				return func() { projected = append(projected, r) }, nil
			}
			var s *Store
			var err error
			if prepared {
				s, err = CreatePrepared(d, options, prepare)
			} else {
				s, err = Create(d, options, apply)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					_ = s.Close()
				}
			}()
			var records []Record
			var receipts []storage.PublishedTransaction
			var compactions uint64
			checkReceipts := func() {
				t.Helper()
				before, err := s.journal.Published()
				if err != nil {
					t.Fatal(err)
				}
				d.mutations = nil
				for i, r := range records {
					got, ok, err := s.Lookup(r.Request)
					if err != nil || !ok || !reflect.DeepEqual(got, receipts[i]) {
						t.Fatalf("lookup %d: %+v %v %v; want %+v", i, got, ok, err, receipts[i])
					}
					got, err = s.Append(ctx, r)
					if err != nil || !reflect.DeepEqual(got, receipts[i]) {
						t.Fatalf("retry %d: %+v %v; want %+v", i, got, err, receipts[i])
					}
					got.RecordLocations[0].PayloadOffset = -1
				}
				after, err := s.journal.Published()
				if err != nil || after != before || len(d.mutations) != 0 || !reflect.DeepEqual(projected, records) {
					t.Fatalf("retry mutated state: before=%+v after=%+v err=%v mutations=%v", before, after, err, d.mutations)
				}
				got, err := s.ReadRecords()
				if err != nil || !reflect.DeepEqual(got, records) {
					t.Fatalf("records after compaction: %+v %v", got, err)
				}
			}
			reopen := func() {
				t.Helper()
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				projected = nil
				var report Report
				if prepared {
					s, report, err = OpenPrepared(d, options, prepare)
				} else {
					s, report, err = Open(d, options, apply)
				}
				if err != nil || report.ScannedTransactions != uint64(len(records)) {
					t.Fatalf("reopen after compaction and append: %+v %v", report, err)
				}
				checkReceipts()
			}
			for round := 0; round < 3; round++ {
				if round > 0 {
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					j, _, err := storage.OpenJournal(d, options)
					if err != nil {
						t.Fatal(err)
					}
					for n := 0; n < round; n++ {
						report, err := j.CompactAuditFull(ctx)
						if err != nil {
							_ = j.Close()
							t.Fatal(err)
						}
						if report.After.Generation != report.Before.Generation+1 ||
							report.After.Position != report.Before.Position ||
							report.After.PublishedEnd != report.Before.PublishedEnd ||
							report.After.PostRootSet != report.Before.PostRootSet {
							t.Fatalf("compaction changed logical publication: %+v", report)
						}
						compactions++
					}
					if err := j.Close(); err != nil {
						t.Fatal(err)
					}
					reopen()
				}
				for n := 0; n < 2; n++ {
					r := testRecord(fmt.Sprintf("record-%d", len(records)+1))
					ack, err := s.Append(ctx, r)
					if err != nil {
						t.Fatal(err)
					}
					records = append(records, r)
					receipts = append(receipts, cloneResult(ack))
					if ack.Publication.Position.LSN != uint64(len(records)) ||
						ack.Publication.Generation != uint64(len(records))+compactions {
						t.Fatalf("logical and physical counters: %+v", ack)
					}
					checkReceipts()
				}
				reopen()
			}
			d.mutations = nil
			out, err := s.Append(ctx, testRecord("over-quota"))
			zeroResult(t, out)
			if !errors.Is(err, storage.ErrLimit) || len(d.mutations) != 0 {
				t.Fatalf("quota rejection mutated journal: %v %v", err, d.mutations)
			}
			changed := testRecord("record-1")
			changed.Payload = []byte("different")
			out, err = s.Append(ctx, changed)
			zeroResult(t, out)
			if !errors.Is(err, ErrIdempotencyConflict) || len(d.mutations) != 0 {
				t.Fatalf("conflict lost at quota: %v %v", err, d.mutations)
			}
			checkReceipts()
		})
	}
}

func TestNextMetadataChecksBothCounters(t *testing.T) {
	const max = ^uint64(0)
	for _, tc := range []struct {
		name            string
		lsn, generation uint64
		overflow        bool
	}{
		{"genesis", 0, 0, false},
		{"after compaction", 7, 10, false},
		{"last generation", 7, max - 1, false},
		{"last LSN", max - 1, max - 1, false},
		{"generation exhausted", 7, max, true},
		{"LSN exhausted", max, max - 1, true},
		{"both exhausted", max, max, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := nextMetadata(storage.Publication{Position: storage.Position{LSN: tc.lsn}, Generation: tc.generation})
			if tc.overflow {
				if !errors.Is(err, storage.ErrLimit) || raw != nil {
					t.Fatalf("overflow returned metadata: %x %v", raw, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeMetadata(raw)
			if err != nil || got.lsn != tc.lsn+1 || got.generation != tc.generation+1 {
				t.Fatalf("counter mismatch: %+v %v", got, err)
			}
		})
	}
}

func TestPreparedRejectionDoesNotWriteOrInstall(t *testing.T) {
	_, _, d := nativeIO(t)
	visible := []string{}
	s, err := CreatePrepared(d, storage.JournalOptions{}, func(r Record) (func(), error) {
		if string(r.Payload) == "reject" {
			return nil, errInjected
		}
		candidate := append(append([]string(nil), visible...), string(r.Payload))
		return func() { visible = candidate }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d.mutations = nil
	r := testRecord("reject")
	r.Payload = []byte("reject")
	out, err := s.Append(context.Background(), r)
	zeroResult(t, out)
	if !errors.Is(err, errInjected) || len(visible) != 0 || len(d.mutations) != 0 {
		t.Fatalf("rejection wrote/installed: %v %v %v", visible, d.mutations, err)
	}
	r.Payload = []byte("accept")
	if _, err = s.Append(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(visible, []string{"accept"}) {
		t.Fatal(visible)
	}
	if _, err = s.Append(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if len(visible) != 1 {
		t.Fatal("idempotent retry installed twice")
	}
}

func TestPreparedPublishFailureDoesNotInstallAndFences(t *testing.T) {
	path, _, d := nativeIO(t)
	installed := false
	s, err := CreatePrepared(d, storage.JournalOptions{}, func(Record) (func(), error) { return func() { installed = true }, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d.failWrite = activeSegment(t, path)
	out, err := s.Append(context.Background(), testRecord("unknown"))
	zeroResult(t, out)
	if !errors.Is(err, storage.ErrCommitUnknown) || installed {
		t.Fatalf("unknown installed=%v err=%v", installed, err)
	}
	if got, err := s.ReadRecords(); got != nil || !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatalf("fenced read: %+v %v", got, err)
	}
	if got := s.Records(); got != nil {
		t.Fatal("compat Records exposed fenced state")
	}
	if out, _, err := s.Lookup(testRecord("unknown").Request); !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatal(err)
	} else {
		zeroResult(t, out)
	}
}

func TestLegacyApplyFailureAfterCommitFencesWithoutAck(t *testing.T) {
	_, _, d := nativeIO(t)
	s, err := Create(d, storage.JournalOptions{}, func(Record) error { return errInjected })
	if err != nil {
		t.Fatal(err)
	}
	r := testRecord("legacy-apply")
	out, err := s.Append(context.Background(), r)
	zeroResult(t, out)
	if !errors.Is(err, storage.ErrCommitUnknown) || !errors.Is(err, storage.ErrStoreFenced) || !errors.Is(err, errInjected) {
		t.Fatalf("post-P1 failure not fenced: %v", err)
	}
	if _, err = s.ReadRecords(); !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := Open(d, storage.JournalOptions{}, nil)
	if err != nil || report.ScannedTransactions != 1 {
		t.Fatalf("published record missing: %+v %v", report, err)
	}
	defer reopened.Close()
	if _, found, err := reopened.Lookup(r.Request); err != nil || !found {
		t.Fatalf("reconcile: %v %v", found, err)
	}
}

func TestSemanticReplayFailurePrecedesTailRepairAndReleasesOwner(t *testing.T) {
	path, _, d := nativeIO(t)
	s, err := Create(d, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(context.Background(), testRecord("valid")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	name := activeSegment(t, path)
	f, err := d.DurableIO.OpenAppend(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("unpublished evidence")); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(path, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	d.mutations = nil
	opened, report, err := Open(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, func(Record) error { return errInjected })
	if opened != nil || !errors.Is(err, errInjected) || report.Verified || report.PublicationSynced {
		t.Fatalf("semantic failure returned ready: %+v %v", report, err)
	}
	after, err := os.ReadFile(filepath.Join(path, filepath.FromSlash(name)))
	if err != nil || !bytes.Equal(before, after) || len(d.mutations) != 0 {
		t.Fatalf("semantic failure repaired tail: %v mutations=%v", err, d.mutations)
	}
	// Owner acquired during rejected recovery is fully retired.
	owner, err := d.AcquireOwnerLock()
	if err != nil {
		t.Fatalf("owner leaked: %v", err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedReplayValidatesBeforeRepair(t *testing.T) {
	path, _, d := nativeIO(t)
	s, err := Create(d, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(context.Background(), testRecord("candidate")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	name := activeSegment(t, path)
	f, err := d.DurableIO.OpenAppend(name)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte("tail"))
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Sync()
	_ = f.Close()
	d.mutations = nil
	candidate := 0
	opened, report, err := OpenPrepared(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, func(Record) (func(), error) { candidate++; return nil, errInjected })
	if opened != nil || !errors.Is(err, errInjected) || candidate != 1 || report.Verified || len(d.mutations) != 0 {
		t.Fatalf("prepare rejected after repair: candidate=%d report=%+v mutations=%v err=%v", candidate, report, d.mutations, err)
	}
}

func TestRejectedReplayCleanupRetainsOwnerUntilCloseRetry(t *testing.T) {
	path, _, d := nativeIO(t)
	s, err := Create(d, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(context.Background(), testRecord("cleanup")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	d.failClose = activeSegment(t, path)
	opened, _, err := Open(d, storage.JournalOptions{}, func(Record) error { return errInjected })
	var cleanup *storage.CleanupError
	if opened != nil || !errors.Is(err, errInjected) || !errors.As(err, &cleanup) {
		t.Fatalf("cleanup responsibility lost: %v", err)
	}
	owner, busy := d.AcquireOwnerLock()
	if owner != nil {
		_ = owner.Close()
	}
	if !errors.Is(busy, storage.ErrOwnerBusy) {
		t.Fatalf("owner released before file retirement: %v", busy)
	}
	if err = cleanup.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = d.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	_ = owner.Close()
}

func TestClosingFailureRejectsReadsAndWritesUntilRetry(t *testing.T) {
	path, _, d := nativeIO(t)
	s, err := Create(d, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(context.Background(), testRecord("close")); err != nil {
		t.Fatal(err)
	}
	d.failClose = activeSegment(t, path)
	if err = s.Close(); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	if _, err = s.ReadRecords(); !errors.Is(err, ErrClosing) {
		t.Fatal(err)
	}
	if got := s.Records(); got != nil {
		t.Fatal("closing read")
	}
	if out, err := s.Append(context.Background(), testRecord("late")); !errors.Is(err, ErrClosing) {
		t.Fatal(err)
	} else {
		zeroResult(t, out)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadRecords(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayChecksKindScopeCommitmentsAndDuplicateKeys(t *testing.T) {
	for _, kind := range []string{
		"kind", "request", "receipt", "root", "metadata", "duplicate",
		"zero LSN", "future LSN", "zero generation", "generation before LSN",
		"repeated generation", "regressed generation", "legacy metadata",
	} {
		t.Run(kind, func(t *testing.T) {
			_, _, d := nativeIO(t)
			j, err := storage.CreateJournal(d, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			count := 1
			if kind == "duplicate" || kind == "generation before LSN" || kind == "repeated generation" || kind == "regressed generation" {
				count = 2
			}
			if kind == "repeated generation" || kind == "regressed generation" {
				for i := 0; i < 2; i++ {
					if _, err := j.CompactAuditFull(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
			}
			for i := 0; i < count; i++ {
				r := testRecord("r")
				if kind != "duplicate" {
					r = testRecord(fmt.Sprintf("r-%d", i))
				}
				p, err := j.Published()
				if err != nil {
					t.Fatal(err)
				}
				tx := storage.Transaction{
					TxID:           model.TxID(fmt.Sprintf("%032x", i+1)),
					Request:        r.Request,
					RequestDigest:  requestDigest(r),
					LSN:            p.Position.LSN + 1,
					PreviousDigest: p.Position.Digest,
					Records: []storage.Record{{
						Kind:    r.Kind,
						Payload: r.Payload,
					}},
					RefUpdates:  encodeMetadata(p.Position.LSN+1, p.Generation+1),
					Receipt:     receiptDigest(r),
					PostRootSet: recordDigest(r),
				}
				switch kind {
				case "kind":
					tx.Records[0].Kind = storage.KindRevision
				case "request":
					tx.Request.Space = "other"
				case "receipt":
					tx.Receipt[0] ^= 1
				case "root":
					tx.PostRootSet[0] ^= 1
				case "metadata":
					tx.RefUpdates = nil
				case "zero LSN":
					tx.RefUpdates = encodeMetadata(0, 1)
				case "future LSN":
					tx.RefUpdates = encodeMetadata(2, 2)
				case "zero generation":
					tx.RefUpdates = encodeMetadata(1, 0)
				case "generation before LSN":
					if i == 1 {
						tx.RefUpdates = encodeMetadata(2, 1)
					}
				case "repeated generation":
					if i == 1 {
						tx.RefUpdates = encodeMetadata(2, 3)
					}
				case "regressed generation":
					if i == 1 {
						tx.RefUpdates = encodeMetadata(2, 2)
					}
				case "legacy metadata":
					tx.RefUpdates = append([]byte("ATE-JOURNALSTORE-1\x00"), make([]byte, 8)...)
					binary.BigEndian.PutUint64(tx.RefUpdates[len(tx.RefUpdates)-8:], 1)
				}
				if _, err = j.Publish(context.Background(), tx); err != nil {
					t.Fatal(err)
				}
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			d.mutations = nil
			called := false
			opened, report, err := Open(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, func(Record) error { called = true; return nil })
			if opened != nil || err == nil || report.Verified || len(d.mutations) != 0 {
				t.Fatalf("malformed adapter log accepted: %+v %v mutations=%v", report, err, d.mutations)
			}
			if kind == "legacy metadata" && !errors.Is(err, storage.ErrUnsupported) {
				t.Fatalf("old schema not explicitly rejected: %v", err)
			}
			if count == 1 && called {
				t.Fatal("unchecked commitments reached semantic callback")
			}
		})
	}
}

func TestCallbacksAreOwnedAndPanicsFenceOrRejectWithoutLeak(t *testing.T) {
	for _, phase := range []string{"prepare", "install", "apply", "replay"} {
		t.Run(phase, func(t *testing.T) {
			_, _, d := nativeIO(t)
			var s *Store
			var err error
			if phase == "prepare" || phase == "install" {
				s, err = CreatePrepared(d, storage.JournalOptions{}, func(Record) (func(), error) {
					if phase == "prepare" {
						panic("prepare")
					}
					return func() { panic("install") }, nil
				})
			} else {
				s, err = Create(d, storage.JournalOptions{}, func(Record) error {
					if phase == "apply" {
						panic("apply")
					}
					return nil
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			r := testRecord(phase)
			out, err := s.Append(context.Background(), r)
			if phase == "prepare" {
				zeroResult(t, out)
				if !errors.Is(err, ErrProjectionFailed) {
					t.Fatal(err)
				}
				if err = s.Ready(); err != nil {
					t.Fatalf("pre-IO prepare panic fenced clean store: %v", err)
				}
			} else if phase != "replay" {
				zeroResult(t, out)
				if !errors.Is(err, storage.ErrCommitUnknown) || !errors.Is(err, storage.ErrStoreFenced) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if phase == "replay" {
				s, _, err = Open(d, storage.JournalOptions{}, func(Record) error { panic("replay") })
				if s != nil || !errors.Is(err, ErrProjectionFailed) {
					t.Fatal(err)
				}
				owner, err := d.AcquireOwnerLock()
				if err != nil {
					t.Fatal("panic leaked owner", err)
				}
				_ = owner.Close()
			}
		})
	}
}

func TestRecordsPageReadsOwnedPayloadAndFencesRuntimeTamper(t *testing.T) {
	path, _, d := nativeIO(t)
	s, err := Create(d, storage.JournalOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ack, err := s.Append(context.Background(), testRecord("page"))
	if err != nil {
		t.Fatal(err)
	}
	page, next, err := s.RecordsPage(context.Background(), 0, 1)
	if err != nil || next != 1 || len(page) != 1 {
		t.Fatalf("page: %+v %d %v", page, next, err)
	}
	page[0].Payload[0] = 'X'
	again, next, err := s.RecordsPage(context.Background(), 0, 1)
	if err != nil || next != 1 || !bytes.Equal(again[0].Payload, testRecord("page").Payload) {
		t.Fatalf("page owns accepted bytes: %+v %v", again, err)
	}
	name := activeSegment(t, path)
	f, err := os.OpenFile(filepath.Join(path, filepath.FromSlash(name)), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("!"), ack.RecordLocations[0].PayloadOffset)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if records, err := s.ReadRecords(); records != nil || !errors.Is(err, storage.ErrStoreFenced) || !errors.Is(err, storage.ErrCorruption) {
		t.Fatalf("runtime tamper leaked records: %+v %v", records, err)
	}
	if _, _, err = s.Lookup(testRecord("page").Request); !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatal(err)
	}
}

func TestRequestScopeAndKindHaveDistinctIdentities(t *testing.T) {
	r := testRecord("digest")
	want := requestDigest(r)
	cases := []Record{r, r, r, r}
	cases[0].Request.Space = "s2"
	cases[1].Request.Principal = "p2"
	cases[2].Request.RequestID = "r2"
	cases[3].Kind = storage.KindObject
	for _, other := range cases {
		if requestDigest(other) == want {
			t.Fatalf("unbound request identity: %+v", other)
		}
	}
	if strings.Contains(string(encodeMetadata(1, 1)), "payload") {
		t.Fatal("metadata unexpectedly contains caller payload")
	}
}
