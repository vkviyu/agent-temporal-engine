package graphstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var errInjected = errors.New("injected graphstore IO failure")

func treeBytes(t testing.TB, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(path, p)
		if err != nil {
			return err
		}
		out[rel] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func checkOriginalProof(t testing.TB, got, want storage.PublishedTransaction) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("publication proof changed: got=%+v want=%+v", got, want)
	}
}

type stateSnapshot struct {
	space   model.SpaceID
	entries []graph.Entry
}
type stateOracle struct {
	mu     sync.Mutex
	states map[model.ObjectID]stateSnapshot
	calls  int
}

func (r *stateOracle) ReadEntries(space model.SpaceID, root model.ObjectID) ([]graph.Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	value, ok := r.states[root]
	if !ok {
		return nil, graph.ErrStateUnverified
	}
	if value.space != space {
		return nil, graph.ErrSpaceMismatch
	}
	out := make([]graph.Entry, len(value.entries))
	for i, e := range value.entries {
		out[i] = graph.Entry{Key: append([]byte(nil), e.Key...), Value: e.Value}
	}
	return out, nil
}
func TestVerifiedRevisionChecksBeforeWritesAndKeepsOriginalProof(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	if opened, err := CreateVerified(d, storage.JournalOptions{}, nil); opened != nil || !errors.Is(err, graph.ErrStateUnverified) {
		t.Fatalf("missing verifier accepted: %v", err)
	}
	if len(treeBytes(t, path)) != 0 {
		t.Fatal("failed verifier acquisition modified empty directory")
	}
	oracle := &stateOracle{states: map[model.ObjectID]stateSnapshot{gid('a'): {space: "space", entries: []graph.Entry{{Key: []byte("key"), Value: gid('1')}}}, gid('b'): {space: "space", entries: []graph.Entry{{Key: []byte("key"), Value: gid('2')}}}}}
	s, err := CreateVerified(d, storage.JournalOptions{}, oracle)
	if err != nil {
		t.Fatal(err)
	}
	entry := []graph.Entry{{Key: []byte("key"), Value: gid('1')}}
	first, proof, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, entry)
	if err != nil {
		t.Fatal(err)
	}
	entry[0].Key[0] = 'X'
	proofCopy := clonePublication(proof)
	proof.RecordLocations[0].PayloadOffset = -1
	duplicate, again, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, []graph.Entry{{Key: []byte("key"), Value: gid('1')}})
	if err != nil || duplicate.ID != first.ID {
		t.Fatalf("duplicate revision changed: %+v %v", duplicate, err)
	}
	checkOriginalProof(t, again, proofCopy)
	if oracle.calls != 1 {
		t.Fatal("durable retry rechecked mutable state source")
	}
	before := treeBytes(t, path)
	if _, _, err = s.AddRevision(context.Background(), "space", first.ID, nil, gid('b'), nil, []graph.Entry{{Key: []byte("key"), Value: gid('3')}}); !errors.Is(err, graph.ErrStateMismatch) {
		t.Fatalf("invalid state accepted: %v", err)
	}
	if !reflect.DeepEqual(before, treeBytes(t, path)) {
		t.Fatal("state mismatch wrote before rejection")
	}
	// Parent scope is a pure precondition and cannot be discovered after P1.
	if _, _, err = s.AddRevision(context.Background(), "other", first.ID, nil, gid('b'), nil, nil); !errors.Is(err, graph.ErrSpaceMismatch) {
		t.Fatalf("cross-space parent accepted: %v", err)
	}
	if !reflect.DeepEqual(before, treeBytes(t, path)) {
		t.Fatal("cross-space parent wrote before rejection")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	reopened, report, err := OpenVerified(d, storage.JournalOptions{}, oracle)
	if err != nil || !report.PublicationSynced || report.ScannedTransactions != 1 {
		t.Fatalf("reopen: %+v %v", report, err)
	}
	defer func() { _ = reopened.Close(); _ = d.Close() }()
	_, proofAfter, err := reopened.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, []graph.Entry{{Key: []byte("key"), Value: gid('1')}})
	if err != nil {
		t.Fatal(err)
	}
	checkOriginalProof(t, proofAfter, proofCopy)
	next, nextProof, err := reopened.AddRevision(context.Background(), "space", first.ID, nil, gid('b'), nil, []graph.Entry{{Key: []byte("key"), Value: gid('2')}})
	if err != nil {
		t.Fatal(err)
	}
	if next.ID == first.ID || nextProof.Publication.Position.LSN != 2 {
		t.Fatalf("reopen reused request counter: %+v", nextProof)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	reopened, report, err = OpenVerified(d, storage.JournalOptions{}, oracle)
	if err != nil || report.ScannedTransactions != 2 {
		t.Fatalf("second replay: %+v %v", report, err)
	}
	_, third, err := reopened.AddRevision(context.Background(), "space", first.ID, nil, gid('b'), nil, []graph.Entry{{Key: []byte("key"), Value: gid('2')}})
	if err != nil {
		t.Fatal(err)
	}
	checkOriginalProof(t, third, nextProof)
}
func appendTail(t testing.TB, d storage.DurableIO, raw []byte) {
	t.Helper()
	names, err := d.List("segments")
	if err != nil || len(names) != 1 {
		t.Fatalf("segment fixture: %v %v", names, err)
	}
	file, err := d.OpenAppend("segments/" + names[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestVerifiedReplayFailsBeforeTailQuarantine(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	s, err := Create(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, []graph.Entry{{Key: []byte("key"), Value: gid('1')}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	appendTail(t, d, []byte("raw unpublished tail"))
	before := treeBytes(t, path)
	if opened, _, err := OpenVerified(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, nil); opened != nil || !errors.Is(err, graph.ErrStateUnverified) {
		t.Fatal("nil verifier reached recovery")
	}
	if !reflect.DeepEqual(before, treeBytes(t, path)) {
		t.Fatal("nil verifier allowed mutation")
	}
	wrong := &stateOracle{states: map[model.ObjectID]stateSnapshot{gid('a'): {space: "space", entries: []graph.Entry{{Key: []byte("key"), Value: gid('2')}}}}}
	if opened, report, err := OpenVerified(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, wrong); opened != nil || report.Verified || !errors.Is(err, graph.ErrStateMismatch) {
		t.Fatalf("state mismatch became repair: %+v %v", report, err)
	}
	if !reflect.DeepEqual(before, treeBytes(t, path)) {
		t.Fatal("semantic replay failure changed tail/evidence")
	}
	// A failed open releases owner after all files, allowing a real recovery.
	correct := &stateOracle{states: map[model.ObjectID]stateSnapshot{gid('a'): {space: "space", entries: []graph.Entry{{Key: []byte("key"), Value: gid('1')}}}}}
	opened, report, err := OpenVerified(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, correct)
	if err != nil || !report.Verified || report.TailDisposition != "quarantined" {
		t.Fatalf("repair after failed verification: %+v %v", report, err)
	}
	if err = opened.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

type publicationFault struct {
	storage.DurableIO
	fail  bool
	after bool
}

func (f *publicationFault) ReplaceAtomic(from, to string) error {
	if f.fail && !f.after {
		return errInjected
	}
	err := f.DurableIO.ReplaceAtomic(from, to)
	if err == nil && f.fail && f.after {
		return errInjected
	}
	return err
}
func TestUnknownPublicationFencesAllGraphReadsAndRetries(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint("after-current=", after), func(t *testing.T) {
			path := t.TempDir()
			d := openDir(t, path)
			fault := &publicationFault{DurableIO: d, after: after}
			s, err := Create(fault, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			first, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			fault.fail = true
			candidate, proof, err := s.AddRevision(context.Background(), "space", first.ID, nil, gid('b'), nil, nil)
			if candidate.ID != "" || !reflect.DeepEqual(proof, storage.PublishedTransaction{}) || !errors.Is(err, storage.ErrCommitUnknown) {
				t.Fatalf("unknown granted result: %+v %+v %v", candidate, proof, err)
			}
			if got, err := s.Get(first.ID); got.ID != "" || !errors.Is(err, storage.ErrStoreFenced) {
				t.Fatalf("fenced Get leaked: %+v %v", got, err)
			}
			if got, err := s.Diff(first.ID, first.ID); len(got.Changes) != 0 || got.From != "" || !errors.Is(err, storage.ErrStoreFenced) {
				t.Fatalf("fenced Diff leaked: %+v %v", got, err)
			}
			if got, err := s.PlanJoin(first.ID, first.ID); got.Left != "" || !errors.Is(err, storage.ErrStoreFenced) {
				t.Fatalf("fenced PlanJoin leaked: %+v %v", got, err)
			}
			if got, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil); got.ID != "" || !errors.Is(err, storage.ErrStoreFenced) {
				t.Fatalf("fenced retry bypassed gate: %+v %v", got, err)
			}
			fault.fail = false
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d = openDir(t, path)
			reopened, report, err := Open(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close(); _ = d.Close() }()
			want := uint64(1)
			if after {
				want = 2
			}
			if report.Publication.Position.LSN != want {
				t.Fatalf("unexpected reconciled boundary: %+v", report)
			}
		})
	}
}

type closeFault struct {
	storage.DurableIO
	failClose   bool
	ownerCloses int
}
type trackedFile struct {
	storage.DurableFile
	owner   *closeFault
	name    string
	retired bool
}

func (f *trackedFile) Close() error {
	if f.retired {
		return nil
	}
	if f.owner.failClose && len(f.name) >= 9 && f.name[:9] == "segments/" {
		return errInjected
	}
	err := f.DurableFile.Close()
	if err == nil {
		f.retired = true
	}
	return err
}

type trackedOwner struct {
	storage.OwnerLock
	state *closeFault
}

func (o trackedOwner) Close() error { o.state.ownerCloses++; return o.OwnerLock.Close() }
func (d *closeFault) CreateExclusive(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.CreateExclusive(name)
	if f != nil {
		return &trackedFile{DurableFile: f, owner: d, name: name}, err
	}
	return f, err
}
func (d *closeFault) OpenAppend(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.OpenAppend(name)
	if f != nil {
		return &trackedFile{DurableFile: f, owner: d, name: name}, err
	}
	return f, err
}
func (d *closeFault) AcquireOwnerLock() (storage.OwnerLock, error) {
	owner, err := d.DurableIO.AcquireOwnerLock()
	if owner != nil {
		return trackedOwner{OwnerLock: owner, state: d}, err
	}
	return owner, err
}
func TestCloseRetainsOwnerAndRejectsReadsUntilRetried(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	fault := &closeFault{DurableIO: d}
	s, err := Create(fault, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.AddRevision(context.Background(), "s", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fault.failClose = true
	if err = s.Close(); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	if fault.ownerCloses != 0 {
		t.Fatal("owner released ahead of failed file")
	}
	if _, err = s.Get(r.ID); !errors.Is(err, storage.ErrClosing) {
		t.Fatalf("closing Get: %v", err)
	}
	if _, err = s.Diff(r.ID, r.ID); !errors.Is(err, storage.ErrClosing) {
		t.Fatalf("closing Diff: %v", err)
	}
	if _, err = s.PlanJoin(r.ID, r.ID); !errors.Is(err, storage.ErrClosing) {
		t.Fatalf("closing PlanJoin: %v", err)
	}
	if _, _, err = s.AddRevision(context.Background(), "s", "", nil, gid('a'), nil, nil); !errors.Is(err, storage.ErrClosing) {
		t.Fatalf("closing retry: %v", err)
	}
	fault.failClose = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if fault.ownerCloses != 1 {
		t.Fatalf("owner not retired once: %d", fault.ownerCloses)
	}
	if err = s.Close(); err != nil || fault.ownerCloses != 1 {
		t.Fatal("Close not idempotent")
	}
	if _, err = s.Get(r.ID); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestUnversionedAdapterRecordsRejectedBeforeRepair(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	j, err := storage.CreateJournal(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := graph.BuildRevision("space", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := graph.EncodeRevision(r)
	if err != nil {
		t.Fatal(err)
	}
	p, err := j.Published()
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.Publish(context.Background(), storage.Transaction{TxID: model.TxID("01010101010101010101010101010101"), Request: model.RequestKey{Space: "space", Principal: principal, RequestID: "revision-1"}, RequestDigest: digestOfBytes(raw), LSN: 1, PreviousDigest: p.Position.Digest, Records: []storage.Record{{Kind: storage.KindRevision, Payload: raw}}, Receipt: digestOf(string(r.ID)), PostRootSet: digestOf(string(r.ID))})
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	appendTail(t, d, []byte("must survive unsupported metadata"))
	before := treeBytes(t, path)
	opened, report, err := Open(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
	if opened != nil || report.Verified || !errors.Is(err, ErrUnsupportedAdapterSchema) || !errors.Is(err, storage.ErrUnsupported) {
		t.Fatalf("unversioned adapter reinterpreted: %+v %v", report, err)
	}
	if !reflect.DeepEqual(before, treeBytes(t, path)) {
		t.Fatal("unsupported adapter repaired tail")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayRejectsSemanticBindingErrorsBeforeRepair(t *testing.T) {
	changes := []struct {
		name   string
		change func(*storage.Transaction)
	}{
		{"space", func(tx *storage.Transaction) { tx.Request.Space = "other" }},
		{"principal", func(tx *storage.Transaction) { tx.Request.Principal = "other" }},
		{"request identity", func(tx *storage.Transaction) { tx.Request.RequestID = "different" }},
		{"request digest", func(tx *storage.Transaction) { tx.RequestDigest = digestOf(string(gid('b'))) }},
		{"receipt", func(tx *storage.Transaction) { tx.Receipt = digestOf(string(gid('b'))) }},
		{"post root", func(tx *storage.Transaction) { tx.PostRootSet = digestOf(string(gid('b'))) }},
		{"record kind", func(tx *storage.Transaction) { tx.Records[0].Kind = storage.KindObject }},
		{"ref update", func(tx *storage.Transaction) { tx.RefUpdates = []byte("unexpected update") }},
		{"future generation", func(tx *storage.Transaction) { tx.ControlUpdates = encodePublicationMetadata(2) }},
		{"metadata trailing bytes", func(tx *storage.Transaction) { tx.ControlUpdates = append(tx.ControlUpdates, 0) }},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			d := openDir(t, path)
			j, err := storage.CreateJournal(d, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			r, err := graph.BuildRevision("space", "", nil, gid('a'), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := graph.EncodeRevision(r)
			if err != nil {
				t.Fatal(err)
			}
			p, err := j.Published()
			if err != nil {
				t.Fatal(err)
			}
			tx := storage.Transaction{TxID: model.TxID("01010101010101010101010101010101"), Request: model.RequestKey{Space: r.Space, Principal: principal, RequestID: revisionRequestID(r.ID)}, RequestDigest: digestOfBytes(raw), LSN: 1, PreviousDigest: p.Position.Digest, Records: []storage.Record{{Kind: storage.KindRevision, Payload: raw}}, ControlUpdates: encodePublicationMetadata(1), Receipt: digestOf(string(r.ID)), PostRootSet: digestOf(string(r.ID))}
			tc.change(&tx)
			if _, err = j.Publish(context.Background(), tx); err != nil {
				t.Fatalf("fixture not physically valid: %v", err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			appendTail(t, d, []byte("preserve after semantic mismatch"))
			before := treeBytes(t, path)
			opened, report, err := Open(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
			if opened != nil || report.Verified || err == nil {
				t.Fatalf("semantic mismatch accepted: %+v %v", report, err)
			}
			if !reflect.DeepEqual(before, treeBytes(t, path)) {
				t.Fatal("semantic mismatch allowed repair")
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDurableRetryPrecedesPublishedQuota(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	s, err := Create(d, storage.JournalOptions{MaxPublishedTransactions: 1})
	if err != nil {
		t.Fatal(err)
	}
	r, first, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.AddRevision(context.Background(), "space", r.ID, nil, gid('b'), nil, nil); !errors.Is(err, storage.ErrLimit) {
		t.Fatalf("new request bypassed published quota: %v", err)
	}
	_, retry, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkOriginalProof(t, retry, first)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	opened, _, err := Open(d, storage.JournalOptions{MaxPublishedTransactions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = opened.Close(); _ = d.Close() }()
	_, retry, err = opened.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkOriginalProof(t, retry, first)
}
