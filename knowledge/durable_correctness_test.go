package knowledge

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/journalstore"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var errDurableBoundary = errors.New("injected knowledge durable boundary")

type durableTraceIO struct {
	storage.DurableIO
	mutations []string
	failClose string
	failWrite string
	gate      *durableWriteGate
}
type durableWriteGate struct {
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}

func (d *durableTraceIO) CreateExclusive(name string) (storage.DurableFile, error) {
	d.mutations = append(d.mutations, "create "+name)
	f, err := d.DurableIO.CreateExclusive(name)
	return d.wrap(f, name), err
}
func (d *durableTraceIO) OpenAppend(name string) (storage.DurableFile, error) {
	f, err := d.DurableIO.OpenAppend(name)
	return d.wrap(f, name), err
}
func (d *durableTraceIO) Mkdir(name string) error {
	d.mutations = append(d.mutations, "mkdir "+name)
	return d.DurableIO.Mkdir(name)
}
func (d *durableTraceIO) SyncDir(name string) error {
	d.mutations = append(d.mutations, "syncdir "+name)
	return d.DurableIO.SyncDir(name)
}
func (d *durableTraceIO) Remove(name string) error {
	d.mutations = append(d.mutations, "remove "+name)
	return d.DurableIO.Remove(name)
}
func (d *durableTraceIO) ReplaceAtomic(from, to string) error {
	d.mutations = append(d.mutations, "replace "+to)
	return d.DurableIO.ReplaceAtomic(from, to)
}
func (d *durableTraceIO) InstallNew(from, to string) error {
	d.mutations = append(d.mutations, "install "+to)
	return d.DurableIO.InstallNew(from, to)
}
func (d *durableTraceIO) wrap(f storage.DurableFile, name string) storage.DurableFile {
	if f == nil {
		return nil
	}
	return &durableTraceFile{DurableFile: f, owner: d, name: name}
}

type durableTraceFile struct {
	storage.DurableFile
	owner *durableTraceIO
	name  string
}

func (f *durableTraceFile) Write(b []byte) (int, error) {
	f.owner.mutations = append(f.owner.mutations, "write "+f.name)
	if f.owner.gate != nil {
		f.owner.gate.once.Do(func() { close(f.owner.gate.reached); <-f.owner.gate.release })
	}
	n, err := f.DurableFile.Write(b)
	if f.owner.failWrite == f.name {
		f.owner.failWrite = ""
		return n, errors.Join(err, errDurableBoundary)
	}
	return n, err
}
func (f *durableTraceFile) Sync() error {
	f.owner.mutations = append(f.owner.mutations, "sync "+f.name)
	return f.DurableFile.Sync()
}
func (f *durableTraceFile) Truncate(n int64) error {
	f.owner.mutations = append(f.owner.mutations, "truncate "+f.name)
	return f.DurableFile.Truncate(n)
}
func (f *durableTraceFile) Close() error {
	if f.owner.failClose == f.name {
		f.owner.failClose = ""
		return errDurableBoundary
	}
	return f.DurableFile.Close()
}

func durableNative(t *testing.T) (string, *durableTraceIO) {
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
	return path, &durableTraceIO{DurableIO: d}
}
func durableSegment(t *testing.T, path string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(path, "segments"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatal("test expects one segment")
	}
	return "segments/" + entries[0].Name()
}
func durableConsumer(lsn uint64) model.CommitFence {
	return model.CommitFence{Store: "s", LSN: lsn, CommitDigest: model.CommitDigest(dig('e'))}
}
func durableRequest(id string) model.RequestKey {
	return model.RequestKey{Space: "space", Principal: "host", RequestID: model.RequestID(id)}
}
func durableObservation(seq uint64) Observation {
	return Observation{Key: model.KnowledgeKey{Actor: "actor", Execution: "exec"}, Source: source(1), Payload: dig('d'), Sequence: seq, Label: "accepted result"}
}
func durablePacket(o Observation) ContextPacket {
	return ContextPacket{Space: "space", Key: &model.KnowledgeKey{Actor: "actor", Execution: "exec"}, Source: source(3), GoalRef: dig('c'), Items: []ContextItem{{Ordinal: 0, Observation: o.ID, Content: o.Payload, Label: "result"}}, Renderer: "renderer/v1", Tokenizer: "tokens/v1", EstimatedTokens: 42}
}

func TestDurableObservationPacketReopenAndOwnedResults(t *testing.T) {
	_, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input := durableObservation(1)
	original, err := s.AppendObservation(context.Background(), durableRequest("observation"), input, durableConsumer(2))
	if err != nil {
		t.Fatal(err)
	}
	input.Source.Anchor.Cursor.Revision = model.RevisionID(dig('b'))
	input.Source.Anchor.Knowledge.Execution = "changed"
	packetInput := durablePacket(original)
	packet, err := s.AppendPacket(context.Background(), durableRequest("packet"), packetInput, durableConsumer(4))
	if err != nil {
		t.Fatal(err)
	}
	packetInput.Key.Execution = "changed"
	packetInput.Items[0].Content = dig('a')
	packetInput.Source.Anchor.Cursor.Revision = model.RevisionID(dig('c'))
	again, err := s.AppendObservation(context.Background(), durableRequest("observation"), durableObservation(1), durableConsumer(2))
	if err != nil || !reflect.DeepEqual(again, original) {
		t.Fatalf("idempotent observation: %+v %v", again, err)
	}
	if len(s.store.Records()) != 2 {
		t.Fatal("same RequestKey appended twice")
	}
	got, err := s.GetObservation(original.ID)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("observation changed: %+v %v", got, err)
	}
	got.Source.Anchor.Cursor.Revision = model.RevisionID(dig('b'))
	got.Source.Anchor.Knowledge.Actor = "changed"
	gotPacket, err := s.GetPacket(packet.ID)
	if err != nil || !reflect.DeepEqual(gotPacket, packet) {
		t.Fatalf("packet changed: %+v %v", gotPacket, err)
	}
	gotPacket.Key.Actor = "changed"
	gotPacket.Items[0].Label = "changed"
	gotPacket.Source.Anchor.Cursor.Revision = model.RevisionID(dig('a'))
	if fresh, err := s.GetPacket(packet.ID); err != nil || !reflect.DeepEqual(fresh, packet) {
		t.Fatalf("GetPacket aliases projection: %+v %v", fresh, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenDurable(d, storage.JournalOptions{})
	if err != nil || report.ScannedTransactions != 2 {
		t.Fatalf("reopen: %+v %v", report, err)
	}
	defer reopened.Close()
	got, err = reopened.GetObservation(original.ID)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("recovered observation: %+v %v", got, err)
	}
	gotPacket, err = reopened.GetPacket(packet.ID)
	if err != nil || !reflect.DeepEqual(gotPacket, packet) {
		t.Fatalf("recovered packet: %+v %v", gotPacket, err)
	}
	again, err = reopened.AppendObservation(context.Background(), durableRequest("observation"), durableObservation(1), durableConsumer(2))
	if err != nil || !reflect.DeepEqual(again, original) {
		t.Fatalf("replayed idempotency: %+v %v", again, err)
	}
	if len(reopened.store.Records()) != 2 {
		t.Fatal("reopened retry appended")
	}
}

func TestDurablePreparedScopeSequenceAndReferenceRejectionsDoNotWrite(t *testing.T) {
	_, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	original, err := s.AppendObservation(context.Background(), durableRequest("o1"), durableObservation(1), durableConsumer(2))
	if err != nil {
		t.Fatal(err)
	}
	d.mutations = nil
	badScope := durableRequest("bad-scope")
	badScope.Space = "other"
	if _, err = s.AppendObservation(context.Background(), badScope, durableObservation(2), durableConsumer(2)); err == nil {
		t.Fatal("cross-Space observation accepted")
	}
	if _, err = s.AppendObservation(context.Background(), durableRequest("gap"), durableObservation(3), durableConsumer(2)); !errors.Is(err, ErrObservationSequence) {
		t.Fatal(err)
	}
	changed := durableObservation(1)
	changed.Payload = dig('b')
	if _, err = s.AppendObservation(context.Background(), durableRequest("sequence-conflict"), changed, durableConsumer(2)); !errors.Is(err, ErrObservationSequence) {
		t.Fatal(err)
	}
	if _, err = s.AppendObservation(context.Background(), durableRequest("same-cut"), durableObservation(2), durableConsumer(1)); !errors.Is(err, ErrFutureSource) {
		t.Fatal(err)
	}
	for _, which := range []string{"missing", "payload", "execution", "cut", "scope"} {
		p := durablePacket(original)
		switch which {
		case "missing":
			p.Items[0].Observation = dig('b')
		case "payload":
			p.Items[0].Content = dig('b')
		case "execution":
			p.Key.Execution = "other"
			p.Source.Anchor.Knowledge.Execution = "other"
		case "cut":
			p.Source = source(1)
		case "scope":
			p.Space = "other"
			p.Source.Anchor.Space = "other"
			p.Source.Anchor.Cursor.Space = "other"
		}
		if _, err = s.AppendPacket(context.Background(), durableRequest(which), p, durableConsumer(4)); err == nil {
			t.Fatalf("bad %s packet accepted", which)
		}
	}
	if len(d.mutations) != 0 {
		t.Fatalf("preflight rejections wrote: %v", d.mutations)
	}
	if records := s.store.Records(); len(records) != 1 {
		t.Fatalf("rejected records adopted: %d", len(records))
	}
	if _, err = s.GetObservation(original.ID); err != nil {
		t.Fatalf("rejections fenced valid state: %v", err)
	}
}

func TestDurableVerifierRunsBeforeP1AndOnReplayWithOwnedProof(t *testing.T) {
	_, d := durableNative(t)
	calls := 0
	verify := func(proof Verification) error {
		calls++
		if proof.Request.Space != "space" || proof.Source.Anchor.Space != "space" || proof.Consumer.LSN != 2 || len(proof.Content) != 1 || proof.Content[0] != dig('d') {
			return errDurableBoundary
		}
		proof.Content[0] = dig('b')
		proof.Source.Anchor.Knowledge.Actor = "changed"
		proof.Knowledge.Execution = "changed"
		return nil
	}
	s, err := CreateDurableWithVerifier(d, storage.JournalOptions{}, verify)
	if err != nil {
		t.Fatal(err)
	}
	original, err := s.AppendObservation(context.Background(), durableRequest("verified"), durableObservation(1), durableConsumer(2))
	if err != nil || calls != 1 {
		t.Fatalf("verify append calls=%d err=%v", calls, err)
	}
	if original.Key.Actor != "actor" || original.Source.Anchor.Knowledge.Actor != "actor" {
		t.Fatal("verifier mutated projection")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := OpenDurableWithVerifier(d, storage.JournalOptions{}, verify)
	if err != nil || calls != 2 {
		t.Fatalf("verify replay calls=%d err=%v", calls, err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	d.mutations = nil
	rejected, report, err := OpenDurableWithVerifier(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine}, func(Verification) error { return errDurableBoundary })
	if rejected != nil || !errors.Is(err, errDurableBoundary) || report.Verified || len(d.mutations) != 0 {
		t.Fatalf("replay verifier rejection repaired: %+v %v mutations=%v", report, err, d.mutations)
	}
	owner, err := d.AcquireOwnerLock()
	if err != nil {
		t.Fatal("verifier failure leaked owner", err)
	}
	_ = owner.Close()
}

func TestDurableSemanticReplayRejectsBeforeTailRepairAndCleanupIsRetryable(t *testing.T) {
	for _, broken := range []string{"kind", "scope", "cut", "packet-ref"} {
		t.Run(broken, func(t *testing.T) {
			path, d := durableNative(t)
			// Construct a physically valid generic adapter record whose knowledge
			// transition is invalid, to exercise semantic replay rather than CRC errors.
			js, err := journalstore.Create(d, storage.JournalOptions{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			o := durableObservation(1)
			o.ID = ObservationID(o)
			raw, err := o.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			payload, err := encodeDurable(observationRecord, durableConsumer(2), raw)
			if err != nil {
				t.Fatal(err)
			}
			r := journalstore.Record{Kind: storage.KindObject, Payload: payload, Request: durableRequest("bad")}
			switch broken {
			case "kind":
				r.Kind = storage.KindEvent
			case "scope":
				r.Request.Space = "other"
			case "cut":
				r.Payload, err = encodeDurable(observationRecord, durableConsumer(1), raw)
			case "packet-ref":
				p := durablePacket(o)
				p.ID = PacketID(p)
				raw, err = p.Canonical()
				if err != nil {
					t.Fatal(err)
				}
				r.Payload, err = encodeDurable(packetRecord, durableConsumer(4), raw)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = js.Append(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			if err = js.Close(); err != nil {
				t.Fatal(err)
			}
			name := durableSegment(t, path)
			file, err := d.DurableIO.OpenAppend(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = file.Write([]byte("preserve raw tail")); err != nil {
				t.Fatal(err)
			}
			_ = file.Sync()
			_ = file.Close()
			before, err := os.ReadFile(filepath.Join(path, filepath.FromSlash(name)))
			if err != nil {
				t.Fatal(err)
			}
			d.mutations = nil
			d.failClose = name
			opened, report, err := OpenDurable(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
			var cleanup *storage.CleanupError
			if opened != nil || err == nil || report.Verified || !errors.As(err, &cleanup) || len(d.mutations) != 0 {
				t.Fatalf("invalid transition repaired/leaked cleanup: %+v %v %v", report, err, d.mutations)
			}
			after, readErr := os.ReadFile(filepath.Join(path, filepath.FromSlash(name)))
			if readErr != nil || !bytes.Equal(before, after) {
				t.Fatal("semantic failure changed evidence")
			}
			owner, busy := d.AcquireOwnerLock()
			if owner != nil {
				_ = owner.Close()
			}
			if !errors.Is(busy, storage.ErrOwnerBusy) {
				t.Fatalf("owner released before file retired: %v", busy)
			}
			if err = cleanup.Close(); err != nil {
				t.Fatal(err)
			}
			owner, err = d.AcquireOwnerLock()
			if err != nil {
				t.Fatal(err)
			}
			_ = owner.Close()
		})
	}
}

func TestDurableUnknownPublicationFencesAllProjectionReads(t *testing.T) {
	path, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	original, err := s.AppendObservation(context.Background(), durableRequest("old"), durableObservation(1), durableConsumer(2))
	if err != nil {
		t.Fatal(err)
	}
	d.failWrite = durableSegment(t, path)
	if got, err := s.AppendObservation(context.Background(), durableRequest("unknown"), durableObservation(2), durableConsumer(2)); got.ID != "" || !errors.Is(err, storage.ErrCommitUnknown) {
		t.Fatalf("unknown append: %+v %v", got, err)
	}
	if got, err := s.GetObservation(original.ID); got.ID != "" || !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatalf("fenced old observation: %+v %v", got, err)
	}
	if got, err := s.GetPacket(dig('a')); got.ID != "" || !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatalf("fenced absence: %+v %v", got, err)
	}
	if got, err := s.AppendObservation(context.Background(), durableRequest("old"), durableObservation(1), durableConsumer(2)); got.ID != "" || !errors.Is(err, storage.ErrStoreFenced) {
		t.Fatalf("fenced original receipt: %+v %v", got, err)
	}
}

func TestDurableCloseDrainsAcceptedAppendAndRejectsNewCalls(t *testing.T) {
	_, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &durableWriteGate{reached: make(chan struct{}), release: make(chan struct{})}
	d.gate = gate
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
		_ = s.Close()
	})
	appendDone := make(chan error, 1)
	go func() {
		_, err := s.AppendObservation(context.Background(), durableRequest("accepted"), durableObservation(1), durableConsumer(2))
		appendDone <- err
	}()
	select {
	case <-gate.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("append did not reach P1 gate")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !s.closing.Load() {
		select {
		case err := <-closeDone:
			t.Fatalf("close finished before P1 release: %v", err)
		case <-deadline.C:
			t.Fatal("Close did not enter Closing")
		case <-time.After(time.Millisecond):
		}
	}
	if _, err = s.GetObservation(dig('a')); !errors.Is(err, ErrDurableClosing) {
		t.Fatalf("new Get while closing: %v", err)
	}
	if _, err = s.AppendObservation(context.Background(), durableRequest("late"), durableObservation(2), durableConsumer(2)); !errors.Is(err, ErrDurableClosing) {
		t.Fatalf("new Append while closing: %v", err)
	}
	close(gate.release)
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accepted Append/Close deadlocked")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not drain")
	}
	if _, err = s.GetObservation(dig('a')); !errors.Is(err, ErrDurableClosed) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableCloseFailureKeepsClosingAndCanRetry(t *testing.T) {
	path, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original, err := s.AppendObservation(context.Background(), durableRequest("old"), durableObservation(1), durableConsumer(2))
	if err != nil {
		t.Fatal(err)
	}
	d.failClose = durableSegment(t, path)
	if err = s.Close(); !errors.Is(err, errDurableBoundary) {
		t.Fatal(err)
	}
	if got, err := s.GetObservation(original.ID); got.ID != "" || !errors.Is(err, ErrDurableClosing) {
		t.Fatalf("Close failure leaked state: %+v %v", got, err)
	}
	owner, busy := d.AcquireOwnerLock()
	if owner != nil {
		_ = owner.Close()
	}
	if !errors.Is(busy, storage.ErrOwnerBusy) {
		t.Fatalf("owner lost on close failure: %v", busy)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = d.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	_ = owner.Close()
}

func TestDurableWaitingAppendCanCancelWithoutReservingRequest(t *testing.T) {
	_, d := durableNative(t)
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &durableWriteGate{reached: make(chan struct{}), release: make(chan struct{})}
	d.gate = gate
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
		_ = s.Close()
	})
	first := make(chan error, 1)
	go func() {
		_, err := s.AppendObservation(context.Background(), durableRequest("first"), durableObservation(1), durableConsumer(2))
		first <- err
	}()
	select {
	case <-gate.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("no P1 gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got, err := s.AppendObservation(ctx, durableRequest("retry"), durableObservation(2), durableConsumer(2)); got.ID != "" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled queued call: %+v %v", got, err)
	}
	close(gate.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if _, err = s.AppendObservation(context.Background(), durableRequest("retry"), durableObservation(2), durableConsumer(2)); err != nil {
		t.Fatal("canceled wait consumed request", err)
	}
}
