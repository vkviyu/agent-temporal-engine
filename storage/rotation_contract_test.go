package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func rotationSeed(t testing.TB) (*faultIO, *Journal, Publication, Transaction) {
	t.Helper()
	f := newFaultIO()
	j, err := CreateJournal(f, JournalOptions{SegmentTargetBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	tx := faultNextTransaction(faultPublished(t, j), 1)
	tx.Records = []Record{{KindObject, []byte("acknowledged before rotation")}}
	ack, err := j.Publish(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	return f, j, ack, tx
}

func rotationBatch(t testing.TB, old Publication) ([]Transaction, []*PreparedTransaction) {
	t.Helper()
	txs := make([]Transaction, 2)
	prepared := make([]*PreparedTransaction, len(txs))
	for i := range txs {
		txs[i] = faultNextTransaction(old, i+2)
		txs[i].Records = []Record{
			{KindObject, bytes.Repeat([]byte{byte('a' + i)}, 600)},
			{KindEvent, bytes.Repeat([]byte{byte('c' + i)}, 600)},
			{KindStateOps, bytes.Repeat([]byte{byte('e' + i)}, 600)},
		}
		if i == 1 {
			txs[i].Records[1].Payload = bytes.Repeat([]byte("oversized-frame"), 150)
		}
		var err error
		prepared[i], err = PrepareTransaction(txs[i], Limits{})
		if err != nil {
			t.Fatal(err)
		}
		old.Position = prepared[i].Position()
	}
	return txs, prepared
}

func rotationManifest(t testing.TB, f *faultIO) Manifest {
	t.Helper()
	c, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeManifest([]byte(faultReadAll(t, f, manifestName(c.ManifestID))))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func rotationDescriptors(m Manifest) []SegmentDescriptor {
	if len(m.Segments) != 0 {
		return m.Segments
	}
	return []SegmentDescriptor{{SegmentID: m.SegmentID, HeaderSize: SegmentHeaderSize, DataEnd: m.PublishedEnd}}
}

func rotationAssertLogicalTransactions(t testing.TB, j *Journal, want []Transaction) {
	t.Helper()
	p := faultPublished(t, j)
	var expected []byte
	for _, tx := range want {
		encoded, _, err := EncodeTransaction(tx, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, encoded...)
	}
	if p.PublishedEnd != SegmentHeaderSize+uint64(len(expected)) {
		t.Fatal("physical overhead leaked into logical end")
	}
	got := make([]byte, len(expected))
	if n, err := j.ReadAt(got, SegmentHeaderSize); n != len(got) || err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("logical concatenation differs: n=%d err=%v bytes-equal=%v", n, err, bytes.Equal(got, expected))
	}
	var one [1]byte
	if n, err := j.ReadAt(one[:], int64(p.PublishedEnd)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read beyond prefix: %d %v", n, err)
	}
}

func TestRotationGroupCommitKeepsFramesWholeAndLogicalProofs(t *testing.T) {
	f, j, old, first := rotationSeed(t)
	defer j.Close()
	txs, batch := rotationBatch(t, old)
	result, err := j.PublishPreparedBatch(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	if result.Publication != batchTip(old, batch) || len(result.Transactions) != len(batch) {
		t.Fatalf("bad group publication: %+v", result)
	}
	var installs, manifests int
	for _, call := range f.calls {
		if call.Op == "ReplaceAtomic" && call.To == "CURRENT" {
			installs++
		}
		if call.Op == "InstallNew" && strings.HasPrefix(call.To, "manifests/") {
			manifests++
		}
	}
	if installs != 1 || manifests != 1 {
		t.Fatalf("rotation installed multiple publication points: CURRENT=%d manifest=%d", installs, manifests)
	}
	m := rotationManifest(t, f)
	if len(m.Segments) < 3 {
		t.Fatalf("fixture did not rotate within transactions: %d segments", len(m.Segments))
	}
	var continued, oversized bool
	for i, d := range m.Segments {
		data := []byte(faultReadAll(t, f, segmentName(d.SegmentID)))
		if d.DataEnd > uint64(len(data)) || d.HeaderSize >= d.DataEnd {
			t.Fatalf("invalid/empty descriptor: %+v", d)
		}
		r := bytes.NewReader(data[d.HeaderSize:d.DataEnd])
		for {
			_, _, err := ReadFrame(r, Limits{})
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("frame split in segment %d: %v", i, err)
			}
		}
		if d.DataEnd > 1024 {
			oversized = true
		}
		if i > 0 && d.StartPosition == m.Segments[i-1].StartPosition {
			continued = true
		}
		if d.Sealed && uint64(len(data)) != d.DataEnd+SegmentFooterSize {
			t.Fatal("sealed range does not include exactly one footer")
		}
	}
	if !continued || !oversized {
		t.Fatalf("fixture missed continuation or oversized frame: continuation=%v oversized=%v", continued, oversized)
	}
	end := old.PublishedEnd
	for i, p := range batch {
		end += p.Size()
		own := result.Transactions[i]
		if own.Publication.Position != p.Position() || own.Publication.PostRootSet != txs[i].PostRootSet || own.Publication.Generation != old.Generation+1 || own.Publication.PublishedEnd != end {
			t.Fatal("member proof replaced by physical batch tip")
		}
		if len(own.RecordLocations) != len(txs[i].Records) {
			t.Fatal("missing record locations")
		}
		for n, loc := range own.RecordLocations {
			b := make([]byte, loc.PayloadLength)
			if count, err := j.ReadAt(b, loc.PayloadOffset); err != nil || count != len(b) || !bytes.Equal(b, txs[i].Records[n].Payload) {
				t.Fatalf("member %d record %d logical address changed: %v", i, n, err)
			}
		}
	}
	want := append([]Transaction{first}, txs...)
	rotationAssertLogicalTransactions(t, j, want)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	var replayed []Transaction
	j, report, err := OpenJournalWithReplay(f, JournalOptions{SegmentTargetBytes: 1024}, func(tx ScannedTransaction, reader io.ReaderAt) error {
		for n, loc := range tx.RecordLocations {
			b := make([]byte, loc.PayloadLength)
			if _, err := reader.ReadAt(b, loc.PayloadOffset); err != nil {
				return err
			}
			if !bytes.Equal(b, tx.Records[n].Payload) {
				return errors.New("replayed payload address changed")
			}
		}
		replayed = append(replayed, tx.Transaction)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if !report.Verified || !report.PublicationSynced || report.Publication != result.Publication || !reflect.DeepEqual(replayed, want) {
		t.Fatalf("recovery changed transaction history: %+v", report)
	}
	rotationAssertLogicalTransactions(t, j, want)
	// A new append after recovery must neither touch sealed bytes nor lose the
	// earlier logical address mapping when it rotates again.
	sealed := make(map[string]string)
	for _, d := range m.Segments {
		if d.Sealed {
			sealed[d.SegmentID] = faultReadAll(t, f, segmentName(d.SegmentID))
		}
	}
	next := faultNextTransaction(result.Publication, 4)
	if _, err := j.Publish(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	for id, raw := range sealed {
		if faultReadAll(t, f, segmentName(id)) != raw {
			t.Fatal("append modified a sealed segment")
		}
	}
	rotationAssertLogicalTransactions(t, j, append(want, next))
}

func rotationAssertEvidence(t testing.TB, f *faultIO, before map[string]string, m Manifest) {
	t.Helper()
	remaining := make(map[string]uint64)
	for _, d := range rotationDescriptors(m) {
		remaining[segmentName(d.SegmentID)] = d.DataEnd
		if d.Sealed {
			remaining[segmentName(d.SegmentID)] += SegmentFooterSize
		}
	}
	evidence := make(map[string]int)
	for name, raw := range faultTreeSnapshot(t, f) {
		if strings.HasPrefix(name, "quarantine/") && !strings.HasSuffix(name, "/") {
			evidence[raw]++
		}
	}
	for name, raw := range before {
		if !strings.HasPrefix(name, "segments/SEGMENT.") {
			continue
		}
		if end, referenced := remaining[name]; referenced {
			if uint64(len(raw)) < end {
				t.Fatal("fixture lost referenced bytes")
			}
			if got := faultReadAll(t, f, name); got != raw[:end] {
				t.Fatalf("repair modified published bytes in %s", name)
			}
			if uint64(len(raw)) == end {
				continue
			}
			raw = raw[end:]
		} else {
			if _, _, err := f.lookup(name); err == nil {
				t.Fatalf("orphan remained after repair: %s", name)
			}
		}
		if evidence[raw] == 0 {
			t.Fatalf("repair did not preserve exact unpublished evidence for %s", name)
		}
		evidence[raw]--
	}
}

func TestRotationCrashAtEveryMutationPublishesOnlyWholeBatch(t *testing.T) {
	baseline, j, old, _ := rotationSeed(t)
	_, batch := rotationBatch(t, old)
	if _, err := j.PublishPreparedBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), baseline.calls...)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if len(trace) == 0 || trace[len(trace)-1].Op != "SyncDir" || trace[len(trace)-1].Name != "." {
		t.Fatalf("missing final publication sync: %+v", trace)
	}
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%03d-%s/after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f, publisher, old, first := rotationSeed(t)
				txs, batch := rotationBatch(t, old)
				tip := batchTip(old, batch)
				f.failAt(call.Step, after)
				got, err := publisher.PublishPreparedBatch(context.Background(), batch)
				if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) || got.Publication != (Publication{}) || got.Transactions != nil {
					t.Fatalf("partial/known result after rotation failure: %+v %v", got, err)
				}
				if len(f.calls) < call.Step || f.calls[call.Step-1].Op != call.Op {
					t.Fatalf("mutation trace diverged: expected %+v actual %+v", call, f.calls)
				}
				if status := publisher.Status(); status.State != JournalFenced || status.LastPublication != old {
					t.Fatalf("unknown rotation changed ACK: %+v", status)
				}
				if _, err := publisher.Published(); !errors.Is(err, ErrStoreFenced) {
					t.Fatal("fenced publisher exposed a current fence")
				}
				reboot := f.crash()
				before := faultTreeSnapshot(t, reboot)
				m := rotationManifest(t, reboot)
				recovered, report, openErr := OpenJournal(reboot, JournalOptions{SegmentTargetBytes: 1024})
				if errors.Is(openErr, ErrRecoveryRequired) {
					if recovered != nil || !report.Verified || report.PublicationSynced || report.Publication != old {
						t.Fatalf("tail rejection lost old publication: %+v %v", report, openErr)
					}
					if !reflect.DeepEqual(before, faultTreeSnapshot(t, reboot)) {
						t.Fatal("default recovery modified active tail/orphans")
					}
					recovered, report, openErr = OpenJournal(reboot, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
					if openErr != nil {
						t.Fatalf("explicit repair: %v", openErr)
					}
					rotationAssertEvidence(t, reboot, before, m)
				} else if openErr != nil {
					t.Fatalf("valid old/new crash prefix rejected: %+v %v", report, openErr)
				}
				if recovered == nil || !report.Verified || !report.PublicationSynced {
					t.Fatalf("recovered before verifying publication: %+v", report)
				}
				p := faultPublished(t, recovered)
				if p != old && p != tip {
					t.Fatalf("partial/mixed batch recovered: %+v old=%+v tip=%+v", p, old, tip)
				}
				if call.Step == trace[len(trace)-1].Step && after && p != tip {
					t.Fatal("error after final sync lost durable batch")
				}
				want := []Transaction{first}
				if p == tip {
					want = append(want, txs...)
				}
				rotationAssertLogicalTransactions(t, recovered, want)
				if err := recovered.Close(); err != nil {
					t.Fatal(err)
				}
				reboot = reboot.crash()
				recovered, again, err := OpenJournal(reboot, JournalOptions{SegmentTargetBytes: 1024})
				if err != nil || again.Publication != p || again.TailBytes != 0 {
					t.Fatalf("recovery cleanup was not durable: %+v %v", again, err)
				}
				defer recovered.Close()
				rotationAssertLogicalTransactions(t, recovered, want)
			})
		}
	}
}

func TestRotationPublishedCorruptionNeverFallsBackOrRepairs(t *testing.T) {
	for _, kind := range []string{"sealed-footer", "successor-header", "sealed-frame", "missing-segment", "rebound-predecessor"} {
		t.Run(kind, func(t *testing.T) {
			f, j, old, _ := rotationSeed(t)
			_, batch := rotationBatch(t, old)
			if _, err := j.PublishPreparedBatch(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			m := rotationManifest(t, f)
			if len(m.Segments) < 2 {
				t.Fatal("fixture did not rotate")
			}
			d := m.Segments[0]
			name := segmentName(d.SegmentID)
			raw := []byte(faultReadAll(t, f, name))
			switch kind {
			case "sealed-footer":
				raw[d.DataEnd+16] ^= 1
				replayReplaceFile(t, f, name, raw)
			case "sealed-frame":
				raw[d.HeaderSize+HeaderSize] ^= 1
				replayReplaceFile(t, f, name, raw)
			case "missing-segment":
				if err := f.Remove(name); err != nil {
					t.Fatal(err)
				}
			case "successor-header":
				d = m.Segments[1]
				name = segmentName(d.SegmentID)
				raw = []byte(faultReadAll(t, f, name))
				raw[32] ^= 1
				replayReplaceFile(t, f, name, raw)
			case "rebound-predecessor":
				i := len(m.Segments) - 1
				d = m.Segments[i]
				name = segmentName(d.SegmentID)
				raw = []byte(faultReadAll(t, f, name))
				header, err := DecodeSuccessorHeader(raw[:SuccessorHeaderSize])
				if err != nil {
					t.Fatal(err)
				}
				header.PreviousSegmentDigest = testDigest("unrelated physical predecessor")
				b, err := EncodeSuccessorHeader(header)
				if err != nil {
					t.Fatal(err)
				}
				copy(raw, b)
				replayReplaceFile(t, f, name, raw)
				d.HeaderDigest = Digest(sha256.Sum256(b))
				d.PrefixDigest = Digest(sha256.Sum256(raw[:d.DataEnd]))
				m.Segments[i] = d
				m.SegmentHeaderDigest = d.HeaderDigest
				m.PrefixDigest = d.PrefixDigest
				b, err = EncodeManifest(m)
				if err != nil {
					t.Fatal(err)
				}
				replayReplaceFile(t, f, manifestName(m.ManifestID), b)
				c, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
				if err != nil {
					t.Fatal(err)
				}
				c.ManifestDigest = Digest(sha256.Sum256(b))
				b, err = EncodeCurrent(c)
				if err != nil {
					t.Fatal(err)
				}
				replayReplaceFile(t, f, "CURRENT", b)
			}
			f.resetFaults()
			before := faultTreeSnapshot(t, f)
			got, report, err := OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
			if got != nil || !errors.Is(err, ErrCorruption) || report.PublicationSynced {
				t.Fatalf("published corruption fell back or was repaired: %+v %v", report, err)
			}
			if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("corruption rejection mutated files")
			}
			replayAssertNoMutation(t, f.calls)
		})
	}
}

func TestRotationSemanticReplayFailurePrecedesAllTailRepair(t *testing.T) {
	f, j, old, _ := rotationSeed(t)
	_, batch := rotationBatch(t, old)
	if _, err := j.PublishPreparedBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	m := rotationManifest(t, f)
	replayAppendTail(t, f, segmentName(m.SegmentID), []byte("unpublished active tail"))
	orphan := "segments/SEGMENT.ffffffffffffffffffffffffffffffff"
	if err := writeSyncedFile(f, orphan, []byte("unpublished orphan segment")); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("segments"); err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	before := faultTreeSnapshot(t, f)
	rejected := errors.New("host semantic rejection")
	got, report, err := OpenJournalWithReplay(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine}, func(ScannedTransaction, io.ReaderAt) error { return rejected })
	if got != nil || !errors.Is(err, rejected) || report.PublicationSynced {
		t.Fatalf("semantic rejection not propagated: %+v %v", report, err)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("semantic failure repaired active/orphan bytes")
	}
	replayAssertNoMutation(t, f.calls)
}

type rotationAcquireErrorIO struct {
	DurableIO
	armed    bool
	acquired *rotationCloseFile
}

type rotationCloseFile struct {
	DurableFile
	failOnce bool
	closed   bool
}

func (f *rotationCloseFile) Close() error {
	if f.failOnce {
		f.failOnce = false
		return errFaultIO
	}
	if err := f.DurableFile.Close(); err != nil {
		return err
	}
	f.closed = true
	return nil
}

func (d *rotationAcquireErrorIO) CreateExclusive(name string) (DurableFile, error) {
	f, err := d.DurableIO.CreateExclusive(name)
	if err != nil || !d.armed || !strings.HasPrefix(name, "segments/SEGMENT.") {
		return f, err
	}
	d.armed = false
	d.acquired = &rotationCloseFile{DurableFile: f, failOnce: true}
	return d.acquired, errFaultIO
}

func TestRotationAcquisitionErrorKeepsHandleAndOwnerUntilRetirement(t *testing.T) {
	f := newFaultIO()
	wrapped := &rotationAcquireErrorIO{DurableIO: f}
	j, err := CreateJournal(wrapped, JournalOptions{SegmentTargetBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	old := faultPublished(t, j)
	_, batch := rotationBatch(t, old)
	wrapped.armed = true
	got, err := j.PublishPreparedBatch(context.Background(), batch)
	if !errors.Is(err, ErrCommitUnknown) || got.Publication != (Publication{}) || got.Transactions != nil || wrapped.acquired == nil {
		t.Fatalf("new segment acquisition failure lost/acknowledged work: %+v %v", got, err)
	}
	f.resetFaults()
	if err := j.Close(); !errors.Is(err, errFaultIO) {
		t.Fatalf("failed child close was hidden: %v", err)
	}
	if wrapped.acquired.closed {
		t.Fatal("failed close marked acquired handle retired")
	}
	for _, call := range f.calls {
		if call.Op == "OwnerLock.Close" {
			t.Fatal("owner released before acquired new segment was retired")
		}
	}
	if owner, err := f.AcquireOwnerLock(); !errors.Is(err, errFaultLocked) {
		if owner != nil {
			_ = owner.Close()
		}
		t.Fatalf("failed close released exclusive owner: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if !wrapped.acquired.closed {
		t.Fatal("retry did not retire acquired segment handle")
	}
	owner, err := f.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRotationCancellationAfterSealStartsDoesNotInterruptP1(t *testing.T) {
	f, j, old, _ := rotationSeed(t)
	defer j.Close()
	_, batch := rotationBatch(t, old)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var canceled bool
	f.onBoundary = func(b faultBoundary) error {
		if !canceled && b.Op == "Sync" && strings.HasPrefix(b.Name, "segments/") && b.After {
			canceled = true
			cancel()
		}
		return nil
	}
	got, err := j.PublishPreparedBatch(ctx, batch)
	if !canceled || !errors.Is(ctx.Err(), context.Canceled) || err != nil || got.Publication != batchTip(old, batch) {
		t.Fatalf("cancel after seal interrupted the batch: %+v %v", got, err)
	}
}

// rotationUnpublishedLayout leaves a durable old CURRENT, a sealed-looking
// active suffix and synced successor files. Even complete seals/transactions
// cannot authorize promotion. The zero-byte orphan models a created name whose
// header was never successfully written.
func rotationUnpublishedLayout(t testing.TB) (*faultIO, Publication, Transaction) {
	t.Helper()
	f, j, old, first := rotationSeed(t)
	_, batch := rotationBatch(t, old)
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "ReplaceAtomic" && b.To == "CURRENT" && !b.After {
			return errFaultIO
		}
		return nil
	}
	if _, err := j.PublishPreparedBatch(context.Background(), batch); !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("fixture did not interrupt publication: %v", err)
	}
	f = f.crash()
	zero, err := f.CreateExclusive("segments/SEGMENT.ffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if err := zero.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("segments"); err != nil {
		t.Fatal(err)
	}
	f = f.crash()
	f.resetFaults()
	return f, old, first
}

func rotationAssertRemovedExtentsHaveDurableEvidence(t testing.TB, before, after map[string]string, m Manifest) {
	t.Helper()
	referenced := make(map[string]uint64)
	for _, d := range rotationDescriptors(m) {
		end := d.DataEnd
		if d.Sealed {
			end += SegmentFooterSize
		}
		referenced[segmentName(d.SegmentID)] = end
	}
	evidence := make(map[string]bool)
	for name, raw := range after {
		if strings.HasPrefix(name, "quarantine/") && !strings.HasSuffix(name, "/") {
			evidence[raw] = true
		}
	}
	for name, raw := range before {
		if !strings.HasPrefix(name, "segments/SEGMENT.") {
			continue
		}
		current, present := after[name]
		if end, ok := referenced[name]; ok {
			if !present || uint64(len(current)) < end || current[:end] != raw[:end] {
				t.Fatalf("interrupted repair damaged published bytes: %s", name)
			}
			if len(current) < len(raw) && !evidence[raw[end:]] {
				t.Fatalf("truncation became durable before full tail evidence: %s", name)
			}
		} else if !present {
			if !evidence[raw] {
				t.Fatalf("orphan removal became durable before full evidence: %s", name)
			}
		} else if current != raw {
			t.Fatalf("repair rewrote orphan bytes: %s", name)
		}
	}
}

func TestRotationQuarantineCrashAtEveryMutationPreservesEvidenceAndPrefix(t *testing.T) {
	f, old, _ := rotationUnpublishedLayout(t)
	reachableAfter := make(map[int]bool)
	f.onBoundary = func(b faultBoundary) error {
		if b.After {
			reachableAfter[b.Step] = true
		}
		return nil
	}
	j, report, err := OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
	if err != nil || report.Publication != old {
		t.Fatalf("baseline quarantine: %+v %v", report, err)
	}
	trace := append([]faultCall(nil), f.calls...)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if len(trace) == 0 {
		t.Fatal("empty quarantine trace")
	}
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			// Idempotent Close on an already retired handle has no state change
			// and no After hook in this fault model. Cover only real boundaries.
			if after && !reachableAfter[call.Step] {
				continue
			}
			t.Run(fmt.Sprintf("%03d-%s/after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f, old, first := rotationUnpublishedLayout(t)
				before := faultTreeSnapshot(t, f)
				m := rotationManifest(t, f)
				// The snapshot/manifest helpers close read handles, which the IO
				// model counts. Start the recovery trace only after inspection.
				f.resetFaults()
				f.failAt(call.Step, after)
				got, report, err := OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
				if got != nil || !errors.Is(err, errFaultIO) || report.PublicationSynced {
					t.Fatalf("repair mutation failure was hidden: %+v %v", report, err)
				}
				if len(f.calls) < call.Step || f.calls[call.Step-1].Op != call.Op {
					t.Fatalf("repair trace diverged: expected %+v actual %+v", call, f.calls)
				}
				f = f.crash()
				rotationAssertRemovedExtentsHaveDurableEvidence(t, before, faultTreeSnapshot(t, f), m)
				got, report, err = OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
				if err != nil || got == nil || !report.Verified || !report.PublicationSynced || report.Publication != old {
					t.Fatalf("retry changed old publication or failed: %+v %v", report, err)
				}
				rotationAssertEvidence(t, f, before, m)
				rotationAssertLogicalTransactions(t, got, []Transaction{first})
				if err := got.Close(); err != nil {
					t.Fatal(err)
				}
				f = f.crash()
				got, report, err = OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024})
				if err != nil || report.Publication != old || report.TailBytes != 0 || len(report.OrphanSegments) != 0 {
					t.Fatalf("repair retry was not durable: %+v %v", report, err)
				}
				if err := got.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRotationZeroByteOrphanRequiresExplicitRecovery(t *testing.T) {
	f, j, old, first := rotationSeed(t)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	name := "segments/SEGMENT.ffffffffffffffffffffffffffffffff"
	file, err := f.CreateExclusive(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("segments"); err != nil {
		t.Fatal(err)
	}
	f = f.crash()
	before := faultTreeSnapshot(t, f)
	m := rotationManifest(t, f)
	j, report, err := OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024})
	if j != nil || !errors.Is(err, ErrRecoveryRequired) || !report.Verified || report.PublicationSynced || report.Publication != old || len(report.OrphanSegments) != 1 {
		t.Fatalf("zero-byte orphan silently ignored/promoted: %+v %v", report, err)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("default open removed empty orphan")
	}
	j, report, err = OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
	if err != nil || report.Publication != old {
		t.Fatalf("explicit empty orphan repair: %+v %v", report, err)
	}
	defer j.Close()
	rotationAssertEvidence(t, f, before, m)
	rotationAssertLogicalTransactions(t, j, []Transaction{first})
}

func TestRotationUnmanagedSegmentFilenameIsPreservedAndRejected(t *testing.T) {
	for _, name := range []string{"segments/notes.txt", "segments/SEGMENT.not-an-id"} {
		t.Run(name, func(t *testing.T) {
			f, j, _, _ := rotationSeed(t)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if err := writeSyncedFile(f, name, []byte("not managed by ATE")); err != nil {
				t.Fatal(err)
			}
			if err := f.SyncDir("segments"); err != nil {
				t.Fatal(err)
			}
			f.resetFaults()
			before := faultTreeSnapshot(t, f)
			j, report, err := OpenJournal(f, JournalOptions{SegmentTargetBytes: 1024, TailPolicy: TailQuarantine})
			if j != nil || err == nil || report.PublicationSynced {
				t.Fatalf("unknown file was accepted or claimed repaired: %+v %v", report, err)
			}
			if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("unknown file was deleted or rewritten")
			}
			replayAssertNoMutation(t, f.calls)
		})
	}
}
