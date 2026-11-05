package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
)

type fullEOFFile struct {
	DurableFile
	short bool
}

func (f fullEOFFile) ReadAt(p []byte, off int64) (int, error) {
	if f.short && off == 0 && len(p) == SegmentHeaderSize {
		n, _ := f.DurableFile.ReadAt(p[:len(p)-1], off)
		return n, io.EOF
	}
	n, err := f.DurableFile.ReadAt(p, off)
	if err == nil && n == len(p) {
		return n, io.EOF
	}
	return n, err
}

type fullEOFIO struct {
	DurableIO
	short bool
}

func (d fullEOFIO) OpenAppend(name string) (DurableFile, error) {
	f, err := d.DurableIO.OpenAppend(name)
	if err != nil {
		return nil, err
	}
	return fullEOFFile{f, d.short}, nil
}

func TestJournalReadAtFullEOFAndMissingHeader(t *testing.T) {
	f, j, acknowledged, _ := faultSeedAcknowledged(t)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, short := range []bool{false, true} {
		opened, report, err := OpenJournal(fullEOFIO{f, short}, JournalOptions{})
		if short {
			if opened != nil || !errors.Is(err, ErrCorruption) || !errors.Is(err, io.ErrUnexpectedEOF) || report.Verified {
				t.Fatalf("missing published header misclassified: %v %+v", err, report)
			}
		} else {
			if err != nil || !report.Verified || !report.PublicationSynced || faultPublished(t, opened) != acknowledged {
				t.Fatalf("legal full EOF rejected: %v %+v", err, report)
			}
			if err = opened.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestJournalValidationAndResourceLimitsDoNotWrite(t *testing.T) {
	f, j, p, first := faultSeedAcknowledged(t)
	defer j.Close()
	cases := []struct {
		name string
		edit func(*Transaction)
		want error
	}{
		{"stale-position", func(tx *Transaction) { tx.LSN-- }, ErrPrecondition},
		{"wrong-previous", func(tx *Transaction) { tx.PreviousDigest[0] ^= 1 }, ErrPrecondition},
		{"duplicate-tx", func(tx *Transaction) { tx.TxID = first.TxID }, ErrDuplicateTransaction},
		{"malformed", func(tx *Transaction) { tx.Request.Space = "" }, ErrMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := faultTreeSnapshot(t, f)
			f.resetFaults()
			tx := faultNextTransaction(p, 2)
			tc.edit(&tx)
			if result, err := j.Publish(context.Background(), tx); !errors.Is(err, tc.want) || result != (Publication{}) {
				t.Fatalf("%+v %v", result, err)
			}
			if len(f.calls) != 0 || !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) || faultPublished(t, j) != p {
				t.Fatal("rejection changed store")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.resetFaults()
	if _, err := j.Publish(ctx, faultNextTransaction(p, 2)); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatalf("canceled command wrote: %v", err)
	}
}

func TestJournalExplicitPublishedTransactionBudget(t *testing.T) {
	f := newFaultIO()
	j, err := CreateJournal(f, JournalOptions{MaxPublishedTransactions: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err := j.Publish(context.Background(), faultNextTransaction(faultPublished(t, j), 1))
	if err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	if _, err = j.Publish(context.Background(), faultNextTransaction(p, 2)); !errors.Is(err, ErrLimit) || len(f.calls) != 0 {
		t.Fatalf("transaction budget: %v", err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, _, err = OpenJournal(f, JournalOptions{MaxPublishedTransactions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = CreateJournal(newFaultIO(), JournalOptions{MaxPublishedTransactions: MaxPublishedTransactions + 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	// The byte ceiling also rejects before an append and does not fence ready state.
	limited := newFaultIO()
	j, err = CreateJournal(limited, JournalOptions{MaxPublishedBytes: SegmentHeaderSize})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	limited.resetFaults()
	if _, err = j.Publish(context.Background(), faultNextTransaction(faultPublished(t, j), 1)); !errors.Is(err, ErrLimit) || len(limited.calls) != 0 {
		t.Fatalf("byte budget: %v", err)
	}
}

func TestJournalCorruptionNeverRepairsPublishedPrefix(t *testing.T) {
	for _, name := range []string{"CURRENT", "IDENTITY", "manifest", "header", "body", "missing-prefix", "tip-root"} {
		t.Run(name, func(t *testing.T) {
			f, j, p, _ := faultSeedAcknowledged(t)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			current, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
			if err != nil {
				t.Fatal(err)
			}
			segment, data := faultSegmentBytes(t, f)
			target := name
			var bad []byte
			switch name {
			case "CURRENT", "IDENTITY":
				bad = []byte(faultReadAll(t, f, name))
				bad[8] ^= 1
			case "manifest":
				target = manifestName(current.ManifestID)
				bad = []byte(faultReadAll(t, f, target))
				bad[8] ^= 1
			case "header":
				target = segment
				bad = data
				bad[20] ^= 1
			case "body":
				target = segment
				bad = data
				bad[SegmentHeaderSize+HeaderSize] ^= 1
			case "missing-prefix":
				target = segment
				bad = data[:len(data)-1]
			case "tip-root":
				target = segment
				frames := unpackFrames(t, data[SegmentHeaderSize:])
				last := &frames[len(frames)-1]
				commit, err := decodeCommit(last.Payload)
				if err != nil {
					t.Fatal(err)
				}
				commit.root[0] ^= 1
				last.Payload = encodeCommit(commit)
				bad = append(bytes.Clone(data[:SegmentHeaderSize]), repackFrames(t, frames)...)
			}
			// Deliberate on-disk corruption outside the cooperating-writer model.
			file, err := f.OpenAppend(target)
			if err != nil {
				t.Fatal(err)
			}
			if err = file.Truncate(0); err != nil {
				t.Fatal(err)
			}
			if err = writeAll(file, bad); err != nil {
				t.Fatal(err)
			}
			if err = file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			before := faultTreeSnapshot(t, f)
			opened, report, err := OpenJournal(f, JournalOptions{TailPolicy: TailQuarantine})
			if !errors.Is(err, ErrCorruption) || opened != nil || report.Verified {
				t.Fatalf("corruption accepted: %v %+v (old %+v)", err, report, p)
			}
			if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("repair modified corrupt published prefix")
			}
		})
	}
}

func TestJournalMissingCurrentDoesNotSelectOrphan(t *testing.T) {
	f, j, _, _ := faultSeedAcknowledged(t)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("CURRENT"); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	before := faultTreeSnapshot(t, f)
	opened, report, err := OpenJournal(f, JournalOptions{TailPolicy: TailQuarantine})
	if opened != nil || !errors.Is(err, ErrIncompleteStore) || report.Verified {
		t.Fatalf("orphan promoted: %v %+v", err, report)
	}
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("open changed incomplete store")
	}
	if opened, err := CreateJournal(f, JournalOptions{}); opened != nil || !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("create overwrote incomplete store: %v", err)
	}
}

func TestJournalUnpublishedTailIsPreservedRegardlessOfStructure(t *testing.T) {
	for _, kind := range []string{"partial-header", "partial-record", "partial-transaction", "bad-crc", "garbage", "complete"} {
		t.Run(kind, func(t *testing.T) {
			f, j, p, _ := faultSeedAcknowledged(t)
			tail, _, err := EncodeTransaction(faultNextTransaction(p, 2), Limits{})
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "partial-header":
				tail = tail[:HeaderSize-1]
			case "partial-record":
				tail = tail[:HeaderSize+1]
			case "partial-transaction":
				tail = tail[:len(tail)-1]
			case "bad-crc":
				tail[HeaderSize] ^= 1
			case "garbage":
				tail = []byte("unpublished, not a frame")
			}
			name := segmentName(j.segmentID)
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			file, err := f.OpenAppend(name)
			if err != nil {
				t.Fatal(err)
			}
			if err = writeAll(file, tail); err != nil {
				t.Fatal(err)
			}
			if err = file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			f = f.crash()
			opened, report, err := OpenJournal(f, JournalOptions{})
			if opened != nil || !errors.Is(err, ErrRecoveryRequired) || !report.Verified || report.Publication != p || report.TailBytes != uint64(len(tail)) {
				t.Fatalf("tail mistaken for published corruption or commit: %v %+v", err, report)
			}
			opened, report, err = OpenJournal(f, JournalOptions{TailPolicy: TailQuarantine})
			if err != nil || faultPublished(t, opened) != p || faultReadAll(t, f, report.QuarantineFile) != string(tail) {
				t.Fatalf("tail not preserved exactly: %v %+v", err, report)
			}
			if err = opened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJournalConcurrentPreparedTransactionsExtendOnce(t *testing.T) {
	f := newFaultIO()
	j, err := CreateJournal(f, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial := faultPublished(t, j)
	const writers = 16
	start := make(chan struct{})
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 1; i <= writers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			_, err := j.Publish(context.Background(), faultNextTransaction(initial, n))
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrPrecondition) {
			t.Fatal(err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d prepared extensions of the same prefix", accepted)
	}
	p := faultPublished(t, j)
	if p.Position.LSN != 1 || p.Generation != 1 {
		t.Fatalf("multiple physical extensions: %+v", p)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, report, err := OpenJournal(f.crash(), JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if faultPublished(t, recovered) != p || report.ScannedTransactions != 1 {
		t.Fatal("concurrent publication changed on recovery")
	}
	if err = recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

type cancelOnSizeFile struct {
	DurableFile
	cancel context.CancelFunc
}

func (f cancelOnSizeFile) Size() (int64, error) {
	n, err := f.DurableFile.Size()
	f.cancel()
	return n, err
}

func TestJournalCancellationDuringPreAppendReadDoesNotWrite(t *testing.T) {
	f := newFaultIO()
	j, err := CreateJournal(f, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	j.segment = cancelOnSizeFile{j.segment, cancel}
	p := faultPublished(t, j)
	f.resetFaults()
	got, err := j.Publish(ctx, faultNextTransaction(p, 1))
	if !errors.Is(err, context.Canceled) || got != (Publication{}) || len(f.calls) != 0 || faultPublished(t, j) != p {
		t.Fatalf("cancellation after Size still appended: %v %+v", err, got)
	}
}
