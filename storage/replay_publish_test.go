package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestJournalPublishWithRecordsReturnsPublishedPayloadLocations(t *testing.T) {
	f, j, previous, _ := faultSeedAcknowledged(t)
	defer j.Close()
	tx := faultNextTransaction(previous, 2)
	tx.Records = []Record{
		{KindObject, []byte("object payload with padding")},
		{KindStateOps, []byte{}},
		{KindEvent, bytes.Repeat([]byte("event"), 17)},
	}
	result, err := j.PublishWithRecords(context.Background(), tx)
	if err != nil || result.Publication != faultPublished(t, j) || result.Publication.Position.LSN != tx.LSN || len(result.RecordLocations) != len(tx.Records) {
		t.Fatalf("detailed publication: %+v %v", result, err)
	}
	for i, location := range result.RecordLocations {
		payload := make([]byte, location.PayloadLength)
		if location.PayloadOffset < int64(previous.PublishedEnd) || location.PayloadOffset+int64(location.PayloadLength) > int64(result.Publication.PublishedEnd) {
			t.Fatalf("record %d location is outside the newly published transaction: %+v", i, location)
		}
		if n, err := j.ReadAt(payload, location.PayloadOffset); err != nil || n != len(payload) || !bytes.Equal(payload, tx.Records[i].Payload) {
			t.Fatalf("record %d payload mismatch: got=%q n=%d error=%v", i, payload, n, err)
		}
	}
	// Compare the independent physical scanner's locations and commitment to
	// ensure detailed publication reports exactly the bytes that were written.
	_, segment := faultSegmentBytes(t, f)
	scanner, err := NewScanner(bytes.NewReader(segment[previous.PublishedEnd:result.Publication.PublishedEnd]), previous.Position, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	scanned, err := scanner.Next()
	if err != nil || scanned.CommitDigest != result.Publication.Position.Digest {
		t.Fatalf("published bytes failed independent scan: %v", err)
	}
	for i := range scanned.RecordLocations {
		scanned.RecordLocations[i].PayloadOffset += int64(previous.PublishedEnd)
	}
	if !reflect.DeepEqual(scanned.RecordLocations, result.RecordLocations) {
		t.Fatalf("runtime locations differ from scan: runtime=%+v scan=%+v", result.RecordLocations, scanned.RecordLocations)
	}
	if _, err := scanner.Next(); err != io.EOF {
		t.Fatalf("detailed publication contained extra transaction: %v", err)
	}
	// No-body transactions publish successfully and have no synthetic begin or
	// metadata locations.
	empty := faultNextTransaction(result.Publication, 3)
	empty.Records = nil
	noBodies, err := j.PublishWithRecords(context.Background(), empty)
	if err != nil || noBodies.Publication != faultPublished(t, j) || noBodies.RecordLocations != nil {
		t.Fatalf("empty-body publication: %+v %v", noBodies, err)
	}
}

func TestJournalPublishWithRecordsFailuresNeverReturnStagingLocations(t *testing.T) {
	baselineIO, baselineJournal, baselinePublication, _ := faultSeedAcknowledged(t)
	_, err := baselineJournal.PublishWithRecords(context.Background(), faultNextTransaction(baselinePublication, 2))
	if err != nil {
		t.Fatal(err)
	}
	steps := len(baselineIO.calls)
	if err = baselineJournal.Close(); err != nil {
		t.Fatal(err)
	}
	for step := 1; step <= steps; step++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step=%d/after=%t", step, after), func(t *testing.T) {
				f, j, previous, _ := faultSeedAcknowledged(t)
				f.failAt(step, after)
				result, err := j.PublishWithRecords(context.Background(), faultNextTransaction(previous, 2))
				if !errors.Is(err, errFaultIO) || !errors.Is(err, ErrCommitUnknown) || !reflect.DeepEqual(result, PublishedTransaction{}) {
					t.Fatalf("failed mutation exposed staging locations or lost uncertainty: result=%+v err=%v", result, err)
				}
				if status := j.Status(); status.State != JournalFenced || status.LastPublication != previous {
					t.Fatalf("failed detailed publication changed active publication: %+v", status)
				}
				f.resetFaults()
				result, err = j.PublishWithRecords(context.Background(), faultNextTransaction(previous, 3))
				if !errors.Is(err, ErrStoreFenced) || !reflect.DeepEqual(result, PublishedTransaction{}) || len(f.calls) != 0 {
					t.Fatalf("fenced detailed publication exposed locations or attempted IO: %+v %v", result, err)
				}
				if err = j.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestJournalPublishWithRecordsPreAppendRejectionsHaveNoLocations(t *testing.T) {
	f, j, previous, _ := faultSeedAcknowledged(t)
	defer j.Close()
	stale := faultNextTransaction(previous, 2)
	stale.LSN--
	result, err := j.PublishWithRecords(context.Background(), stale)
	if !errors.Is(err, ErrPrecondition) || !reflect.DeepEqual(result, PublishedTransaction{}) || len(f.calls) != 0 {
		t.Fatalf("precondition failure exposed locations or wrote: %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = j.PublishWithRecords(ctx, faultNextTransaction(previous, 2))
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(result, PublishedTransaction{}) || len(f.calls) != 0 {
		t.Fatalf("canceled detailed publication exposed locations or wrote: %+v %v", result, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	j.segment = cancelOnSizeFile{j.segment, cancel}
	result, err = j.PublishWithRecords(ctx, faultNextTransaction(previous, 2))
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(result, PublishedTransaction{}) || len(f.calls) != 0 || j.Status().State != JournalReady {
		t.Fatalf("pre-append cancellation exposed locations, wrote, or fenced: %+v %v", result, err)
	}
}
