package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"
)

func replayAppendTail(t testing.TB, f *faultIO, name string, tail []byte) {
	t.Helper()
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
}

func replayReplaceFile(t testing.TB, f *faultIO, name string, contents []byte) {
	t.Helper()
	file, err := f.OpenAppend(name)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if err = writeAll(file, contents); err != nil {
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func replayAssertNoMutation(t testing.TB, calls []faultCall) {
	t.Helper()
	for _, call := range calls {
		switch call.Op {
		case "AcquireOwnerLock", "OpenAppend", "Close", "OwnerLock.Close":
		default:
			t.Fatalf("replay rejection attempted mutation or witness sync: %+v", calls)
		}
	}
}

func TestReplayScannerRecordLocationsRemainStreamRelative(t *testing.T) {
	tx := exampleTransaction()
	tx.Records[1].Payload = []byte("object with padding")
	b, _ := mustEncode(t, tx)
	s := scannerFor(t, b, Limits{})
	got, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	var expected []RecordLocation
	reader := bytes.NewReader(b)
	var offset int64
	for {
		frame, n, err := ReadFrame(reader, Limits{})
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if frame.Kind.body() {
			expected = append(expected, RecordLocation{offset + HeaderSize, uint32(len(frame.Payload))})
		}
		offset += n
	}
	if got.Store != "" || !reflect.DeepEqual(got.RecordLocations, expected) || !reflect.DeepEqual(got.Transaction, tx) {
		t.Fatalf("body locations differ from physical frames: got=%+v want=%+v", got.RecordLocations, expected)
	}
	for i, location := range got.RecordLocations {
		if !bytes.Equal(b[location.PayloadOffset:location.PayloadOffset+int64(location.PayloadLength)], tx.Records[i].Payload) {
			t.Fatalf("record %d points outside its payload", i)
		}
	}
}

func TestJournalReplayReadsLocationsHistoryAndOnlyPublishedBytes(t *testing.T) {
	f, j, firstPublication, first := faultSeedAcknowledged(t)
	second := faultNextTransaction(firstPublication, 2)
	second.Records = []Record{{KindObject, []byte("second object")}, {KindStateOps, []byte{}}}
	last, err := j.Publish(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	name, segment := faultSegmentBytes(t, f)
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	tail := []byte("unpublished diagnostic bytes")
	replayAppendTail(t, f, name, tail)
	f.resetFaults()
	var history RecordLocation
	var retained io.ReaderAt
	previousEnd := int64(SegmentHeaderSize)
	seen := 0
	opened, report, err := OpenJournalWithReplay(f, JournalOptions{TailPolicy: TailQuarantine}, func(tx ScannedTransaction, reader io.ReaderAt) error {
		replayAssertNoMutation(t, f.calls)
		if _, ok := reader.(io.Closer); ok {
			t.Fatal("callback can close the journal file")
		}
		if _, ok := reader.(io.Writer); ok {
			t.Fatal("callback can write the journal file")
		}
		if _, ok := reader.(io.WriterAt); ok {
			t.Fatal("callback can write at an offset")
		}
		want := []Transaction{first, second}[seen]
		if tx.Store != last.Store || tx.StartOffset != previousEnd || tx.EndOffset <= tx.StartOffset || !reflect.DeepEqual(tx.Transaction, want) {
			t.Fatalf("transaction offsets are not absolute/contiguous: %+v", tx)
		}
		previousEnd = tx.EndOffset
		if len(tx.RecordLocations) != len(tx.Records) {
			t.Fatal("location count includes non-body frames")
		}
		for i, location := range tx.RecordLocations {
			payload := make([]byte, location.PayloadLength)
			n, err := reader.ReadAt(payload, location.PayloadOffset)
			if err != nil || n != len(payload) || !bytes.Equal(payload, tx.Records[i].Payload) {
				t.Fatalf("record %d location=%+v read=%q/%d err=%v", i, location, payload, n, err)
			}
		}
		if seen == 0 {
			history = tx.RecordLocations[1]
		} else {
			payload := make([]byte, history.PayloadLength)
			if n, err := reader.ReadAt(payload, history.PayloadOffset); err != nil || n != len(payload) || !bytes.Equal(payload, first.Records[1].Payload) {
				t.Fatalf("earlier object unavailable: %q %v", payload, err)
			}
		}
		buffer := []byte{0x7f, 0x7f, 0x7f}
		if n, err := reader.ReadAt(buffer, int64(last.PublishedEnd)-1); n != 1 || err != io.EOF || buffer[0] != segment[len(segment)-1] || buffer[1] != 0x7f {
			t.Fatalf("reader exposed unpublished tail: n=%d err=%v bytes=%x", n, err, buffer)
		}
		if n, err := reader.ReadAt(buffer, int64(last.PublishedEnd)); n != 0 || err != io.EOF {
			t.Fatalf("read at publication end: %d %v", n, err)
		}
		if n, err := reader.ReadAt(buffer, math.MaxInt64); n != 0 || err != io.EOF {
			t.Fatalf("huge offset overflowed: %d %v", n, err)
		}
		if n, err := reader.ReadAt(buffer, -1); n != 0 || !errors.Is(err, ErrMalformed) {
			t.Fatalf("negative offset: %d %v", n, err)
		}
		retained = reader
		seen++
		return nil
	})
	if err != nil || opened == nil || seen != 2 || previousEnd != int64(last.PublishedEnd) || report.Publication != last || !report.Verified || !report.PublicationSynced || report.TailDisposition != "quarantined" {
		t.Fatalf("replay open: seen=%d report=%+v err=%v", seen, report, err)
	}
	if n, err := retained.ReadAt(make([]byte, 1), 0); n != 0 || !errors.Is(err, ErrClosed) {
		t.Fatalf("borrowed reader survived callback: %d %v", n, err)
	}
	if faultReadAll(t, f, report.QuarantineFile) != string(tail) {
		t.Fatal("successful replay did not preserve the tail")
	}
	if err = opened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJournalReplayFailurePrecedesEveryTailMutation(t *testing.T) {
	original := errors.New("semantic replay failed")
	for _, classification := range []error{nil, ErrCorruption, ErrUnsupported, ErrMalformed, io.ErrUnexpectedEOF} {
		t.Run(fmt.Sprint(classification), func(t *testing.T) {
			f, j, _, _ := faultSeedAcknowledged(t)
			name := segmentName(j.segmentID)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			replayAppendTail(t, f, name, []byte("tail must survive replay failure"))
			before := faultTreeSnapshot(t, f)
			f.resetFaults()
			callbackErr := errors.Join(original, classification)
			opened, report, err := OpenJournalWithReplay(f, JournalOptions{TailPolicy: TailQuarantine}, func(ScannedTransaction, io.ReaderAt) error {
				return callbackErr
			})
			if opened != nil || report.Verified || report.PublicationSynced || !errors.Is(err, original) || errors.Is(err, ErrCommitUnknown) {
				t.Fatalf("callback error yielded usable recovery or lost cause: %+v %v", report, err)
			}
			if classification != nil && !errors.Is(err, classification) {
				t.Fatalf("callback classification lost: %v", err)
			}
			if errors.Is(err, ErrCorruption) != errors.Is(callbackErr, ErrCorruption) || errors.Is(err, ErrRecoveryIncomplete) {
				t.Fatalf("callback error was guessed to be corruption/recovery: %v", err)
			}
			replayAssertNoMutation(t, f.calls)
			if f.locked || !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("callback rejection changed store or leaked ownership")
			}
		})
	}
}

func TestJournalReplayFinalAuthenticationFailureCannotReturnReady(t *testing.T) {
	f, j, _, _ := faultSeedAcknowledged(t)
	name := segmentName(j.segmentID)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeManifest([]byte(faultReadAll(t, f, manifestName(current.ManifestID))))
	if err != nil {
		t.Fatal(err)
	}
	m.PrefixDigest[0] ^= 1
	manifest, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	current.ManifestDigest = Digest(sha256.Sum256(manifest))
	currentBytes, err := EncodeCurrent(current)
	if err != nil {
		t.Fatal(err)
	}
	replayReplaceFile(t, f, manifestName(current.ManifestID), manifest)
	replayReplaceFile(t, f, "CURRENT", currentBytes)
	replayAppendTail(t, f, name, []byte("tail"))
	before := faultTreeSnapshot(t, f)
	f.resetFaults()
	calls := 0
	opened, report, err := OpenJournalWithReplay(f, JournalOptions{TailPolicy: TailQuarantine}, func(ScannedTransaction, io.ReaderAt) error {
		calls++
		return nil
	})
	if opened != nil || calls != 1 || report.Verified || report.PublicationSynced || !errors.Is(err, ErrCorruption) {
		t.Fatalf("successful callbacks escaped final prefix verification: calls=%d report=%+v err=%v", calls, report, err)
	}
	replayAssertNoMutation(t, f.calls)
	if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) || f.locked {
		t.Fatal("final authentication failure modified store or leaked ownership")
	}
}

func TestJournalReplayRetainsOwnerUntilFailedCleanupFinishes(t *testing.T) {
	f, j, _, _ := faultSeedAcknowledged(t)
	name := segmentName(j.segmentID)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Close" && b.Name == name && !b.After {
			return errFaultIO
		}
		return nil
	}
	original := errors.New("semantic failure")
	opened, _, err := OpenJournalWithReplay(f, JournalOptions{}, func(ScannedTransaction, io.ReaderAt) error { return original })
	var cleanup *CleanupError
	if opened != nil || !errors.Is(err, original) || !errors.Is(err, errFaultIO) || !errors.As(err, &cleanup) || !f.locked {
		t.Fatalf("replay cleanup lost its cause, handle, or owner: %v", err)
	}
	cleanupAssertNoOwnerClose(t, f.calls)
	f.resetFaults()
	if err = cleanup.Close(); err != nil {
		t.Fatal(err)
	}
	cleanupAssertCloseOrder(t, f.calls, name)
	if f.locked {
		t.Fatal("finished replay cleanup retained owner")
	}
}

type replayReadErrorFile struct {
	DurableFile
	err error
}

func (f replayReadErrorFile) ReadAt([]byte, int64) (int, error) { return 0, f.err }

func TestJournalReadAtBoundsAndSemanticFence(t *testing.T) {
	f, j, p, _ := faultSeedAcknowledged(t)
	_, segment := faultSegmentBytes(t, f)
	// Out-of-contract bytes after CURRENT are invisible to reads even before a
	// publisher would detect and fence an externally extended file.
	replayAppendTail(t, f, segmentName(j.segmentID), []byte("unpublished"))
	buffer := make([]byte, 3)
	if n, err := j.ReadAt(buffer, int64(p.PublishedEnd)-1); n != 1 || err != io.EOF || buffer[0] != segment[len(segment)-1] {
		t.Fatalf("read crossed publication bound: %d %v", n, err)
	}
	for _, off := range []int64{int64(p.PublishedEnd), math.MaxInt64} {
		if n, err := j.ReadAt(buffer, off); n != 0 || err != io.EOF {
			t.Fatalf("past publication end: %d %v", n, err)
		}
	}
	if n, err := j.ReadAt(nil, int64(p.PublishedEnd)); n != 0 || err != nil {
		t.Fatalf("empty read: %d %v", n, err)
	}
	if n, err := j.ReadAt(buffer, -1); n != 0 || !errors.Is(err, ErrMalformed) {
		t.Fatalf("negative offset: %d %v", n, err)
	}
	if err := j.Fence(nil); !errors.Is(err, ErrMalformed) || j.Status().State != JournalReady {
		t.Fatalf("nil cause fenced ready journal: %v", err)
	}
	physical := j.segment
	readErr := errors.New("transient read failure")
	j.segment = replayReadErrorFile{physical, readErr}
	if n, err := j.ReadAt(buffer, 0); n != 0 || !errors.Is(err, readErr) || errors.Is(err, ErrCorruption) || j.Status().State != JournalReady {
		t.Fatalf("read IO was reclassified or fenced: %d %v", n, err)
	}
	j.segment = physical
	cause := fmt.Errorf("object digest: %w", ErrCorruption)
	err := j.Fence(cause)
	if !errors.Is(err, ErrStoreFenced) || !errors.Is(err, cause) || errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("semantic fence misclassified publication outcome: %v", err)
	}
	if status := j.Status(); status.State != JournalFenced || status.LastPublication != p {
		t.Fatalf("semantic fence changed last publication: %+v", status)
	}
	if n, err := j.ReadAt(buffer, 0); n != 0 || !errors.Is(err, ErrStoreFenced) || !errors.Is(err, ErrCorruption) {
		t.Fatalf("fenced journal exposed new read: %d %v", n, err)
	}
	if err := j.Fence(errors.New("second cause")); !errors.Is(err, cause) {
		t.Fatalf("repeated fence erased original cause: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := j.ReadAt(buffer, 0); n != 0 || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed journal exposed read: %d %v", n, err)
	}
	if err := j.Fence(cause); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed journal fenced: %v", err)
	}
}

func TestJournalReadAtRejectsClosing(t *testing.T) {
	f, j, p, _ := faultSeedAcknowledged(t)
	f.resetFaults()
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Close" && b.Name == segmentName(j.segmentID) && !b.After {
			return errFaultIO
		}
		return nil
	}
	if err := j.Close(); !errors.Is(err, errFaultIO) {
		t.Fatal(err)
	}
	if n, err := j.ReadAt(make([]byte, 1), 0); n != 0 || !errors.Is(err, ErrClosing) {
		t.Fatalf("closing journal exposed read: %d %v", n, err)
	}
	if err := j.Fence(ErrCorruption); !errors.Is(err, ErrClosing) || j.Status().LastPublication != p {
		t.Fatalf("closing journal changed fence: %v", err)
	}
	f.resetFaults()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}
