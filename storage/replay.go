package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// PublishedTransaction adds the logical body payload locations from a
// successful publication. Failed publications return the zero value, including
// a nil locations slice; staging bytes never grant readable object locations.
type PublishedTransaction struct {
	Publication     Publication
	RecordLocations []RecordLocation
}

// PublishWithRecords follows the same serialized P1 publication path as Publish
// and returns locations only after CURRENT's final directory sync succeeds.
// Locations correspond one-for-one with tx.Records and can be read with ReadAt.
func (j *Journal) PublishWithRecords(ctx context.Context, tx Transaction) (PublishedTransaction, error) {
	return j.publish(ctx, tx)
}

// publishedRecordLocations walks only the frame headers of bytes just produced
// by EncodeTransaction. The owned encoding has already fixed and validated each
// kind and payload length, so no second payload allocation or re-encoding is
// needed. This helper must not be used to authenticate external journal input.
func publishedRecordLocations(encoded []byte, base int64) []RecordLocation {
	var locations []RecordLocation
	for at := 0; at < len(encoded); {
		kind := Kind(binary.LittleEndian.Uint16(encoded[at+10 : at+12]))
		length := binary.LittleEndian.Uint32(encoded[at+20 : at+24])
		if kind.body() {
			locations = append(locations, RecordLocation{PayloadOffset: base + int64(at) + HeaderSize, PayloadLength: length})
		}
		at += HeaderSize + int(length) + padding(length) + TrailerSize
	}
	return locations
}

// ReplayFunc checks a physically valid transaction while OpenJournalWithReplay
// reconstructs the complete published prefix. Store comes from the authenticated
// identity, and offsets and RecordLocations are logical log offsets. The
// reader is borrowed for this invocation only,
// exposes no close or write operations, and cannot read beyond PublishedEnd.
// It may read earlier records, but callbacks must not reenter the Journal.
//
// A callback must keep reconstructed state private until Open succeeds: a later
// transaction or the final prefix authentication may still reject the store.
// Callback errors are wrapped with their replay stage, without reclassification.
type ReplayFunc func(ScannedTransaction, io.ReaderAt) error

// OpenJournalWithReplay follows CURRENT and runs replay before any quarantine,
// truncation, or publication-witness sync. It returns a usable Journal only
// after the entire physical prefix and every callback have succeeded.
func OpenJournalWithReplay(d DurableIO, options JournalOptions, replay ReplayFunc) (*Journal, RecoveryReport, error) {
	return openJournal(d, options, replay)
}

// boundedReaderAt retains segment-relative addressing while hiding all bytes
// outside an immutable published prefix. Subtracting before converting the
// length avoids overflow for offsets near the ReaderAt range limit.
type boundedReaderAt struct {
	reader io.ReaderAt
	end    int64
}

func (r boundedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative read offset", ErrMalformed)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.end {
		return 0, io.EOF
	}
	length := len(p)
	if int64(length) > r.end-off {
		length = int(r.end - off)
	}
	n, err := r.reader.ReadAt(p[:length], off)
	if err == nil && (n < len(p)) {
		err = io.EOF
	}
	return n, err
}

// replayReader revokes a borrowed view when its callback returns. Its sole
// exported method is ReadAt, so the underlying writable/closable file cannot be
// recovered through an interface assertion.
type replayReader struct {
	mu     sync.Mutex
	reader boundedReaderAt
	active bool
}

func (r *replayReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active {
		return 0, ErrClosed
	}
	return r.reader.ReadAt(p, off)
}

func (r *replayReader) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active = false
	r.reader.reader = nil
}

// ReadAt captures the current published prefix under the state mutex and reads
// its retained files outside that mutex. Underlying read failures remain IO
// failures; a concurrent semantic Fence suppresses the in-progress result.
func (j *Journal) ReadAt(p []byte, off int64) (int, error) {
	j.mu.Lock()
	if err := j.ready(); err != nil {
		j.mu.Unlock()
		return 0, err
	}
	reader := j.beginRead(j.logicalReader())
	j.mu.Unlock()
	n, err := reader.ReadAt(p, off)
	return j.finishRead(reader, p, n, err)
}

// Fence disables new reads and publications after a trusted semantic layer
// finds invalid content. It preserves LastPublication and does not imply an
// uncertain commit, because no publication was attempted by this operation.
func (j *Journal) Fence(cause error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if cause == nil {
		return fmt.Errorf("%w: missing fence cause", ErrMalformed)
	}
	j.state = JournalFenced
	j.fenceCause = cause
	return fmt.Errorf("%w: %w", ErrStoreFenced, cause)
}
