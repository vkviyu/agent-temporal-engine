package storage

import (
	"errors"
	"fmt"
	"io"
)

var (
	ErrMaintenance   = errors.New("journal maintenance is in progress")
	ErrReadersPinned = errors.New("journal readers still pin a physical layout")
	ErrRetirement    = errors.New("published layout has pending physical retirement")
)

const MaxReaderPins = 4096

// PinnedReader is a bounded snapshot of a verified publication layout. Close
// rejects new reads; active read leases retain its files until completion.
// A pin survives compaction but never a semantic Fence.
type PinnedReader struct {
	journal     *Journal
	publication Publication
	segments    []readSegment
	closed      bool
}

func (j *Journal) PinReader() (*PinnedReader, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return nil, err
	}
	if j.state == JournalMaintenance {
		return nil, ErrMaintenance
	}
	if len(j.readerPins) >= MaxReaderPins {
		return nil, ErrLimit
	}
	if j.readerPins == nil {
		j.readerPins = map[*PinnedReader]struct{}{}
	}
	if len(j.segments) == 0 {
		return nil, fmt.Errorf("%w: missing published layout", ErrMalformed)
	}
	reader := j.logicalReader()
	p := &PinnedReader{journal: j, publication: j.publication, segments: reader.segments}
	j.readerPins[p] = struct{}{}
	return p, nil
}
func (p *PinnedReader) Publication() Publication { return p.publication }
func (p *PinnedReader) ReadAt(b []byte, off int64) (int, error) {
	if p == nil || p.journal == nil {
		return 0, ErrClosed
	}
	j := p.journal
	j.mu.Lock()
	if p.closed || j.state == JournalClosed {
		j.mu.Unlock()
		return 0, ErrClosed
	}
	if j.fenceCause != nil {
		err := fmt.Errorf("%w: %w", ErrStoreFenced, j.fenceCause)
		j.mu.Unlock()
		return 0, err
	}
	reader := j.beginRead(logicalReader{segments: p.segments, end: p.publication.PublishedEnd})
	j.mu.Unlock()
	n, err := reader.ReadAt(b, off)
	return j.finishRead(reader, b, n, err)
}
func (p *PinnedReader) Close() error {
	if p == nil || p.journal == nil {
		return nil
	}
	j := p.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if !p.closed {
		p.closed = true
		delete(j.readerPins, p)
	}
	return nil
}

var _ io.ReaderAt = (*PinnedReader)(nil)

// beginRead is called under j.mu. An active lease is independent of the
// persistent Pin, so Pin.Close cannot retire files still used by ReadAt.
func (j *Journal) beginRead(reader logicalReader) *logicalReader {
	if j.activeReads == nil {
		j.activeReads = make(map[*logicalReader]struct{})
	}
	if len(j.activeReads) == 0 {
		j.readsDone = make(chan struct{})
	}
	j.activeReads[&reader] = struct{}{}
	return &reader
}

func (j *Journal) finishRead(reader *logicalReader, buffer []byte, n int, err error) (int, error) {
	j.mu.Lock()
	if j.fenceCause != nil {
		err = fmt.Errorf("%w: %w", ErrStoreFenced, j.fenceCause)
		j.mu.Unlock()
		// Keep the lease while suppressing the result. Close can wait without
		// owning the state mutex, including when it has changed State to Closing.
		clear(buffer)
		j.mu.Lock()
		n = 0
	}
	delete(j.activeReads, reader)
	if len(j.activeReads) == 0 {
		close(j.readsDone)
		j.readsDone = nil
	}
	j.mu.Unlock()
	return n, err
}

type retiredSegment struct {
	segment *journalSegment
	removed bool
}

func (j *journalWriterState) retirementDescriptors() []SegmentDescriptor {
	out := make([]SegmentDescriptor, 0, len(j.retiredSegments))
	for _, r := range j.retiredSegments {
		out = append(out, r.segment.descriptor)
	}
	return out
}
func (j *Journal) segmentPinned(id string) bool {
	for p := range j.readerPins {
		for _, s := range p.segments {
			if s.descriptor.SegmentID == id {
				return true
			}
		}
	}
	for reader := range j.activeReads {
		for _, segment := range reader.segments {
			if segment.descriptor.SegmentID == id {
				return true
			}
		}
	}
	return false
}
