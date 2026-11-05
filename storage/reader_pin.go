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
// releases its physical retention pin; it cannot grant future transaction bytes.
// A pin survives compaction but never a semantic Fence.
type PinnedReader struct {
	journal     *Journal
	publication Publication
	segments    []*journalSegment
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
	p := &PinnedReader{journal: j, publication: j.publication, segments: append([]*journalSegment(nil), j.segments...)}
	if len(p.segments) == 0 {
		return nil, fmt.Errorf("%w: missing published layout", ErrMalformed)
	}
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
	defer j.mu.Unlock()
	if p.closed || j.state == JournalClosed {
		return 0, ErrClosed
	}
	if j.state == JournalFenced {
		return 0, fmt.Errorf("%w: %w", ErrStoreFenced, j.fenceCause)
	}
	return (logicalReader{segments: p.segments, end: p.publication.PublishedEnd}).ReadAt(b, off)
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

type retiredSegment struct {
	segment *journalSegment
	removed bool
}

func (j *Journal) retirementDescriptors() []SegmentDescriptor {
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
	return false
}
