package storage

import (
	"fmt"
	"io"
	"sort"
)

// logicalReader presents the immutable canonical log address space. Only the
// initial header has an address; successor headers and all footers are layout
// metadata. A frame's address therefore survives rotations unchanged.
type logicalReader struct {
	segments []readSegment
	end      uint64
}

// readSegment owns the descriptor value selected at one published cut. The
// file capability stays alive through a persistent pin or an active read lease.
type readSegment struct {
	descriptor SegmentDescriptor
	file       io.ReaderAt
}

func snapshotReadSegments(segments []*journalSegment) []readSegment {
	snapshot := make([]readSegment, len(segments))
	for i, segment := range segments {
		snapshot[i] = readSegment{descriptor: segment.descriptor, file: segment.file}
	}
	return snapshot
}

func (r logicalReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative read offset", ErrMalformed)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off) >= r.end {
		return 0, io.EOF
	}
	total := 0
	for len(p) > 0 && uint64(off) < r.end {
		at := uint64(off)
		index := 0
		var physical, available uint64
		if at < SegmentHeaderSize {
			physical = at
			available = SegmentHeaderSize - at
		} else {
			index = sort.Search(len(r.segments), func(i int) bool { return r.segments[i].descriptor.LogicalEnd > at })
			if index == len(r.segments) {
				return total, io.EOF
			}
			d := r.segments[index].descriptor
			if at < d.LogicalStart {
				return total, fmt.Errorf("%w: gap in logical segment ranges", ErrCorruption)
			}
			physical = d.HeaderSize + (at - d.LogicalStart)
			available = d.LogicalEnd - at
		}
		if len(r.segments) == 0 {
			return total, io.EOF
		}
		if available > r.end-at {
			available = r.end - at
		}
		nbytes := len(p)
		if uint64(nbytes) > available {
			nbytes = int(available)
		}
		n, err := r.segments[index].file.ReadAt(p[:nbytes], int64(physical))
		if n < 0 || n > nbytes {
			return total, fmt.Errorf("%w: invalid ReadAt count", ErrMalformed)
		}
		total += n
		off += int64(n)
		p = p[n:]
		if n == nbytes && err == io.EOF {
			err = nil
		}
		if err != nil {
			return total, err
		}
		if n < nbytes {
			return total, io.EOF
		}
	}
	if len(p) > 0 {
		return total, io.EOF
	}
	return total, nil
}

// The caller holds j.mu or owns an unopened recovery candidate.
func (j *Journal) logicalReader() logicalReader {
	if len(j.segments) == 0 {
		return logicalReader{segments: []readSegment{{file: j.segment, descriptor: SegmentDescriptor{
			SegmentID: j.segmentID, HeaderSize: SegmentHeaderSize,
			LogicalStart: SegmentHeaderSize, LogicalEnd: j.publication.PublishedEnd,
		}}}, end: j.publication.PublishedEnd}
	}
	segments := snapshotReadSegments(j.segments)
	segments[len(segments)-1].file = j.segment
	return logicalReader{segments: segments, end: j.publication.PublishedEnd}
}
