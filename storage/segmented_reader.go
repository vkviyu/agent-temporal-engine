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
	segments []*journalSegment
	active   io.ReaderAt
	end      uint64
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
		var source io.ReaderAt = r.segments[index].file
		if index == len(r.segments)-1 && r.active != nil {
			source = r.active
		}
		n, err := source.ReadAt(p[:nbytes], int64(physical))
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

func (j *Journal) logicalReader() io.ReaderAt {
	if len(j.segments) == 0 {
		return boundedReaderAt{reader: j.segment, end: int64(j.publication.PublishedEnd)}
	}
	return logicalReader{segments: j.segments, active: j.segment, end: j.publication.PublishedEnd}
}
