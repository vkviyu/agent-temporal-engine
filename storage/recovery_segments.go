package storage

import (
	"fmt"
)

func openReferencedSegments(
	d DurableIO,
	descriptors []SegmentDescriptor,
) ([]*journalSegment, int64, error) {
	var segments []*journalSegment
	var activeSize int64
	for _, desc := range descriptors {
		f, openErr := d.OpenAppend(segmentName(desc.SegmentID))
		if f != nil {
			segments = append(segments, &journalSegment{descriptor: desc, file: f})
		}
		if openErr != nil {
			return segments, activeSize, recoveryInputError("segment", openErr)
		}
		if f == nil {
			return segments, activeSize, fmt.Errorf("%w: segment open returned nil handle", ErrMalformed)
		}
		size, sizeErr := f.Size()
		if sizeErr != nil {
			return segments, activeSize, sizeErr
		}
		end := desc.DataEnd
		if desc.Sealed {
			end += SegmentFooterSize
		}
		if size < 0 || uint64(size) < end || (desc.Sealed && uint64(size) != end) {
			return segments, activeSize, fmt.Errorf("%w: referenced segment size", ErrCorruption)
		}
		activeSize = size
	}
	return segments, activeSize, nil
}
