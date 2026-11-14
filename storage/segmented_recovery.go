package storage

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// readExactAt distinguishes a missing referenced byte range from ordinary IO.
func readExactAt(r io.ReaderAt, p []byte, off int64) error {
	n, err := r.ReadAt(p, off)
	if n == len(p) && err == io.EOF {
		err = nil
	}
	if n < len(p) && (err == nil || err == io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return recoveryInputError("referenced segment bytes", err)
	}
	if n != len(p) {
		return fmt.Errorf("%w: invalid read count", ErrCorruption)
	}
	return nil
}

func verifySegmentedPrefix(segments []*journalSegment, id Identity, m Manifest, genesis Position, o JournalOptions, replay ReplayFunc) (map[model.TxID]struct{}, uint64, error) {
	for i, segment := range segments {
		d := segment.descriptor
		header := make([]byte, int(d.HeaderSize))
		if err := readExactAt(segment.file, header, 0); err != nil {
			return nil, 0, err
		}
		if Digest(sha256.Sum256(header)) != d.HeaderDigest {
			return nil, 0, fmt.Errorf("%w: segment header digest", ErrCorruption)
		}
		if i == 0 {
			h, err := DecodeSegmentHeader(header)
			if err != nil {
				return nil, 0, recoveryInputError("initial segment header", err)
			}
			if h.Store != id.Store || h.SegmentID != d.SegmentID || h.PreviousDigest != genesis.Digest || d.StartPosition != genesis {
				return nil, 0, fmt.Errorf("%w: initial segment identity/genesis", ErrCorruption)
			}
		} else {
			h, err := DecodeSuccessorHeader(header)
			if err != nil {
				return nil, 0, recoveryInputError("successor header", err)
			}
			prev := segments[i-1].descriptor
			if h.Store != id.Store || h.SegmentID != d.SegmentID || h.LogicalStart != d.LogicalStart || h.Position != d.StartPosition || h.PreviousSegmentID != prev.SegmentID || h.PreviousSegmentDigest != prev.SealedDigest {
				return nil, 0, fmt.Errorf("%w: successor physical chain", ErrCorruption)
			}
		}
		h := sha256.New()
		h.Write(header)
		// Verify each physical interval independently, so an otherwise valid
		// concatenated stream cannot smuggle a frame split across a seal boundary.
		stream := io.TeeReader(io.NewSectionReader(segment.file, int64(d.HeaderSize), int64(d.DataEnd-d.HeaderSize)), h)
		position := d.StartPosition
		var consumed uint64
		for {
			frame, n, err := ReadFrame(stream, o.Limits)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, 0, recoveryInputError("segment frame boundary", err)
			}
			consumed += uint64(n)
			if frame.Kind == KindCommit {
				c, err := decodeCommit(frame.Payload)
				if err != nil {
					return nil, 0, recoveryInputError("segment commit", err)
				}
				position = Position{LSN: c.lsn, Digest: commitDigest(frame.TxID, frame.Payload)}
			}
		}
		if consumed != d.DataEnd-d.HeaderSize || hashDigest(h) != d.PrefixDigest || position != d.EndPosition {
			return nil, 0, fmt.Errorf("%w: segment prefix/range", ErrCorruption)
		}
		if d.Sealed {
			b := make([]byte, SegmentFooterSize)
			if err := readExactAt(segment.file, b, int64(d.DataEnd)); err != nil {
				return nil, 0, err
			}
			footer, err := DecodeSegmentFooter(b)
			if err != nil {
				return nil, 0, recoveryInputError("seal footer", err)
			}
			if footer.Store != id.Store || footer.SegmentID != d.SegmentID || footer.DataEnd != d.DataEnd || footer.PrefixDigest != d.PrefixDigest || footer.LogicalEnd != d.LogicalEnd || footer.Position != d.EndPosition {
				return nil, 0, fmt.Errorf("%w: seal binding", ErrCorruption)
			}
			// SHA256 state cloning is local and contains no mutable external bytes.
			whole, err := clonePrefix(h)
			if err != nil {
				return nil, 0, err
			}
			whole.Write(b)
			if hashDigest(whole) != d.SealedDigest {
				return nil, 0, fmt.Errorf("%w: sealed physical digest", ErrCorruption)
			}
		}
		segment.prefix = h
	}
	reader := logicalReader{segments: snapshotReadSegments(segments), end: m.PublishedEnd}
	scanner, err := NewScanner(io.NewSectionReader(reader, SegmentHeaderSize, int64(m.PublishedEnd-SegmentHeaderSize)), genesis, o.Limits)
	if err != nil {
		return nil, 0, err
	}
	seen := make(map[model.TxID]struct{})
	var root Digest
	var count uint64
	for {
		tx, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, recoveryInputError("published transactions", err)
		}
		if _, ok := seen[tx.TxID]; ok {
			return nil, 0, fmt.Errorf("%w: duplicate transaction ID", ErrCorruption)
		}
		if count >= o.MaxPublishedTransactions {
			return nil, 0, ErrLimit
		}
		seen[tx.TxID] = struct{}{}
		count++
		root = tx.PostRootSet
		if replay != nil {
			tx.Store = id.Store
			tx.StartOffset += SegmentHeaderSize
			tx.EndOffset += SegmentHeaderSize
			for i := range tx.RecordLocations {
				tx.RecordLocations[i].PayloadOffset += SegmentHeaderSize
			}
			borrowed := &replayReader{reader: boundedReaderAt{reader: reader, end: int64(m.PublishedEnd)}, active: true}
			err = replay(tx, borrowed)
			borrowed.release()
			if err != nil {
				return nil, 0, fmt.Errorf("replay transaction %d: %w", tx.LSN, err)
			}
		}
	}
	if scanner.Position() != m.Position || root != m.PostRootSet || uint64(scanner.LastCompleteOffset())+SegmentHeaderSize != m.PublishedEnd {
		return nil, 0, fmt.Errorf("%w: published logical tip/root/boundary", ErrCorruption)
	}
	return seen, count, nil
}
