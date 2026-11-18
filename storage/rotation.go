package storage

import (
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"fmt"
	"hash"
)

// segmentWritePlan contains only owned prepared bytes and private hash/metadata
// candidates. Preparing it performs no IO and does not mutate published state.
type segmentWritePlan struct {
	segment *journalSegment
	header  []byte // non-nil only for a new successor
	chunks  [][]byte
	footer  []byte
}

type publicationLayout struct {
	segments []*journalSegment
	writes   []*segmentWritePlan
	manifest Manifest
}

func clonePrefix(source hash.Hash) (hash.Hash, error) {
	marshaler, ok := source.(encoding.BinaryMarshaler)
	if !ok {
		return nil, fmt.Errorf("%w: prefix hash cannot be copied", ErrUnsupported)
	}
	state, err := marshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}
	copied := sha256.New()
	if err := copied.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return nil, err
	}
	return copied, nil
}

// prepareLayout keeps the initial header in the logical address space but
// excludes all later headers and seals. Canonical transaction bytes, their
// fingerprints, record addresses, and receipts are independent of rotation.
func (j *journalWriterState) prepareLayout(batch []*PreparedTransaction, total uint64, manifestID string) (*publicationLayout, error) {
	p := j.publication
	segments := append([]*journalSegment(nil), j.segments...)
	if len(segments) == 0 {
		// Single-segment stores opened by a legacy constructor have the same
		// descriptor as their schema1 manifest. This fallback is not a repair.
		genesis, err := GenesisPosition(j.identity)
		if err != nil {
			return nil, err
		}
		segments = []*journalSegment{{descriptor: SegmentDescriptor{SegmentID: j.segmentID,
			HeaderSize: SegmentHeaderSize, HeaderDigest: j.headerDigest, DataEnd: p.PublishedEnd,
			PrefixDigest: hashDigest(j.prefix), LogicalStart: SegmentHeaderSize, LogicalEnd: p.PublishedEnd,
			StartPosition: genesis, EndPosition: p.Position}, file: j.segment, prefix: j.prefix}}
	}
	last := segments[len(segments)-1]
	prefix, err := clonePrefix(j.prefix)
	if err != nil {
		return nil, err
	}
	active := &journalSegment{descriptor: last.descriptor, file: j.segment, prefix: prefix}
	segments[len(segments)-1] = active
	write := &segmentWritePlan{segment: active}
	layout := &publicationLayout{segments: segments, writes: []*segmentWritePlan{write}}
	ids := make(map[string]struct{}, len(segments))
	for _, segment := range segments {
		ids[segment.descriptor.SegmentID] = struct{}{}
	}
	rotate := func() error {
		if len(layout.segments) >= MaxSegments {
			return ErrLimit
		}
		active.descriptor.Sealed = true
		active.descriptor.PrefixDigest = hashDigest(active.prefix)
		footer, err := EncodeSegmentFooter(SegmentFooter{Store: p.Store, SegmentID: active.descriptor.SegmentID,
			DataEnd: active.descriptor.DataEnd, PrefixDigest: active.descriptor.PrefixDigest,
			LogicalEnd: active.descriptor.LogicalEnd, Position: active.descriptor.EndPosition})
		if err != nil {
			return err
		}
		full, err := clonePrefix(active.prefix)
		if err != nil {
			return err
		}
		_, _ = full.Write(footer)
		active.descriptor.SealedDigest = hashDigest(full)
		write.footer = footer
		var id string
		for attempt := 0; attempt < 8; attempt++ {
			id, err = randomID()
			if err != nil {
				return err
			}
			if _, exists := ids[id]; !exists {
				break
			}
			id = ""
		}
		if id == "" {
			return fmt.Errorf("%w: segment identity collision", ErrPrecondition)
		}
		ids[id] = struct{}{}
		header, err := EncodeSuccessorHeader(SuccessorHeader{Store: p.Store, SegmentID: id,
			LogicalStart: active.descriptor.LogicalEnd, PreviousSegmentID: active.descriptor.SegmentID,
			PreviousSegmentDigest: active.descriptor.SealedDigest, Position: active.descriptor.EndPosition})
		if err != nil {
			return err
		}
		nextPrefix := sha256.New()
		_, _ = nextPrefix.Write(header)
		next := &journalSegment{descriptor: SegmentDescriptor{SegmentID: id, HeaderSize: SuccessorHeaderSize,
			HeaderDigest: Digest(sha256.Sum256(header)), DataEnd: SuccessorHeaderSize,
			PrefixDigest: hashDigest(nextPrefix), LogicalStart: active.descriptor.LogicalEnd,
			LogicalEnd: active.descriptor.LogicalEnd, StartPosition: active.descriptor.EndPosition,
			EndPosition: active.descriptor.EndPosition}, prefix: nextPrefix}
		active = next
		write = &segmentWritePlan{segment: next, header: header}
		layout.segments = append(layout.segments, next)
		layout.writes = append(layout.writes, write)
		return nil
	}
	for _, prepared := range batch {
		chunkStart := 0
		for at := 0; at < len(prepared.encoded); {
			kind := Kind(binary.LittleEndian.Uint16(prepared.encoded[at+10 : at+12]))
			payload := binary.LittleEndian.Uint32(prepared.encoded[at+20 : at+24])
			length := HeaderSize + int(payload) + padding(payload) + TrailerSize
			// An empty segment accepts any single valid frame whole. The target
			// is soft; a large frame must not cause an infinite sequence of seals.
			if active.descriptor.DataEnd > active.descriptor.HeaderSize &&
				active.descriptor.DataEnd+uint64(length) > j.options.SegmentTargetBytes {
				if at > chunkStart {
					write.chunks = append(write.chunks, prepared.encoded[chunkStart:at])
				}
				if err := rotate(); err != nil {
					return nil, err
				}
				chunkStart = at
			}
			end := at + length
			_, _ = active.prefix.Write(prepared.encoded[at:end])
			active.descriptor.DataEnd += uint64(length)
			active.descriptor.LogicalEnd += uint64(length)
			if kind == KindCommit {
				active.descriptor.EndPosition = prepared.position
			}
			at = end
		}
		if chunkStart < len(prepared.encoded) {
			write.chunks = append(write.chunks, prepared.encoded[chunkStart:])
		}
	}
	active.descriptor.PrefixDigest = hashDigest(active.prefix)
	tip := batch[len(batch)-1]
	m := Manifest{Store: p.Store, Generation: p.Generation + 1, ManifestID: manifestID,
		SegmentID: active.descriptor.SegmentID, SegmentHeaderDigest: active.descriptor.HeaderDigest,
		PublishedEnd: p.PublishedEnd + total, PrefixDigest: active.descriptor.PrefixDigest,
		Position: tip.position, PostRootSet: tip.postRootSet}
	m.RecoveryCheckpoint = j.manifest.RecoveryCheckpoint
	if pending := j.retirementDescriptors(); len(pending) > 0 {
		m.LayoutSource = j.manifest.LayoutSource
		m.RetiredSegments = pending
	}
	if len(layout.segments) > 1 {
		m.Segments = make([]SegmentDescriptor, len(layout.segments))
		for i, segment := range layout.segments {
			m.Segments[i] = segment.descriptor
		}
	}
	// Validate and bound all metadata before the first append/create attempt.
	if _, err := EncodeManifest(m); err != nil {
		return nil, err
	}
	layout.manifest = m
	return layout, nil
}

// writeLayout starts P1. Callers must not inspect ctx after entering: an IO
// error can leave a seal or successor, and the entire batch then is unknown.
func (j *journalWriterState) writeLayout(layout *publicationLayout, guard func() error) error {
	for _, write := range layout.writes {
		segment := write.segment
		if write.header != nil {
			file, err := j.io.CreateExclusive(segmentName(segment.descriptor.SegmentID))
			if file != nil {
				segment.file = file
				// Retain handles even when acquisition reports an error. On
				// success these transfer to segments; on failure Close owns them.
				j.pendingSegments = append(j.pendingSegments, segment)
			}
			if err != nil {
				return err
			}
			if file == nil {
				return fmt.Errorf("%w: segment create returned nil handle", ErrMalformed)
			}
			if err := writeAll(file, write.header); err != nil {
				return err
			}
		}
		for _, chunk := range write.chunks {
			if err := writeAll(segment.file, chunk); err != nil {
				return err
			}
		}
		if write.footer != nil {
			if err := writeAll(segment.file, write.footer); err != nil {
				return err
			}
		}
		// In particular, a sealed predecessor is synced BEFORE any successor
		// is created with a header referring to its complete physical digest.
		if err := segment.file.Sync(); err != nil {
			return err
		}
	}
	if len(layout.writes) > 1 {
		if err := j.io.SyncDir("segments"); err != nil {
			return err
		}
	}
	if err := guard(); err != nil {
		return err
	}
	return installPublication(j.io, layout.manifest, j.identityDigest)
}
