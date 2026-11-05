package storage

import (
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/vkviyu/agent-temporal-engine/codec"
)

func (m Manifest) validateRetirement() error {
	if m.LayoutSource == nil {
		if len(m.RetiredSegments) != 0 {
			return fmt.Errorf("%w: retired layout without source", ErrMalformed)
		}
		return nil
	}
	s := *m.LayoutSource
	if _, err := metadataID(s.ManifestID); err != nil {
		return err
	}
	if s.ManifestID == m.ManifestID || s.ManifestDigest == (Digest{}) || s.Generation >= m.Generation || s.PublishedEnd < SegmentHeaderSize || s.PublishedEnd > m.PublishedEnd || s.Position.Digest == (Digest{}) || s.Position.LSN > m.Position.LSN || (s.Position.LSN == m.Position.LSN && (s.Position != m.Position || s.PostRootSet != m.PostRootSet || s.PublishedEnd != m.PublishedEnd)) || ((s.Position.LSN == 0) != (s.PostRootSet == (Digest{}))) {
		return fmt.Errorf("%w: compaction source", ErrMalformed)
	}
	if len(m.RetiredSegments) == 0 || len(m.RetiredSegments) > 2*MaxSegments {
		return fmt.Errorf("%w: retired segment count", ErrMalformed)
	}
	used := map[string]bool{m.SegmentID: true}
	for _, d := range m.Segments {
		used[d.SegmentID] = true
	}
	for _, d := range m.RetiredSegments {
		if _, err := metadataID(d.SegmentID); err != nil {
			return err
		}
		if used[d.SegmentID] || d.HeaderDigest == (Digest{}) || d.PrefixDigest == (Digest{}) || (d.HeaderSize != SegmentHeaderSize && d.HeaderSize != SuccessorHeaderSize) || d.DataEnd < d.HeaderSize || d.DataEnd > math.MaxInt64-SegmentFooterSize || d.LogicalStart < SegmentHeaderSize || d.LogicalEnd < d.LogicalStart || d.LogicalEnd > s.PublishedEnd || d.LogicalEnd-d.LogicalStart != d.DataEnd-d.HeaderSize || d.EndPosition.LSN > s.Position.LSN || d.StartPosition.Digest == (Digest{}) || d.EndPosition.Digest == (Digest{}) || d.EndPosition.LSN < d.StartPosition.LSN || ((d.SealedDigest != (Digest{})) != d.Sealed) {
			return fmt.Errorf("%w: retired segment descriptor", ErrMalformed)
		}
		used[d.SegmentID] = true
	}
	return nil
}

func appendLayoutSource(b []byte, s LayoutSource) []byte {
	b = metadataAppendID(b, s.ManifestID)
	b = appendDigest(b, s.ManifestDigest)
	b = codec.AppendUint(b, s.Generation)
	b = codec.AppendUint(b, s.PublishedEnd)
	b = codec.AppendUint(b, s.Position.LSN)
	b = appendDigest(b, s.Position.Digest)
	return appendDigest(b, s.PostRootSet)
}
func readLayoutSource(d *codec.Decoder) (LayoutSource, error) {
	s := LayoutSource{}
	var err error
	if s.ManifestID, err = metadataReadID(d); err != nil {
		return s, metadataError(err)
	}
	if s.ManifestDigest, err = readDigest(d); err != nil {
		return s, metadataError(err)
	}
	if s.Generation, err = d.Uint(); err != nil {
		return s, metadataError(err)
	}
	if s.PublishedEnd, err = d.Uint(); err != nil {
		return s, metadataError(err)
	}
	if s.Position.LSN, err = d.Uint(); err != nil {
		return s, metadataError(err)
	}
	if s.Position.Digest, err = readDigest(d); err != nil {
		return s, metadataError(err)
	}
	if s.PostRootSet, err = readDigest(d); err != nil {
		return s, metadataError(err)
	}
	return s, nil
}

// validateRetirementSource authenticates authorization to retire physical
// names. It never uses an unreferenced manifest to choose logical history.
func validateRetirementSource(d DurableIO, m Manifest) error {
	if m.LayoutSource == nil {
		return nil
	}
	s := *m.LayoutSource
	b, err := readMetadata(d, manifestName(s.ManifestID))
	if err != nil {
		return recoveryInputError("compaction source manifest", err)
	}
	if Digest(sha256.Sum256(b)) != s.ManifestDigest {
		return fmt.Errorf("%w: compaction source manifest digest", ErrCorruption)
	}
	old, err := DecodeManifest(b)
	if err != nil {
		return recoveryInputError("compaction source manifest", err)
	}
	if old.Store != m.Store || old.ManifestID != s.ManifestID || old.Generation != s.Generation || old.Position != s.Position || old.PublishedEnd != s.PublishedEnd || old.PostRootSet != s.PostRootSet {
		return fmt.Errorf("%w: compaction source binding", ErrCorruption)
	}
	genesis, err := GenesisPosition(Identity{Store: m.Store, Profile: FileSyncExperimental})
	if err != nil {
		return err
	}
	allowed := map[string]SegmentDescriptor{}
	for _, x := range manifestDescriptors(old, genesis) {
		allowed[x.SegmentID] = x
	}
	for _, x := range old.RetiredSegments {
		allowed[x.SegmentID] = x
	}
	for _, x := range m.RetiredSegments {
		if a, ok := allowed[x.SegmentID]; !ok || a != x {
			return fmt.Errorf("%w: retirement not authorized by source layout", ErrCorruption)
		}
	}
	return nil
}

func manifestDescriptors(m Manifest, genesis Position) []SegmentDescriptor {
	if len(m.Segments) != 0 {
		return append([]SegmentDescriptor(nil), m.Segments...)
	}
	return []SegmentDescriptor{{SegmentID: m.SegmentID, HeaderSize: SegmentHeaderSize, HeaderDigest: m.SegmentHeaderDigest, DataEnd: m.PublishedEnd, PrefixDigest: m.PrefixDigest, LogicalStart: SegmentHeaderSize, LogicalEnd: m.PublishedEnd, StartPosition: genesis, EndPosition: m.Position}}
}
