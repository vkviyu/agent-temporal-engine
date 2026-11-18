package storage

import (
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// MaxRecoveryCheckpointBytes bounds streamed checkpoint IO, independently of
// the per-record bounds imposed by the semantic checkpoint codec.
const MaxRecoveryCheckpointBytes uint64 = 1 << 30

// RecoveryCheckpoint authenticates a certified projection of Source. Digest
// covers the entire checkpoint file; PrefixDigest covers logical frame bytes
// [SegmentHeaderSize, Source.PublishedEnd), independent of physical layout.
type RecoveryCheckpoint struct {
	Store        model.StoreID
	ID           string
	Schema       uint64
	Size         uint64
	Digest       Digest
	PrefixDigest Digest
	Source       LayoutSource
}

func boolUint(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func checkpointName(id string) string { return "checkpoints/CHECKPOINT." + id }

func (m Manifest) validateRecoveryCheckpoint() error {
	c := m.RecoveryCheckpoint
	if c == nil {
		return nil
	}
	if _, err := metadataID(c.ID); err != nil {
		return err
	}
	s := c.Source
	if _, err := metadataID(s.ManifestID); err != nil {
		return err
	}
	if c.Store != m.Store || c.Schema == 0 || c.Size == 0 || c.Size > MaxRecoveryCheckpointBytes ||
		c.Digest == (Digest{}) || c.PrefixDigest == (Digest{}) ||
		s.ManifestID == m.ManifestID || s.ManifestDigest == (Digest{}) ||
		s.Generation >= m.Generation || s.PublishedEnd < SegmentHeaderSize ||
		s.PublishedEnd > m.PublishedEnd || s.Position.Digest == (Digest{}) ||
		s.Position.LSN > m.Position.LSN ||
		((s.Position.LSN == 0) != (s.PostRootSet == (Digest{}))) ||
		(s.Position.LSN == m.Position.LSN && (s.Position != m.Position || s.PostRootSet != m.PostRootSet || s.PublishedEnd != m.PublishedEnd)) {
		return ErrMalformed
	}
	return nil
}

func appendRecoveryCheckpoint(b []byte, c RecoveryCheckpoint) []byte {
	b = metadataAppendID(b, string(c.Store))
	b = metadataAppendID(b, c.ID)
	b = codec.AppendUint(b, c.Schema)
	b = codec.AppendUint(b, c.Size)
	b = appendDigest(b, c.Digest)
	b = appendDigest(b, c.PrefixDigest)
	return appendLayoutSource(b, c.Source)
}

func readRecoveryCheckpoint(d *codec.Decoder) (c RecoveryCheckpoint, err error) {
	var store string
	if store, err = metadataReadID(d); err != nil {
		return
	}
	c.Store = model.StoreID(store)
	if c.ID, err = metadataReadID(d); err != nil {
		return
	}
	if c.Schema, err = d.Uint(); err != nil {
		return
	}
	if c.Size, err = d.Uint(); err != nil {
		return
	}
	if c.Digest, err = readDigest(d); err != nil {
		return
	}
	if c.PrefixDigest, err = readDigest(d); err != nil {
		return
	}
	c.Source, err = readLayoutSource(d)
	return
}
