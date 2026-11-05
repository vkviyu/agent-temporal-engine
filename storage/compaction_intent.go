package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const maxCompactionIntentBytes = 64 << 10
const maxCompactionIntents = 16
const compactionIntentMagic = "ATECMP01"

type compactionIntent struct {
	Store    model.StoreID
	ID       string
	Source   LayoutSource
	Segments []string
}
type stagedCompaction struct {
	name   string
	intent *compactionIntent
	file   ReadFile
	size   uint64
}

func intentName(id string) string { return "compactions/INTENT." + id }

func encodeCompactionIntent(in compactionIntent) ([]byte, error) {
	if _, err := metadataID(string(in.Store)); err != nil {
		return nil, err
	}
	if _, err := metadataID(in.ID); err != nil {
		return nil, err
	}
	if len(in.Segments) == 0 || len(in.Segments) > MaxSegments {
		return nil, ErrLimit
	}
	b := metadataAppendID(metadataPayload(), string(in.Store))
	b = metadataAppendID(b, in.ID)
	b = appendLayoutSource(b, in.Source)
	b = codec.AppendUint(b, uint64(len(in.Segments)))
	seen := map[string]bool{}
	for _, id := range in.Segments {
		if _, err := metadataID(id); err != nil {
			return nil, err
		}
		if seen[id] {
			return nil, ErrMalformed
		}
		seen[id] = true
		b = metadataAppendID(b, id)
	}
	return metadataEnvelopeBounded(compactionIntentMagic, b, maxCompactionIntentBytes)
}
func decodeCompactionIntent(b []byte) (compactionIntent, error) {
	in := compactionIntent{}
	d, _, err := metadataDecoderVersioned(b, compactionIntentMagic, maxCompactionIntentBytes, 1)
	if err != nil {
		return in, err
	}
	s, err := metadataReadID(d)
	if err != nil {
		return in, err
	}
	in.Store = model.StoreID(s)
	if in.ID, err = metadataReadID(d); err != nil {
		return in, err
	}
	if in.Source, err = readLayoutSource(d); err != nil {
		return in, err
	}
	n, err := d.Uint()
	if err != nil || n == 0 || n > MaxSegments || n > uint64(d.Remaining()/17) {
		return in, ErrMalformed
	}
	for ; n > 0; n-- {
		id, e := metadataReadID(d)
		if e != nil {
			return in, e
		}
		in.Segments = append(in.Segments, id)
	}
	if err = d.Done(); err != nil {
		return in, err
	}
	canonical, err := encodeCompactionIntent(in)
	if err != nil || string(canonical) != string(b) {
		return in, ErrMalformed
	}
	return in, nil
}

func readCompactionIntents(d DurableIO, store model.StoreID) (out []stagedCompaction, err error) {
	names, err := d.List("compactions")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(names) > maxCompactionIntents {
		return nil, ErrLimit
	}
	sort.Strings(names)
	for _, name := range names {
		id := strings.TrimPrefix(name, "INTENT.")
		if id == name {
			return out, fmt.Errorf("%w: unknown compaction entry", ErrRecoveryRequired)
		}
		if _, e := metadataID(id); e != nil {
			return out, e
		}
		file, e := d.OpenRead("compactions/" + name)
		if file != nil {
			out = append(out, stagedCompaction{name: "compactions/" + name, file: file})
		}
		if e != nil {
			return out, e
		}
		if file == nil {
			return out, ErrMalformed
		}
		size, e := file.Size()
		if e != nil {
			return out, e
		}
		if size < 0 || size > maxCompactionIntentBytes {
			return out, ErrLimit
		}
		out[len(out)-1].size = uint64(size)
		b := make([]byte, int(size))
		if size > 0 {
			n, e := file.ReadAt(b, 0)
			if n == len(b) && e == io.EOF {
				e = nil
			}
			if e != nil {
				return out, e
			}
			if n != len(b) {
				return out, io.ErrUnexpectedEOF
			}
		}
		in, e := decodeCompactionIntent(b)
		if e == nil {
			if in.Store != store || in.ID != id {
				return out, fmt.Errorf("%w: compaction intent owner", ErrCorruption)
			}
			out[len(out)-1].intent = &in
		}
	}
	return out, nil
}

func verifyCompactionSources(d DurableIO, m Manifest, reader io.ReaderAt, genesis Position, o JournalOptions, intents []stagedCompaction) error {
	var sources []LayoutSource
	if m.LayoutSource != nil {
		sources = append(sources, *m.LayoutSource)
	}
	for _, staged := range intents {
		if in := staged.intent; in != nil {
			sources = append(sources, in.Source)
		}
	}
	if len(sources) == 0 {
		return nil
	}
	for _, source := range sources {
		if source.Generation >= m.Generation && source.ManifestID != m.ManifestID {
			return fmt.Errorf("%w: future compaction source", ErrRecoveryRequired)
		}
		b, err := readMetadata(d, manifestName(source.ManifestID))
		if err != nil {
			return recoveryInputError("compaction source", err)
		}
		if Digest(sha256.Sum256(b)) != source.ManifestDigest {
			return fmt.Errorf("%w: compaction source digest", ErrCorruption)
		}
		old, err := DecodeManifest(b)
		if err != nil {
			return err
		}
		if old.Store != m.Store || old.ManifestID != source.ManifestID || old.Generation != source.Generation || old.PublishedEnd != source.PublishedEnd || old.Position != source.Position || old.PostRootSet != source.PostRootSet {
			return fmt.Errorf("%w: compaction source binding", ErrCorruption)
		}
	}
	matched := make([]bool, len(sources))
	remaining := len(sources)
	for i, s := range sources {
		if s.Position.LSN == 0 {
			if s.Position != genesis || s.PostRootSet != (Digest{}) || s.PublishedEnd != SegmentHeaderSize {
				return fmt.Errorf("%w: compaction genesis source", ErrCorruption)
			}
			matched[i] = true
			remaining--
		}
	}
	scanner, err := NewScanner(io.NewSectionReader(reader, SegmentHeaderSize, int64(m.PublishedEnd-SegmentHeaderSize)), genesis, o.Limits)
	if err != nil {
		return err
	}
	for remaining > 0 {
		tx, err := scanner.Next()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("%w: source not in published ledger", ErrCorruption)
			}
			return err
		}
		for i, s := range sources {
			if !matched[i] && s.Position.LSN == tx.LSN {
				if s.Position.Digest != tx.CommitDigest || s.PostRootSet != tx.PostRootSet || s.PublishedEnd != uint64(tx.EndOffset)+SegmentHeaderSize {
					return fmt.Errorf("%w: source ledger anchor", ErrCorruption)
				}
				matched[i] = true
				remaining--
			}
		}
	}
	return nil
}

func compactionIgnoredDescriptors(current []SegmentDescriptor, m Manifest, intents []stagedCompaction) []SegmentDescriptor {
	out := append([]SegmentDescriptor(nil), current...)
	out = append(out, m.RetiredSegments...)
	for _, staged := range intents {
		if in := staged.intent; in != nil {
			for _, id := range in.Segments {
				out = append(out, SegmentDescriptor{SegmentID: id})
			}
		}
	}
	return out
}

// Intents authorize deleting only redundant physical candidates after the
// authoritative ledger and source anchors have both passed verification.
func cleanupCompactionIntents(d DurableIO, m Manifest, genesis Position, intents []stagedCompaction, report *RecoveryReport) error {
	keep := map[string]bool{}
	for _, x := range manifestDescriptors(m, genesis) {
		keep[x.SegmentID] = true
	}
	for _, x := range m.RetiredSegments {
		keep[x.SegmentID] = true
	}
	for _, staged := range intents {
		if staged.intent == nil {
			if err := d.Mkdir("quarantine"); err != nil && !errors.Is(err, fs.ErrExist) {
				return err
			}
			if err := d.SyncDir("."); err != nil {
				return err
			}
			id, err := randomID()
			if err != nil {
				return err
			}
			name := "quarantine/COMPACTION." + id
			if err = copyEvidence(d, name, staged.file, 0, staged.size); err != nil {
				return err
			}
			if err = d.SyncDir("quarantine"); err != nil {
				return err
			}
			report.QuarantineFiles = append(report.QuarantineFiles, name)
		} else {
			for _, id := range staged.intent.Segments {
				if keep[id] {
					continue
				}
				name := segmentName(id)
				if err := d.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
				report.DiscardedCompactionSegments = append(report.DiscardedCompactionSegments, name)
			}
		}
		if err := staged.file.Close(); err != nil {
			return err
		}
		if err := d.Remove(staged.name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	if len(intents) > 0 {
		if err := d.SyncDir("segments"); err != nil {
			return err
		}
		if err := d.SyncDir("compactions"); err != nil {
			return err
		}
	}
	return nil
}
