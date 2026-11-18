package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"math"
)

// CheckpointEncoder serializes the private projection built by a complete
// VerifyPublished replay. It must not reenter Journal or retain the writer.
type CheckpointEncoder func(Publication, io.Writer) error

// CreateRecoveryCheckpoint certifies through a complete physical and semantic
// replay under the sole writer, then publishes the encoded projection through
// CURRENT. The semantic layer supplies a fresh verifier, never a warm cache.
// No audit bytes or old manifests/checkpoints are removed.
func (j *Journal) CreateRecoveryCheckpoint(ctx context.Context, schema uint64, replay ReplayFunc, encode CheckpointEncoder) (after Publication, err error) {
	if schema == 0 || replay == nil || encode == nil {
		return after, ErrMalformed
	}
	if err = j.lockWriter(ctx); err != nil {
		return after, err
	}
	defer j.writerMu.Unlock()
	before, _, err := j.verifyPublishedLocked(ctx, replay)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrLimit) {
			return after, err
		}
		return after, j.compactionFence(err, false)
	}
	if before.Generation == math.MaxUint64 {
		return after, ErrLimit
	}
	source := j.manifest
	// Recheck the on-disk authority as well as the physical prefix. A mutated
	// CURRENT or manifest cannot be blessed by the in-memory writer state.
	canonical, err := EncodeManifest(source)
	if err != nil {
		return after, err
	}
	raw, err := readMetadata(j.io, manifestName(source.ManifestID))
	if err != nil {
		return after, j.compactionFence(err, false)
	}
	identityBytes, err := readMetadata(j.io, "IDENTITY")
	if err != nil {
		return after, j.compactionFence(err, false)
	}
	if Digest(sha256.Sum256(identityBytes)) != j.identityDigest {
		return after, j.compactionFence(ErrCorruption, false)
	}
	currentBytes, err := readMetadata(j.io, "CURRENT")
	if err != nil {
		return after, j.compactionFence(err, false)
	}
	current, err := DecodeCurrent(currentBytes)
	if err != nil || string(raw) != string(canonical) ||
		current != (Current{Store: before.Store, Generation: before.Generation, ManifestID: source.ManifestID, ManifestDigest: Digest(sha256.Sum256(raw)), IdentityDigest: j.identityDigest}) {
		return after, j.compactionFence(ErrCorruption, false)
	}
	c := RecoveryCheckpoint{Store: before.Store, Schema: schema, Source: LayoutSource{
		ManifestID: source.ManifestID, ManifestDigest: current.ManifestDigest,
		Generation: before.Generation, PublishedEnd: before.PublishedEnd,
		Position: before.Position, PostRootSet: before.PostRootSet,
	}}
	c.PrefixDigest, err = checkpointPrefix(j.logicalReader(), before.PublishedEnd)
	if err != nil {
		return after, j.compactionFence(err, false)
	}
	if c.ID, err = randomID(); err != nil {
		return after, err
	}
	m := source
	if m.ManifestID, err = randomID(); err != nil {
		return after, err
	}
	m.Generation++
	if err = ctx.Err(); err != nil {
		return after, err
	}
	if err = j.publicationAllowed(); err != nil {
		return after, err
	}
	// Once staging starts, finish or fence. Failed files stay unselected;
	// cleanup never guesses authority from filenames.
	defer func() {
		if err != nil {
			err = j.compactionFence(err, true)
		}
	}()
	if err = j.io.Mkdir("checkpoints"); err != nil && !errors.Is(err, fs.ErrExist) {
		return after, err
	}
	if err = j.io.SyncDir("."); err != nil {
		return after, err
	}
	if err = j.writeRecoveryCheckpoint(before, &c, encode); err != nil {
		return after, err
	}
	if err = j.io.SyncDir("checkpoints"); err != nil {
		return after, err
	}
	m.RecoveryCheckpoint = &c
	if err = j.publicationAllowed(); err != nil {
		return after, err
	}
	if err = installPublication(j.io, m, j.identityDigest); err != nil {
		return after, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fenceCause != nil {
		return after, ErrStoreFenced
	}
	j.manifest = m
	j.publication = publicationFor(j.identity, m)
	return j.publication, nil
}

func (j *Journal) writeRecoveryCheckpoint(p Publication, c *RecoveryCheckpoint, encode CheckpointEncoder) (err error) {
	f, err := j.io.CreateExclusive(checkpointName(c.ID))
	if err != nil {
		return cleanupOnError(err, f)
	}
	defer closeInto(&err, f)
	h := sha256.New()
	w := &checkpointWriter{writer: io.MultiWriter(f, h)}
	if err = encode(p, w); err != nil {
		return err
	}
	if w.size == 0 {
		return ErrMalformed
	}
	c.Size, c.Digest = w.size, hashDigest(h)
	return f.Sync()
}

type checkpointWriter struct {
	writer io.Writer
	size   uint64
}

func (w *checkpointWriter) Write(b []byte) (int, error) {
	if uint64(len(b)) > MaxRecoveryCheckpointBytes-w.size {
		return 0, ErrLimit
	}
	// io.MultiWriter reports short writes; encode cannot accidentally certify
	// a truncated record by ignoring a positive short write.
	n, err := w.writer.Write(b)
	w.size += uint64(n)
	return n, err
}

func checkpointPrefix(reader io.ReaderAt, end uint64) (Digest, error) {
	h := sha256.New()
	_, err := io.CopyBuffer(h, io.NewSectionReader(reader, SegmentHeaderSize, int64(end-SegmentHeaderSize)), make([]byte, 64<<10))
	return hashDigest(h), err
}
