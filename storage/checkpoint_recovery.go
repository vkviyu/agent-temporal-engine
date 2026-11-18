package storage

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
)

// CheckpointLoader receives an authenticated selected file and a borrowed
// logical reader. It returns the suffix reducer. On ErrUnsupported it must
// leave the original replay target untouched so Open can replay from genesis.
// Other errors fail closed, before any tail repair. Readers may not be retained.
type CheckpointLoader func(RecoveryCheckpoint, io.Reader, io.ReaderAt) (ReplayFunc, error)

func OpenJournalWithCheckpoint(d DurableIO, options JournalOptions, replay ReplayFunc, load CheckpointLoader) (*Journal, RecoveryReport, error) {
	return openJournalCheckpoint(d, options, replay, load)
}

func prepareCheckpointReplay(d DurableIO, m Manifest, source io.ReaderAt, replay ReplayFunc, load CheckpointLoader) (ReplayFunc, uint64, error) {
	c := m.RecoveryCheckpoint
	if c == nil || load == nil {
		return replay, 0, nil
	}
	raw, err := readMetadata(d, manifestName(c.Source.ManifestID))
	if err != nil {
		return nil, 0, recoveryInputError("checkpoint source manifest", err)
	}
	old, err := DecodeManifest(raw)
	if err != nil {
		return nil, 0, recoveryInputError("checkpoint source manifest", err)
	}
	s := c.Source
	if Digest(sha256.Sum256(raw)) != s.ManifestDigest || old.Store != m.Store ||
		old.ManifestID != s.ManifestID || old.Generation != s.Generation ||
		old.PublishedEnd != s.PublishedEnd || old.Position != s.Position || old.PostRootSet != s.PostRootSet {
		return nil, 0, ErrCorruption
	}
	// Bind the exact logical prefix even after the original physical segments
	// have been compacted and retired. The ordinary scan still checks all bytes.
	digest, err := checkpointPrefix(source, s.PublishedEnd)
	if err != nil {
		return nil, 0, err
	}
	if digest != c.PrefixDigest {
		return nil, 0, ErrCorruption
	}
	suffix, err := loadRecoveryCheckpoint(d, *c, source, load)
	if len(pendingCleanups(err)) == 0 && (errors.Is(err, fs.ErrNotExist) || errors.Is(err, ErrUnsupported)) {
		return replay, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if suffix == nil {
		return nil, 0, ErrMalformed
	}
	return func(tx ScannedTransaction, reader io.ReaderAt) error {
		if tx.LSN <= s.Position.LSN {
			if tx.LSN == s.Position.LSN && (tx.CommitDigest != s.Position.Digest ||
				tx.PostRootSet != s.PostRootSet || uint64(tx.EndOffset) != s.PublishedEnd) {
				return ErrCorruption
			}
			return nil
		}
		return suffix(tx, reader)
	}, s.Position.LSN, nil
}

func loadRecoveryCheckpoint(d DurableIO, c RecoveryCheckpoint, source io.ReaderAt, load CheckpointLoader) (replay ReplayFunc, err error) {
	f, err := d.OpenRead(checkpointName(c.ID))
	if err != nil {
		return nil, cleanupOnError(err, f)
	}
	defer closeInto(&err, f)
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size <= 0 || uint64(size) != c.Size || uint64(size) > MaxRecoveryCheckpointBytes {
		return nil, ErrCorruption
	}
	h := sha256.New()
	if _, err = io.CopyBuffer(h, io.NewSectionReader(f, 0, size), make([]byte, 64<<10)); err != nil {
		return nil, err
	}
	if hashDigest(h) != c.Digest {
		return nil, ErrCorruption
	}
	reader := &replayReader{reader: boundedReaderAt{reader: source, end: int64(c.Source.PublishedEnd)}, active: true}
	defer reader.release()
	h.Reset()
	input := bufio.NewReaderSize(io.TeeReader(io.NewSectionReader(f, 0, size), h), 64<<10)
	replay, err = load(c, input, reader)
	if err != nil {
		return nil, err
	}
	// Require complete consumption and recheck the bytes actually decoded.
	var trailing [1]byte
	n, endErr := input.Read(trailing[:])
	if n != 0 || endErr != io.EOF || hashDigest(h) != c.Digest {
		return nil, ErrCorruption
	}
	return replay, nil
}
