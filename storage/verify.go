package storage

import (
	"context"
	"io"
)

// VerifyPublished authenticates the current physical prefix and optionally
// reconstructs a private semantic projection without changing CURRENT or any
// file. Replay callbacks borrow the reader exactly as during Open and must not
// reenter this Journal. The publication owner excludes concurrent installation.
func (j *Journal) VerifyPublished(ctx context.Context, replay ReplayFunc) (Publication, uint64, error) {
	if err := ctx.Err(); err != nil {
		return Publication{}, 0, err
	}
	if err := j.lockWriter(ctx); err != nil {
		return Publication{}, 0, err
	}
	defer j.writerMu.Unlock()
	return j.verifyPublishedLocked(ctx, replay)
}

// Caller holds writerMu, including across certification and publication.
func (j *Journal) verifyPublishedLocked(ctx context.Context, replay ReplayFunc) (Publication, uint64, error) {
	j.mu.Lock()
	source := j.journalWriterState
	j.mu.Unlock()
	genesis, err := GenesisPosition(source.identity)
	if err != nil {
		return Publication{}, 0, err
	}
	callback := func(tx ScannedTransaction, reader io.ReaderAt) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if replay != nil {
			return replay(tx, reader)
		}
		return nil
	}
	var count uint64
	if len(source.manifest.Segments) == 0 {
		_, _, count, err = verifyPrefixWithReplay(source.segment, source.identity, source.manifest, genesis, source.options.Limits, source.options.MaxPublishedTransactions, callback)
	} else {
		_, count, err = verifySegmentedPrefix(source.segments, source.identity, source.manifest, genesis, source.options, callback)
	}
	if err != nil {
		return Publication{}, 0, err
	}
	if err = ctx.Err(); err != nil {
		return Publication{}, 0, err
	}
	if err := j.publicationAllowed(); err != nil {
		return Publication{}, 0, err
	}
	return source.publication, count, nil
}
