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
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.writable(); err != nil {
		return Publication{}, 0, err
	}
	genesis, err := GenesisPosition(j.identity)
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
	if len(j.manifest.Segments) == 0 {
		_, _, count, err = verifyPrefixWithReplay(j.segment, j.identity, j.manifest, genesis, j.options.Limits, j.options.MaxPublishedTransactions, callback)
	} else {
		_, count, err = verifySegmentedPrefix(j.segments, j.identity, j.manifest, genesis, j.options, callback)
	}
	if err != nil {
		return Publication{}, 0, err
	}
	if err = ctx.Err(); err != nil {
		return Publication{}, 0, err
	}
	return j.publication, count, nil
}
