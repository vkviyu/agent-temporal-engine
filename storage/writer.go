package storage

import (
	"context"
	"errors"
	"fmt"
)

// lockWriter excludes physical writers without blocking reader registration or
// publication metadata. Both checks preserve immediate maintenance/close errors.
func (j *Journal) lockWriter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.mu.Lock()
	err := j.writable()
	j.mu.Unlock()
	if err != nil {
		return err
	}
	j.writerMu.Lock()
	if err := ctx.Err(); err != nil {
		j.writerMu.Unlock()
		return err
	}
	j.mu.Lock()
	err = j.writable()
	j.mu.Unlock()
	if err != nil {
		j.writerMu.Unlock()
	}
	return err
}

// An operation already selected by writerMu may finish while Close waits for
// it. A semantic Fence revokes its right to install a new publication.
func (j *Journal) publicationAllowed() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fenceCause != nil {
		return fmt.Errorf("%w: %w", ErrStoreFenced, j.fenceCause)
	}
	return nil
}

func (j *Journal) publishBatch(ctx context.Context, batch []*PreparedTransaction, single *Transaction) (PublishedBatch, error) {
	if err := j.lockWriter(ctx); err != nil {
		return PublishedBatch{}, err
	}
	defer j.writerMu.Unlock()
	j.mu.Lock()
	candidate := j.journalWriterState
	j.mu.Unlock()
	if single != nil {
		// Preserve the original single-transaction rejection order before encoding.
		position := candidate.publication.Position
		if candidate.publication.Generation == ^uint64(0) || position.LSN == ^uint64(0) || position.LSN >= candidate.options.MaxPublishedTransactions {
			return PublishedBatch{}, ErrLimit
		}
		if single.LSN != position.LSN+1 || single.PreviousDigest != position.Digest {
			return PublishedBatch{}, ErrPrecondition
		}
		if _, exists := candidate.txIDs[single.TxID]; exists {
			return PublishedBatch{}, ErrDuplicateTransaction
		}
		prepared, err := PrepareTransaction(*single, candidate.options.Limits)
		if err != nil {
			return PublishedBatch{}, err
		}
		batch = []*PreparedTransaction{prepared}
	}
	result, err := candidate.publishPreparedBatch(ctx, batch, j.publicationAllowed)
	j.mu.Lock()
	defer j.mu.Unlock()
	if err != nil {
		j.retainCandidate(&candidate)
		if errors.Is(err, ErrCommitUnknown) || errors.Is(err, ErrStoreFenced) {
			j.state = JournalFenced
			j.fenceCause = errors.Join(j.fenceCause, err)
		}
		return PublishedBatch{}, err
	}
	if j.fenceCause != nil {
		// CURRENT may already be durable. Preserve the old public cut and all
		// acquired capabilities, and let reopening adjudicate the new prefix.
		j.retainCandidate(&candidate)
		err = unknownBatch(j.publication, "fenced during publication", batch, j.fenceCause)
		j.state = JournalFenced
		j.fenceCause = errors.Join(j.fenceCause, err)
		return PublishedBatch{}, err
	}
	j.journalWriterState = candidate
	return result, nil
}

// retainCandidate runs under mu after writer IO finishes. Shared files already
// belong to the current/retired layout; newly acquired files remain Close-owned.
func (j *Journal) retainCandidate(candidate *journalWriterState) {
	known := make(map[string]bool, len(j.segments)+len(j.pendingSegments)+len(j.retiredSegments))
	for _, segment := range j.segments {
		known[segment.descriptor.SegmentID] = true
	}
	for _, segment := range j.pendingSegments {
		known[segment.descriptor.SegmentID] = true
	}
	for _, retired := range j.retiredSegments {
		known[retired.segment.descriptor.SegmentID] = true
	}
	for _, segments := range [][]*journalSegment{candidate.pendingSegments, candidate.segments} {
		for _, segment := range segments {
			id := segment.descriptor.SegmentID
			if segment.file != nil && !known[id] {
				known[id] = true
				j.pendingSegments = append(j.pendingSegments, segment)
			}
		}
	}
}
