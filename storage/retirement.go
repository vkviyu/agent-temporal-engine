package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
)

func (j *Journal) compactionFence(cause error, uncertain bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state = JournalFenced
	j.fenceCause = errors.Join(j.fenceCause, cause)
	if uncertain {
		return fmt.Errorf("%w: physical compaction: %w", ErrCommitUnknown, cause)
	}
	return fmt.Errorf("%w: physical compaction: %w", ErrStoreFenced, cause)
}

// ReclaimRetired retries physical retirement after readers have released their
// pins. It never removes any file in the current layout. Reopening also retries
// this obligation from the authenticated manifest, including after a crash.
func (j *Journal) ReclaimRetired(ctx context.Context) ([]string, int, error) {
	if err := j.lockWriter(ctx); err != nil {
		return nil, 0, err
	}
	defer j.writerMu.Unlock()
	return j.reclaimRetired()
}

// The caller owns writerMu. Once selected under mu, an unpinned retired layout
// cannot gain new readers: new pins only capture the current layout, and every
// earlier persistent pin or active read was checked before selection.
func (j *Journal) reclaimRetired() (removed []string, pinned int, err error) {
	j.mu.Lock()
	intent := j.retirementIntent
	j.mu.Unlock()
	if intent != "" {
		if err = j.io.Remove(intent); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, 0, fmt.Errorf("%w: %w", ErrRetirement, err)
		}
		if err = j.io.SyncDir("compactions"); err != nil {
			return nil, 0, fmt.Errorf("%w: %w", ErrRetirement, err)
		}
		j.mu.Lock()
		j.retirementIntent = ""
		j.mu.Unlock()
	}
	j.mu.Lock()
	selected := make([]*retiredSegment, 0, len(j.retiredSegments))
	for _, r := range j.retiredSegments {
		if r.removed {
			continue
		}
		if j.segmentPinned(r.segment.descriptor.SegmentID) {
			pinned++
			continue
		}
		selected = append(selected, r)
	}
	needsSync := len(j.retiredSegments) != 0
	j.mu.Unlock()
	for _, r := range selected {
		s := r.segment
		if !s.closed {
			if err = s.file.Close(); err != nil {
				return removed, pinned, fmt.Errorf("%w: %w", ErrRetirement, err)
			}
			s.closed = true
		}
		name := segmentName(s.descriptor.SegmentID)
		if err = j.io.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removed, pinned, fmt.Errorf("%w: %w", ErrRetirement, err)
		}
		r.removed = true
		removed = append(removed, name)
	}
	if needsSync {
		if err = j.io.SyncDir("segments"); err != nil {
			return removed, pinned, fmt.Errorf("%w: %w", ErrRetirement, err)
		}
	}
	j.mu.Lock()
	remaining := j.retiredSegments[:0]
	for _, r := range j.retiredSegments {
		if !r.removed {
			remaining = append(remaining, r)
		}
	}
	j.retiredSegments = remaining
	j.mu.Unlock()
	return removed, pinned, nil
}
