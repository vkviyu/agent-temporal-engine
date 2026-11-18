package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
)

// CompactionReport describes a physical layout replacement. Position, roots,
// transaction bytes and logical object addresses remain identical. Generation
// advances, since CURRENT has selected a new authenticated physical layout.
type CompactionReport struct {
	Before            Publication
	After             Publication
	CopiedBytes       uint64
	RetiredSegments   int
	ReclaimedSegments []string
	PinnedSegments    int
}

// CompactAuditFull packs the complete published canonical frame stream into a
// new segment. It does not discard transactions or change retention policy.
// Readers already pinned to the source layout remain valid. New writes/pins
// receive ErrMaintenance until installation finishes. Context cancellation is
// honored before staging starts; after the first mutation the protocol must
// finish or fence, exactly like logical publication.
func (j *Journal) CompactAuditFull(ctx context.Context) (report CompactionReport, err error) {
	if err = ctx.Err(); err != nil {
		return report, err
	}
	if err = j.lockWriter(ctx); err != nil {
		return report, err
	}
	defer j.writerMu.Unlock()
	j.mu.Lock()
	if err = j.writable(); err != nil {
		j.mu.Unlock()
		return report, err
	}
	if len(j.retiredSegments) != 0 || j.retirementIntent != "" {
		j.mu.Unlock()
		return report, ErrRetirement
	}
	if j.publication.Generation == math.MaxUint64 {
		j.mu.Unlock()
		return report, ErrLimit
	}
	before := j.publication
	sourceManifest := j.manifest
	sourceSegments := append([]*journalSegment(nil), j.segments...)
	j.state = JournalMaintenance
	j.mu.Unlock()
	defer func() {
		j.mu.Lock()
		if j.state == JournalMaintenance {
			j.state = JournalReady
		}
		j.mu.Unlock()
	}()
	// An earlier interrupted rewrite must be reconciled by Open. Directory IO
	// belongs to the writer, while reader metadata remains available under mu.
	intents, listErr := j.io.List("compactions")
	if listErr != nil && !errors.Is(listErr, fs.ErrNotExist) {
		return report, listErr
	}
	if len(intents) != 0 {
		return report, ErrRecoveryRequired
	}
	genesis, err := GenesisPosition(j.identity)
	if err != nil {
		return report, err
	}
	// Authenticate every physical byte and canonical transaction before any
	// mutation. A corrupt source can never authorize removing its own files.
	if len(sourceManifest.Segments) == 0 {
		_, _, _, err = verifyPrefixWithReplay(sourceSegments[0].file, j.identity, sourceManifest, genesis, j.options.Limits, j.options.MaxPublishedTransactions, nil)
	} else {
		_, _, err = verifySegmentedPrefix(sourceSegments, j.identity, sourceManifest, genesis, j.options, nil)
	}
	if err != nil {
		return report, j.compactionFence(err, false)
	}
	manifestBytes, err := readMetadata(j.io, manifestName(sourceManifest.ManifestID))
	if err != nil {
		// Metadata reads can return a CleanupError retaining a live file.
		// Keep it behind Journal ownership until Close has retired it, just
		// like a source verification failure or uncertain publication.
		return report, j.compactionFence(err, false)
	}
	canonical, err := EncodeManifest(sourceManifest)
	if err != nil || string(manifestBytes) != string(canonical) {
		return report, j.compactionFence(ErrCorruption, false)
	}
	segmentID, err := randomID()
	if err != nil {
		return report, err
	}
	manifestID, err := randomID()
	if err != nil {
		return report, err
	}
	intentID, err := randomID()
	if err != nil {
		return report, err
	}
	source := LayoutSource{ManifestID: sourceManifest.ManifestID, ManifestDigest: Digest(sha256.Sum256(manifestBytes)), Generation: before.Generation, PublishedEnd: before.PublishedEnd, Position: before.Position, PostRootSet: before.PostRootSet}
	intent := compactionIntent{Store: before.Store, ID: intentID, Source: source, Segments: []string{segmentID}}
	intentBytes, err := encodeCompactionIntent(intent)
	if err != nil {
		return report, err
	}
	header, err := EncodeSegmentHeader(SegmentHeader{Store: before.Store, SegmentID: segmentID, FirstLSN: 1, PreviousDigest: genesis.Digest})
	if err != nil {
		return report, err
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	if err = j.publicationAllowed(); err != nil {
		return report, err
	}
	// A durable intent precedes creating candidate segments. Recovery may only
	// delete candidates named by this authenticated intent after verifying the
	// authoritative ledger and its source anchor.
	started := true
	defer func() {
		if err != nil && started {
			err = j.compactionFence(err, true)
		}
	}()
	if err = j.io.Mkdir("compactions"); err != nil && !errors.Is(err, fs.ErrExist) {
		return report, err
	}
	if err = j.io.SyncDir("."); err != nil {
		return report, err
	}
	if err = writeSyncedFile(j.io, intentName(intentID), intentBytes); err != nil {
		return report, err
	}
	if err = j.io.SyncDir("compactions"); err != nil {
		return report, err
	}
	file, err := j.io.CreateExclusive(segmentName(segmentID))
	candidate := &journalSegment{file: file, descriptor: SegmentDescriptor{SegmentID: segmentID}}
	if file != nil {
		j.mu.Lock()
		j.pendingSegments = append(j.pendingSegments, candidate)
		j.mu.Unlock()
	}
	if err != nil {
		return report, err
	}
	if file == nil {
		return report, ErrMalformed
	}
	prefix := sha256.New()
	writer := io.MultiWriter(file, prefix)
	if err = writeAll(writer, header); err != nil {
		return report, err
	}
	reader := logicalReader{segments: snapshotReadSegments(sourceSegments), end: before.PublishedEnd}
	remaining := before.PublishedEnd - SegmentHeaderSize
	buffer := make([]byte, 64<<10)
	for off := uint64(SegmentHeaderSize); remaining > 0; {
		n := uint64(len(buffer))
		if n > remaining {
			n = remaining
		}
		if _, err = io.ReadFull(io.NewSectionReader(reader, int64(off), int64(n)), buffer[:int(n)]); err != nil {
			return report, err
		}
		if err = writeAll(writer, buffer[:int(n)]); err != nil {
			return report, err
		}
		off += n
		remaining -= n
	}
	if err = file.Sync(); err != nil {
		return report, err
	}
	if err = j.io.SyncDir("segments"); err != nil {
		return report, err
	}
	m := Manifest{Store: before.Store, Generation: before.Generation + 1, ManifestID: manifestID, SegmentID: segmentID, SegmentHeaderDigest: Digest(sha256.Sum256(header)), PublishedEnd: before.PublishedEnd, PrefixDigest: hashDigest(prefix), Position: before.Position, PostRootSet: before.PostRootSet, LayoutSource: &source}
	m.RecoveryCheckpoint = sourceManifest.RecoveryCheckpoint
	for _, s := range sourceSegments {
		m.RetiredSegments = append(m.RetiredSegments, s.descriptor)
	}
	if _, err = EncodeManifest(m); err != nil {
		return report, err
	}
	if _, _, _, err = verifyPrefixWithReplay(file, j.identity, m, genesis, j.options.Limits, j.options.MaxPublishedTransactions, nil); err != nil {
		return report, err
	}
	candidate.descriptor = manifestDescriptors(m, genesis)[0]
	candidate.prefix = prefix
	// Journal is the unique publication owner. Check the captured generation
	// under its mutex immediately before publishing CURRENT and the catalog.
	j.mu.Lock()
	if j.state != JournalMaintenance || j.publication != before {
		j.mu.Unlock()
		return report, ErrPrecondition
	}
	j.mu.Unlock()
	err = installPublication(j.io, m, j.identityDigest)
	if err != nil {
		return report, err
	}
	j.mu.Lock()
	if j.fenceCause != nil {
		err = fmt.Errorf("%w: %w", ErrStoreFenced, j.fenceCause)
		j.mu.Unlock()
		return report, err
	}
	j.segments = []*journalSegment{candidate}
	j.segment, j.segmentID, j.headerDigest, j.prefix = file, segmentID, m.SegmentHeaderDigest, prefix
	j.manifest, j.publication = m, publicationFor(j.identity, m)
	j.retirementIntent = intentName(intentID)
	j.pendingSegments = nil
	for _, old := range sourceSegments {
		j.retiredSegments = append(j.retiredSegments, &retiredSegment{segment: old})
	}
	report = CompactionReport{Before: before, After: j.publication, CopiedBytes: before.PublishedEnd - SegmentHeaderSize, RetiredSegments: len(sourceSegments)}
	j.mu.Unlock()
	// The new publication is known durable. Cleanup errors leave a retryable
	// retirement obligation, rather than declaring the old layout authoritative.
	started = false
	report.ReclaimedSegments, report.PinnedSegments, err = j.reclaimRetired()
	return report, err
}
