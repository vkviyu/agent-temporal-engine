package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"

	"github.com/vkviyu/agent-temporal-engine/model"
)

type RecoveryReport struct {
	Verified                    bool
	PublicationSynced           bool
	Publication                 Publication
	PublishedBytes              uint64
	ScannedTransactions         uint64
	TailBytes                   uint64
	TailDisposition             string
	QuarantineFile              string
	OrphanSegments              []string
	QuarantineFiles             []string
	ReclaimedSegments           []string
	DiscardedCompactionSegments []string
}

// OpenJournal follows CURRENT only. It never falls back to a different manifest
// or promotes a complete unreferenced transaction. TailReject returns a verified
// report without changing the tail. TailQuarantine first preserves and syncs a
// diagnostic copy, then truncates/syncs strictly beyond the verified prefix.
func OpenJournal(d DurableIO, options JournalOptions) (*Journal, RecoveryReport, error) {
	return openJournal(d, options, nil)
}

func openJournal(d DurableIO, options JournalOptions, replay ReplayFunc) (_ *Journal, report RecoveryReport, err error) {
	o, err := checkIO(d, options)
	if err != nil {
		return nil, report, err
	}
	owner, err := d.AcquireOwnerLock()
	if err != nil {
		return nil, report, cleanupOnError(err, owner)
	}
	var segments []*journalSegment
	var orphans []orphanSegment
	var compactions []stagedCompaction
	defer func() {
		if err != nil {
			var resources []io.Closer
			for _, staged := range compactions {
				if staged.file != nil {
					resources = append(resources, staged.file)
				}
			}
			for _, orphan := range orphans {
				resources = append(resources, orphan.file)
			}
			for _, segment := range segments {
				if segment.file != nil {
					resources = append(resources, segment.file)
				}
			}
			err = cleanupOnError(err, append(resources, owner)...)
		}
	}()
	currentBytes, err := readMetadata(d, "CURRENT")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, report, fmt.Errorf("%w: CURRENT missing: %w", ErrIncompleteStore, err)
		}
		return nil, report, recoveryInputError("CURRENT", err)
	}
	current, err := DecodeCurrent(currentBytes)
	if err != nil {
		return nil, report, recoveryInputError("CURRENT", err)
	}
	idBytes, err := readMetadata(d, "IDENTITY")
	if err != nil {
		return nil, report, recoveryInputError("IDENTITY", err)
	}
	if Digest(sha256.Sum256(idBytes)) != current.IdentityDigest {
		return nil, report, fmt.Errorf("%w: identity digest", ErrCorruption)
	}
	id, err := DecodeIdentity(idBytes)
	if err != nil {
		return nil, report, recoveryInputError("IDENTITY", err)
	}
	if id.Store != current.Store {
		return nil, report, fmt.Errorf("%w: identity/store mismatch", ErrCorruption)
	}
	if id.Profile != d.Profile() {
		return nil, report, fmt.Errorf("%w: durability profile differs from identity", ErrUnsupported)
	}
	manifestBytes, err := readMetadata(d, manifestName(current.ManifestID))
	if err != nil {
		return nil, report, recoveryInputError("manifest", err)
	}
	if Digest(sha256.Sum256(manifestBytes)) != current.ManifestDigest {
		return nil, report, fmt.Errorf("%w: manifest digest", ErrCorruption)
	}
	m, err := DecodeManifest(manifestBytes)
	if err != nil {
		return nil, report, recoveryInputError("manifest", err)
	}
	if m.Store != id.Store || m.Generation != current.Generation || m.ManifestID != current.ManifestID {
		return nil, report, fmt.Errorf("%w: CURRENT/manifest binding", ErrCorruption)
	}
	if err = validateRetirementSource(d, m); err != nil {
		return nil, report, err
	}
	compactions, err = readCompactionIntents(d, id.Store)
	if err != nil {
		return nil, report, err
	}
	if m.PublishedEnd > o.MaxPublishedBytes {
		return nil, report, ErrLimit
	}
	if m.Position.LSN > o.MaxPublishedTransactions {
		return nil, report, ErrLimit
	}
	genesis, err := GenesisPosition(id)
	if err != nil {
		return nil, report, err
	}
	descriptors := manifestDescriptors(m, genesis)
	var activeSize int64
	for _, desc := range descriptors {
		f, openErr := d.OpenAppend(segmentName(desc.SegmentID))
		if f != nil {
			segments = append(segments, &journalSegment{descriptor: desc, file: f})
		}
		if openErr != nil {
			return nil, report, recoveryInputError("segment", openErr)
		}
		if f == nil {
			return nil, report, fmt.Errorf("%w: segment open returned nil handle", ErrMalformed)
		}
		size, sizeErr := f.Size()
		if sizeErr != nil {
			return nil, report, sizeErr
		}
		end := desc.DataEnd
		if desc.Sealed {
			end += SegmentFooterSize
		}
		if size < 0 || uint64(size) < end || (desc.Sealed && uint64(size) != end) {
			return nil, report, fmt.Errorf("%w: referenced segment size", ErrCorruption)
		}
		activeSize = size
	}
	var txIDs map[model.TxID]struct{}
	var count uint64
	if len(m.Segments) == 0 {
		var prefix hash.Hash
		prefix, txIDs, count, err = verifyPrefixWithReplay(segments[0].file, id, m, genesis, o.Limits, o.MaxPublishedTransactions, replay)
		segments[0].prefix = prefix
	} else {
		txIDs, count, err = verifySegmentedPrefix(segments, id, m, genesis, o, replay)
	}
	if err != nil {
		return nil, report, err
	}
	if err = verifyCompactionSources(d, m, logicalReader{segments: segments, end: m.PublishedEnd}, genesis, o, compactions); err != nil {
		return nil, report, err
	}
	active := segments[len(segments)-1]
	report.Verified = true
	report.Publication = publicationFor(id, m)
	report.PublishedBytes = m.PublishedEnd
	report.ScannedTransactions = count
	report.TailBytes = uint64(activeSize) - active.descriptor.DataEnd
	report.TailDisposition = "none"
	orphans, err = collectOrphans(d, compactionIgnoredDescriptors(descriptors, m, compactions), &report)
	if err != nil {
		return nil, report, err
	}
	if err = cleanupCompactionIntents(d, m, genesis, compactions, &report); err != nil {
		return nil, report, fmt.Errorf("%w: compaction staging cleanup: %w", ErrRecoveryIncomplete, err)
	}
	for _, retired := range m.RetiredSegments {
		name := segmentName(retired.SegmentID)
		if err = d.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, report, fmt.Errorf("%w: retired layout removal: %w", ErrRecoveryIncomplete, err)
		}
		report.ReclaimedSegments = append(report.ReclaimedSegments, name)
	}
	if len(m.RetiredSegments) > 0 {
		if err = d.SyncDir("segments"); err != nil {
			return nil, report, fmt.Errorf("%w: retired layout directory sync: %w", ErrRecoveryIncomplete, err)
		}
	}
	if report.TailBytes != 0 || len(orphans) > 0 {
		report.TailDisposition = "preserved"
		if o.TailPolicy != TailQuarantine {
			return nil, report, ErrRecoveryRequired
		}
		if report.TailBytes > MaxUnpublishedLayoutBytes {
			return nil, report, fmt.Errorf("%w: unpublished layout exceeds ceiling", ErrLimit)
		}
		if len(orphans) == 0 {
			// Preserve the legacy raw-tail evidence and IO protocol when no orphan
			// layout is involved. The complete published prefix was verified above.
			if err = quarantineTail(d, active.file, active.descriptor.DataEnd, &report); err != nil {
				return nil, report, fmt.Errorf("%w: %w", ErrRecoveryIncomplete, err)
			}
			report.QuarantineFiles = []string{report.QuarantineFile}
		} else if err = quarantineLayout(d, active, orphans, &report); err != nil {
			return nil, report, fmt.Errorf("%w: %w", ErrRecoveryIncomplete, err)
		}
	}
	// Successful quarantine already retired every orphan before removal.
	// Reestablish the verified publication witness after a process restart.
	if err = d.SyncDir("."); err != nil {
		return nil, report, fmt.Errorf("%w: publication witness sync: %w", ErrRecoveryIncomplete, err)
	}
	report.PublicationSynced = true
	return &Journal{io: d, owner: owner, segment: active.file, identity: id, identityDigest: current.IdentityDigest, headerDigest: active.descriptor.HeaderDigest, segmentID: active.descriptor.SegmentID, options: o, publication: report.Publication, prefix: active.prefix, txIDs: txIDs, state: JournalReady, segments: segments, manifest: m}, report, nil
}

// Resource ceilings, unsupported formats and transient IO failures are not
// asserted to be corruption. Structural/identity failures within CURRENT's
// referenced prefix are corruption and are never treated as discardable tail.
func recoveryInputError(stage string, err error) error {
	if errors.Is(err, ErrUnsupported) || errors.Is(err, ErrLimit) {
		return fmt.Errorf("%s: %w", stage, err)
	}
	if errors.Is(err, ErrMalformed) || errors.Is(err, ErrIntegrity) || errors.Is(err, ErrIncomplete) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %s: %w", ErrCorruption, stage, err)
	}
	return fmt.Errorf("%s: %w", stage, err)
}

func verifyPrefix(segment ReadFile, id Identity, m Manifest, genesis Position, limits Limits, maxTransactions uint64) (hash.Hash, map[model.TxID]struct{}, uint64, error) {
	return verifyPrefixWithReplay(segment, id, m, genesis, limits, maxTransactions, nil)
}

func verifyPrefixWithReplay(segment ReadFile, id Identity, m Manifest, genesis Position, limits Limits, maxTransactions uint64, replay ReplayFunc) (hash.Hash, map[model.TxID]struct{}, uint64, error) {
	headerBytes := make([]byte, SegmentHeaderSize)
	n, err := segment.ReadAt(headerBytes, 0)
	if n == len(headerBytes) && err == io.EOF {
		err = nil
	}
	if n < len(headerBytes) && errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	if err == nil && n != len(headerBytes) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, nil, 0, recoveryInputError("segment header", err)
	}
	if Digest(sha256.Sum256(headerBytes)) != m.SegmentHeaderDigest {
		return nil, nil, 0, fmt.Errorf("%w: segment header digest", ErrCorruption)
	}
	header, err := DecodeSegmentHeader(headerBytes)
	if err != nil {
		return nil, nil, 0, recoveryInputError("segment header", err)
	}
	if header.Store != id.Store || header.SegmentID != m.SegmentID || header.FirstLSN != 1 || header.PreviousDigest != genesis.Digest {
		return nil, nil, 0, fmt.Errorf("%w: segment identity/genesis", ErrCorruption)
	}
	h := sha256.New()
	h.Write(headerBytes)
	reader := io.TeeReader(io.NewSectionReader(segment, SegmentHeaderSize, int64(m.PublishedEnd-SegmentHeaderSize)), h)
	scanner, err := NewScanner(reader, genesis, limits)
	if err != nil {
		return nil, nil, 0, err
	}
	seen := make(map[model.TxID]struct{})
	var root Digest
	count := uint64(0)
	for {
		tx, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, 0, recoveryInputError("published transactions", err)
		}
		if _, exists := seen[tx.TxID]; exists {
			return nil, nil, 0, fmt.Errorf("%w: duplicate transaction ID", ErrCorruption)
		}
		if count >= maxTransactions {
			return nil, nil, 0, ErrLimit
		}
		seen[tx.TxID] = struct{}{}
		root = tx.PostRootSet
		count++
		if replay != nil {
			// Scanner offsets are relative to its unsegmented input. Replay
			// locations address the segment, including its physical header.
			tx.Store = id.Store
			tx.StartOffset += SegmentHeaderSize
			tx.EndOffset += SegmentHeaderSize
			for i := range tx.RecordLocations {
				tx.RecordLocations[i].PayloadOffset += SegmentHeaderSize
			}
			reader := &replayReader{reader: boundedReaderAt{reader: segment, end: int64(m.PublishedEnd)}, active: true}
			err := replay(tx, reader)
			reader.release()
			if err != nil {
				return nil, nil, 0, fmt.Errorf("replay transaction %d: %w", tx.LSN, err)
			}
		}
	}
	if scanner.Position() != m.Position || root != m.PostRootSet || hashDigest(h) != m.PrefixDigest || uint64(scanner.LastCompleteOffset())+SegmentHeaderSize != m.PublishedEnd {
		return nil, nil, 0, fmt.Errorf("%w: published tip/root/prefix mismatch", ErrCorruption)
	}
	return h, seen, count, nil
}

func quarantineTail(d DurableIO, segment DurableFile, end uint64, report *RecoveryReport) (err error) {
	if err = d.Mkdir("quarantine"); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	// Persist the quarantine directory's root entry before relying on a child.
	if err = d.SyncDir("."); err != nil {
		return err
	}
	id, err := randomID()
	if err != nil {
		return err
	}
	name := "quarantine/TAIL." + id
	f, err := d.CreateExclusive(name)
	if err != nil {
		return cleanupOnError(err, f)
	}
	closed := false
	defer func() {
		if !closed {
			closeInto(&err, f)
		}
	}()
	reader := io.NewSectionReader(segment, int64(end), int64(report.TailBytes))
	buffer := make([]byte, 64<<10)
	remaining := report.TailBytes
	for remaining > 0 {
		n := uint64(len(buffer))
		if n > remaining {
			n = remaining
		}
		if _, err = io.ReadFull(reader, buffer[:int(n)]); err != nil {
			return err
		}
		if err = writeAll(f, buffer[:int(n)]); err != nil {
			return err
		}
		remaining -= n
	}
	if err = f.Sync(); err != nil {
		return err
	}
	closeInto(&err, f)
	closed = true
	if err != nil {
		return err
	}
	if err = d.SyncDir("quarantine"); err != nil {
		return err
	}
	report.QuarantineFile = name
	// Only after the copy and its name are synced may verified unpublished
	// bytes be removed from the active file. Never truncate inside CURRENT.
	if err = segment.Truncate(int64(end)); err != nil {
		return err
	}
	if err = segment.Sync(); err != nil {
		return err
	}
	report.TailDisposition = "quarantined"
	return nil
}
