package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
)

// These budgets cover the whole inspection, including non-metadata directory
// entries. DurableIO.List itself is not paginated; its allocation and a single
// underlying IO call cannot be interrupted by this API.
const (
	MaxMaintenanceEntries       = 16_384
	MaxMaintenanceMetadataBytes = 64 << 20
)

var errMaintenanceBudget = fmt.Errorf("%w: maintenance scan budget", ErrLimit)

type publicationMaintenance struct {
	ctx     context.Context
	io      DurableIO
	entries int
	bytes   int64
}

func maintenanceBaseName(n string) bool {
	return n != "" && n != "." && n != ".." && !strings.ContainsAny(n, "/\\\x00")
}

func maintenanceID(name, prefix string) (string, bool) {
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	id := strings.TrimPrefix(name, prefix)
	_, err := metadataID(id)
	return id, err == nil
}

func (s *publicationMaintenance) list(dir string, optional bool) ([]string, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	names, err := s.io.List(dir)
	if optional && errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(names) > MaxMaintenanceEntries-s.entries {
		return nil, errMaintenanceBudget
	}
	s.entries += len(names)
	sort.Strings(names)
	return names, nil
}

// Like readMetadata, but also budgets aggregate allocation and admits the
// larger compaction-intent envelope. Close failures must never be downgraded
// to an unknown-file classification: the caller still owns the file.
func (s *publicationMaintenance) read(name string, maximum int64) (_ []byte, err error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.io.OpenRead(name)
	if err != nil {
		return nil, CloseOnError(err, f)
	}
	if f == nil {
		return nil, ErrMalformed
	}
	defer closeInto(&err, f)
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, ErrMalformed
	}
	if size > maximum {
		return nil, ErrLimit
	}
	if size > MaxMaintenanceMetadataBytes-s.bytes {
		return nil, errMaintenanceBudget
	}
	s.bytes += size
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	b := make([]byte, int(size))
	n, err := f.ReadAt(b, 0)
	if n == len(b) && err == io.EOF {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, io.ErrUnexpectedEOF
	}
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	return b, nil
}

func maintenanceUnknownInput(err error) bool {
	if len(pendingCleanups(err)) != 0 || errors.Is(err, errMaintenanceBudget) {
		return false
	}
	return errors.Is(err, ErrMalformed) || errors.Is(err, ErrLimit) ||
		errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrInvalid) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

type maintenanceManifest struct {
	name   string
	m      Manifest
	digest Digest
	valid  bool
}

func decodeMaintenanceManifest(name, id string, b []byte) (*maintenanceManifest, error) {
	m, err := DecodeManifest(b)
	if err != nil {
		return nil, err
	}
	canonical, err := EncodeManifest(m)
	if err != nil || !bytes.Equal(b, canonical) || m.ManifestID != id {
		return nil, ErrMalformed
	}
	return &maintenanceManifest{name: name, m: m, digest: Digest(sha256.Sum256(b))}, nil
}

// Authenticate exactly the recovery metadata authority, without opening a
// Journal (which can sync/repair/retire files). Cross-manifest retirement
// authorization is checked below against the same source descriptors as
// validateRetirementSource. No segment or checkpoint stream is read or changed.
func (s *publicationMaintenance) authority() (Current, Identity, *maintenanceManifest, error) {
	var c Current
	var id Identity
	b, err := s.read("CURRENT", MaxMetadataBytes)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return c, id, nil, fmt.Errorf("%w: CURRENT missing: %w", ErrIncompleteStore, err)
		}
		return c, id, nil, recoveryInputError("CURRENT", err)
	}
	c, err = DecodeCurrent(b)
	if err != nil {
		return c, id, nil, recoveryInputError("CURRENT", err)
	}
	canonical, err := EncodeCurrent(c)
	if err != nil || !bytes.Equal(b, canonical) {
		return c, id, nil, ErrCorruption
	}
	b, err = s.read("IDENTITY", MaxMetadataBytes)
	if err != nil {
		return c, id, nil, recoveryInputError("IDENTITY", err)
	}
	if Digest(sha256.Sum256(b)) != c.IdentityDigest {
		return c, id, nil, fmt.Errorf("%w: identity digest", ErrCorruption)
	}
	id, err = DecodeIdentity(b)
	if err != nil {
		return c, id, nil, recoveryInputError("IDENTITY", err)
	}
	canonical, err = EncodeIdentity(id)
	if err != nil || !bytes.Equal(b, canonical) || id.Store != c.Store {
		return c, id, nil, fmt.Errorf("%w: identity/store binding", ErrCorruption)
	}
	if id.Profile != s.io.Profile() {
		return c, id, nil, fmt.Errorf("%w: durability profile differs from identity", ErrUnsupported)
	}
	b, err = s.read(manifestName(c.ManifestID), MaxManifestBytes)
	if err != nil {
		return c, id, nil, recoveryInputError("manifest", err)
	}
	if Digest(sha256.Sum256(b)) != c.ManifestDigest {
		return c, id, nil, fmt.Errorf("%w: manifest digest", ErrCorruption)
	}
	m, err := decodeMaintenanceManifest(manifestName(c.ManifestID), c.ManifestID, b)
	if err != nil {
		return c, id, nil, recoveryInputError("manifest", err)
	}
	if m.m.Store != id.Store || m.m.Generation != c.Generation {
		return c, id, nil, fmt.Errorf("%w: CURRENT/manifest binding", ErrCorruption)
	}
	return c, id, m, nil
}

func maintenanceSource(source LayoutSource, finals map[string]*maintenanceManifest) *maintenanceManifest {
	old := finals[source.ManifestID]
	if old == nil || !old.valid || old.digest != source.ManifestDigest ||
		old.m.Generation != source.Generation || old.m.PublishedEnd != source.PublishedEnd ||
		old.m.Position != source.Position || old.m.PostRootSet != source.PostRootSet {
		return nil
	}
	return old
}

func maintenanceSources(m Manifest) []LayoutSource {
	var out []LayoutSource
	if m.LayoutSource != nil {
		out = append(out, *m.LayoutSource)
	}
	if m.RecoveryCheckpoint != nil {
		out = append(out, m.RecoveryCheckpoint.Source)
	}
	return out
}

func (s *publicationMaintenance) inspect(names []string, report *MaintenanceReport) error {
	current, identity, selected, err := s.authority()
	if err != nil {
		return err
	}
	unknown := map[string]bool{}
	proofs := map[string]bool{}
	chosen := map[string]bool{"CURRENT": true, "IDENTITY": true, selected.name: true}
	if c := selected.m.RecoveryCheckpoint; c != nil {
		chosen[checkpointName(c.ID)] = true
	}
	finals := map[string]*maintenanceManifest{current.ManifestID: selected}
	byID := map[string][]*maintenanceManifest{current.ManifestID: {selected}}
	records := []*maintenanceManifest{selected}
	for _, name := range names {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		full := "manifests/" + name
		if full == selected.name {
			continue
		}
		id, final := maintenanceID(name, "MANIFEST.")
		if !final {
			var stage bool
			id, stage = maintenanceID(name, ".STAGE.")
			if !stage {
				unknown[full] = true
				continue
			}
		}
		b, err := s.read(full, MaxManifestBytes)
		if err != nil {
			if !maintenanceUnknownInput(err) {
				return err
			}
			unknown[full] = true
			continue
		}
		record, err := decodeMaintenanceManifest(full, id, b)
		if err != nil || record.m.Store != identity.Store {
			unknown[full] = true
			continue
		}
		records = append(records, record)
		byID[id] = append(byID[id], record)
		if final {
			finals[id] = record
		}
	}

	// Every source edge has a strictly lower generation (codec validation).
	// Ascending validation therefore proves the transitive closure without
	// recursion or trusting a cached node before its own sources are verified.
	sort.Slice(records, func(i, j int) bool { return records[i].m.Generation < records[j].m.Generation })
	genesis, err := GenesisPosition(identity)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		r.valid = true
		if final := finals[r.m.ManifestID]; final != nil && final.digest != r.digest {
			r.valid = false // conflicting .STAGE, never overwrite/guess
		}
		for _, source := range maintenanceSources(r.m) {
			if maintenanceSource(source, finals) == nil {
				r.valid = false
			}
		}
		if r.valid && r.m.LayoutSource != nil {
			old := finals[r.m.LayoutSource.ManifestID].m
			allowed := map[string]SegmentDescriptor{}
			for _, d := range manifestDescriptors(old, genesis) {
				allowed[d.SegmentID] = d
			}
			for _, d := range old.RetiredSegments {
				allowed[d.SegmentID] = d
			}
			for _, d := range r.m.RetiredSegments {
				if a, ok := allowed[d.SegmentID]; !ok || a != d {
					r.valid = false
				}
			}
		}
		if !r.valid {
			unknown[r.name] = true
			continue
		}
		// Pin sources of candidates too. This intentionally needs multiple
		// passes for obsolete chains but is safe at EVERY removal boundary.
		for _, source := range maintenanceSources(r.m) {
			proofs[manifestName(source.ManifestID)] = true
		}
		if c := r.m.RecoveryCheckpoint; c != nil {
			proofs[checkpointName(c.ID)] = true
		}
	}
	if !selected.valid {
		return fmt.Errorf("%w: selected manifest source closure", ErrCorruption)
	}

	intents, err := s.list("compactions", true)
	if err != nil {
		return err
	}
	for _, name := range intents {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		full := "compactions/" + name
		id, ok := maintenanceID(name, "INTENT.")
		if !ok {
			unknown[full] = true
			continue
		}
		b, err := s.read(full, maxCompactionIntentBytes)
		if err != nil {
			if !maintenanceUnknownInput(err) {
				return err
			}
			unknown[full] = true
			continue
		}
		in, err := decodeCompactionIntent(b)
		if err != nil || in.ID != id || in.Store != identity.Store ||
			maintenanceSource(in.Source, finals) == nil ||
			(in.Source.Generation >= current.Generation && in.Source.ManifestID != current.ManifestID) ||
			in.Source.PublishedEnd > selected.m.PublishedEnd || in.Source.Position.LSN > selected.m.Position.LSN ||
			(in.Source.Position.LSN == selected.m.Position.LSN &&
				(in.Source.Position != selected.m.Position || in.Source.PostRootSet != selected.m.PostRootSet ||
					in.Source.PublishedEnd != selected.m.PublishedEnd)) ||
			(in.Source.Position.LSN == 0 && (in.Source.Position != genesis || in.Source.PublishedEnd != SegmentHeaderSize)) {
			unknown[full] = true
			continue
		}
		// Maintenance does not consume intents or reclaim their segments.
		proofs[full] = true
		proofs[manifestName(in.Source.ManifestID)] = true
	}

	root, err := s.list(".", false)
	if err != nil {
		return err
	}
	var staged []string
	for _, name := range root {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		switch name {
		case "CURRENT", "IDENTITY", ".owner.lock", "segments", "manifests", "quarantine", "compactions", "checkpoints":
			continue
		}
		id, ok := maintenanceID(name, ".CURRENT.")
		if !ok {
			unknown[name] = true
			continue
		}
		b, err := s.read(name, MaxMetadataBytes)
		if err != nil {
			if !maintenanceUnknownInput(err) {
				return err
			}
			unknown[name] = true
			continue
		}
		c, err := DecodeCurrent(b)
		if err != nil {
			unknown[name] = true
			continue
		}
		canonical, err := EncodeCurrent(c)
		if err != nil || !bytes.Equal(b, canonical) || c.ManifestID != id ||
			c.Store != identity.Store || c.IdentityDigest != current.IdentityDigest {
			unknown[name] = true
			continue
		}
		// If a corresponding manifest is present, bind the exact bytes and
		// generation. A missing manifest is safe: the canonical CURRENT still
		// proves its own identity and is never publication authority.
		for _, r := range byID[id] {
			if !r.valid || r.digest != c.ManifestDigest || r.m.Generation != c.Generation {
				ok = false
			}
		}
		if unknown[manifestName(id)] || unknown["manifests/.STAGE."+id] {
			ok = false
		}
		if !ok {
			unknown[name] = true
			continue
		}
		staged = append(staged, name)
	}
	checkpoints, err := s.list("checkpoints", true)
	if err != nil {
		return err
	}
	for _, name := range checkpoints {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		full := "checkpoints/" + name
		if !chosen[full] && !proofs[full] {
			// A loose stream has no standalone ownership certificate.
			unknown[full] = true
		}
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	for name := range chosen {
		report.SelectedMetadata = append(report.SelectedMetadata, name)
	}
	for name := range proofs {
		if !chosen[name] {
			report.RetainedProofMetadata = append(report.RetainedProofMetadata, name)
		}
	}
	for _, r := range records {
		if r.valid && !chosen[r.name] && !proofs[r.name] {
			staged = append(staged, r.name)
		}
	}
	report.StagedMetadata = staged
	for name := range unknown {
		report.UnknownMetadata = append(report.UnknownMetadata, name)
	}
	report.PublicationReclamationBlocked = len(unknown) != 0
	report.ReclamationBlockers = append([]string(nil), report.UnknownMetadata...)
	sortMaintenanceReport(report)
	return nil
}

func sortMaintenanceReport(r *MaintenanceReport) {
	for _, paths := range [][]string{r.QuarantineBefore, r.RemovedQuarantine,
		r.SelectedMetadata, r.RetainedProofMetadata, r.StagedMetadata,
		r.UnknownMetadata, r.RemovedStagedMetadata, r.ReclamationBlockers} {
		sort.Strings(paths)
	}
}
