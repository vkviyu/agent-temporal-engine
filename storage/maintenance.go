package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// RetentionPolicy defaults to inspection only. Neither option removes segments
// or checkpoint streams, rewrites authority, or performs recovery/tail repair.
type RetentionPolicy struct {
	DropQuarantine         bool
	DropPublicationStaging bool
}

func (p RetentionPolicy) Validate() error {
	return nil
}

type MaintenanceReport struct {
	QuarantineBefore  []string
	RemovedQuarantine []string
	// Physical legacy prefix counts, NOT counts of authoritative publications.
	// PublishedManifests includes obsolete and even malformed MANIFEST.* files.
	PublishedSegments  int
	PublishedManifests int
	// The four inventory categories below are disjoint, sorted store-relative
	// paths. SelectedMetadata includes CURRENT, IDENTITY, the selected manifest
	// and its checkpoint path. Checkpoint paths are references (possibly absent),
	// not verification of the stream contents.
	SelectedMetadata      []string
	RetainedProofMetadata []string
	// StagedMetadata includes authenticated obsolete final manifests as well
	// as .STAGE.* and .CURRENT.* files. It is an inventory, not authorization
	// to remove files when PublicationReclamationBlocked is true.
	StagedMetadata        []string
	UnknownMetadata       []string
	RemovedStagedMetadata []string
	// Unknown metadata might conceal a source reference. Preserve it and block
	// ALL publication removal, without blocking explicitly requested quarantine
	// removal. ReclamationBlockers names the unknown paths causing the block.
	PublicationReclamationBlocked bool
	ReclamationBlockers           []string
}

var ErrMaintenanceBusy = errors.New("maintenance requires exclusive store ownership")

// Maintain performs a bounded, crash-safe maintenance pass. It is deliberately
// conservative: only authenticated, unselected publication metadata outside
// the proof closure can be removed. Sources referenced even by obsolete
// manifests are retained for this pass, so interrupted removal cannot strand
// a surviving reference; reclaiming their sources may require later passes.
// Inspection completes before any mutation. Requested cleanup always syncs its
// directories, including on an empty retry following an unsynced removal. Before
// reclamation it stabilizes the observed CURRENT namespace, which might have
// been left unsynced by an interrupted publisher; it never rewrites file bytes.
// If owner cleanup fails, the returned error retains a *CleanupError. Callers
// must keep d alive and retry that handle's Close until ownership is released.
func Maintain(ctx context.Context, d DurableIO, p RetentionPolicy) (report MaintenanceReport, err error) {
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if d == nil {
		return report, ErrClosed
	}
	if err := p.Validate(); err != nil {
		return report, err
	}
	owner, err := d.AcquireOwnerLock()
	if err != nil {
		// Acquisition can return a live owner together with an error.
		return report, CloseOnError(fmt.Errorf("%w: %w", ErrMaintenanceBusy, err), owner)
	}
	defer func() {
		// In particular, do NOT close the owner behind readMetadata's pending
		// file handle. CloseOnError composes FILE -> OWNER retry ownership.
		err = CloseOnError(err, owner)
		sortMaintenanceReport(&report)
	}()
	scan := publicationMaintenance{ctx: ctx, io: d}
	segments, err := scan.list("segments", false)
	if err != nil {
		return report, err
	}
	for _, n := range segments {
		if strings.HasPrefix(n, "SEGMENT.") {
			report.PublishedSegments++
		}
	}
	manifests, err := scan.list("manifests", false)
	if err != nil {
		return report, err
	}
	for _, n := range manifests {
		if strings.HasPrefix(n, "MANIFEST.") {
			report.PublishedManifests++
		}
	}
	q, err := scan.list("quarantine", true)
	if err != nil {
		return report, err
	}
	sort.Strings(q)
	report.QuarantineBefore = append([]string(nil), q...)
	for _, n := range q {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if p.DropQuarantine && !maintenanceBaseName(n) {
			return report, ErrMalformed
		}
	}
	if err = scan.inspect(manifests, &report); err != nil {
		return report, err
	}
	if p.DropPublicationStaging && !report.PublicationReclamationBlocked {
		// A process may have died after replacing CURRENT but before syncing
		// the root. Stabilize the observed publication BEFORE deleting an old
		// final manifest; otherwise a crash could restore the old CURRENT
		// after its manifest's removal was made durable. Publication already
		// synced the file bytes before installation. Never rewrite those bytes.
		for _, dir := range []string{"manifests", "."} {
			if err = ctx.Err(); err != nil {
				return report, err
			}
			if err = d.SyncDir(dir); err != nil {
				return report, err
			}
		}
	}
	if p.DropQuarantine {
		for _, n := range q {
			if err = ctx.Err(); err != nil {
				return report, err
			}
			if err = d.Remove("quarantine/" + n); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return report, err
			}
			report.RemovedQuarantine = append(report.RemovedQuarantine, n)
		}
		if err = ctx.Err(); err != nil {
			return report, err
		}
		if err = d.SyncDir("quarantine"); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return report, err
		}
	}
	if p.DropPublicationStaging {
		if !report.PublicationReclamationBlocked {
			for _, name := range report.StagedMetadata {
				if err = ctx.Err(); err != nil {
					return report, err
				}
				if err = d.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return report, err
				}
				report.RemovedStagedMetadata = append(report.RemovedStagedMetadata, name)
			}
		}
		if err = ctx.Err(); err != nil {
			return report, err
		}
		if err = d.SyncDir("manifests"); err != nil {
			return report, err
		}
	}
	if p.DropQuarantine || p.DropPublicationStaging {
		if err = ctx.Err(); err != nil {
			return report, err
		}
		if err = d.SyncDir("."); err != nil {
			return report, err
		}
	}
	return report, nil
}
