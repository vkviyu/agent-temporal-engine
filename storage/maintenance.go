package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// RetentionPolicy describes the only destructive maintenance currently
// permitted by the storage layer. Published segments and manifests are never
// deleted by this API: they are part of the publication proof and require a
// future manifest-rewrite protocol. Quarantine evidence may be removed only
// when the caller explicitly opts in and the store owner lock is acquired.
type RetentionPolicy struct {
	DropQuarantine bool
}

func (p RetentionPolicy) Validate() error {
	return nil
}

type MaintenanceReport struct {
	QuarantineBefore   []string
	RemovedQuarantine  []string
	PublishedSegments  int
	PublishedManifests int
}

var ErrMaintenanceBusy = errors.New("maintenance requires exclusive store ownership")

// Maintain performs a bounded, crash-safe maintenance pass. It is deliberately
// conservative: published history is retained until a manifest/reclamation
// protocol can prove that no retained RootSet, Anchor, or recovery path needs
// the bytes. The operation is idempotent; an interrupted removal is retried on
// the next pass and never changes CURRENT or a published manifest.
func Maintain(ctx context.Context, d DurableIO, p RetentionPolicy) (MaintenanceReport, error) {
	var report MaintenanceReport
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
		return report, fmt.Errorf("%w: %v", ErrMaintenanceBusy, err)
	}
	defer owner.Close()
	segments, err := d.List("segments")
	if err != nil {
		return report, err
	}
	for _, n := range segments {
		if strings.HasPrefix(n, "SEGMENT.") {
			report.PublishedSegments++
		}
	}
	manifests, err := d.List("manifests")
	if err != nil {
		return report, err
	}
	for _, n := range manifests {
		if strings.HasPrefix(n, "MANIFEST.") {
			report.PublishedManifests++
		}
	}
	q, err := d.List("quarantine")
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return report, err
		}
		q = nil
	}
	sort.Strings(q)
	report.QuarantineBefore = append([]string(nil), q...)
	if !p.DropQuarantine || len(q) == 0 {
		return report, nil
	}
	for _, n := range q {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if n == "." || n == ".." || strings.Contains(n, "/") || n == "" {
			return report, ErrMalformed
		}
		if err := d.Remove("quarantine/" + n); err != nil {
			return report, err
		}
		report.RemovedQuarantine = append(report.RemovedQuarantine, n)
	}
	if err := d.SyncDir("quarantine"); err != nil {
		return report, err
	}
	if err := d.SyncDir("."); err != nil {
		return report, err
	}
	return report, nil
}
