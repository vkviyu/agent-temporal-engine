package engine

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// CompactAuditFull serializes with the command coordinator, authenticates the
// complete typed transition history into a private projection, then replaces
// only its physical layout. Full history is the retention root: no current or
// historical object, receipt, unknown effect or independent event is swept.
// Existing views keep their exact logical object addresses and anchors.
func (e *Engine) CompactAuditFull(ctx context.Context) (report storage.CompactionReport, err error) {
	start := e.options.Observation.Start()
	defer func() {
		e.options.Observation.Finish(start, observation.Event{Kind: observation.Compaction, Outcome: observationOutcome(err), Count: uint64(report.RetiredSegments), Bytes: report.CopiedBytes})
	}()
	if err := e.publicGate(); err != nil {
		return storage.CompactionReport{}, err
	}
	if err := e.writerMu.LockContext(ctx); err != nil {
		return storage.CompactionReport{}, err
	}
	defer e.writerMu.Unlock()
	if err := e.mu.LockContext(ctx); err != nil {
		return storage.CompactionReport{}, err
	}
	if err := e.ready(); err != nil {
		e.mu.Unlock()
		return storage.CompactionReport{}, err
	}
	roots, rootSet := e.roots, e.rootSet
	e.publicationPending.Store(true)
	e.mu.Unlock()
	defer e.publicationPending.Store(false)
	candidate := newAuditVerifier(e.options)
	p, count, err := e.journal.VerifyPublished(ctx, candidate.replay)
	if err == nil && count > 0 && (candidate.roots != roots || candidate.rootSet != rootSet || candidate.publication.Fence() != p.Fence() || candidate.index.count() != e.index.count()) {
		err = ErrInvalidStateData
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isLimit(err) {
			return storage.CompactionReport{}, err
		}
		return storage.CompactionReport{}, e.fenceWriter(err)
	}
	e.mu.Lock()
	err = e.workerReady()
	e.mu.Unlock()
	if err != nil {
		return storage.CompactionReport{}, err
	}
	report, err = e.journal.CompactAuditFull(ctx)
	e.mu.Lock()
	if report.After.Store != "" {
		e.publication = report.After
	}
	if errors.Is(err, storage.ErrCommitUnknown) || errors.Is(err, storage.ErrStoreFenced) {
		e.fenced = err
	}
	e.mu.Unlock()
	if errors.Is(err, storage.ErrCommitUnknown) || errors.Is(err, storage.ErrStoreFenced) {
		e.stopAdmission(err)
	}
	return report, err
}

func (e *Engine) ReclaimRetired(ctx context.Context) ([]string, int, error) {
	if err := e.publicGate(); err != nil {
		return nil, 0, err
	}
	if err := e.writerMu.LockContext(ctx); err != nil {
		return nil, 0, err
	}
	defer e.writerMu.Unlock()
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, 0, err
	}
	if err := e.ready(); err != nil {
		e.mu.Unlock()
		return nil, 0, err
	}
	e.publicationPending.Store(true)
	e.mu.Unlock()
	defer e.publicationPending.Store(false)
	return e.journal.ReclaimRetired(ctx)
}
