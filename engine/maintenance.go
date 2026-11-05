package engine

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/storage"
)

// CompactAuditFull serializes with the command coordinator, authenticates the
// complete typed transition history into a private projection, then replaces
// only its physical layout. Full history is the retention root: no current or
// historical object, receipt, unknown effect or independent event is swept.
// Existing views keep their exact logical object addresses and anchors.
func (e *Engine) CompactAuditFull(ctx context.Context) (storage.CompactionReport, error) {
	if err := e.publicGate(); err != nil {
		return storage.CompactionReport{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return storage.CompactionReport{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return storage.CompactionReport{}, err
	}
	candidate := newEngine(e.options)
	p, count, err := e.journal.VerifyPublished(ctx, candidate.replay)
	if err == nil && count > 0 && (candidate.roots != e.roots || candidate.rootSet != e.rootSet || candidate.publication.Fence() != p.Fence() || candidate.index.count() != e.index.count()) {
		err = ErrInvalidStateData
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isLimit(err) {
			return storage.CompactionReport{}, err
		}
		return storage.CompactionReport{}, e.readFailure(err)
	}
	report, err := e.journal.CompactAuditFull(ctx)
	if report.After.Store != "" {
		e.publication = report.After
	}
	if errors.Is(err, storage.ErrCommitUnknown) || errors.Is(err, storage.ErrStoreFenced) {
		e.fenced = err
		e.stopAdmission(err)
	}
	return report, err
}

func (e *Engine) ReclaimRetired(ctx context.Context) ([]string, int, error) {
	if err := e.publicGate(); err != nil {
		return nil, 0, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, 0, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return nil, 0, err
	}
	return e.journal.ReclaimRetired(ctx)
}
