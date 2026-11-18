package engine

import (
	"context"
	"errors"
	"io"

	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// recoveryCheckpointSchema is a certification/replay compatibility epoch,
// independent of reducer state representation. Schema 2 certifies indexed-only
// Knowledge replay. Unsupported epochs, including schema 1, replay from genesis.
const recoveryCheckpointSchema uint64 = 2

// CreateRecoveryCheckpoint verifies the entire published physical and semantic
// history into a fresh private engine, then certifies its generic projection.
// It preserves all audit history and uses the same sole maintenance writer as
// compaction. The returned publication changes generation, never the fence.
func (e *Engine) CreateRecoveryCheckpoint(ctx context.Context) (_ storage.Publication, err error) {
	start := e.options.Observation.Start()
	defer func() {
		e.options.Observation.Finish(start, observation.Event{Kind: observation.Checkpoint, Outcome: observationOutcome(err)})
	}()
	if err := e.publicGate(); err != nil {
		return storage.Publication{}, err
	}
	if err := e.writerMu.LockContext(ctx); err != nil {
		return storage.Publication{}, err
	}
	defer e.writerMu.Unlock()
	if err := e.mu.LockContext(ctx); err != nil {
		return storage.Publication{}, err
	}
	if err := e.ready(); err != nil {
		e.mu.Unlock()
		return storage.Publication{}, err
	}
	before, roots, rootSet := e.publication, e.roots, e.rootSet
	e.publicationPending.Store(true)
	e.mu.Unlock()
	defer e.publicationPending.Store(false)
	candidate := newAuditVerifier(e.options)
	after, err := e.journal.CreateRecoveryCheckpoint(ctx, recoveryCheckpointSchema, candidate.replay,
		func(p storage.Publication, w io.Writer) error {
			if p != before || candidate.roots != roots || candidate.rootSet != rootSet ||
				candidate.index.count() != e.index.count() ||
				(p.Position.LSN > 0 && candidate.publication.Fence() != p.Fence()) {
				return ErrInvalidStateData
			}
			candidate.publication = p
			if p.Position.LSN == 0 {
				candidate.history[0] = commitEnvelope{fence: p.Fence()}
			}
			return candidate.encodeRecoveryCheckpoint(w)
		})
	e.mu.Lock()
	if after.Store != "" {
		e.publication = after
	}
	if errors.Is(err, storage.ErrCommitUnknown) || errors.Is(err, storage.ErrStoreFenced) {
		e.fenced = err
	}
	e.mu.Unlock()
	if errors.Is(err, storage.ErrCommitUnknown) || errors.Is(err, storage.ErrStoreFenced) {
		e.stopAdmission(err)
	}
	return after, err
}
