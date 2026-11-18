package engine

import (
	"bytes"
	"math"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// stagingView is captured under mu and private to one batch while writerMu is
// held. The published catalog and proof maps stay read-only until that writer
// installs the finished batch under mu. Public readers may use the same base.
// Only new batch data is retained here; it never owns or advances the journal.
func (e *Engine) stagingView() *Engine {
	options := e.options
	options.Observation = nil
	return &Engine{
		// The publication owner builds query indexes when it installs the batch.
		skipTaskTraceIndex: true,
		journal:            e.journal, options: options, roots: e.roots, rootSet: e.rootSet,
		publication: e.publication, index: newStagingIndex(e.index),
		envelopes: make(map[model.TxID]commitEnvelope), baseEnvelopes: e.envelopes,
		history: make(map[uint64]commitEnvelope), baseHistory: e.history,
		events: make(map[model.EventID]eventEntry), baseEvents: e.events,
		eventHeads: make(map[string]eventHead), baseEventHeads: e.eventHeads,
	}
}

// checkPrepared uses the encoded size, including all framing and metadata.
// Group scheduling has its own byte budget; these limits cover the complete
// published journal prefix and are also enforced by the physical publisher.
func (e *Engine) checkPrepared(p *storage.PreparedTransaction) error {
	if p == nil {
		return ErrInvalidStateData
	}
	options, err := storage.NormalizeJournalOptions(e.options.Journal)
	if err != nil {
		return err
	}
	if e.publication.Generation == math.MaxUint64 || e.publication.Position.LSN == math.MaxUint64 ||
		e.publication.Position.LSN >= options.MaxPublishedTransactions || p.Position().LSN > options.MaxPublishedTransactions ||
		e.publication.PublishedEnd > options.MaxPublishedBytes || p.Size() > options.MaxPublishedBytes-e.publication.PublishedEnd {
		return storage.ErrLimit
	}
	return nil
}

// acceptStaged makes a prepared predecessor visible only to subsequent work in
// this batch. Its objects are owned memory, not unpublished journal locations.
func (e *Engine) acceptStaged(plan *commitPlan, p *storage.PreparedTransaction) error {
	if plan == nil || plan.builder == nil || plan.builder.base != e.index ||
		e.index.parent == nil || e.index.staged == nil || p == nil || p.Position().LSN != plan.transaction.LSN {
		return ErrInvalidStateData
	}
	if err := e.checkPrepared(p); err != nil {
		return err
	}
	for _, obj := range plan.builder.objects {
		// The builder already verified canonical bytes and topological order.
		// Clone them so neither a retained plan nor a returned read can mutate
		// the view used to prepare later members.
		e.index.staged[obj.id] = bytes.Clone(obj.encoded)
		e.index.entries[obj.id] = storage.RecordLocation{PayloadOffset: -1, PayloadLength: uint32(len(obj.encoded))}
	}
	e.roots = plan.roots
	e.rootSet = plan.rootSet
	e.publication.Position = p.Position()
	e.publication.PostRootSet = asDigest(plan.rootSet)
	e.publication.PublishedEnd += p.Size()
	// A group advances the physical generation once, so retain the generation
	// of its base here; each member still has its own position and proof.
	e.envelopes[plan.transaction.TxID] = commitEnvelope{fence: e.publication.Fence(), rootSet: plan.rootSet, space: plan.transaction.Request.Space, principal: plan.transaction.Request.Principal}
	e.history[p.Position().LSN] = e.envelopes[plan.transaction.TxID]
	e.installEvent(plan)
	return nil
}
