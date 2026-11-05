package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// The profile checks identities; the engine proves either the exact published
// knowledge cut or a private Work revision bound to its preallocated producer.
func (e *Engine) checkContinuationKnowledge(ctx context.Context, req api.CommitRequest, c agentprofile.Continuation) error {
	p := c.Position
	if p == nil {
		return agentprofile.ErrCommandShape
	}
	if p.Space != req.Space || p.Principal != req.Principal {
		return agentprofile.ErrCommandScope
	}
	if source := p.KnowledgeSource; source != nil {
		if source.Validate() != nil || source.Space != req.Space || source.Store != e.publication.Store || source.Knowledge == nil || source.Cursor == nil {
			return model.ErrAnchorMismatch
		}
		if seed, ok := ctx.Value(initialWorkKey{}).(*initialWorkSeed); ok && e.privateReduction && source.ProducerTx == seed.tx && source.Knowledge.Execution == c.Execution {
			if err := e.authorizeKnowledge(ctx, e.roots, req.Space, req.Principal, *source.Knowledge); err != nil {
				return err
			}
			anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.publication.Fence(), Cursor: source.Cursor, Knowledge: source.Knowledge}
			_, _, _, _, err := e.selectedKnowledge(ctx, e.roots, anchor, req.Principal)
			return err
		}
		// A public save may only name a real published producer. Earlier private
		// batch members are not a publication proof and never substitute latest.
		envelopes := e.envelopes
		if e.baseEnvelopes != nil {
			envelopes = e.baseEnvelopes
		}
		proof, ok := envelopes[source.ProducerTx]
		if !ok || proof.space != req.Space || proof.principal != req.Principal {
			return agentprofile.ErrUnknown
		}
		anchor := model.SnapshotAnchor{Space: req.Space, Fence: proof.fence, Cursor: source.Cursor, Knowledge: source.Knowledge}
		roots, id, err := e.rootsAtFence(ctx, proof.fence, true)
		if err != nil {
			return err
		}
		if id != proof.rootSet {
			return ErrInvalidStateData
		}
		if err = e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, *source.Knowledge); err != nil {
			return err
		}
		if err = e.authorizeKnowledge(ctx, e.roots, req.Space, req.Principal, *source.Knowledge); err != nil {
			return err
		}
		_, _, _, _, err = e.selectedKnowledge(ctx, roots, anchor, req.Principal)
		return err
	}
	if p.KnowledgeFence == (model.CommitFence{}) && p.KnowledgeCursor == (model.Cursor{}) && p.KnowledgeKey == (model.KnowledgeKey{}) {
		ledger, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
		if err != nil {
			return err
		}
		execution, err := ledger.GetExecution(c.Execution)
		if err != nil {
			return err
		}
		snapshot, _, _, err := e.loadKnowledge(ctx, e.roots, req.Space, req.Principal)
		if err != nil {
			return err
		}
		key := model.KnowledgeKey{Actor: execution.Actor, Execution: execution.ID}
		for _, observation := range snapshot.Observations {
			if observation.Key == key {
				return model.ErrAnchorMismatch
			}
		}
		for _, packet := range snapshot.Packets {
			if packet.Key != nil && *packet.Key == key {
				return model.ErrAnchorMismatch
			}
		}
		return nil
	}
	anchor := model.SnapshotAnchor{Space: p.Space, Fence: p.KnowledgeFence, Cursor: &p.KnowledgeCursor, Knowledge: &p.KnowledgeKey}
	roots, _, err := e.rootsAtFence(ctx, anchor.Fence, true)
	if err != nil {
		return err
	}
	if err := e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, p.KnowledgeKey); err != nil {
		return err
	}
	_, _, _, _, err = e.selectedKnowledge(ctx, roots, anchor, req.Principal)
	return err
}
