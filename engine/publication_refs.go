package engine

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ResolvePublication never substitutes latest, a group tip, or a private
// predecessor. Per-transaction envelopes are installed with their own RootSet
// after P1 and reconstructed from the authenticated log on reopening.
func (e *Engine) ResolvePublication(ctx context.Context, req api.ResolvePublicationRequest) (api.SourcePublication, error) {
	if err := req.Source.Validate(); err != nil {
		return api.SourcePublication{}, err
	}
	if err := validAgentScope(req.Source.Space, req.Principal); err != nil {
		return api.SourcePublication{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.SourcePublication{}, err
	}
	if err := e.publicGate(); err != nil {
		return api.SourcePublication{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.SourcePublication{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.SourcePublication{}, err
	}
	if req.Source.Store != e.publication.Store {
		return api.SourcePublication{}, model.ErrAnchorMismatch
	}
	envelopes := e.envelopes
	if e.baseEnvelopes != nil {
		envelopes = e.baseEnvelopes
	}
	proof, ok := envelopes[req.Source.ProducerTx]
	if !ok || proof.space != req.Source.Space || proof.principal != req.Principal {
		return api.SourcePublication{}, ErrNodeNotFound
	}
	anchor := model.SnapshotAnchor{Space: req.Source.Space, Fence: proof.fence, Cursor: req.Source.Clone().Cursor, Knowledge: req.Source.Clone().Knowledge}
	roots, rootID, err := e.rootsAtFence(ctx, proof.fence, true)
	if err != nil {
		return api.SourcePublication{}, e.publicationRefReadError(err)
	}
	if rootID != proof.rootSet || rootID == "" {
		return api.SourcePublication{}, e.readFailure(ErrInvalidStateData)
	}
	if key := anchor.Knowledge; key != nil {
		if err = e.authorizeKnowledge(ctx, roots, anchor.Space, req.Principal, *key); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(err)
		}
		if err = e.authorizeKnowledge(ctx, e.roots, anchor.Space, req.Principal, *key); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(err)
		}
		if _, _, _, _, err = e.selectedKnowledge(ctx, roots, anchor, req.Principal); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(err)
		}
	} else if anchor.Cursor != nil {
		if _, err = e.resolveDomainCursor(ctx, roots, anchor); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(err)
		}
	}
	if err = ctx.Err(); err != nil {
		return api.SourcePublication{}, err
	}
	if err = e.viewOutputGate(); err != nil {
		return api.SourcePublication{}, err
	}
	result := api.SourcePublication{Source: req.Source.Clone(), Anchor: anchor, RootSet: rootID}
	var dependencies []model.ObjectID
	if readDeliveryPrincipal(ctx) != "" {
		dependencies = []model.ObjectID{rootID}
		if anchor.Cursor != nil {
			dependencies = append(dependencies, model.ObjectID(anchor.Cursor.Revision))
		}
		if err := e.checkDisclosureIDs(ctx, anchor.Space, dependencies); err != nil {
			return api.SourcePublication{}, err
		}
	}
	return finishReadDelivery(ctx, e, "resolve_publication", req.Principal, result, anchor, "", "", dependencies)
}

func (e *Engine) publicationRefReadError(err error) error {
	if errors.Is(err, ErrNodeNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || knowledgeCommandRejection(err) || graphCommandRejection(err) || errors.Is(err, ErrUnsupportedMethod) || isLimit(err) {
		return err
	}
	return e.readFailure(err)
}

var _ api.PublicationResolver = (*Engine)(nil)
