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
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "resolve_publication", req.Principal, req, func(scoped context.Context) (api.SourcePublication, error) {
			return e.ResolvePublication(scoped, req)
		})
	}
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
	if err := e.readReady(ctx); err != nil {
		return api.SourcePublication{}, err
	}
	if req.Source.Store != e.readPublication(ctx).Store {
		return api.SourcePublication{}, model.ErrAnchorMismatch
	}
	proof, ok, err := e.readEnvelope(ctx, req.Source.ProducerTx)
	if err != nil {
		return api.SourcePublication{}, err
	}
	if !ok || proof.space != req.Source.Space || proof.principal != req.Principal {
		return api.SourcePublication{}, ErrNodeNotFound
	}
	anchor := model.SnapshotAnchor{Space: req.Source.Space, Fence: proof.fence, Cursor: req.Source.Clone().Cursor, Knowledge: req.Source.Clone().Knowledge}
	roots, rootID, err := e.rootsAtFence(ctx, proof.fence, true)
	if err != nil {
		return api.SourcePublication{}, e.publicationRefReadError(ctx, err)
	}
	if rootID != proof.rootSet || rootID == "" {
		return api.SourcePublication{}, e.readFailureAt(ctx, ErrInvalidStateData)
	}
	if key := anchor.Knowledge; key != nil {
		if err = e.authorizeKnowledge(ctx, roots, anchor.Space, req.Principal, *key); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(ctx, err)
		}
		if err = e.authorizeKnowledge(ctx, e.readRoots(ctx), anchor.Space, req.Principal, *key); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(ctx, err)
		}
		if _, _, _, _, err = e.selectedKnowledge(ctx, roots, anchor, req.Principal); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(ctx, err)
		}
	} else if anchor.Cursor != nil {
		if _, err = e.resolveDomainCursor(ctx, roots, anchor); err != nil {
			return api.SourcePublication{}, e.publicationRefReadError(ctx, err)
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

func (e *Engine) publicationRefReadError(ctx context.Context, err error) error {
	if errors.Is(err, ErrNodeNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || knowledgeCommandRejection(err) || graphCommandRejection(err) || errors.Is(err, ErrUnsupportedMethod) || isLimit(err) {
		return err
	}
	return e.readFailureAt(ctx, err)
}

var _ api.PublicationResolver = (*Engine)(nil)
