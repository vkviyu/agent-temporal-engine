package engine

import (
	"context"
	"crypto/rand"
	"sync/atomic"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const readEmissionGrantTTL = 5 * time.Second

// The request, destination and owned content are immutable until the grant is
// consumed. The proof captures the historical View and the live decision cut.
type contentReadGrant struct {
	destination api.ReadDestination
	request     api.ReadContentRequest
	content     api.ContentRange
	proof       contentReadProof
	expires     time.Time
	used        atomic.Bool
}

type contentReadProof struct {
	viewDigest       model.ObjectID
	rangeDigest      model.ObjectID
	dependencyDigest model.ObjectID
	policyFence      model.CommitFence
	policyRoot       model.ObjectID
	controlRoot      model.ObjectID
}

func (g *contentReadGrant) Consume(destination api.ReadDestination, request api.ReadContentRequest) (api.ContentRange, error) {
	if g.destination != destination || g.request != request || !time.Now().Before(g.expires) || !g.used.CompareAndSwap(false, true) {
		return api.ContentRange{}, api.ErrReadEmissionDenied
	}
	if emissionDigest(g.content.Data) != g.proof.rangeDigest {
		return api.ContentRange{}, api.ErrReadEmissionDenied
	}
	return g.content, nil
}

func embeddedReadDestination(principal model.PrincipalID) api.ReadDestination {
	return api.ReadDestination{Principal: principal, Session: "embedded", Request: rand.Text()}
}

func validReadDestination(destination api.ReadDestination) bool {
	return destination.Principal != "" && destination.Session != "" && destination.Request != ""
}

// Resolve historical dependencies before entering the publication lock. A
// selected knowledge catalog has no unscoped Agent or Annotation data.
func (e *Engine) contentReadDependencies(ctx context.Context, entry *viewEntry, catalog nodeCatalog, object model.ObjectID) ([]model.ObjectID, error) {
	ids := []model.ObjectID{object}
	if entry.descriptor.Anchor.Knowledge == nil {
		return ids, nil
	}
	selected, err := e.selectedReadDependencies(ctx, entry)
	if err != nil {
		return nil, err
	}
	ids = append(ids, selected...)
	for id := range catalog.objects {
		ids = append(ids, id)
	}
	for ref, node := range catalog.nodes {
		ids = append(ids, node.detail.Payload)
		if ref.Kind == model.NodeEntity && ref.Entity.Profile == "memory" {
			ids = append(ids, model.ObjectID(ref.Entity.ID))
		}
	}
	return ids, nil
}

func (e *Engine) selectedReadDependencies(ctx context.Context, entry *viewEntry) ([]model.ObjectID, error) {
	anchor := entry.descriptor.Anchor
	if anchor.Knowledge == nil {
		return nil, nil
	}
	snapshot, _, _, _, err := e.selectedKnowledge(ctx, entry.roots, anchor, entry.principal)
	if err != nil {
		return nil, err
	}
	summaries, err := e.summaryDependencyIDs(ctx, entry.roots, anchor.Space, entry.principal, observationSummaryIDs(snapshot.Observations))
	if err != nil {
		return nil, err
	}
	return append(knowledgePolicyIDs(snapshot), summaries...), nil
}

func (e *Engine) issueContentRead(ctx context.Context, entry *viewEntry, grant *contentReadGrant, ids []model.ObjectID) error {
	space := entry.descriptor.Anchor.Space
	if key := entry.descriptor.Anchor.Knowledge; key != nil {
		if err := e.authorizeKnowledge(ctx, e.readRoots(ctx), space, entry.principal, *key); err != nil {
			return e.knowledgeReadFailure(ctx, err)
		}
	} else if err := e.checkViewDisclosure(ctx, entry); err != nil {
		return err
	}
	policy, err := e.readPolicy(ctx, space)
	if err != nil {
		return err
	}
	dependencies, err := e.policyDependencyIDs(ctx, space, policy, ids)
	if err != nil {
		return err
	}
	if err := policy.Check(dependencies, memory.AccessAudit); err != nil {
		return err
	}
	dependencyDigest, err := api.EmissionDependencyDigest(dependencies)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	defer e.mu.Unlock()
	if err := e.checkReadCutLocked(ctx); err != nil {
		return err
	}
	grant.proof.dependencyDigest = dependencyDigest
	grant.proof.policyFence = e.readPublication(ctx).Fence()
	grant.proof.policyRoot = e.readRoots(ctx).ExecutionKnowledge
	grant.proof.controlRoot = e.readRoots(ctx).LiveControl
	grant.expires = time.Now().Add(readEmissionGrantTTL)
	return nil
}
