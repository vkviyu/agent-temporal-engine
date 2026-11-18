package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func summaryStateKey(space model.SpaceID, principal model.PrincipalID, id model.ObjectID) []byte {
	owned, _ := codec.AppendString(nil, string(space))
	owned, _ = codec.AppendString(owned, string(principal))
	digest := sha256.Sum256(owned)
	return []byte("ate/summary/v1/" + hex.EncodeToString(digest[:]) + "/" + string(id))
}

func (e *Engine) loadSummary(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id model.ObjectID) (knowledge.Summary, model.ObjectID, bool, error) {
	root, err := slotSpaceRoot(ctx, e.reader(ctx), roots.ExecutionKnowledge, space)
	if err != nil {
		return knowledge.Summary{}, "", false, err
	}
	record, found, err := state.Get(ctx, e.reader(ctx), space, root, summaryStateKey(space, principal, id))
	if err != nil || !found {
		return knowledge.Summary{}, "", found, err
	}
	object, err := readObject(ctx, e.reader(ctx), record)
	if err != nil {
		return knowledge.Summary{}, "", false, err
	}
	value, err := readValue(object)
	if err != nil || object.Space != space || value.Kind != api.ValueBytes {
		return knowledge.Summary{}, "", false, fmt.Errorf("%w: summary record", ErrStoredObject)
	}
	summary, err := knowledge.DecodeSummary(value.Data)
	if err != nil || summary.Space != space || summary.ID != id {
		return knowledge.Summary{}, "", false, fmt.Errorf("%w: summary identity", ErrStoredObject)
	}
	return summary, record, true, nil
}

func (e *Engine) ApplySummary(ctx context.Context, req api.ApplySummaryRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := req.Summary.Canonical()
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplySummary, Body: body, ExpectedFence: req.ExpectedFence})
}

func summaryPolicyIDs(summary knowledge.Summary) []model.ObjectID {
	return policyIdentities(summaryPolicyDependencies(summary))
}

func summaryPolicyDependencies(summary knowledge.Summary) []policyDependency {
	dependencies := []policyDependency{
		{kind: policySummary, id: summary.ID},
		{kind: policySummary, id: summary.Replaces},
	}
	// Preserve the legacy sorted, unique physical-content segment before the
	// ordered source/omission entries, including their duplicates and empties.
	dependencies = append(dependencies, objectPolicyDependencies(knowledge.SummaryContentReferences(summary)...)...)
	for _, source := range summary.Sources {
		dependencies = append(dependencies,
			policyDependency{kind: policyObservation, id: source.Observation},
			payloadPolicyDependency(source.Content, source.PayloadKind),
		)
	}
	for _, omitted := range summary.Omissions {
		dependencies = append(dependencies, policyDependency{kind: policyObservation, id: omitted.Observation})
	}
	return dependencies
}

func summaryParents(summary knowledge.Summary) []model.ObjectID {
	var parents []model.ObjectID
	for _, source := range summary.Sources {
		if source.PayloadKind == "summary" {
			parents = append(parents, source.Content)
		}
	}
	return parents
}

func (e *Engine) prepareSummary(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if !e.options.AgentProfile {
		return nil, ErrUnsupportedMethod
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	summary, err := knowledge.DecodeSummary(req.Body)
	if err != nil {
		return nil, err
	}
	if summary.Space != req.Space {
		return nil, agentprofile.ErrCommandScope
	}
	if err := e.checkSummaryInputs(ctx, req, summary); err != nil {
		return nil, err
	}
	links := map[model.ObjectID][]model.ObjectID{
		summary.ID:                       summaryPolicyIDs(summary),
		summary.Generation.Output.Object: {summary.ID},
	}
	return e.prepareDerivedValue(ctx, req, digest, txID, lsn, recorded, summaryStateKey(req.Space, req.Principal, summary.ID), req.Body, links)
}

func (e *Engine) GetSummary(ctx context.Context, req api.ReadSummaryRequest) (api.SummaryResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_summary", req.Principal, req, func(scoped context.Context) (api.SummaryResult, error) {
			return e.GetSummary(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.SummaryResult{}, err
	}
	if !e.options.AgentProfile {
		return api.SummaryResult{}, ErrUnsupportedMethod
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil || !model.ValidDigest(string(req.ID)) {
		return api.SummaryResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.SummaryResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.SummaryResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Knowledge != nil || anchor.Cursor != nil {
		return api.SummaryResult{}, model.ErrAnchorMismatch
	}
	roots, rootID, err := e.rootsAtFence(ctx, anchor.Fence, false)
	if err != nil {
		return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
	}
	summary, record, found, err := e.loadSummary(ctx, roots, req.Space, req.Principal, req.ID)
	if err != nil {
		return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
	}
	var dependencies []model.ObjectID
	if found {
		if err := e.authorizeKnowledge(ctx, e.readRoots(ctx), req.Space, req.Principal, *summary.Source.Anchor.Knowledge); err != nil {
			return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
		}
		dependencies, err = e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, []model.ObjectID{summary.ID})
		if err != nil {
			return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
		}
		if err := e.checkDisclosureIDs(ctx, req.Space, dependencies); err != nil {
			return api.SummaryResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return api.SummaryResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.SummaryResult{}, err
	}
	out := api.SummaryResult{Found: found, Summary: summary, Record: record, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID}
	return finishReadDelivery(ctx, e, "get_summary", req.Principal, out, anchor, "", "", dependencies)
}

func summaryCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrInvalidStateData) || errors.Is(err, ErrObjectIO) || errors.Is(err, ErrSemanticCorruption) {
		return false
	}
	return errors.Is(err, knowledge.ErrInvalidSummary) || knowledgeCommandRejection(err) || agentCommandRejection(err) || memoryCommandRejection(err)
}

var _ api.SummaryCommands = (*Engine)(nil)
