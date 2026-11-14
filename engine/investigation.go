package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func investigationStateKey(principal model.PrincipalID, id string) []byte {
	key, _ := codec.AppendString(nil, string(principal))
	key, _ = codec.AppendString(key, id)
	digest := sha256.Sum256(key)
	return []byte("ate/investigation/v1/" + hex.EncodeToString(digest[:]))
}

func (e *Engine) loadInvestigation(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id string) (*investigation.Checkpoint, model.ObjectID, error) {
	raw, object, found, err := domainEventValue(ctx, e.reader(ctx), roots.ExecutionKnowledge, space, investigationStateKey(principal, id))
	if err != nil || !found {
		return nil, "", err
	}
	checkpoint, err := investigation.DecodeCheckpoint(raw)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if checkpoint.ID != id || checkpoint.Subject.Anchor.Space != space {
		return nil, "", ErrInvalidStateData
	}
	return &checkpoint, object, nil
}

func (e *Engine) UpdateInvestigation(ctx context.Context, req api.UpdateInvestigationRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := investigation.EncodeUpdate(req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodUpdateInvestigation, Body: body})
}

func (e *Engine) prepareInvestigation(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if req.ExpectedFence != nil || validAgentScope(req.Space, req.Principal) != nil {
		return nil, api.ErrInvalidRequest
	}
	update, err := investigation.DecodeUpdate(req.Body)
	if err != nil {
		return nil, err
	}
	current, _, err := e.loadInvestigation(ctx, e.roots, req.Space, req.Principal, update.ID)
	if err != nil {
		return nil, err
	}
	next, err := investigation.Apply(current, update)
	if errors.Is(err, investigation.ErrConflict) {
		return nil, errCAS
	}
	if err != nil {
		return nil, err
	}
	dependencies, err := e.investigationDependencies(ctx, req.Space, req.Principal, next)
	if err != nil {
		return nil, err
	}
	raw, err := investigation.EncodeCheckpoint(next)
	if err != nil {
		return nil, err
	}
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := codec.ObjectID(command)
	if err != nil {
		return nil, err
	}
	value, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		return nil, err
	}
	valueID, err := codec.ObjectID(value)
	if err != nil {
		return nil, err
	}
	// Both objects contain derived text. An audit event can expose the original
	// command even after the investigation has advanced past this checkpoint.
	links := map[model.ObjectID][]model.ObjectID{commandID: dependencies, valueID: dependencies}
	key := investigationStateKey(req.Principal, update.ID)
	return e.prepareDerivedValue(ctx, req, digest, txID, lsn, recorded, key, raw, links)
}

func (e *Engine) investigationDependencies(ctx context.Context, space model.SpaceID, principal model.PrincipalID, checkpoint investigation.Checkpoint) ([]model.ObjectID, error) {
	var dependencies []model.ObjectID
	if p := checkpoint.Position; p != nil && p.ViewMode() == "audit_snapshot" && p.Focus.Anchor.Cursor != nil {
		// An audit position can visit another branch at the same publication.
		// Ordinary evidence citations still use their own branch scope.
		ids, err := e.investigationCitationDependencies(ctx, space, principal, p.Focus, p.ViewMode())
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, ids...)
		checkpoint.Position = nil
	}
	for _, citation := range checkpoint.Citations() {
		ids, err := e.investigationCitationDependencies(ctx, space, principal, citation, "state")
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, ids...)
	}
	return dependencies, e.checkDisclosureIDs(ctx, space, dependencies)
}

func (e *Engine) investigationCitationDependencies(ctx context.Context, space model.SpaceID, principal model.PrincipalID, citation investigation.Citation, mode string) ([]model.ObjectID, error) {
	entry, err := e.historicalSourceEntry(ctx, space, principal, citation.Anchor, mode)
	if err != nil {
		return nil, err
	}
	var node nodeProjection
	if citation.Ref.Kind == model.NodeEvent && citation.Anchor.Knowledge == nil && (citation.Anchor.Cursor == nil || mode == "audit_snapshot") {
		node, err = e.projectUnfilteredEvent(ctx, entry, citation.Ref)
	} else {
		node, _, err = e.projectedNode(ctx, entry, citation.Ref)
	}
	if err != nil {
		return nil, err
	}
	dependencies, err := e.selectedReadDependencies(ctx, entry)
	if err != nil {
		return nil, err
	}
	dependencies = append(dependencies, nodeDisclosureIDs(node)...)
	if id := nodeReadRefID(citation.Ref); id != "" {
		dependencies = append(dependencies, id)
	}
	if node.detail.Encoding == "annotation/Record/v1+json" {
		var record annotation.Record
		if err := json.Unmarshal(node.detail.Data, &record); err != nil {
			return nil, ErrStoredObject
		}
		_, ids, err := e.checkAnnotationReferences(ctx, space, principal, record)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, ids...)
	}
	return dependencies, nil
}

func (e *Engine) GetInvestigation(ctx context.Context, req api.GetInvestigationRequest) (api.InvestigationResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_investigation", req.Principal, req, func(scoped context.Context) (api.InvestigationResult, error) {
			return e.GetInvestigation(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.InvestigationResult{}, err
	}
	if validAgentScope(req.Space, req.Principal) != nil || !investigation.ValidID(req.ID) {
		return api.InvestigationResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.InvestigationResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.InvestigationResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
		return api.InvestigationResult{}, model.ErrAnchorMismatch
	}
	roots, rootID, err := e.rootsAtFence(ctx, anchor.Fence, false)
	if err != nil {
		return api.InvestigationResult{}, e.investigationReadFailure(ctx, err)
	}
	checkpoint, object, err := e.loadInvestigation(ctx, roots, req.Space, req.Principal, req.ID)
	if err != nil {
		return api.InvestigationResult{}, e.investigationReadFailure(ctx, err)
	}
	out := api.InvestigationResult{Found: checkpoint != nil, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID}
	var dependencies []model.ObjectID
	if checkpoint != nil {
		dependencies, err = e.investigationDependencies(ctx, req.Space, req.Principal, *checkpoint)
		if err != nil {
			return api.InvestigationResult{}, e.investigationReadFailure(ctx, err)
		}
		dependencies = append(dependencies, object)
		out.Checkpoint = *checkpoint
	}
	if err := e.viewOutputGate(); err != nil {
		return api.InvestigationResult{}, err
	}
	return finishReadDelivery(ctx, e, "get_investigation", req.Principal, out, anchor, "", "", dependencies)
}

func (e *Engine) investigationReadFailure(ctx context.Context, err error) error {
	if investigationCommandRejection(err) || isLimit(err) {
		return err
	}
	return e.readFailureAt(ctx, err)
}

func investigationCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) {
		return false
	}
	return errors.Is(err, investigation.ErrInvalid) || errors.Is(err, model.ErrInvalidNodeRef) || annotationCommandRejection(err)
}

var _ api.InvestigationCommands = (*Engine)(nil)
