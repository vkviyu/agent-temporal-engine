package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func annotationStateKey(space model.SpaceID, principal model.PrincipalID) []byte {
	raw, _ := codec.AppendString(nil, string(space))
	raw, _ = codec.AppendString(raw, string(principal))
	digest := sha256.Sum256(raw)
	return []byte("ate/annotation/v1/" + hex.EncodeToString(digest[:]))
}

func (e *Engine) loadAnnotations(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID) (annotation.Snapshot, error) {
	root, err := slotSpaceRoot(ctx, e.reader(ctx), roots.ExecutionKnowledge, space)
	if err != nil {
		return annotation.Snapshot{}, err
	}
	id, found, err := state.Get(ctx, e.reader(ctx), space, root, annotationStateKey(space, principal))
	if err != nil || !found {
		return annotation.Snapshot{}, err
	}
	object, err := readObject(ctx, e.reader(ctx), id)
	if err != nil {
		return annotation.Snapshot{}, err
	}
	value, err := readValue(object)
	if err != nil || object.Space != space || value.Kind != api.ValueBytes {
		return annotation.Snapshot{}, fmt.Errorf("%w: annotation snapshot", ErrStoredObject)
	}
	snapshot, err := annotation.DecodeSnapshot(value.Data)
	if err != nil {
		return annotation.Snapshot{}, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	for _, record := range snapshot.Records {
		if record.Source.Space != space {
			return annotation.Snapshot{}, ErrInvalidStateData
		}
	}
	return snapshot, nil
}

func (e *Engine) ApplyAnnotation(ctx context.Context, req api.ApplyAnnotationRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	raw, err := annotation.EncodeRecord(req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyAnnotation, Body: raw, ExpectedFence: req.ExpectedFence})
}

// historicalSourceEntry authenticates the actual historical read position.
// Public reads authorize against their captured cut; isolated reducers use
// their exclusively owned candidate roots.
func (e *Engine) historicalSourceEntry(ctx context.Context, space model.SpaceID, principal model.PrincipalID, source model.SnapshotAnchor, mode string) (*viewEntry, error) {
	if source.Validate() != nil || source.Space != space {
		return nil, model.ErrAnchorMismatch
	}
	roots, rootID, err := e.rootsAtFence(ctx, source.Fence, true)
	if err != nil {
		return nil, err
	}
	entry := &viewEntry{coreLocked: e.readCut(ctx) == nil, descriptor: api.ViewDescriptor{Anchor: source.Clone()}, roots: roots, rootSet: rootID, principal: principal, mode: mode}
	if source.Knowledge != nil {
		entry.mode = "execution_knowledge"
		for _, cut := range []model.RootSet{roots, e.readRoots(ctx)} {
			if err := e.authorizeKnowledge(ctx, cut, space, principal, *source.Knowledge); err != nil {
				return nil, err
			}
		}
		if _, _, _, _, err := e.selectedKnowledge(ctx, roots, source, principal); err != nil {
			return nil, err
		}
	} else if source.Cursor != nil {
		cursorRoot, err := e.resolveDomainCursor(ctx, roots, source)
		if err != nil {
			return nil, err
		}
		entry.cursorRoot = &cursorRoot
	}
	return entry, nil
}

func (e *Engine) checkAnnotationProvenance(ctx context.Context, space model.SpaceID, principal model.PrincipalID, r annotation.Record) (*viewEntry, []model.ObjectID, error) {
	entry, ids, err := e.checkAnnotationReferences(ctx, space, principal, r)
	if err != nil {
		return nil, nil, err
	}
	if err := e.checkViewDisclosure(ctx, entry); err != nil {
		return nil, nil, err
	}
	return entry, ids, nil
}

// The reference-only half is also used by the disclosure gate itself. Keeping
// it free of recursive output gates makes annotation-event bytes participate in
// current privacy checks even after their source left the current domain root.
func (e *Engine) checkAnnotationReferences(ctx context.Context, space model.SpaceID, principal model.PrincipalID, r annotation.Record) (*viewEntry, []model.ObjectID, error) {
	entry, err := e.historicalSourceEntry(ctx, space, principal, r.Source, "state")
	if err != nil {
		return nil, nil, err
	}
	ids := []model.ObjectID{r.ID, r.Retracts}
	refs := append([]model.NodeRef{r.Target}, r.Evidence...)
	for _, ref := range refs {
		node, _, err := e.projectedNode(ctx, entry, ref)
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, nodeDisclosureIDs(node)...)
		if ref.Kind == model.NodeObject {
			ids = append(ids, ref.Object)
		}
		if ref.Kind == model.NodeRevision {
			ids = append(ids, model.ObjectID(ref.Revision))
		}
		if ref.Kind == model.NodeEvent {
			ids = append(ids, model.ObjectID(ref.Event))
		}
	}
	if err := e.checkDisclosureIDs(ctx, space, ids); err != nil {
		return nil, nil, err
	}
	return entry, ids, nil
}

func (e *Engine) prepareAnnotation(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	record, err := annotation.DecodeRecord(req.Body)
	if err != nil {
		return nil, err
	}
	entry, _, err := e.checkAnnotationProvenance(ctx, req.Space, req.Principal, record)
	if err != nil {
		return nil, err
	}
	if record.Retracts != "" {
		source, err := e.loadAnnotations(ctx, entry.roots, req.Space, req.Principal)
		if err != nil {
			return nil, err
		}
		found := false
		for _, prior := range source.Records {
			if prior.ID == record.Retracts {
				found = true
				break
			}
		}
		if !found {
			return nil, annotation.ErrConflict
		}
	}
	current, err := e.loadAnnotations(ctx, e.roots, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	next, err := annotation.Apply(current, record)
	if err != nil {
		return nil, err
	}
	raw, err := annotation.EncodeSnapshot(next)
	if err != nil {
		return nil, err
	}
	return e.prepareTypedValue(ctx, req, digest, txID, lsn, recorded, annotationStateKey(req.Space, req.Principal), raw)
}

func (e *Engine) GetAnnotations(ctx context.Context, req api.ReadAnnotationsRequest) (api.AnnotationsResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_annotations", req.Principal, req, func(scoped context.Context) (api.AnnotationsResult, error) {
			return e.GetAnnotations(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.AnnotationsResult{}, err
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return api.AnnotationsResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.AnnotationsResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.AnnotationsResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
		return api.AnnotationsResult{}, model.ErrAnchorMismatch
	}
	roots, rootID, err := e.rootsAtFence(ctx, anchor.Fence, false)
	if err != nil {
		return api.AnnotationsResult{}, e.annotationReadFailure(ctx, err)
	}
	snapshot, err := e.loadAnnotations(ctx, roots, req.Space, req.Principal)
	if err != nil {
		return api.AnnotationsResult{}, e.annotationReadFailure(ctx, err)
	}
	var dependencies []model.ObjectID
	collect := ctx.Value(readDeliveryKey{}) != nil
	for _, record := range snapshot.Records {
		entry, ids, err := e.checkAnnotationProvenance(ctx, req.Space, req.Principal, record)
		if err != nil {
			return api.AnnotationsResult{}, e.annotationReadFailure(ctx, err)
		}
		if collect {
			selected, err := e.selectedReadDependencies(ctx, entry)
			if err != nil {
				return api.AnnotationsResult{}, e.annotationReadFailure(ctx, err)
			}
			dependencies = append(dependencies, ids...)
			dependencies = append(dependencies, selected...)
		}
	}
	if err := ctx.Err(); err != nil {
		return api.AnnotationsResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.AnnotationsResult{}, err
	}
	out := api.AnnotationsResult{Snapshot: snapshot, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID}
	return finishReadDelivery(ctx, e, "get_annotations", req.Principal, out, anchor, "", "", dependencies)
}

func (e *Engine) annotationReadFailure(ctx context.Context, err error) error {
	if annotationCommandRejection(err) || isLimit(err) {
		return err
	}
	return e.readFailureAt(ctx, err)
}
func annotationCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrInvalidStateData) || errors.Is(err, ErrObjectIO) || errors.Is(err, ErrSemanticCorruption) {
		return false
	}
	return errors.Is(err, annotation.ErrInvalid) || errors.Is(err, annotation.ErrConflict) || errors.Is(err, ErrNodeNotFound) || knowledgeCommandRejection(err) || memoryCommandRejection(err) || agentCommandRejection(err)
}

// Navigation applies only judgments present at this View's immutable cut and
// owned by its principal. Explicit reads, traces and context stay complete.
func (e *Engine) hiddenNodes(ctx context.Context, entry *viewEntry, include bool) (map[model.NodeRef]bool, error) {
	// Owner navigation judgments are not adopted execution knowledge. They
	// cannot alter an older Knowledge Cursor merely because its global Fence
	// includes a newer review. Knowledge members remain selected by adoption.
	if include || entry.descriptor.Anchor.Knowledge != nil {
		return nil, nil
	}
	snapshot, err := e.loadAnnotations(ctx, entry.roots, entry.descriptor.Anchor.Space, entry.principal)
	if err != nil {
		return nil, err
	}
	return snapshot.Hidden(), nil
}

var _ api.AnnotationCommands = (*Engine)(nil)
