package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func memoryStateKey(space model.SpaceID) []byte {
	return append([]byte("ate/memory/v1/"), []byte(space)...)
}

func (e *Engine) loadMemory(ctx context.Context, roots model.RootSet, space model.SpaceID) (*memory.Store, model.ObjectID, bool, error) {
	root, err := slotSpaceRoot(ctx, e.reader(ctx), roots.ExecutionKnowledge, space)
	if err != nil {
		return nil, "", false, err
	}
	id, found, err := state.Get(ctx, e.reader(ctx), space, root, memoryStateKey(space))
	if err != nil || !found {
		return memory.New(), root, found, err
	}
	obj, err := readObject(ctx, e.reader(ctx), id)
	if err != nil {
		return nil, "", false, err
	}
	value, err := readValue(obj)
	if err != nil || obj.Space != space || value.Kind != api.ValueBytes {
		return nil, "", false, fmt.Errorf("%w: memory snapshot", ErrStoredObject)
	}
	store, err := memory.RestoreSnapshot(value.Data)
	if err != nil {
		return nil, "", false, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	snapshot := store.Snapshot()
	for _, m := range snapshot.Memories {
		if m.Space != space {
			return nil, "", false, ErrInvalidStateData
		}
	}
	for _, p := range snapshot.Paths {
		if p.Space != space {
			return nil, "", false, ErrInvalidStateData
		}
	}
	return store, root, true, nil
}

// ApplyMemory submits canonical intent. Receipt lookup and the pure transition
// run inside the same candidate coordinator as the other typed domains.
func (e *Engine) ApplyMemory(ctx context.Context, req api.ApplyMemoryRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := memory.EncodeCommand(req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyMemory, Body: body, ExpectedFence: req.ExpectedFence})
}

func (e *Engine) prepareMemory(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	command, err := memory.DecodeCommand(req.Body)
	if err != nil {
		return nil, err
	}
	candidate, _, _, err := e.loadMemory(ctx, e.roots, req.Space)
	if err != nil {
		return nil, err
	}
	// New requests cannot weaken an existing disclosure restriction. Preserve
	// historical reducer behavior during replay: old canonical roots must not
	// be recalculated under a newly introduced admission policy.
	if recorded == nil {
		var id model.ObjectID
		var kind memory.RetractionKind
		if command.RetractSource != nil {
			id, kind = command.RetractSource.Source, command.RetractSource.Retraction.Kind
		} else if command.RetractPath != nil {
			id, kind = command.RetractPath.ID, command.RetractPath.Retraction.Kind
		}
		if id != "" && kind != memory.RetractionPrivacyErasure && kind != memory.RetractionACLRevocation {
			if err := candidate.AccessPolicy().Check([]model.ObjectID{id}, memory.AccessAudit); errors.Is(err, memory.ErrHistoryAccessDenied) {
				return nil, memory.ErrInvalidTransition
			} else if err != nil {
				return nil, err
			}
		}
	}
	for _, src := range command.Sources() {
		roots, err := e.memoryAnchor(ctx, req, src.Anchor)
		if err != nil {
			return nil, err
		}
		if src.Anchor.Knowledge != nil {
			prior, _, _, _, err := e.selectedKnowledge(ctx, roots, src.Anchor, req.Principal)
			if err != nil {
				return nil, err
			}
			observed := false
			for _, o := range prior.Observations {
				if o.Key == *src.Anchor.Knowledge && (o.ID == src.SourceID || o.Payload == src.SourceID) {
					observed = true
					break
				}
			}
			if !observed {
				for _, p := range prior.Packets {
					if p.Key != nil && *p.Key == *src.Anchor.Knowledge && p.ID == src.SourceID {
						observed = true
						break
					}
				}
			}
			if !observed {
				return nil, memory.ErrSourceUnavailable
			}
		} else if err = e.memorySourceVisible(ctx, roots, req.Space, req.Principal, src.Anchor, src.SourceID); err != nil {
			return nil, err
		}
	}
	// Earlier accepted BuildPath transactions wrote only the Memory snapshot.
	// The strict form also publishes the resolved dependency records in the
	// same transaction. Replay checks the exact operation set, retaining the
	// historical reducer for already published legacy paths and their receipts.
	strictPath := command.BuildPath != nil && (recorded == nil || !legacyPathOperations(recorded, req.Space))
	var pathDependencies []model.ObjectID
	if path := command.BuildPath; path != nil {
		roots, err := e.memoryAnchor(ctx, req, path.Anchor)
		if err != nil {
			return nil, err
		}
		prior, _, _, err := e.loadMemory(ctx, roots, req.Space)
		if err != nil {
			return nil, err
		}
		for _, step := range path.Steps {
			m, err := prior.Get(step.Memory)
			if err != nil || m.Status != memory.StatusPromoted {
				return nil, memory.ErrSourceUnavailable
			}
		}
		for _, parent := range path.DerivedFrom {
			if _, err := prior.Get(parent); err == nil {
				continue
			}
			if _, err := prior.GetPath(parent); err != nil {
				return nil, memory.ErrSourceUnavailable
			}
		}
		if strictPath {
			pathDependencies, err = e.checkExperiencePathReferences(ctx, req, path, prior)
			if err != nil {
				return nil, err
			}
		}
	}
	result, err := candidate.ApplyCommand(req.Key(), command)
	if err != nil {
		return nil, err
	}
	raw, err := memory.EncodeSnapshot(candidate.Snapshot())
	if err != nil {
		return nil, err
	}
	if strictPath {
		return e.prepareExperiencePathValue(ctx, req, digest, txID, lsn, recorded, raw, *result.Path, pathDependencies)
	}
	return e.prepareTypedValue(ctx, req, digest, txID, lsn, recorded, memoryStateKey(req.Space), raw)
}

func legacyPathOperations(recorded *operationsRecord, space model.SpaceID) bool {
	return len(recorded.Changes) == 1 && string(recorded.Changes[0].Key) == string(memoryStateKey(space))
}

func (e *Engine) memoryAnchor(ctx context.Context, req api.CommitRequest, anchor model.SnapshotAnchor) (model.RootSet, error) {
	if anchor.Validate() != nil || anchor.Space != req.Space {
		return model.RootSet{}, model.ErrAnchorMismatch
	}
	roots, _, err := e.rootsAtFence(ctx, anchor.Fence, true)
	if err != nil {
		return model.RootSet{}, err
	}
	if k := anchor.Knowledge; k != nil {
		if err = e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, *k); err != nil {
			return model.RootSet{}, err
		}
		if err = e.authorizeKnowledge(ctx, e.readRoots(ctx), req.Space, req.Principal, *k); err != nil {
			return model.RootSet{}, err
		}
		if _, _, _, _, err = e.selectedKnowledge(ctx, roots, anchor, req.Principal); err != nil {
			return model.RootSet{}, err
		}
	} else if anchor.Cursor != nil {
		if _, err = e.resolveDomainCursor(ctx, roots, anchor); err != nil {
			return model.RootSet{}, err
		}
	}
	return roots, nil
}

// Provenance accepts an exact domain value or an immutable memory/path audit
// version that already existed at Source.Anchor. A digest alone is no proof.
func (e *Engine) memorySourceVisible(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, anchor model.SnapshotAnchor, id model.ObjectID) error {
	err := e.visibleSourceObject(ctx, roots, anchor, principal, id)
	if err == nil {
		return nil
	}
	if !errors.Is(err, knowledge.ErrUnknownObservation) {
		return err
	}
	prior, _, _, err := e.loadMemory(ctx, roots, space)
	if err != nil {
		return err
	}
	if _, err = prior.Get(id); err == nil {
		return nil
	}
	if _, err = prior.GetPath(id); err == nil {
		return nil
	}
	return memory.ErrSourceUnavailable
}

// visibleTypedKnowledgeSource authenticates a logical memory reference at the
// exact historical root selected by the observation. Logical memory/path IDs
// are never resolved through the raw object index.
func (e *Engine) visibleTypedKnowledgeSource(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, kind string, id model.ObjectID) error {
	if kind == "summary" {
		s, _, found, err := e.loadSummary(ctx, roots, space, principal, id)
		if err != nil {
			return err
		}
		if !found || s.Validation.Status == "rejected" {
			return knowledge.ErrInvalidSummary
		}
		return nil
	}
	store, _, _, err := e.loadMemory(ctx, roots, space)
	if err != nil {
		return err
	}
	switch kind {
	case "memory":
		value, err := store.Get(id)
		if err != nil || value.Space != space || value.Status != memory.StatusPromoted {
			return memory.ErrSourceUnavailable
		}
		return nil
	case "experience_path":
		value, err := store.GetPath(id)
		if err != nil || value.Space != space || value.Status != memory.StatusPromoted {
			return memory.ErrSourceUnavailable
		}
		return nil
	default:
		return knowledge.ErrInvalid
	}
}

func (e *Engine) GetMemoryState(ctx context.Context, req api.ReadMemoryStateRequest) (api.MemoryStateResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_memory", "embedded-host", req, func(scoped context.Context) (api.MemoryStateResult, error) {
			return e.GetMemoryState(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.MemoryStateResult{}, err
	}
	if err := validSpace(req.Space); err != nil {
		return api.MemoryStateResult{}, err
	}
	if err := e.publicGate(); err != nil {
		return api.MemoryStateResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.MemoryStateResult{}, err
	}
	store, root, found, err := e.loadMemory(ctx, e.readRoots(ctx), req.Space)
	if err != nil {
		return api.MemoryStateResult{}, e.readFailureAt(ctx, err)
	}
	out := api.MemoryStateResult{Found: found, Snapshot: store.Snapshot(), StateRoot: root, Fence: e.readPublication(ctx).Fence(), RootSet: e.readRootSet(ctx)}
	ids := make([]model.ObjectID, 0, len(out.Snapshot.Memories)+len(out.Snapshot.Paths))
	for _, value := range out.Snapshot.Memories {
		ids = append(ids, value.ID)
	}
	for _, value := range out.Snapshot.Paths {
		ids = append(ids, value.ID)
	}
	if err := e.checkPolicyIDs(ctx, req.Space, ids, memory.AccessAudit); err != nil {
		return api.MemoryStateResult{}, err
	}
	if err = ctx.Err(); err != nil {
		return api.MemoryStateResult{}, err
	}
	if err = e.viewOutputGate(); err != nil {
		return api.MemoryStateResult{}, err
	}
	// The Memory snapshot is space-scoped. Unlike an owned projection, its
	// transport principal is the authenticated destination of the scoped read.
	var principal model.PrincipalID
	if capture, _ := ctx.Value(readDeliveryKey{}).(*readDeliveryCapture); capture != nil {
		principal = capture.binding.Destination.Principal
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: out.Fence}
	return finishReadDelivery(ctx, e, "get_memory", principal, out, anchor, "", "", ids)
}

func memoryCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrInvalidStateData) || errors.Is(err, ErrObjectIO) || errors.Is(err, ErrSemanticCorruption) {
		return false
	}
	return errors.Is(err, memory.ErrCommandShape) || errors.Is(err, memory.ErrCommandScope) || errors.Is(err, memory.ErrInvalidMemory) || errors.Is(err, memory.ErrInvalidTransition) || errors.Is(err, memory.ErrSourceUnavailable) || errors.Is(err, memory.ErrNotFound) || errors.Is(err, memory.ErrAlreadyExists) || errors.Is(err, memory.ErrNotEligible) || errors.Is(err, memory.ErrPathInvalid) || errors.Is(err, memory.ErrStepEvidenceMismatch) || errors.Is(err, memory.ErrUseDenied) || errors.Is(err, memory.ErrHistoryAccessDenied) || errors.Is(err, model.ErrAnchorMismatch) || errors.Is(err, ErrUnsupportedMethod) || knowledgeCommandRejection(err)
}

var _ api.MemoryCommands = (*Engine)(nil)
