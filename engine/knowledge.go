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
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func knowledgeStateKey(space model.SpaceID, principal model.PrincipalID) []byte {
	// Scope must remain unambiguous and bounded even at maximum identity sizes.
	b, _ := codec.AppendString(nil, string(space))
	b, _ = codec.AppendString(b, string(principal))
	sum := sha256.Sum256(b)
	return []byte("ate/knowledge/v1/" + hex.EncodeToString(sum[:]))
}

func (e *Engine) loadKnowledge(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID) (knowledge.Snapshot, model.ObjectID, bool, error) {
	root, err := slotSpaceRoot(ctx, e.index, roots.ExecutionKnowledge, space)
	if err != nil {
		return knowledge.Snapshot{}, "", false, err
	}
	id, found, err := state.Get(ctx, e.index, space, root, knowledgeStateKey(space, principal))
	if err != nil || !found {
		return knowledge.Snapshot{}, root, found, err
	}
	obj, err := readObject(ctx, e.index, id)
	if err != nil {
		return knowledge.Snapshot{}, "", false, err
	}
	value, err := readValue(obj)
	if err != nil || obj.Space != space || value.Kind != api.ValueBytes {
		return knowledge.Snapshot{}, "", false, fmt.Errorf("%w: knowledge snapshot", ErrStoredObject)
	}
	snap, err := knowledge.DecodeSnapshot(value.Data)
	if err != nil {
		return knowledge.Snapshot{}, "", false, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	for _, o := range snap.Observations {
		if o.Source.Anchor.Space != space {
			return knowledge.Snapshot{}, "", false, ErrInvalidStateData
		}
	}
	for _, p := range snap.Packets {
		if p.Space != space {
			return knowledge.Snapshot{}, "", false, ErrInvalidStateData
		}
	}
	return snap, root, true, nil
}

// An Execution selector is an authority check, not merely a string filter.
func (e *Engine) authorizeKnowledge(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, key model.KnowledgeKey) error {
	if !e.options.AgentProfile {
		return ErrUnsupportedMethod
	}
	if err := validAgentScope(space, principal); err != nil {
		return err
	}
	l, _, _, err := loadAgentLedger(ctx, e.index, roots.LiveControl, space, principal)
	if err != nil {
		return err
	}
	x, err := l.GetExecution(key.Execution)
	if err != nil || x.Actor != key.Actor {
		return agentprofile.ErrCommandScope
	}
	t, err := l.GetTask(x.Task)
	if err != nil || t.Space != space || t.Owner != principal {
		return agentprofile.ErrCommandScope
	}
	return nil
}

func (e *Engine) ApplyKnowledge(ctx context.Context, req api.ApplyKnowledgeRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := knowledge.EncodeCommand(req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, fmt.Errorf("knowledge command encode: %w", err))
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyKnowledge, Body: body, ExpectedFence: req.ExpectedFence})
}

func (e *Engine) prepareKnowledge(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	c, err := knowledge.DecodeCommand(req.Body)
	if err != nil {
		return nil, err
	}
	current, _, _, err := e.loadKnowledge(ctx, e.roots, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	if c.Import != nil {
		return e.prepareKnowledgeImport(ctx, req, digest, txID, lsn, recorded, current, *c.Import)
	}
	var source knowledge.SourceAnchor
	var key *model.KnowledgeKey
	if c.Observation != nil {
		source = c.Observation.Source
		key = &c.Observation.Key
	} else {
		source = c.Packet.Source
		key = c.Packet.Key
	}
	if c.Packet != nil && c.Packet.Input != nil {
		input := c.Packet.Input
		inputRoots, _, err := e.rootsAtFence(ctx, input.Anchor.Fence, true)
		if err != nil {
			return nil, err
		}
		if err = e.visibleSourceObject(ctx, inputRoots, input.Anchor, req.Principal, input.Object); err != nil {
			return nil, err
		}
		object, err := readObject(ctx, e.index, input.Object)
		if err != nil {
			return nil, err
		}
		value, err := readValue(object)
		if err != nil || value.Kind != api.ValueBytes || object.Space != req.Space {
			return nil, knowledge.ErrInvalid
		}
	}
	if source.Anchor.Space != req.Space {
		return nil, knowledge.ErrKnowledgeMismatch
	}
	if source.Anchor.Knowledge != nil && (key == nil || *source.Anchor.Knowledge != *key) {
		return nil, knowledge.ErrKnowledgeMismatch
	}
	// Published-only history excludes even earlier private group members.
	roots, sourceRoot, err := e.rootsAtFence(ctx, source.Anchor.Fence, true)
	if err != nil {
		return nil, err
	}
	if key != nil {
		if err = e.authorizeKnowledge(ctx, e.roots, req.Space, req.Principal, *key); err != nil {
			return nil, err
		}
		if !initialWorkObservation(ctx, req, c.Observation) {
			if err = e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, *key); err != nil {
				return nil, err
			}
		}
	}
	sourceSnap, _, _, err := e.loadKnowledge(ctx, roots, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	if source.Anchor.Knowledge != nil {
		sourceSnap, _, _, _, err = e.selectedKnowledge(ctx, roots, source.Anchor, req.Principal)
		if err != nil {
			return nil, err
		}
	} else if source.Anchor.Cursor != nil {
		if _, err = e.resolveDomainCursor(ctx, roots, source.Anchor); err != nil {
			return nil, err
		}
	}
	if source.Frontier != "" {
		if key == nil {
			return nil, knowledge.ErrKnowledgeMismatch
		}
		frontier, e := knowledge.Frontier(sourceSnap, *key)
		if e != nil {
			return nil, e
		}
		if frontier != source.Frontier {
			return nil, model.ErrAnchorMismatch
		}
	}
	if source.ViewDigest != "" {
		mode := "state"
		if source.Anchor.Knowledge != nil {
			mode = "execution_knowledge"
		}
		if source.ViewDigest != viewDigest(source.Anchor, sourceRoot, req.Principal, mode) {
			return nil, model.ErrAnchorMismatch
		}
	}
	if c.Observation != nil {
		if source.Anchor.Knowledge != nil {
			observed := false
			for _, o := range sourceSnap.Observations {
				if o.Key == *key && o.Payload == c.Observation.Payload && o.PayloadKind == c.Observation.PayloadKind {
					observed = true
					break
				}
			}
			if !observed {
				return nil, knowledge.ErrUnknownObservation
			}
			if c.Observation.PayloadKind != "" {
				if err = e.visibleTypedKnowledgeSource(ctx, roots, req.Space, req.Principal, c.Observation.PayloadKind, c.Observation.Payload); err != nil {
					return nil, err
				}
			}
		} else if c.Observation.PayloadKind != "" {
			if err = e.visibleTypedKnowledgeSource(ctx, roots, req.Space, req.Principal, c.Observation.PayloadKind, c.Observation.Payload); err != nil {
				return nil, err
			}
		} else if err = e.visibleSourceObject(ctx, roots, source.Anchor, req.Principal, c.Observation.Payload); err != nil {
			return nil, err
		}
	} else {
		// Query packets without an Execution have no implicit permission to
		// import another execution's private observations.
		if key == nil && len(c.Packet.Items) > 0 {
			return nil, knowledge.ErrKnowledgeMismatch
		}
		// A current observation is insufficient: it must exist at Source.Fence.
		adopted := make(map[model.ObjectID]knowledge.Observation, len(sourceSnap.Observations))
		for _, o := range sourceSnap.Observations {
			adopted[o.ID] = o
		}
		for _, item := range c.Packet.Items {
			o, ok := adopted[item.Observation]
			if !ok || o.Payload != item.Content || (key != nil && o.Key != *key) {
				return nil, knowledge.ErrUnknownObservation
			}
			if o.PayloadKind != "" {
				if err = e.visibleTypedKnowledgeSource(ctx, roots, req.Space, req.Principal, o.PayloadKind, o.Payload); err != nil {
					return nil, err
				}
			}
		}
		if c.Packet.GoalRef != "" {
			if key != nil {
				observed := false
				for _, o := range sourceSnap.Observations {
					if o.Key == *key && o.Payload == c.Packet.GoalRef {
						observed = true
						break
					}
				}
				if !observed {
					return nil, knowledge.ErrUnknownObservation
				}
			} else if err = e.visibleSourceObject(ctx, roots, source.Anchor, req.Principal, c.Packet.GoalRef); err != nil {
				return nil, err
			}
		}
		if err = knowledge.VerifyPacketSelection(*c.Packet, sourceSnap); err != nil {
			return nil, err
		}
	}
	next, err := knowledge.ApplyCommand(current, c)
	if err != nil {
		return nil, fmt.Errorf("knowledge candidate: %w", err)
	}
	if err = verifyContentClosure(ctx, e.index, knowledge.ContentReferences(next)); err != nil {
		return nil, err
	}
	raw, err := knowledge.EncodeSnapshot(next)
	if err != nil {
		return nil, err
	}
	return e.prepareTypedValue(ctx, req, digest, txID, lsn, recorded, knowledgeStateKey(req.Space, req.Principal), raw)
}

func (e *Engine) visibleSourceObject(ctx context.Context, roots model.RootSet, anchor model.SnapshotAnchor, principal model.PrincipalID, id model.ObjectID) error {
	root, err := e.resolveDomainCursor(ctx, roots, anchor)
	if err != nil {
		return err
	}
	err = e.visibleDomainRootObject(ctx, root, anchor.Space, id)
	if err == nil || !errors.Is(err, knowledge.ErrUnknownObservation) {
		return err
	}
	if anchor.Cursor == nil && anchor.Knowledge == nil {
		if _, found, archiveErr := e.transferArchive(ctx, roots, anchor.Space, principal, id); archiveErr != nil {
			return archiveErr
		} else if found {
			return nil
		}
	}
	if !e.options.EventProfile {
		return err
	}
	_, eventErr := e.visibleEvent(ctx, model.EventID(id), anchor, principal)
	if errors.Is(eventErr, ErrNodeNotFound) {
		return knowledge.ErrUnknownObservation
	}
	if eventErr == nil && anchor.Cursor != nil {
		// A domain Cursor is a strict ancestry selection, including audit
		// occurrences. Owner/Fence membership alone would import an event
		// from an unrelated branch at the same global cut.
		_, revisions, err := loadGraphParents(ctx, &cursorReader{source: e.index}, roots, anchor.Space, []model.RevisionID{anchor.Cursor.Revision})
		if err != nil {
			if errors.Is(err, graph.ErrUnknownRevision) {
				return model.ErrAnchorMismatch
			}
			return err
		}
		for _, revision := range revisions {
			for _, event := range revision.Events {
				if event == model.EventID(id) {
					return nil
				}
			}
		}
		return knowledge.ErrUnknownObservation
	}
	return eventErr
}

// Visible source content belongs to the authenticated historical domain tree.
func (e *Engine) visibleDomainObject(ctx context.Context, roots model.RootSet, space model.SpaceID, id model.ObjectID) error {
	root, err := domainRoot(ctx, e.index, roots, space)
	if err != nil {
		return err
	}
	return e.visibleDomainRootObject(ctx, root, space, id)
}

func (e *Engine) visibleDomainRootObject(ctx context.Context, root model.ObjectID, space model.SpaceID, id model.ObjectID) error {
	entries, err := state.Entries(ctx, e.index, space, root, state.ScanLimits{})
	if err != nil {
		return err
	}
	for _, v := range entries {
		if v.Value == id {
			o, err := readObject(ctx, e.index, id)
			if err != nil {
				return err
			}
			if o.Space != space {
				return ErrInvalidStateData
			}
			_, err = readValue(o)
			return err
		}
	}
	return knowledge.ErrUnknownObservation
}

func (e *Engine) GetKnowledgeState(ctx context.Context, req api.ReadKnowledgeStateRequest) (api.KnowledgeStateResult, error) {
	if err := ctx.Err(); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	if err := validSpace(req.Space); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return api.KnowledgeStateResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.publication.Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space {
		return api.KnowledgeStateResult{}, model.ErrAnchorMismatch
	}
	if req.Knowledge != nil {
		if anchor.Knowledge != nil && *anchor.Knowledge != *req.Knowledge {
			return api.KnowledgeStateResult{}, model.ErrAnchorMismatch
		}
		k := *req.Knowledge
		anchor.Knowledge = &k
	}
	if anchor.Cursor != nil && anchor.Knowledge == nil {
		return api.KnowledgeStateResult{}, ErrUnsupportedMethod
	}
	roots, rootID, err := e.rootsAtFence(ctx, anchor.Fence, false)
	if err != nil {
		return api.KnowledgeStateResult{}, e.knowledgeReadFailure(err)
	}
	snap, root, found, err := e.loadKnowledge(ctx, roots, req.Space, req.Principal)
	if err != nil {
		return api.KnowledgeStateResult{}, e.knowledgeReadFailure(err)
	}
	var frontier model.ObjectID
	var revision *knowledge.Revision
	if k := anchor.Knowledge; k != nil {
		if err = e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, *k); err != nil {
			return api.KnowledgeStateResult{}, e.knowledgeReadFailure(err)
		}
		if err = e.authorizeKnowledge(ctx, e.roots, req.Space, req.Principal, *k); err != nil {
			return api.KnowledgeStateResult{}, e.knowledgeReadFailure(err)
		}
		var selected knowledge.Revision
		snap, selected, root, found, err = e.selectedKnowledge(ctx, roots, anchor, req.Principal)
		if err != nil {
			return api.KnowledgeStateResult{}, err
		}
		revision = &selected
		frontier, err = knowledge.Frontier(snap, *k)
		if err != nil {
			return api.KnowledgeStateResult{}, err
		}
		found = len(snap.Observations) > 0 || len(snap.Packets) > 0
	}
	if err = ctx.Err(); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	dependencies, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, observationSummaryIDs(snap.Observations))
	if err != nil {
		return api.KnowledgeStateResult{}, e.knowledgeReadFailure(err)
	}
	ids := append(knowledgePolicyIDs(snap), dependencies...)
	if err := e.checkDisclosureIDs(ctx, req.Space, ids); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	if err = e.viewOutputGate(); err != nil {
		return api.KnowledgeStateResult{}, err
	}
	out := api.KnowledgeStateResult{Found: found, Snapshot: snap, StateRoot: root, RootSet: rootID, Fence: anchor.Fence, Anchor: anchor, Frontier: frontier, Revision: revision}
	return finishReadDelivery(ctx, e, "get_knowledge", req.Principal, out, anchor, "", "", ids)
}

func (e *Engine) knowledgeReadFailure(err error) error {
	if knowledgeCommandRejection(err) || errors.Is(err, ErrUnsupportedMethod) || isLimit(err) {
		return err
	}
	return e.readFailure(err)
}
func knowledgeCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrObjectIO) || errors.Is(err, ErrSemanticCorruption) {
		return false
	}
	return errors.Is(err, knowledge.ErrInvalidSummary) || errors.Is(err, knowledge.ErrInvalid) || errors.Is(err, knowledge.ErrObservationSequence) || errors.Is(err, knowledge.ErrUnknownObservation) || errors.Is(err, knowledge.ErrKnowledgeMismatch) || errors.Is(err, knowledge.ErrFutureSource) || errors.Is(err, knowledge.ErrDuplicateItem) || errors.Is(err, knowledge.ErrContextSelectionInvalid) || errors.Is(err, knowledge.ErrContextSelectionLimit) || errors.Is(err, knowledge.ErrContextBudgetUnsatisfied) || errors.Is(err, model.ErrAnchorMismatch) || errors.Is(err, agentprofile.ErrCommandScope) || errors.Is(err, ErrUnsupportedMethod)
}

var _ api.KnowledgeCommands = (*Engine)(nil)
