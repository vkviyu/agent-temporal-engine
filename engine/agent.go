package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const agentSnapshotType = "ate.agent.snapshot"

var _ api.AgentCommands = (*Engine)(nil)

// All profile partitions are protected by the system LiveControl radix. The
// key is canonical and unambiguous even when identities contain delimiters.
func agentPartitionKey(space model.SpaceID, principal model.PrincipalID) []byte {
	b, _ := codec.AppendString(nil, "agent/v1")
	b, _ = codec.AppendString(b, string(space))
	b, _ = codec.AppendString(b, string(principal))
	return b
}

func validAgentScope(space model.SpaceID, principal model.PrincipalID) error {
	if err := validSpace(space); err != nil {
		return err
	}
	if principal == "" || len(principal) > model.MaxIdentityBytes || !utf8.ValidString(string(principal)) {
		return invalidState("agent principal", nil)
	}
	return nil
}

func makeAgentSnapshot(space model.SpaceID, owner model.PrincipalID, snapshot agentprofile.Snapshot) (codec.Object, error) {
	if err := validAgentScope(space, owner); err != nil {
		return codec.Object{}, err
	}
	for _, task := range snapshot.Tasks {
		if task.Space != space || task.Owner != owner {
			return codec.Object{}, agentprofile.ErrCommandScope
		}
	}
	raw, err := agentprofile.EncodeSnapshot(snapshot)
	if err != nil {
		return codec.Object{}, err
	}
	payload, err := codec.AppendString(corePrefix(), string(owner))
	if err != nil {
		return codec.Object{}, err
	}
	payload, err = codec.AppendBytes(payload, raw)
	if err != nil {
		return codec.Object{}, err
	}
	if len(payload) > codec.MaxObjectPayload {
		return codec.Object{}, storage.ErrLimit
	}
	// Preserve the original snapshot envelope. Its schema-specific references
	// are extracted by agentprofile.ContentReferences; source fences and logical
	// Knowledge PacketIDs also retain history. Object.Refs alone is insufficient
	// for selective sweeping. AuditFull retains every canonical transaction.
	return coreObject(agentSnapshotType, space, nil, payload)
}

func readAgentSnapshot(obj codec.Object) (agentprofile.Snapshot, model.PrincipalID, error) {
	if err := checkCoreObject(obj, agentSnapshotType, false); err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	d, err := coreDecoder(obj.Payload)
	if err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	owner, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	raw, err := d.Bytes(codec.MaxObjectPayload)
	if err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	if err = d.Done(); err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	snapshot, err := agentprofile.DecodeSnapshot(raw)
	if err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	expected, err := makeAgentSnapshot(obj.Space, model.PrincipalID(owner), snapshot)
	if err != nil {
		return agentprofile.Snapshot{}, "", err
	}
	if !bytes.Equal(expected.Payload, obj.Payload) || checkRefs(obj, nil) != nil {
		return agentprofile.Snapshot{}, "", ErrInvalidStateData
	}
	return snapshot, model.PrincipalID(owner), nil
}

func loadAgentLedger(ctx context.Context, reader state.ObjectReader, root model.ObjectID, space model.SpaceID, principal model.PrincipalID) (*agentprofile.Ledger, model.ObjectID, bool, error) {
	id, found, err := state.Get(ctx, reader, systemSpace, root, agentPartitionKey(space, principal))
	if err != nil || !found {
		return agentprofile.New(), "", found, err
	}
	obj, err := readObject(ctx, reader, id)
	if err != nil {
		return nil, "", false, err
	}
	_, owner, err := readAgentSnapshot(obj)
	if err != nil {
		return nil, "", false, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	if owner != principal || obj.Space != space {
		return nil, "", false, ErrInvalidStateData
	}
	d, _ := coreDecoder(obj.Payload)
	_, _ = d.String(model.MaxIdentityBytes)
	raw, _ := d.Bytes(codec.MaxObjectPayload)
	ledger, err := agentprofile.RestoreSnapshot(raw)
	if err != nil {
		return nil, "", false, fmt.Errorf("%w: %w", ErrStoredObject, err)
	}
	return ledger, id, true, err
}

func (e *Engine) knownAgentFence(fence model.CommitFence) bool {
	if fence.Validate() != nil || fence.Store != e.publication.Store || fence.LSN > e.publication.Position.LSN {
		return false
	}
	if fence == e.publication.Fence() {
		return true
	}
	proof, ok := e.history[fence.LSN]
	if !ok {
		proof, ok = e.baseHistory[fence.LSN]
	}
	return ok && proof.fence == fence
}

func (e *Engine) checkAgentInputs(ctx context.Context, req api.CommitRequest, l *agentprofile.Ledger, c agentprofile.Command) error {
	var fence model.CommitFence
	switch {
	case c.StartStep != nil:
		fence = c.StartStep.InputAnchor
	case c.PutPending != nil:
		fence = c.PutPending.InputFence
	}
	if fence != (model.CommitFence{}) && !e.knownAgentFence(fence) {
		return model.ErrAnchorMismatch
	}
	return e.checkAgentControlInputs(ctx, req, l, c)
}

// ApplyAgent shares admission, receipt-first retries, batching and the sole P1
// publisher with core state commands. No model or host callback runs here.
func (e *Engine) ApplyAgent(ctx context.Context, req api.ApplyAgentRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := agentprofile.EncodeCommand(req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyAgent, Body: body, ExpectedFence: req.ExpectedFence})
}

func (e *Engine) AgentProfileEnabled() bool { return e.options.AgentProfile }

func (e *Engine) prepareAgent(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if !e.options.AgentProfile {
		return nil, ErrUnsupportedMethod
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	c, err := agentprofile.DecodeCommand(req.Body)
	if err != nil {
		return nil, err
	}
	candidate, before, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	if err = e.checkAgentInputs(ctx, req, candidate, c); err != nil {
		return nil, err
	}
	if err = e.checkTaskValidation(ctx, req, candidate, c, recorded != nil); err != nil {
		return nil, err
	}
	if _, err = candidate.ApplyCommand(req.Key(), c); err != nil {
		return nil, err
	}
	builder := newBuilder(ctx, e.index, e.options.MaxObjects)
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := builder.add(command, storage.KindObject)
	if err != nil {
		return nil, err
	}
	obj, err := makeAgentSnapshot(req.Space, req.Principal, candidate.Snapshot())
	if err != nil {
		return nil, err
	}
	after, err := builder.add(obj, storage.KindObject)
	if err != nil {
		return nil, err
	}
	key := agentPartitionKey(req.Space, req.Principal)
	delta, err := state.Apply(ctx, builder, systemSpace, e.roots.LiveControl, []state.Mutation{{Key: key, Value: after}})
	if err != nil {
		return nil, err
	}
	for _, obj := range delta.Objects {
		if _, err = builder.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	changes := operationsRecord{Space: req.Space, BeforeRoot: e.roots.LiveControl, AfterRoot: delta.Root, Changes: []stateChange{{Key: key, Before: before, After: after}}}
	ops, err := makeOperations(changes)
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		expected, err := makeOperations(*recorded)
		if err != nil || !bytes.Equal(expected.Payload, ops.Payload) || expected.Space != ops.Space {
			return nil, ErrInvalidStateData
		}
	}
	opsID, err := builder.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	roots := e.roots
	roots.LiveControl = delta.Root
	return e.finishPlan(ctx, req, digest, txID, lsn, builder, roots, roots.LiveControl, commandID, opsID)
}

func (e *Engine) GetAgentState(ctx context.Context, req api.ReadAgentStateRequest) (api.AgentStateResult, error) {
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return api.AgentStateResult{}, err
	}
	if !e.options.AgentProfile {
		return api.AgentStateResult{}, ErrUnsupportedMethod
	}
	if err := e.publicGate(); err != nil {
		return api.AgentStateResult{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.AgentStateResult{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.AgentStateResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.publication.Fence()}
	roots, rootID := e.roots, e.rootSet
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
		if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
			return api.AgentStateResult{}, model.ErrAnchorMismatch
		}
		proof, ok := e.history[anchor.Fence.LSN]
		if !ok || proof.fence != anchor.Fence {
			return api.AgentStateResult{}, model.ErrAnchorMismatch
		}
		rootID, roots = proof.rootSet, model.RootSet{}
		if rootID != "" {
			obj, err := readObject(ctx, e.index, rootID)
			if err != nil {
				return api.AgentStateResult{}, e.readFailure(err)
			}
			roots, err = readRootSet(obj)
			if err != nil {
				return api.AgentStateResult{}, e.readFailure(err)
			}
		}
	}
	ledger, _, found, err := loadAgentLedger(ctx, e.index, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return api.AgentStateResult{}, e.readFailure(err)
	}
	snapshot := ledger.Snapshot()
	ids, err := e.checkAgentSnapshotDisclosure(ctx, roots, req.Space, req.Principal, snapshot, ctx.Value(readDeliveryKey{}) != nil)
	if err != nil {
		if errors.Is(err, memory.ErrHistoryAccessDenied) {
			return api.AgentStateResult{}, err
		}
		return api.AgentStateResult{}, e.knowledgeReadFailure(err)
	}
	if err = ctx.Err(); err != nil {
		return api.AgentStateResult{}, err
	}
	if err = e.viewOutputGate(); err != nil {
		return api.AgentStateResult{}, err
	}
	out := api.AgentStateResult{Found: found, Snapshot: snapshot, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID, LiveControlRoot: roots.LiveControl}
	return finishReadDelivery(ctx, e, "get_agent_state", req.Principal, out, anchor, "", "", ids)
}

func agentCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrObjectIO) || errors.Is(err, ErrSemanticCorruption) {
		return false
	}
	return errors.Is(err, ErrWorkBoundary) || errors.Is(err, knowledge.ErrContextSelectionInvalid) || errors.Is(err, agentprofile.ErrValidation) || errors.Is(err, memory.ErrHistoryAccessDenied) || errors.Is(err, memory.ErrUseDenied) || errors.Is(err, agentprofile.ErrInvalid) || errors.Is(err, agentprofile.ErrConflict) || errors.Is(err, agentprofile.ErrUniqueSlot) ||
		errors.Is(err, agentprofile.ErrGoalEpoch) || errors.Is(err, agentprofile.ErrCanceled) || errors.Is(err, agentprofile.ErrUnknown) ||
		errors.Is(err, agentprofile.ErrActionUnknown) || errors.Is(err, agentprofile.ErrDurableScope) || errors.Is(err, model.ErrAnchorMismatch) ||
		errors.Is(err, agentprofile.ErrCommandShape) || errors.Is(err, agentprofile.ErrCommandScope) || errors.Is(err, agentprofile.ErrCommandVersion)
}
