package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var errCAS = errors.New("state compare-and-swap condition failed")

type commitPlan struct {
	builder             *objectBuilder
	roots               model.RootSet
	rootSet             model.ObjectID
	receipt             model.ObjectID
	stateRoot           model.ObjectID
	transaction         storage.Transaction
	event               *model.Event
	profileEvents       []model.Event
	profileEventAliases map[string]eventHead
}

func receiptKey(key model.RequestKey) []byte {
	var b []byte
	for _, s := range []string{string(key.Space), string(key.Principal), string(key.RequestID)} {
		b, _ = codec.AppendString(b, s)
	}
	return b
}

func asDigest(id model.ObjectID) storage.Digest {
	var d storage.Digest
	b, _ := hex.DecodeString(string(id))
	copy(d[:], b)
	return d
}

func domainRoot(ctx context.Context, reader state.ObjectReader, roots model.RootSet, space model.SpaceID) (model.ObjectID, error) {
	return slotSpaceRoot(ctx, reader, roots.DomainStates, space)
}

func slotSpaceRoot(ctx context.Context, reader state.ObjectReader, slot model.ObjectID, space model.SpaceID) (model.ObjectID, error) {
	id, found, err := state.Get(ctx, reader, systemSpace, slot, []byte(space))
	if err != nil {
		return "", err
	}
	if !found {
		return state.EmptyRoot, nil
	}
	o, err := readObject(ctx, reader, id)
	if err != nil {
		return "", err
	}
	if o.Space != space || o.Type != state.NodeType || o.Version != state.NodeVersion {
		return "", ErrInvalidStateData
	}
	return id, nil
}

func methodRoot(roots model.RootSet, method string) model.ObjectID {
	return domainRootForMethod(roots, method)
}

func setMethodRoot(roots *model.RootSet, method string, root model.ObjectID) {
	setDomainRootForMethod(roots, method, root)
}

func noRefUpdates(root model.ObjectID) []byte {
	return rootUpdates(root, root)
}

// prepare compiles accepted host mutations into versioned StateOps. During
// replay the recorded changes are the apply input, checked against the original
// canonical command and historical before-values, never today's external policy.
func (e *Engine) prepare(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if err := e.checkActionReservation(ctx, req, recorded); err != nil {
		return nil, err
	}
	if req.Method == api.MethodImportTransfer {
		return e.prepareTransfer(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyWork {
		return e.prepareWork(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyAnnotation {
		return e.prepareAnnotation(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodUpdateInvestigation {
		return e.prepareInvestigation(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyTaskTrace {
		return e.prepareTaskTrace(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplySummary {
		return e.prepareSummary(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyEmission {
		return e.prepareEmission(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyAgent {
		return e.prepareAgent(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyKnowledge {
		return e.prepareKnowledge(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyGraph {
		return e.prepareGraph(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method == api.MethodApplyMemory {
		return e.prepareMemory(ctx, req, digest, txID, lsn, recorded)
	}
	if req.Method != api.MethodMutateState {
		return nil, ErrUnsupportedMethod
	}
	if err := validSpace(req.Space); err != nil {
		return nil, err
	}
	batch, err := DecodeStateBatch(req.Body)
	if err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	beforeRoot, err := slotSpaceRoot(ctx, e.index, methodRoot(e.roots, req.Method), req.Space)
	if err != nil {
		return nil, err
	}
	if recorded != nil && (recorded.Space != req.Space || recorded.BeforeRoot != beforeRoot || len(recorded.Changes) != len(batch.Operations)) {
		return nil, ErrInvalidStateData
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
	delta, changes, err := applyStateBatch(ctx, builder, req.Space, beforeRoot, batch)
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		for i, change := range changes {
			expected := recorded.Changes[i]
			if !bytes.Equal(change.Key, expected.Key) || change.Before != expected.Before || change.After != expected.After {
				return nil, ErrInvalidStateData
			}
		}
	}
	if recorded != nil && recorded.AfterRoot != delta.Root {
		return nil, ErrInvalidStateData
	}
	// StateOps bind the historical before-values and the exact resulting root.
	ops, err := makeOperations(operationsRecord{Space: req.Space, BeforeRoot: beforeRoot, AfterRoot: delta.Root, Changes: changes})
	if err != nil {
		return nil, err
	}
	opsID, err := builder.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	selectedRoot := methodRoot(e.roots, req.Method)
	catalog, err := state.Apply(ctx, builder, systemSpace, selectedRoot, []state.Mutation{{Key: []byte(req.Space), Value: delta.Root, Delete: delta.Root == state.EmptyRoot}})
	if err != nil {
		return nil, err
	}
	for _, obj := range catalog.Objects {
		if _, err = builder.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	// Carry the protected roots forward; the registered command owns only its
	// domain delta. Receipt and all root transitions share one finalizer.
	roots := e.roots
	setMethodRoot(&roots, req.Method, catalog.Root)
	return e.finishPlan(ctx, req, digest, txID, lsn, builder, roots, delta.Root, commandID, opsID)
}

func (p *commitPlan) compare(actual storage.Transaction) error {
	expected := p.transaction
	if actual.TxID != expected.TxID || actual.Request != expected.Request || actual.RequestDigest != expected.RequestDigest || actual.LSN != expected.LSN || actual.PreviousDigest != expected.PreviousDigest || actual.Receipt != expected.Receipt || actual.PostRootSet != expected.PostRootSet || !bytes.Equal(actual.RefUpdates, expected.RefUpdates) || !bytes.Equal(actual.ControlUpdates, expected.ControlUpdates) || len(actual.Records) != len(expected.Records) {
		return ErrInvalidStateData
	}
	for i, record := range expected.Records {
		if actual.Records[i].Kind != record.Kind || !bytes.Equal(actual.Records[i].Payload, record.Payload) {
			return fmt.Errorf("%w: record %d", ErrInvalidStateData, i)
		}
	}
	return nil
}
