package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func transferOwner(principal model.PrincipalID) string {
	sum := sha256.Sum256([]byte(principal))
	return hex.EncodeToString(sum[:])
}
func transferStateKey(principal model.PrincipalID, id model.ObjectID) []byte {
	return []byte("ate/transfer/manifest/v1/" + transferOwner(principal) + "/" + string(id))
}
func transferArchivePrefix(principal model.PrincipalID) []byte {
	return []byte("ate/transfer/evidence/v1/" + transferOwner(principal) + "/")
}
func transferArchiveKey(principal model.PrincipalID, id model.ObjectID) []byte {
	return append(transferArchivePrefix(principal), []byte(id)...)
}

func (e *Engine) ImportTransfer(ctx context.Context, r api.ImportTransferRequest) (api.CommitResult, error) {
	body, err := api.EncodeImportTransfer(api.ImportTransferCommand{Manifest: r.Manifest, ApprovedDigest: r.ApprovedDigest})
	if err != nil {
		return admissionFailure(r.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: r.Key.Space, Principal: r.Key.Principal, RequestID: r.Key.RequestID, Method: api.MethodImportTransfer, Body: body, ExpectedFence: r.ExpectedFence})
}

func (e *Engine) prepareTransfer(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	c, err := api.DecodeImportTransfer(req.Body)
	if err != nil {
		return nil, err
	}
	if c.Manifest.Source.Space == req.Space {
		return nil, transfer.ErrInvalid
	}
	beforeRoot, err := slotSpaceRoot(ctx, e.index, e.roots.ExecutionKnowledge, req.Space)
	if err != nil {
		return nil, err
	}
	key := transferStateKey(req.Principal, c.Manifest.ID)
	if _, found, err := state.Get(ctx, e.index, req.Space, beforeRoot, key); err != nil {
		return nil, err
	} else if found {
		return nil, errCAS
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := b.add(command, storage.KindObject)
	if err != nil {
		return nil, err
	}
	r := transfer.ImportRecord{Schema: transfer.Schema, Space: req.Space, Principal: req.Principal, Manifest: c.Manifest, Trust: "caller-approved-foreign-evidence"}
	var changes []stateChange
	var mutations []state.Mutation
	for _, entry := range c.Manifest.Entries {
		a := transfer.ArchiveEntry{Schema: transfer.Schema, Manifest: c.Manifest.ID, Source: entry, Classification: "foreign-read-only-evidence"}
		raw, err := a.Canonical()
		if err != nil {
			return nil, err
		}
		if len(raw) > api.MaxStateValueBytes {
			return nil, storage.ErrLimit
		}
		object, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
		if err != nil {
			return nil, err
		}
		id, err := b.add(object, storage.KindObject)
		if err != nil {
			return nil, err
		}
		if id == entry.Source.ID {
			return nil, transfer.ErrInvalid
		}
		r.Mappings = append(r.Mappings, transfer.Mapping{Source: entry.Source, Target: id})
		k := transferArchiveKey(req.Principal, id)
		changes = append(changes, stateChange{Key: k, After: id})
		mutations = append(mutations, state.Mutation{Key: k, Value: id})
	}
	raw, err := r.Canonical()
	if err != nil {
		return nil, err
	}
	if len(raw) > api.MaxStateValueBytes {
		return nil, storage.ErrLimit
	}
	recordObject, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		return nil, err
	}
	recordID, err := b.add(recordObject, storage.KindObject)
	if err != nil {
		return nil, err
	}
	changes = append(changes, stateChange{Key: key, After: recordID})
	mutations = append(mutations, state.Mutation{Key: key, Value: recordID})
	sort.Slice(changes, func(i, j int) bool { return bytes.Compare(changes[i].Key, changes[j].Key) < 0 })
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
	delta, err := state.Apply(ctx, b, req.Space, beforeRoot, mutations)
	if err != nil {
		return nil, err
	}
	for _, o := range delta.Objects {
		if _, err := b.add(o, storage.KindObject); err != nil {
			return nil, err
		}
	}
	opsRecord := operationsRecord{Space: req.Space, BeforeRoot: beforeRoot, AfterRoot: delta.Root, Changes: changes}
	ops, err := makeOperations(opsRecord)
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		expected, err := makeOperations(*recorded)
		if err != nil || !bytes.Equal(expected.Payload, ops.Payload) {
			return nil, ErrInvalidStateData
		}
	}
	opsID, err := b.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	catalog, err := state.Apply(ctx, b, systemSpace, e.roots.ExecutionKnowledge, []state.Mutation{{Key: []byte(req.Space), Value: delta.Root}})
	if err != nil {
		return nil, err
	}
	for _, o := range catalog.Objects {
		if _, err := b.add(o, storage.KindObject); err != nil {
			return nil, err
		}
	}
	roots := e.roots
	roots.ExecutionKnowledge = catalog.Root
	return e.finishPlan(ctx, req, digest, txID, lsn, b, roots, delta.Root, commandID, opsID)
}

func (e *Engine) loadTransfer(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id model.ObjectID) (transfer.ImportRecord, bool, error) {
	root, err := slotSpaceRoot(ctx, e.reader(ctx), roots.ExecutionKnowledge, space)
	if err != nil {
		return transfer.ImportRecord{}, false, err
	}
	record, found, err := state.Get(ctx, e.reader(ctx), space, root, transferStateKey(principal, id))
	if err != nil || !found {
		return transfer.ImportRecord{}, found, err
	}
	o, err := readObject(ctx, e.reader(ctx), record)
	if err != nil {
		return transfer.ImportRecord{}, false, err
	}
	v, err := readValue(o)
	if err != nil || o.Space != space || v.Kind != api.ValueBytes {
		return transfer.ImportRecord{}, false, ErrInvalidStateData
	}
	r, err := transfer.DecodeImportRecord(v.Data)
	if err != nil || r.Space != space || r.Principal != principal || r.Manifest.ID != id {
		return transfer.ImportRecord{}, false, ErrInvalidStateData
	}
	return r, true, nil
}

func (e *Engine) GetTransfer(ctx context.Context, req api.ReadTransferRequest) (api.TransferResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_transfer", req.Principal, req, func(scoped context.Context) (api.TransferResult, error) {
			return e.GetTransfer(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.TransferResult{}, err
	}
	if validAgentScope(req.Space, req.Principal) != nil || !model.ValidDigest(string(req.ID)) {
		return api.TransferResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.TransferResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.TransferResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
		return api.TransferResult{}, model.ErrAnchorMismatch
	}
	roots, rootID, err := e.rootsAtFence(ctx, anchor.Fence, false)
	if err != nil {
		return api.TransferResult{}, err
	}
	r, found, err := e.loadTransfer(ctx, roots, req.Space, req.Principal, req.ID)
	if err != nil {
		return api.TransferResult{}, e.knowledgeReadFailure(ctx, err)
	}
	var disclosed []model.ObjectID
	if found {
		disclosed = transfer.ContentReferences(r)
		if err := e.checkDisclosureIDs(ctx, req.Space, disclosed); err != nil {
			return api.TransferResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return api.TransferResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.TransferResult{}, err
	}
	result := api.TransferResult{Found: found, Record: r, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID}
	return finishReadDelivery(ctx, e, "get_transfer", req.Principal, result, anchor, "", "", disclosed)
}

func (e *Engine) transferArchive(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id model.ObjectID) (transfer.ArchiveEntry, bool, error) {
	root, err := slotSpaceRoot(ctx, e.reader(ctx), roots.ExecutionKnowledge, space)
	if err != nil {
		return transfer.ArchiveEntry{}, false, err
	}
	stored, found, err := state.Get(ctx, e.reader(ctx), space, root, transferArchiveKey(principal, id))
	if err != nil || !found {
		return transfer.ArchiveEntry{}, found, err
	}
	if stored != id {
		return transfer.ArchiveEntry{}, false, ErrInvalidStateData
	}
	o, err := readObject(ctx, e.reader(ctx), id)
	if err != nil {
		return transfer.ArchiveEntry{}, false, err
	}
	v, err := readValue(o)
	if err != nil || o.Space != space || v.Kind != api.ValueBytes {
		return transfer.ArchiveEntry{}, false, ErrInvalidStateData
	}
	a, err := transfer.DecodeArchiveEntry(v.Data)
	if err != nil {
		return transfer.ArchiveEntry{}, false, ErrInvalidStateData
	}
	return a, true, nil
}

var _ api.TransferCommands = (*Engine)(nil)
