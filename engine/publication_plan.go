package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// finishPlan seals a private candidate bundle. Every registered transition
// shares this receipt→receipt-tree→RootSet chain and the existing sole publisher.
// No caller can supply a receipt root, object address or publication proof.
// selectedRoot is the command's result root (domain for host state, LiveControl
// for agent/v1), not an independently published state authority.
func (e *Engine) finishPlan(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, builder *objectBuilder, roots model.RootSet, selectedRoot, commandID, opsID model.ObjectID) (*commitPlan, error) {
	if e.privateReduction {
		return &commitPlan{builder: builder, roots: roots, stateRoot: selectedRoot}, nil
	}
	var appliedEvent *model.Event
	var profileEvents []model.Event
	if e.options.EventProfile {
		if !e.replayingEvents || e.replayEventRegistry != 0 {
			if e.replayingEvents && e.replayEventRegistry != eventlog.ProfileRegistryVersion {
				return nil, ErrUnsupportedSchema
			}
			drafts, err := e.profileEventDrafts(ctx, req, builder, roots)
			if err != nil {
				return nil, err
			}
			var objects []codec.Object
			profileEvents, objects, err = e.prepareProfileEvents(req, txID, commandID, opsID, drafts)
			if err != nil {
				return nil, err
			}
			for _, obj := range objects {
				if _, err = builder.add(obj, storage.KindEvent); err != nil {
					return nil, err
				}
			}
		}
		event, obj, err := e.prepareAppliedEvent(req, txID, commandID, opsID, profileEvents)
		if err != nil {
			return nil, err
		}
		if _, err = builder.add(obj, storage.KindEvent); err != nil {
			return nil, err
		}
		appliedEvent = &event
	}
	receipt, err := makeReceipt(receiptRecord{Key: req.Key(), RequestDigest: digest, Command: commandID, Operations: opsID, TxID: txID, LSN: lsn, StateRoot: selectedRoot})
	if err != nil {
		return nil, err
	}
	receiptID, err := builder.add(receipt, storage.KindObject)
	if err != nil {
		return nil, err
	}
	receipts, err := state.Apply(ctx, builder, systemSpace, e.roots.CommandReceipts, []state.Mutation{{Key: receiptKey(req.Key()), Value: receiptID}})
	if err != nil {
		return nil, err
	}
	for _, obj := range receipts.Objects {
		if _, err = builder.add(obj, storage.KindObject); err != nil {
			return nil, err
		}
	}
	roots.CommandReceipts = receipts.Root
	rootObj, err := makeRootSet(roots)
	if err != nil {
		return nil, err
	}
	rootID, err := builder.add(rootObj, storage.KindObject)
	if err != nil {
		return nil, err
	}
	records := make([]storage.Record, len(builder.objects))
	for i, obj := range builder.objects {
		records[i] = storage.Record{Kind: obj.kind, Payload: obj.encoded}
	}
	controlUpdates := rootUpdates(e.roots.LiveControl, roots.LiveControl)
	if req.Method == api.MethodApplyMemory || req.Method == api.MethodApplyKnowledge || req.Method == api.MethodApplySummary || req.Method == api.MethodApplyAnnotation || req.Method == api.MethodImportTransfer {
		controlUpdates = rootUpdates(e.roots.ExecutionKnowledge, roots.ExecutionKnowledge)
	}
	if req.Method == api.MethodApplyWork {
		controlUpdates = rootUpdates(e.rootSet, selectedRoot)
	}
	return &commitPlan{builder: builder, roots: roots, rootSet: rootID, receipt: receiptID, stateRoot: selectedRoot, event: appliedEvent, profileEvents: profileEvents, transaction: storage.Transaction{
		TxID: txID, Request: req.Key(), RequestDigest: asDigest(digest), LSN: lsn, PreviousDigest: e.publication.Position.Digest,
		Records: records, RefUpdates: rootUpdates(e.roots.BranchRefs, roots.BranchRefs), ControlUpdates: controlUpdates,
		Receipt: asDigest(receiptID), PostRootSet: asDigest(rootID),
	}}, nil
}

func rootUpdates(before, after model.ObjectID) []byte {
	b := corePrefix()
	b, _ = appendOptionalID(b, before)
	b, _ = appendOptionalID(b, after)
	return b
}
