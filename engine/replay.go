package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) replay(tx storage.ScannedTransaction, source io.ReaderAt) error {
	e.index.source = source
	defer func() { e.index.source = nil }()
	if err := e.replayTransaction(tx); err != nil {
		return semanticError(err)
	}
	return nil
}

func (e *Engine) replayTransaction(tx storage.ScannedTransaction) error {
	ctx := context.Background()
	if e.publication.Store == "" {
		e.publication = storage.Publication{Store: tx.Store, Position: storage.Position{Digest: tx.PreviousDigest}, PublishedEnd: storage.SegmentHeaderSize, Profile: storage.FileSyncExperimental}
		e.history[0] = commitEnvelope{fence: e.publication.Fence()}
	}
	if tx.Store == "" || tx.Store != e.publication.Store || tx.LSN != e.publication.Position.LSN+1 || tx.PreviousDigest != e.publication.Position.Digest || tx.StartOffset != int64(e.publication.PublishedEnd) || len(tx.RecordLocations) != len(tx.Records) {
		return ErrInvalidStateData
	}
	if uint64(len(e.index.entries))+uint64(len(tx.Records)) > e.options.MaxObjects {
		return storage.ErrLimit
	}
	actual := newBuilder(ctx, e.index, e.options.MaxObjects)
	var receipt receiptRecord
	foundReceipt := false
	foundEvent := false
	var registry uint64
	for _, record := range tx.Records {
		obj, err := codec.DecodeObject(record.Payload)
		if err != nil {
			return err
		}
		if obj.Type == eventlog.Type || obj.Type == eventlog.ProfileType {
			if !e.options.EventProfile {
				return ErrUnsupportedSchema
			}
			if obj.Type == eventlog.Type {
				if foundEvent {
					return ErrInvalidStateData
				}
				core, err := eventlog.Decode(obj)
				if err != nil {
					return err
				}
				registry = core.ProfileRegistryVersion
				foundEvent = true
			}
		}
		kind := storage.KindObject
		if obj.Type == operationsType {
			kind = storage.KindStateOps
		}
		if obj.Type == eventlog.Type || obj.Type == eventlog.ProfileType {
			kind = storage.KindEvent
		}
		if record.Kind != kind {
			return ErrInvalidStateData
		}
		if err := validateRecord(ctx, obj, actual); err != nil {
			return err
		}
		id, err := codec.ObjectID(obj)
		if err != nil {
			return err
		}
		if _, exists := e.index.entries[id]; exists {
			return fmt.Errorf("%w: duplicate body object", ErrInvalidStateData)
		}
		if _, exists := actual.staged[id]; exists {
			return ErrInvalidStateData
		}
		if _, err = actual.add(obj, record.Kind); err != nil {
			return err
		}
		if asDigest(id) == tx.Receipt {
			if foundReceipt {
				return ErrInvalidStateData
			}
			receipt, err = readReceipt(obj)
			if err != nil {
				return err
			}
			foundReceipt = true
		}
	}
	if e.options.EventProfile && !foundEvent {
		return ErrUnsupportedSchema
	}
	if !foundReceipt || receipt.Key != tx.Request || receipt.TxID != tx.TxID || receipt.LSN != tx.LSN || asDigest(receipt.RequestDigest) != tx.RequestDigest {
		return ErrInvalidStateData
	}
	if _, found, err := e.lookup(ctx, receipt.Key); err != nil {
		return err
	} else if found {
		return fmt.Errorf("%w: repeated RequestKey", ErrInvalidStateData)
	}
	command, err := readObject(ctx, actual, receipt.Command)
	if err != nil {
		return err
	}
	req, err := readCommand(command)
	if err != nil {
		return err
	}
	digest, err := CommandDigest(req)
	if err != nil {
		return err
	}
	if req.Key() != tx.Request || digest != receipt.RequestDigest {
		return ErrInvalidStateData
	}
	opsObj, err := readObject(ctx, actual, receipt.Operations)
	if err != nil {
		return err
	}
	ops, err := readOperations(opsObj)
	if err != nil {
		return err
	}
	if ops.AfterRoot != receipt.StateRoot {
		return ErrInvalidStateData
	}
	e.replayingEvents = true
	e.replayEventRegistry = registry
	defer func() { e.replayingEvents = false; e.replayEventRegistry = 0 }()
	plan, err := e.prepare(ctx, req, digest, tx.TxID, tx.LSN, &ops)
	if err != nil {
		return err
	}
	if err = plan.compare(tx.Transaction); err != nil {
		return err
	}
	for i, obj := range plan.builder.objects {
		loc := tx.RecordLocations[i]
		if loc.PayloadLength != uint32(len(obj.encoded)) || loc.PayloadOffset < tx.StartOffset || loc.PayloadOffset+int64(loc.PayloadLength) > tx.EndOffset {
			return ErrInvalidStateData
		}
		e.index.entries[obj.id] = loc
	}
	e.roots = plan.roots
	e.rootSet = plan.rootSet
	e.publication = storage.Publication{Store: tx.Store, Generation: e.publication.Generation + 1, Position: storage.Position{LSN: tx.LSN, Digest: tx.CommitDigest},
		PostRootSet: tx.PostRootSet, PublishedEnd: uint64(tx.EndOffset), Profile: storage.FileSyncExperimental}
	e.envelopes[tx.TxID] = commitEnvelope{fence: e.publication.Fence(), rootSet: plan.rootSet, space: tx.Request.Space, principal: tx.Request.Principal}
	e.history[tx.LSN] = e.envelopes[tx.TxID]
	e.installEvent(plan)
	e.receiptHints[keyHint(tx.Request)] = struct{}{}
	return nil
}

func validateRecord(ctx context.Context, obj codec.Object, reader state.ObjectReader) error {
	if obj.Version != 1 && !(obj.Type == eventlog.Type && obj.Version == 2) {
		return ErrUnsupportedSchema
	}
	switch obj.Type {
	case state.NodeType:
		return state.ValidateNodeObject(ctx, obj, reader)
	case valueType:
		_, err := readValue(obj)
		return err
	case commandType:
		_, err := readCommand(obj)
		return err
	case operationsType:
		_, err := readOperations(obj)
		return err
	case receiptType:
		_, err := readReceipt(obj)
		return err
	case rootSetType:
		_, err := readRootSet(obj)
		return err
	case agentRecordType:
		_, _, err := readAgentRecord(obj)
		return err
	case agentRecordRootType:
		_, _, err := loadAgentRecordImage(ctx, reader, obj)
		return err
	case eventlog.Type, eventlog.ProfileType:
		_, err := eventlog.Decode(obj)
		return err
	default:
		return ErrUnsupportedSchema
	}
}
