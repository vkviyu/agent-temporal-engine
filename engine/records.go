package engine

import (
	"bytes"
	"encoding/hex"
	"strings"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func makeOperations(record operationsRecord) (codec.Object, error) {
	if err := validSpace(record.Space); err != nil {
		return codec.Object{}, err
	}
	if len(record.Changes) == 0 || len(record.Changes) > api.MaxStateMutations {
		return codec.Object{}, invalidState("change count", nil)
	}
	b := corePrefix()
	var err error
	b, err = appendOptionalID(b, record.BeforeRoot)
	if err != nil {
		return codec.Object{}, err
	}
	b, err = appendOptionalID(b, record.AfterRoot)
	if err != nil {
		return codec.Object{}, err
	}
	b = codec.AppendUint(b, uint64(len(record.Changes)))
	refs := []model.ObjectID{record.BeforeRoot, record.AfterRoot}
	for i, change := range record.Changes {
		if len(change.Key) > api.MaxStateKeyBytes || (i > 0 && bytes.Compare(record.Changes[i-1].Key, change.Key) >= 0) {
			return codec.Object{}, invalidState("change key order", nil)
		}
		b, _ = codec.AppendBytes(b, change.Key)
		b, err = appendOptionalID(b, change.Before)
		if err != nil {
			return codec.Object{}, err
		}
		b, err = appendOptionalID(b, change.After)
		if err != nil {
			return codec.Object{}, err
		}
		refs = append(refs, change.Before, change.After)
	}
	return coreObject(operationsType, record.Space, canonicalRefs(refs...), b)
}

func readOperations(o codec.Object) (operationsRecord, error) {
	if err := checkCoreObject(o, operationsType, false); err != nil {
		return operationsRecord{}, err
	}
	d, err := coreDecoder(o.Payload)
	if err != nil {
		return operationsRecord{}, err
	}
	r := operationsRecord{Space: o.Space}
	if r.BeforeRoot, err = readOptionalID(d); err != nil {
		return operationsRecord{}, err
	}
	if r.AfterRoot, err = readOptionalID(d); err != nil {
		return operationsRecord{}, err
	}
	count, err := d.Uint()
	if err != nil {
		return operationsRecord{}, invalidState("change count", err)
	}
	if count == 0 || count > api.MaxStateMutations {
		return operationsRecord{}, invalidState("change count", nil)
	}
	if count > uint64(d.Remaining()/3) {
		return operationsRecord{}, invalidState("changes", codec.ErrTruncated)
	}
	r.Changes = make([]stateChange, int(count))
	for i := range r.Changes {
		key, err := d.Bytes(api.MaxStateKeyBytes)
		if err != nil {
			return operationsRecord{}, invalidState("change key", err)
		}
		r.Changes[i].Key = bytes.Clone(key)
		if r.Changes[i].Before, err = readOptionalID(d); err != nil {
			return operationsRecord{}, err
		}
		if r.Changes[i].After, err = readOptionalID(d); err != nil {
			return operationsRecord{}, err
		}
	}
	if err := d.Done(); err != nil {
		return operationsRecord{}, invalidState("operations", err)
	}
	expected, err := makeOperations(r)
	if err != nil {
		return operationsRecord{}, err
	}
	if err := checkRefs(o, expected.Refs); err != nil {
		return operationsRecord{}, err
	}
	if !bytes.Equal(o.Payload, expected.Payload) {
		return operationsRecord{}, invalidState("noncanonical operations", nil)
	}
	return r, nil
}

func makeReceipt(r receiptRecord) (codec.Object, error) {
	if err := r.Key.Validate(); err != nil {
		return codec.Object{}, invalidState("receipt key", err)
	}
	if err := validSpace(r.Key.Space); err != nil {
		return codec.Object{}, err
	}
	if r.LSN == 0 || !validTxID(r.TxID) {
		return codec.Object{}, invalidState("receipt transaction", nil)
	}
	b := corePrefix()
	for _, text := range []string{string(r.Key.Space), string(r.Key.Principal), string(r.Key.RequestID)} {
		b, _ = codec.AppendString(b, text)
	}
	var err error
	for _, id := range []model.ObjectID{r.RequestDigest, r.Command, r.Operations} {
		b, err = appendID(b, id)
		if err != nil {
			return codec.Object{}, err
		}
	}
	tx, _ := hex.DecodeString(string(r.TxID))
	b, _ = codec.AppendBytes(b, tx)
	b = codec.AppendUint(b, r.LSN)
	b, err = appendOptionalID(b, r.StateRoot)
	if err != nil {
		return codec.Object{}, err
	}
	return coreObject(receiptType, r.Key.Space, canonicalRefs(r.Command, r.Operations, r.StateRoot), b)
}

func readReceipt(o codec.Object) (receiptRecord, error) {
	if err := checkCoreObject(o, receiptType, false); err != nil {
		return receiptRecord{}, err
	}
	d, err := coreDecoder(o.Payload)
	if err != nil {
		return receiptRecord{}, err
	}
	var r receiptRecord
	fields := make([]string, 3)
	for i := range fields {
		if fields[i], err = d.String(model.MaxIdentityBytes); err != nil {
			return receiptRecord{}, invalidState("receipt key", err)
		}
	}
	r.Key = model.RequestKey{Space: model.SpaceID(fields[0]), Principal: model.PrincipalID(fields[1]), RequestID: model.RequestID(fields[2])}
	if r.Key.Space != o.Space {
		return receiptRecord{}, invalidState("receipt scope", nil)
	}
	if r.RequestDigest, err = readID(d); err != nil {
		return receiptRecord{}, err
	}
	if r.Command, err = readID(d); err != nil {
		return receiptRecord{}, err
	}
	if r.Operations, err = readID(d); err != nil {
		return receiptRecord{}, err
	}
	tx, err := d.Bytes(16)
	if err != nil {
		return receiptRecord{}, invalidState("transaction identity", err)
	}
	if len(tx) != 16 {
		return receiptRecord{}, invalidState("transaction identity length", nil)
	}
	r.TxID = model.TxID(hex.EncodeToString(tx))
	if r.LSN, err = d.Uint(); err != nil {
		return receiptRecord{}, invalidState("receipt LSN", err)
	}
	if r.StateRoot, err = readOptionalID(d); err != nil {
		return receiptRecord{}, err
	}
	if err := d.Done(); err != nil {
		return receiptRecord{}, invalidState("receipt", err)
	}
	expected, err := makeReceipt(r)
	if err != nil {
		return receiptRecord{}, err
	}
	if err := checkRefs(o, expected.Refs); err != nil {
		return receiptRecord{}, err
	}
	if !bytes.Equal(o.Payload, expected.Payload) {
		return receiptRecord{}, invalidState("noncanonical receipt", nil)
	}
	return r, nil
}

func makeRootSet(roots model.RootSet) (codec.Object, error) {
	if err := roots.Validate(); err != nil {
		return codec.Object{}, invalidState("root set", err)
	}
	b := corePrefix()
	ids := rootSetIDs(roots)
	for _, id := range ids {
		var err error
		b, err = appendOptionalID(b, id)
		if err != nil {
			return codec.Object{}, err
		}
	}
	return coreObject(rootSetType, systemSpace, canonicalRefs(ids...), b)
}

func readRootSet(o codec.Object) (model.RootSet, error) {
	if err := checkCoreObject(o, rootSetType, true); err != nil {
		return model.RootSet{}, err
	}
	d, err := coreDecoder(o.Payload)
	if err != nil {
		return model.RootSet{}, err
	}
	ids := make([]model.ObjectID, 5)
	for i := range ids {
		if ids[i], err = readOptionalID(d); err != nil {
			return model.RootSet{}, err
		}
	}
	if err := d.Done(); err != nil {
		return model.RootSet{}, invalidState("root set", err)
	}
	if err := checkRefs(o, canonicalRefs(ids...)); err != nil {
		return model.RootSet{}, err
	}
	roots := model.RootSet{DomainStates: ids[0], ExecutionKnowledge: ids[1], BranchRefs: ids[2], LiveControl: ids[3], CommandReceipts: ids[4]}
	if err := roots.Validate(); err != nil {
		return model.RootSet{}, invalidState("root set", err)
	}
	return roots, nil
}

func rootSetIDs(r model.RootSet) []model.ObjectID {
	return []model.ObjectID{r.DomainStates, r.ExecutionKnowledge, r.BranchRefs, r.LiveControl, r.CommandReceipts}
}

func validTxID(id model.TxID) bool {
	if len(id) != 32 || string(id) != strings.ToLower(string(id)) || id == "00000000000000000000000000000000" {
		return false
	}
	_, err := hex.DecodeString(string(id))
	return err == nil
}
