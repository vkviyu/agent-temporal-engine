package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const checkpointRecordLimit = 1 << 20

// Every record is a bounded canonical JSON struct preceded by a uint32 LE
// byte length. No map, object body, reducer snapshot or process state is encoded.
type checkpointHeader struct {
	Magic                                  string
	Schema                                 uint64
	AgentProfile, EventProfile             bool
	Publication                            storage.Publication
	Roots                                  model.RootSet
	RootSet                                model.ObjectID
	Objects, History, Events, Heads, Hints uint64
}

type checkpointObject struct {
	ID       model.ObjectID
	Location storage.RecordLocation
}

type checkpointEnvelope struct {
	Tx        model.TxID
	Fence     model.CommitFence
	RootSet   model.ObjectID
	Space     model.SpaceID
	Principal model.PrincipalID
}

type checkpointEvent struct {
	ID        model.EventID
	Entry     eventEntry
	Operation string
	Ordinal   uint64
}

type checkpointHead struct {
	// Keys are binary codec strings, not necessarily valid UTF-8.
	Key  []byte
	Head eventHead
}

func writeCheckpointRecord(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) == 0 || len(b) > checkpointRecordLimit {
		return storage.ErrLimit
	}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(b)))
	for _, part := range [][]byte{size[:], b} {
		for len(part) > 0 {
			n, err := w.Write(part)
			if err != nil {
				return err
			}
			if n <= 0 || n > len(part) {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}

func readCheckpointRecord(r io.Reader, v any) error {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return err
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n == 0 || n > checkpointRecordLimit {
		return storage.ErrLimit
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return storage.ErrCorruption
	}
	canonical, err := json.Marshal(v)
	if err != nil || !bytes.Equal(b, canonical) {
		// Reject unknown/duplicate fields, reordered fields, whitespace,
		// alternative number/string spellings and noncanonical UTF-8.
		return storage.ErrCorruption
	}
	return nil
}

func checkpointKeys[K ~string, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func (e *Engine) encodeRecoveryCheckpoint(w io.Writer) error {
	h := checkpointHeader{Magic: "ATE-RECOVERY-CHECKPOINT", Schema: recoveryCheckpointSchema,
		AgentProfile: e.options.AgentProfile, EventProfile: e.options.EventProfile,
		Publication: e.publication, Roots: e.roots, RootSet: e.rootSet,
		Objects: uint64(len(e.index.entries)), History: uint64(len(e.history)),
		Events: uint64(len(e.events)), Heads: uint64(len(e.eventHeads)), Hints: uint64(len(e.receiptHints))}
	if err := writeCheckpointRecord(w, h); err != nil {
		return err
	}
	for _, id := range checkpointKeys(e.index.entries) {
		if err := writeCheckpointRecord(w, checkpointObject{id, e.index.entries[id]}); err != nil {
			return err
		}
	}
	txs := make(map[uint64]model.TxID, len(e.envelopes))
	for tx, envelope := range e.envelopes {
		txs[envelope.fence.LSN] = tx
	}
	for lsn := uint64(0); lsn < h.History; lsn++ {
		v, ok := e.history[lsn]
		if !ok {
			return ErrInvalidStateData
		}
		if err := writeCheckpointRecord(w, checkpointEnvelope{txs[lsn], v.fence, v.rootSet, v.space, v.principal}); err != nil {
			return err
		}
	}
	var blocks []*eventMetadataBlock
	for block := e.eventBlocks; block != nil; block = block.previous {
		blocks = append(blocks, block)
	}
	for i := len(blocks) - 1; i >= 0; i-- {
		for _, item := range blocks[i].entries {
			if err := writeCheckpointRecord(w, checkpointEvent{item.id, item.entry, item.operation, item.ordinal}); err != nil {
				return err
			}
		}
	}
	for _, key := range checkpointKeys(e.eventHeads) {
		if err := writeCheckpointRecord(w, checkpointHead{[]byte(key), e.eventHeads[key]}); err != nil {
			return err
		}
	}
	hints := make([][32]byte, 0, len(e.receiptHints))
	for hint := range e.receiptHints {
		hints = append(hints, hint)
	}
	sort.Slice(hints, func(i, j int) bool { return bytes.Compare(hints[i][:], hints[j][:]) < 0 })
	for _, hint := range hints {
		if err := writeCheckpointRecord(w, hint); err != nil {
			return err
		}
	}
	return nil
}

func decodeRecoveryCheckpoint(o Options, ref storage.RecoveryCheckpoint, r io.Reader, source io.ReaderAt) (*Engine, error) {
	var h checkpointHeader
	if err := readCheckpointRecord(r, &h); err != nil {
		return nil, err
	}
	p, s := h.Publication, ref.Source
	if h.Magic != "ATE-RECOVERY-CHECKPOINT" || h.Schema != ref.Schema || p.Store != ref.Store || p.Generation != s.Generation ||
		p.Position != s.Position || p.PostRootSet != s.PostRootSet || p.PublishedEnd != s.PublishedEnd ||
		p.Profile != storage.FileSyncExperimental || asDigest(h.RootSet) != p.PostRootSet ||
		h.History != p.Position.LSN+1 || (p.Position.LSN > 0 && !model.ValidDigest(string(h.RootSet))) {
		return nil, storage.ErrCorruption
	}
	// The common header is an authority binding even for unsupported epochs.
	// Mismatched schema, magic or source is corruption, never a fallback gate.
	if h.Schema != recoveryCheckpointSchema {
		return nil, storage.ErrUnsupported
	}
	if h.Objects > o.MaxObjects || h.History > o.Journal.MaxPublishedTransactions+1 ||
		h.Events > h.Objects || h.Heads > 4*h.Objects || h.Hints > p.Position.LSN {
		return nil, storage.ErrLimit
	}
	if (p.Position.LSN == 0 && (h.Roots != (model.RootSet{}) || h.RootSet != "" || h.Objects != 0 || h.Events != 0 || h.Heads != 0 || h.Hints != 0)) ||
		(!h.EventProfile && (h.Events != 0 || h.Heads != 0)) ||
		(h.EventProfile && p.Position.LSN > 0 && (h.Events < p.Position.LSN || h.Heads == 0)) {
		return nil, storage.ErrCorruption
	}
	// Validate a known projection in its recorded profile before permitting
	// profile fallback. Malformed projections cannot hide behind option changes.
	recorded := o
	recorded.AgentProfile, recorded.EventProfile = h.AgentProfile, h.EventProfile
	e := newEngine(recorded)
	e.publication, e.roots, e.rootSet = p, h.Roots, h.RootSet
	e.index.source = source
	defer func() { e.index.source = nil }()
	var lastID model.ObjectID
	for i := uint64(0); i < h.Objects; i++ {
		var v checkpointObject
		if err := readCheckpointRecord(r, &v); err != nil {
			return nil, err
		}
		loc := v.Location
		if !model.ValidDigest(string(v.ID)) || v.ID <= lastID || loc.PayloadOffset < storage.SegmentHeaderSize ||
			loc.PayloadLength == 0 || loc.PayloadLength > codec.MaxEncodedObject ||
			uint64(loc.PayloadOffset) > p.PublishedEnd || uint64(loc.PayloadLength) > p.PublishedEnd-uint64(loc.PayloadOffset) {
			return nil, storage.ErrCorruption
		}
		e.index.entries[v.ID] = loc
		lastID = v.ID
	}
	if p.Position.LSN > 0 {
		obj, err := readObject(context.Background(), e.index, h.RootSet)
		if err != nil {
			return nil, err
		}
		roots, err := readRootSet(obj)
		if err != nil || roots != h.Roots {
			return nil, storage.ErrCorruption
		}
	}
	for i := uint64(0); i < h.History; i++ {
		var v checkpointEnvelope
		if err := readCheckpointRecord(r, &v); err != nil {
			return nil, err
		}
		if v.Fence.LSN != i || v.Fence.Store != p.Store || (i == 0) != (v.Tx == "") {
			return nil, storage.ErrCorruption
		}
		value := commitEnvelope{v.Fence, v.RootSet, v.Space, v.Principal}
		e.history[i] = value
		if i > 0 {
			if _, exists := e.envelopes[v.Tx]; exists || !e.index.contains(v.RootSet) {
				return nil, storage.ErrCorruption
			}
			e.envelopes[v.Tx] = value
		}
	}
	if tip := e.history[p.Position.LSN]; tip.fence != p.Fence() || tip.rootSet != h.RootSet {
		return nil, storage.ErrCorruption
	}
	var previousLSN, previousOrdinal uint64
	var coreLSN uint64
	for i := uint64(0); i < h.Events; i++ {
		var v checkpointEvent
		if err := readCheckpointRecord(r, &v); err != nil {
			return nil, err
		}
		if _, exists := e.events[v.ID]; exists || v.Entry.LSN == 0 || v.Entry.LSN > p.Position.LSN || v.Entry.LSN < previousLSN {
			return nil, storage.ErrCorruption
		}
		proof, ok := e.envelopes[v.Entry.Transaction]
		if !ok || proof.fence.LSN != v.Entry.LSN || proof.space != v.Entry.Space || proof.principal != v.Entry.Principal {
			return nil, storage.ErrCorruption
		}
		obj, err := readObject(context.Background(), e.index, model.ObjectID(v.ID))
		if err != nil {
			return nil, err
		}
		event, err := eventlog.Decode(obj)
		if err != nil || event.ID != v.ID || event.Transaction != v.Entry.Transaction ||
			event.Space != v.Entry.Space || event.RecordedBy != v.Entry.Principal {
			return nil, storage.ErrCorruption
		}
		if v.Entry.LSN != previousLSN {
			e.eventBlocks = &eventMetadataBlock{previous: e.eventBlocks}
			previousOrdinal = 0
		}
		if event.Attribution != nil {
			if coreLSN == v.Entry.LSN || v.Entry.LSN != coreLSN+1 ||
				v.Operation != event.Attribution.Operation || v.Ordinal != event.Attribution.Ordinal || v.Ordinal != previousOrdinal+1 {
				return nil, storage.ErrCorruption
			}
			e.indexTaskTraceEvent(event, v.Entry)
			previousOrdinal = v.Ordinal
		} else {
			if v.Operation != "" || v.Ordinal != 0 || v.Entry.LSN != coreLSN+1 {
				return nil, storage.ErrCorruption
			}
			coreLSN = v.Entry.LSN
		}
		e.events[v.ID] = v.Entry
		e.eventBlocks.entries = append(e.eventBlocks.entries, eventMetadataItem{v.ID, v.Entry, v.Operation, v.Ordinal})
		previousLSN = v.Entry.LSN
	}
	if h.EventProfile && coreLSN != p.Position.LSN {
		return nil, storage.ErrCorruption
	}
	var previousKey string
	for i := uint64(0); i < h.Heads; i++ {
		var v checkpointHead
		if err := readCheckpointRecord(r, &v); err != nil {
			return nil, err
		}
		key := string(v.Key)
		if key <= previousKey {
			return nil, storage.ErrCorruption
		}
		if _, exists := e.events[v.Head.ID]; !exists {
			return nil, storage.ErrCorruption
		}
		e.eventHeads[key] = v.Head
		previousKey = key
	}
	var previousHint [32]byte
	for i := uint64(0); i < h.Hints; i++ {
		var hint [32]byte
		if err := readCheckpointRecord(r, &hint); err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(previousHint[:], hint[:]) >= 0 {
			return nil, storage.ErrCorruption
		}
		e.receiptHints[hint] = struct{}{}
		previousHint = hint
	}
	var trailing [1]byte
	if n, err := r.Read(trailing[:]); n != 0 || err != io.EOF {
		return nil, storage.ErrCorruption
	}
	if err := e.validateCheckpointProjection(source); err != nil {
		return nil, err
	}
	if h.AgentProfile != o.AgentProfile || h.EventProfile != o.EventProfile {
		return nil, storage.ErrUnsupported
	}
	return e, nil
}
