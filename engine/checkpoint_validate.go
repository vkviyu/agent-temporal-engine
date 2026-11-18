package engine

import (
	"context"
	"io"
	"maps"
	"slices"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// validateCheckpointProjection reconciles the supplied projection with the
// certified immutable transaction stream. It never executes a reducer or
// recertifies semantics. The ordinary Open scan still authenticates the entire
// physical layout, including headers/seals, before any repair or exposure.
func (e *Engine) validateCheckpointProjection(source io.ReaderAt) error {
	ctx := context.Background()
	p := e.publication
	genesis, err := storage.GenesisPosition(storage.Identity{Store: p.Store, Profile: p.Profile})
	if err != nil {
		return err
	}
	if e.history[0] != (commitEnvelope{fence: (storage.Publication{Store: p.Store, Position: genesis}).Fence()}) {
		return storage.ErrCorruption
	}
	scanner, err := storage.NewScanner(io.NewSectionReader(source, storage.SegmentHeaderSize,
		int64(p.PublishedEnd-storage.SegmentHeaderSize)), genesis, e.options.Journal.Limits)
	if err != nil {
		return err
	}
	heads := make(map[string]eventHead)
	hints := make(map[[32]byte]struct{})
	knownEvents := make(map[model.EventID]eventEntry)
	var objects, transactions uint64
	for {
		tx, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		transactions++
		expected := commitEnvelope{
			fence:   model.CommitFence{Store: p.Store, LSN: tx.LSN, CommitDigest: model.CommitDigest(tx.CommitDigest.String())},
			rootSet: model.ObjectID(tx.PostRootSet.String()), space: tx.Request.Space, principal: tx.Request.Principal,
		}
		if e.history[tx.LSN] != expected || e.envelopes[tx.TxID] != expected {
			return storage.ErrCorruption
		}
		var receipt receiptRecord
		var receiptFound bool
		var events []model.Event
		for i, record := range tx.Records {
			obj, err := codec.DecodeObject(record.Payload)
			if err != nil {
				return err
			}
			if !e.options.AgentProfile && (obj.Type == agentRecordType || obj.Type == agentRecordRootType) {
				return storage.ErrCorruption
			}
			id, err := codec.ObjectID(obj)
			if err != nil {
				return err
			}
			loc := tx.RecordLocations[i]
			loc.PayloadOffset += storage.SegmentHeaderSize
			if actual, ok := e.index.entries[id]; !ok || actual != loc {
				return storage.ErrCorruption
			}
			objects++
			if asDigest(id) == tx.Receipt {
				if receiptFound {
					return storage.ErrCorruption
				}
				receipt, err = readReceipt(obj)
				if err != nil {
					return err
				}
				receiptFound = true
			}
			if obj.Type == eventlog.Type || obj.Type == eventlog.ProfileType {
				event, err := eventlog.Decode(obj)
				if err != nil {
					return err
				}
				events = append(events, event)
			}
		}
		if !receiptFound || receipt.Key != tx.Request || receipt.TxID != tx.TxID ||
			receipt.LSN != tx.LSN || asDigest(receipt.RequestDigest) != tx.RequestDigest {
			return storage.ErrCorruption
		}
		id, found, err := state.Get(ctx, e.index, systemSpace, e.roots.CommandReceipts, receiptKey(tx.Request))
		if err != nil {
			return err
		}
		if !found || asDigest(id) != tx.Receipt {
			return storage.ErrCorruption
		}
		hints[keyHint(tx.Request)] = struct{}{}
		if !e.options.EventProfile {
			if len(events) != 0 {
				return storage.ErrCorruption
			}
			continue
		}
		if err := e.validateCheckpointEvents(ctx, tx, receipt, events, heads, knownEvents); err != nil {
			return err
		}
	}
	if transactions != p.Position.LSN || scanner.Position() != p.Position ||
		uint64(scanner.LastCompleteOffset())+storage.SegmentHeaderSize != p.PublishedEnd ||
		objects != uint64(len(e.index.entries)) || !maps.Equal(hints, e.receiptHints) ||
		!maps.Equal(knownEvents, e.events) || !maps.Equal(heads, e.eventHeads) {
		return storage.ErrCorruption
	}
	// Use the reconstructed map, after exact comparison including aliases.
	e.eventHeads = heads
	return nil
}

func (e *Engine) validateCheckpointEvents(ctx context.Context, tx storage.ScannedTransaction, receipt receiptRecord,
	events []model.Event, heads map[string]eventHead, known map[model.EventID]eventEntry) error {
	if len(events) == 0 {
		return storage.ErrCorruption
	}
	core := events[len(events)-1]
	profile := events[:len(events)-1]
	if core.Kind != eventlog.KindCommandApplied || core.Attribution != nil {
		return storage.ErrCorruption
	}
	ids := make([]model.EventID, len(profile))
	for i, event := range profile {
		if event.Kind != eventlog.KindAgentTransition || event.Attribution == nil || event.Attribution.Ordinal != uint64(i+1) {
			return storage.ErrCorruption
		}
		ids[i] = event.ID
	}
	if !slices.Equal(ids, core.ProfileEvents) {
		return storage.ErrCorruption
	}
	command, err := readObject(ctx, e.index, receipt.Command)
	if err != nil {
		return err
	}
	req, err := readCommand(command)
	if err != nil {
		return err
	}
	digest, err := CommandDigest(req)
	if err != nil || req.Key() != tx.Request || digest != receipt.RequestDigest {
		return storage.ErrCorruption
	}
	aliases := make(map[string]eventHead)
	for _, event := range events {
		entry := eventEntry{Space: tx.Request.Space, Principal: tx.Request.Principal, LSN: tx.LSN, Transaction: tx.TxID}
		if event.Space != entry.Space || event.RecordedBy != entry.Principal || event.Transaction != tx.TxID ||
			event.Command != receipt.Command || event.Payload != receipt.Operations || event.Method != req.Method ||
			e.events[event.ID] != entry {
			return storage.ErrCorruption
		}
		if _, exists := known[event.ID]; exists {
			return storage.ErrCorruption
		}
		key := profileStreamKey(event.Space, event.RecordedBy, event.Stream)
		head := heads[key]
		if head.Sequence == ^uint64(0) || event.Sequence != head.Sequence+1 {
			return storage.ErrCorruption
		}
		if event.Kind == eventlog.KindCommandApplied && event.SchemaVersion == 1 {
			var parents []model.EventID
			if head.ID != "" {
				parents = []model.EventID{head.ID}
			}
			if event.PreviousInStream != "" || !slices.Equal(parents, event.CausalParents) {
				return storage.ErrCorruption
			}
		} else if event.PreviousInStream != head.ID {
			return storage.ErrCorruption
		}
		for _, parent := range event.CausalParents {
			prior, ok := known[parent]
			if !ok || prior.Space != event.Space || prior.Principal != event.RecordedBy {
				return storage.ErrCorruption
			}
		}
		heads[key] = eventHead{ID: event.ID, Sequence: event.Sequence}
		if event.Attribution != nil {
			a := event.Attribution
			heads[profileFactKey(event.Space, event.RecordedBy, a.Operation, string(a.Subject.ID))] = eventHead{ID: event.ID}
			if err := e.checkpointAliases(ctx, req, tx.LSN, core.ProfileRegistryVersion, event, aliases); err != nil {
				return err
			}
		}
		known[event.ID] = entry
	}
	// Live installation applies all aliases after the profile facts.
	maps.Copy(heads, aliases)
	return nil
}

// These are the two alias producers in the certified registry: branch position
// facts and imported observations. Resolve from each transaction's historical
// post-root and command, never from the serialized alias map or today's tip.
func (e *Engine) checkpointAliases(ctx context.Context, req api.CommitRequest, lsn, registry uint64,
	event model.Event, aliases map[string]eventHead) error {
	a := event.Attribution
	branch := false
	switch a.Operation {
	case "Graph.ForkLocal", "Graph.WriteBranch", "Graph.JoinLocal", "Graph.PublishBranch":
		branch = true
	case "Graph.Prune":
		if registry < eventlog.ProfileRegistryVersion {
			return nil
		}
		branch = true
	case "Knowledge.Import":
		if registry < eventlog.ProfileRegistryVersion {
			return nil
		}
	default:
		return nil
	}
	obj, err := readObject(ctx, e.index, e.history[lsn].rootSet)
	if err != nil {
		return err
	}
	roots, err := readRootSet(obj)
	if err != nil {
		return err
	}
	if branch {
		b, _, err := graphBranchAt(ctx, e.index, roots, req.Space, model.BranchID(a.Subject.ID))
		if err != nil {
			return err
		}
		if b.ID != model.BranchID(a.Subject.ID) {
			return storage.ErrCorruption
		}
		if a.Operation == "Graph.Prune" && b.Scope == nil {
			return nil
		}
		fact := branchPositionFact(b.ID, b.Version)
		aliases[profileFactKey(req.Space, req.Principal, fact.operation, fact.subject)] = eventHead{ID: event.ID}
		return nil
	}
	body := req.Body
	if req.Method == api.MethodApplyWork {
		u, err := api.DecodeWorkUnit(body)
		if err != nil {
			return err
		}
		if a.Ordinal < 2 || a.Ordinal-2 >= uint64(len(u.Steps)) || u.Steps[a.Ordinal-2].Method != api.MethodApplyKnowledge {
			return storage.ErrCorruption
		}
		body = u.Steps[a.Ordinal-2].Body
	} else if req.Method != api.MethodApplyKnowledge {
		return storage.ErrCorruption
	}
	c, err := knowledge.DecodeCommand(body)
	if err != nil || c.Import == nil {
		return storage.ErrCorruption
	}
	var draft profileEventDraft
	if err := linkImportedObservationFacts(ctx, e.index, roots, req, *c.Import, &draft); err != nil {
		return err
	}
	for _, fact := range draft.factAliases {
		aliases[profileFactKey(req.Space, req.Principal, fact.operation, fact.subject)] = eventHead{ID: event.ID}
	}
	return nil
}
