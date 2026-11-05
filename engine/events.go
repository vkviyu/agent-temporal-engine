package engine

import (
	"context"
	"math"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type eventHead struct {
	ID       model.EventID
	Sequence uint64
}
type eventEntry struct {
	Space       model.SpaceID
	Principal   model.PrincipalID
	LSN         uint64
	Transaction model.TxID
}

func eventStreamKey(space model.SpaceID, owner model.PrincipalID) string {
	b, _ := codec.AppendString(nil, string(space))
	b, _ = codec.AppendString(b, string(owner))
	return string(b)
}
func (e *Engine) EventProfileEnabled() bool { return e.options.EventProfile }

func (e *Engine) prepareAppliedEvent(req api.CommitRequest, txID model.TxID, commandID, opsID model.ObjectID, profileEvents []model.Event) (model.Event, codec.Object, error) {
	key := eventStreamKey(req.Space, req.Principal)
	head, ok := e.eventHeads[key]
	if !ok {
		head = e.baseEventHeads[key]
	}
	// Profile facts and the core occurrence can belong to the same principal
	// stream. Facts are encoded first and the core manifest follows them in
	// that stream; using a second counter for the core would duplicate a
	// (Space, StreamKey, Sequence) occurrence identity.
	for _, fact := range profileEvents {
		if fact.Stream == (model.EventStreamKey{Kind: "principal", Owner: string(req.Principal)}) {
			head = eventHead{ID: fact.ID, Sequence: fact.Sequence}
		}
	}
	if head.Sequence == math.MaxUint64 {
		return model.Event{}, codec.Object{}, storage.ErrLimit
	}
	event := model.Event{Space: req.Space, Transaction: txID, Stream: model.EventStreamKey{Kind: "principal", Owner: string(req.Principal)}, Sequence: head.Sequence + 1, RecordedBy: req.Principal, Kind: eventlog.KindCommandApplied, SchemaVersion: 1, Method: req.Method, Payload: opsID, Command: commandID}
	if head.ID != "" {
		event.CausalParents = []model.EventID{head.ID}
	}
	if len(profileEvents) > 0 {
		event.SchemaVersion = 2
		event.ProfileRegistryVersion = eventlog.ProfileRegistryVersion
		event.PreviousInStream = head.ID
		event.CausalParents = nil
		for _, v := range profileEvents {
			event.ProfileEvents = append(event.ProfileEvents, v.ID)
		}
	}
	obj, err := eventlog.Object(event)
	if err != nil {
		return model.Event{}, codec.Object{}, err
	}
	id, err := codec.ObjectID(obj)
	if err != nil {
		return model.Event{}, codec.Object{}, err
	}
	event.ID = model.EventID(id)
	return event, obj, nil
}

// This index is rebuilt from the authenticated journal. An event body and its
// stream position enter the index only after the same P1 as the command.
func (e *Engine) installEvent(p *commitPlan) {
	if p == nil || p.event == nil {
		return
	}
	x := p.event
	if e.events == nil {
		e.events = make(map[model.EventID]eventEntry)
	}
	if e.eventHeads == nil {
		e.eventHeads = make(map[string]eventHead)
	}
	for _, event := range p.profileEvents {
		e.events[event.ID] = eventEntry{Space: event.Space, Principal: event.RecordedBy, LSN: p.transaction.LSN, Transaction: event.Transaction}
		e.eventHeads[profileStreamKey(event.Space, event.RecordedBy, event.Stream)] = eventHead{ID: event.ID, Sequence: event.Sequence}
		e.eventHeads[profileFactKey(event.Space, event.RecordedBy, event.Attribution.Operation, string(event.Attribution.Subject.ID))] = eventHead{ID: event.ID}
	}
	e.events[x.ID] = eventEntry{Space: x.Space, Principal: x.RecordedBy, LSN: p.transaction.LSN, Transaction: x.Transaction}
	e.eventHeads[eventStreamKey(x.Space, x.RecordedBy)] = eventHead{ID: x.ID, Sequence: x.Sequence}
}

func profileStreamKey(space model.SpaceID, principal model.PrincipalID, stream model.EventStreamKey) string {
	if stream.Kind == "principal" {
		// This is the very same stream used by core occurrences. Security
		// partitioning still includes the authenticated principal; the event
		// registry rejects a principal stream with a different owner.
		return eventStreamKey(space, principal)
	}
	b := []byte{0}
	for _, s := range []string{"profile-stream/v1", string(space), string(principal), stream.Kind, stream.Owner} {
		b, _ = codec.AppendString(b, s)
	}
	b = codec.AppendUint(b, stream.Incarnation)
	return string(b)
}
func profileFactKey(space model.SpaceID, principal model.PrincipalID, operation, subject string) string {
	b := []byte{0}
	for _, s := range []string{"profile-fact/v1", string(space), string(principal), operation, subject} {
		b, _ = codec.AppendString(b, s)
	}
	return string(b)
}

func (e *Engine) prepareProfileEvents(req api.CommitRequest, txID model.TxID, commandID, opsID model.ObjectID, drafts []profileEventDraft) ([]model.Event, []codec.Object, error) {
	if len(drafts) > eventlog.MaxProfileEvents {
		return nil, nil, storage.ErrLimit
	}
	heads := map[string]eventHead{}
	lookup := func(key string) eventHead {
		if h, ok := heads[key]; ok {
			return h
		}
		if h, ok := e.eventHeads[key]; ok {
			return h
		}
		return e.baseEventHeads[key]
	}
	var events []model.Event
	var objects []codec.Object
	for i, d := range drafts {
		a := d.attribution
		a.Ordinal = uint64(i + 1)
		if d.stream.Kind == "" {
			d.stream = model.EventStreamKey{Kind: "principal", Owner: string(req.Principal)}
		}
		key := profileStreamKey(req.Space, req.Principal, d.stream)
		head := lookup(key)
		if head.Sequence == math.MaxUint64 {
			return nil, nil, storage.ErrLimit
		}
		x := model.Event{Space: req.Space, Transaction: txID, Stream: d.stream, Sequence: head.Sequence + 1, RecordedBy: req.Principal, Kind: eventlog.KindAgentTransition, SchemaVersion: 1, Method: req.Method, Payload: opsID, Command: commandID, PreviousInStream: head.ID, Attribution: &a}
		seen := map[model.EventID]bool{}
		for _, cause := range d.causes {
			id := lookup(profileFactKey(req.Space, req.Principal, cause.operation, cause.subject)).ID
			if id != "" && !seen[id] {
				x.CausalParents = append(x.CausalParents, id)
				seen[id] = true
			}
		}
		sort.Slice(x.CausalParents, func(i, j int) bool { return x.CausalParents[i] < x.CausalParents[j] })
		o, err := eventlog.Object(x)
		if err != nil {
			return nil, nil, err
		}
		id, err := codec.ObjectID(o)
		if err != nil {
			return nil, nil, err
		}
		x.ID = model.EventID(id)
		heads[key] = eventHead{ID: x.ID, Sequence: x.Sequence}
		heads[profileFactKey(req.Space, req.Principal, a.Operation, string(a.Subject.ID))] = eventHead{ID: x.ID}
		events = append(events, x)
		objects = append(objects, o)
	}
	return events, objects, nil
}

func (e *Engine) eventKnown(id model.EventID, space model.SpaceID, principal model.PrincipalID) bool {
	v, ok := e.events[id]
	if !ok {
		v, ok = e.baseEvents[id]
	}
	return ok && v.Space == space && v.Principal == principal
}

func (e *Engine) visibleEvent(ctx context.Context, id model.EventID, anchor model.SnapshotAnchor, principal model.PrincipalID) (model.Event, error) {
	entry, ok := e.events[id]
	if !ok {
		entry, ok = e.baseEvents[id]
	}
	if !ok || entry.Space != anchor.Space || entry.Principal != principal || entry.LSN > anchor.Fence.LSN {
		return model.Event{}, ErrNodeNotFound
	}
	if _, _, err := e.rootsAtFence(ctx, anchor.Fence, true); err != nil {
		return model.Event{}, err
	}
	obj, err := readObject(ctx, e.index, model.ObjectID(id))
	if err != nil {
		return model.Event{}, err
	}
	event, err := eventlog.Decode(obj)
	if err != nil {
		return model.Event{}, ErrStoredObject
	}
	if event.ID != id || event.RecordedBy != principal || event.Transaction != entry.Transaction {
		return model.Event{}, ErrInvalidStateData
	}
	return event, nil
}

// selectedEventIDs is called by a pinned View reader. It copies metadata while
// holding the core lock; immutable object IO is performed after releasing it.
func (e *Engine) selectedEventIDs(ctx context.Context, entry *viewEntry) ([]model.EventID, error) {
	if !e.options.EventProfile {
		return nil, ErrUnsupportedMethod
	}
	if !entry.coreLocked {
		if err := e.mu.LockContext(ctx); err != nil {
			return nil, err
		}
		defer e.mu.Unlock()
	}
	ids := make([]model.EventID, 0)
	seen := map[model.EventID]bool{}
	// Isolated reducers overlay their new occurrences on the immutable base.
	// A published historical Source must see that base, never only the overlay.
	for _, events := range []map[model.EventID]eventEntry{e.events, e.baseEvents} {
		for id, x := range events {
			if seen[id] {
				continue
			}
			seen[id] = true
			if x.Space == entry.descriptor.Anchor.Space && x.Principal == entry.principal && x.LSN <= entry.descriptor.Anchor.Fence.LSN {
				if len(ids) >= maxNodeQueryRecords {
					return nil, storage.ErrLimit
				}
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}
