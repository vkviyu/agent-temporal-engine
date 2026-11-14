package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// Immutable per-publication blocks are enumerated outside global locks.
type eventMetadataBlock struct {
	previous *eventMetadataBlock
	entries  []eventMetadataItem
}

type eventMetadataItem struct {
	id        model.EventID
	entry     eventEntry
	operation string
	ordinal   uint64
}

func (e *Engine) readEventEntry(ctx context.Context, id model.EventID) (eventEntry, bool, error) {
	cut := e.readCut(ctx)
	if cut == nil {
		entry, found := e.events[id]
		if !found {
			entry, found = e.baseEvents[id]
		}
		return entry, found, nil
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return eventEntry{}, false, err
	}
	entry, found := e.events[id]
	e.mu.Unlock()
	return entry, found && entry.LSN <= cut.publication.Position.LSN, nil
}
