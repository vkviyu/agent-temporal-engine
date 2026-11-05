package api

import "github.com/vkviyu/agent-temporal-engine/model"

// ReadViewStateRequest reads a domain key at the immutable anchor of View.
// View is an ephemeral capability; it cannot be used after closing/reconnecting.
type ReadViewStateRequest struct {
	View model.ViewRef
	Key  []byte
}

// StateEntriesLimits bounds authenticated radix traversal. The operation is
// atomic from the reader's perspective: exceeding any limit returns no partial
// page, so callers never mistake a truncated snapshot for a complete one.
type StateEntriesLimits struct {
	MaxNodes     int
	MaxEntries   int
	MaxBytes     int
	MaxReadBytes int
}

type ReadViewEntriesRequest struct {
	View   model.ViewRef
	Limits StateEntriesLimits
}

type StateEntry struct {
	Key   []byte
	Value model.ObjectID
}

type StateEntriesResult struct {
	Anchor    model.SnapshotAnchor
	StateRoot model.ObjectID
	RootSet   model.ObjectID
	Fence     model.CommitFence
	Items     []StateEntry
}
