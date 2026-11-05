package model

import "errors"

var (
	ErrInvalidFence   = errors.New("invalid commit fence")
	ErrAnchorMismatch = errors.New("anchor mismatch")
)

// CommitFence identifies a global store prefix, not a per-space position.
// A fence from the reference model is volatile and is not durable evidence.
type CommitFence struct {
	Store        StoreID
	LSN          uint64
	CommitDigest CommitDigest
}

func (f CommitFence) Validate() error {
	if f.Store == "" || !ValidDigest(string(f.CommitDigest)) {
		return ErrInvalidFence
	}
	return nil
}

type Cursor struct {
	Store    StoreID
	Space    SpaceID
	Revision RevisionID
}
type KnowledgeKey struct {
	Actor     ActorID
	Execution ExecutionID
}

// SnapshotAnchor separates a global publication fence from its space and
// optional selectors. Validate checks structure only; publication, visibility,
// and current authorization must also be checked by the store.
type SnapshotAnchor struct {
	Fence     CommitFence
	Space     SpaceID
	Cursor    *Cursor
	Knowledge *KnowledgeKey
}

func (a SnapshotAnchor) Validate() error {
	if err := a.Fence.Validate(); err != nil {
		return err
	}
	if a.Space == "" {
		return ErrAnchorMismatch
	}
	if c := a.Cursor; c != nil && (c.Store != a.Fence.Store || c.Space != a.Space || !ValidDigest(string(c.Revision))) {
		return ErrAnchorMismatch
	}
	if k := a.Knowledge; k != nil && (k.Actor == "" || k.Execution == "") {
		return ErrAnchorMismatch
	}
	return nil
}

func (a SnapshotAnchor) Clone() SnapshotAnchor {
	if a.Cursor != nil {
		c := *a.Cursor
		a.Cursor = &c
	}
	if a.Knowledge != nil {
		k := *a.Knowledge
		a.Knowledge = &k
	}
	return a
}
