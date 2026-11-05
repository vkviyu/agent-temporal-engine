package graph

import (
	"bytes"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

var ErrBranchPruned = errors.New("branch is pruned and cannot accept work")

// PruneRecord is a durable navigation tombstone. It retains the exact head and
// version rather than removing history or allowing an identity to be reused.
type PruneRecord struct {
	Branch  model.BranchID
	Head    model.RevisionID
	Version uint64
	Reason  string
}

const MaxPruneBytes = 2048

func EncodePrune(p PruneRecord) ([]byte, error) {
	if p.Version == 0 || !validText(p.Reason) {
		return nil, ErrInvalidRevision
	}
	branch, err := EncodeBranch(Branch{ID: p.Branch, Head: p.Head, Version: p.Version})
	if err != nil {
		return nil, err
	}
	b := codec.AppendUint([]byte("ATE-BRANCH-PRUNE\x00"), 1)
	b, _ = codec.AppendBytes(b, branch)
	b, _ = codec.AppendString(b, p.Reason)
	if len(b) > MaxPruneBytes {
		return nil, ErrLimit
	}
	return b, nil
}

func DecodePrune(raw []byte) (PruneRecord, error) {
	const prefix = "ATE-BRANCH-PRUNE\x00"
	if len(raw) > MaxPruneBytes {
		return PruneRecord{}, ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(prefix)) {
		return PruneRecord{}, ErrInvalidRevision
	}
	d := codec.NewDecoder(raw[len(prefix):])
	v, err := d.Uint()
	if err != nil {
		return PruneRecord{}, err
	}
	if v != 1 {
		return PruneRecord{}, ErrUnsupportedSchema
	}
	child, err := d.Bytes(MaxBranchBytes)
	if err != nil {
		return PruneRecord{}, err
	}
	b, err := DecodeBranch(child)
	if err != nil {
		return PruneRecord{}, err
	}
	reason, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return PruneRecord{}, err
	}
	if err = d.Done(); err != nil {
		return PruneRecord{}, err
	}
	p := PruneRecord{Branch: b.ID, Head: b.Head, Version: b.Version, Reason: reason}
	expected, err := EncodePrune(p)
	if err != nil {
		return PruneRecord{}, err
	}
	if !bytes.Equal(expected, raw) {
		return PruneRecord{}, ErrInvalidRevision
	}
	return p, nil
}
