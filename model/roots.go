package model

import "errors"

// RootSet separates domain state, knowledge, mutable branch refs, live control,
// and durable receipts. Empty fields denote the registered empty radix tree,
// not an object whose content ID is an empty string.
type RootSet struct {
	DomainStates       ObjectID
	ExecutionKnowledge ObjectID
	BranchRefs         ObjectID
	LiveControl        ObjectID
	CommandReceipts    ObjectID
}

var ErrInvalidRootSet = errors.New("invalid root set")

// Validate checks the structural identity contract for a published bundle.
// Empty fields denote the registered empty radix root; a non-empty field must
// be a canonical non-zero SHA-256 identity. Semantic ownership of each slot
// belongs to the enabled profile/registry and is checked by that layer.
func (r RootSet) Validate() error {
	for _, id := range []ObjectID{r.DomainStates, r.ExecutionKnowledge, r.BranchRefs, r.LiveControl, r.CommandReceipts} {
		if id != "" && (!ValidDigest(string(id)) || id == ObjectID("0000000000000000000000000000000000000000000000000000000000000000")) {
			return ErrInvalidRootSet
		}
	}
	return nil
}
