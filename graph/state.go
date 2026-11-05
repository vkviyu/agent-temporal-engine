package graph

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/model"
)

var ErrStateUnverified = errors.New("revision state requires an authenticated state reader")
var ErrStateMismatch = errors.New("revision entries differ from its authenticated state root")

// StateReader resolves a complete immutable snapshot from its root. The adapter
// must verify the root's content identity, all traversed nodes, key paths, Space
// and referenced values. A cache or caller-supplied map is not such evidence.
// ReadEntries must report unavailable history or traversal limits as errors,
// rather than returning a silently truncated snapshot.
type StateReader interface {
	ReadEntries(model.SpaceID, model.ObjectID) ([]Entry, error)
}

// VerifyState checks content identity and compares Entries against the complete
// authenticated snapshot. New/PutRevision intentionally do not perform this IO;
// a durable semantic adapter must do so before accepting a new Revision and
// while replaying published Revisions, before any tail repair.
func VerifyState(r Revision, reader StateReader) error {
	canonical, err := BuildRevision(string(r.Space), r.StateParent, r.JoinParents, r.StateRoot, r.Events, r.Entries)
	if err != nil {
		return err
	}
	if canonical.ID != r.ID {
		return ErrInvalidRevision
	}
	if reader == nil {
		return ErrStateUnverified
	}
	actual, err := reader.ReadEntries(r.Space, r.StateRoot)
	if err != nil {
		return err
	}
	actual, err = normalizeEntries(actual)
	if err != nil {
		return err
	}
	if !equalEntries(actual, canonical.Entries) {
		return fmt.Errorf("%w: snapshot", ErrStateMismatch)
	}
	return nil
}
func equalEntries(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}
