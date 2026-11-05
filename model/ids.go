// Package model defines host-independent identities and anchors. Its v0 API is
// experimental; public importability does not imply a frozen storage format.
package model

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type (
	StoreID       string
	SpaceID       string
	PrincipalID   string
	ProfileID     string
	EntityID      string
	EntityKind    string
	NodeKind      string
	ObjectID      string
	EventID       string
	RevisionID    string
	BranchID      string
	ActorID       string
	ExecutionID   string
	ActivityID    string
	AttemptID     string
	RequestID     string
	TxID          string
	CorrelationID string
	CommitDigest  string
	ViewRef       string
)

const (
	NodeEntity   NodeKind = "entity"
	NodeEvent    NodeKind = "event"
	NodeRevision NodeKind = "revision"
	NodeObject   NodeKind = "object"
)

var ErrInvalidNodeRef = errors.New("invalid node reference")

// EntityKey identifies a logical entity. Profile schema versions belong to the
// immutable records, not to this identity (for example Profile="agent").
type EntityKey struct {
	Profile ProfileID
	Kind    EntityKind
	ID      EntityID
}

func (k EntityKey) Valid() bool { return k.Profile != "" && k.Kind != "" && k.ID != "" }

// NodeRef is a tagged union. Exactly the field selected by Kind must be set.
type NodeRef struct {
	Kind     NodeKind
	Entity   EntityKey
	Event    EventID
	Revision RevisionID
	Object   ObjectID
}

func (r NodeRef) Validate() error {
	n := 0
	for _, set := range []bool{r.Entity != (EntityKey{}), r.Event != "", r.Revision != "", r.Object != ""} {
		if set {
			n++
		}
	}
	if n != 1 {
		return ErrInvalidNodeRef
	}
	valid := false
	switch r.Kind {
	case NodeEntity:
		valid = r.Entity.Valid()
	case NodeEvent:
		valid = ValidDigest(string(r.Event))
	case NodeRevision:
		valid = ValidDigest(string(r.Revision))
	case NodeObject:
		valid = ValidDigest(string(r.Object))
	}
	if !valid {
		return ErrInvalidNodeRef
	}
	return nil
}

func (r NodeRef) Valid() bool { return r.Validate() == nil }

// String is diagnostic text, not a canonical identity encoding or wire format.
func (r NodeRef) String() string {
	if !r.Valid() {
		return ""
	}
	switch r.Kind {
	case NodeEntity:
		return fmt.Sprintf("entity:%q/%q/%q", r.Entity.Profile, r.Entity.Kind, r.Entity.ID)
	case NodeEvent:
		return fmt.Sprintf("event:%q", r.Event)
	case NodeRevision:
		return fmt.Sprintf("revision:%q", r.Revision)
	default:
		return "object:" + string(r.Object)
	}
}

// ValidDigest accepts the canonical lowercase hexadecimal SHA-256 spelling.
func ValidDigest(s string) bool {
	if len(s) != 64 || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
