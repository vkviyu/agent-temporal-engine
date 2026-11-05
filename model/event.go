package model

// EventStreamKey identifies a registered occurrence stream. The core command
// stream belongs to the authenticated principal; it never invents an Actor or
// Execution for a system/query operation. Execution streams may add incarnation
// when a profile owns that schema.
type EventStreamKey struct {
	Kind        string
	Owner       string
	Incarnation uint64
}

// Event is a fact about an accepted command, separate from its mutable entity
// projection. ID is the immutable event object's identity, not an entity ID.
// Fence is supplied by publication metadata, never embedded in these bytes.
type Event struct {
	ID            EventID
	Space         SpaceID
	Transaction   TxID
	Stream        EventStreamKey
	Sequence      uint64
	RecordedBy    PrincipalID
	Kind          string
	SchemaVersion uint64
	Method        string
	Payload       ObjectID
	Command       ObjectID
	CausalParents []EventID
	// PreviousInStream is ordering evidence, not proof of semantic consumption.
	// Legacy core schema 1 encoded that predecessor in CausalParents instead.
	PreviousInStream EventID
	// ProfileRegistryVersion/ProfileEvents are present only on core schema 2.
	// They freeze deterministic profile-event generation for historical replay.
	ProfileRegistryVersion uint64
	ProfileEvents          []EventID
	Attribution            *EventAttribution
}

// EventAttribution distinguishes a transition's logical subject from the
// authenticated recorder and from the actor that initiated it. Logical entity
// identities are not object references or authorization capabilities.
type EventAttribution struct {
	Subject          EntityKey
	InitiatingActor  ActorID
	Execution        ExecutionID
	Query            string
	Session          string
	Step             uint64
	DecisionSequence uint64
	Invocation       string
	Operation        string
	Ordinal          uint64
}
