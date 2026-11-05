package reference

// EventTrace is an independent publication-order ledger. A single Stream key
// has one sequence even when a transaction emits typed facts and a core record.
// Semantic causes are named facts; publication order alone never creates one.
type TraceStream struct {
	Space, Principal, Kind, Owner string
	Incarnation                   uint64
}
type TypedFact struct {
	Key, Operation, Profile, Kind, Subject, Execution, Actor, Query, Session, Invocation string
	Step, DecisionSequence                                                               uint64
	Stream                                                                               TraceStream
	Causes                                                                               []string
}
type ExpectedEvent struct {
	Key, Transaction, Method, Previous string
	Publication, Sequence, Ordinal     uint64
	Stream                             TraceStream
	Fact                               *TypedFact
	ProfileEvents                      []string
}
type EventTrace struct {
	Events    []ExpectedEvent
	Heads     map[TraceStream]string
	Sequences map[TraceStream]uint64
}

func NewEventTrace() *EventTrace {
	return &EventTrace{Heads: map[TraceStream]string{}, Sequences: map[TraceStream]uint64{}}
}
func (t *EventTrace) Publish(transaction, method, space, principal string, publication uint64, facts ...TypedFact) {
	principalStream := TraceStream{Space: space, Principal: principal, Kind: "principal", Owner: principal}
	keys := []string{}
	for i, fact := range facts {
		f := fact
		stream := f.Stream
		if stream.Kind == "" {
			stream = principalStream
		} else {
			stream.Space, stream.Principal = space, principal
		}
		f.Stream = stream
		t.Sequences[stream]++
		t.Events = append(t.Events, ExpectedEvent{Key: f.Key, Transaction: transaction, Method: method, Previous: t.Heads[stream], Publication: publication, Sequence: t.Sequences[stream], Ordinal: uint64(i + 1), Stream: stream, Fact: &f})
		t.Heads[stream] = f.Key
		keys = append(keys, f.Key)
	}
	coreKey := "core/" + transaction
	t.Sequences[principalStream]++
	t.Events = append(t.Events, ExpectedEvent{Key: coreKey, Transaction: transaction, Method: method, Previous: t.Heads[principalStream], Publication: publication, Sequence: t.Sequences[principalStream], Stream: principalStream, ProfileEvents: keys})
	t.Heads[principalStream] = coreKey
}
func (t *EventTrace) Visible(space, principal string, fence uint64) []ExpectedEvent {
	out := []ExpectedEvent{}
	for _, event := range t.Events {
		if event.Stream.Space == space && event.Stream.Principal == principal && event.Publication <= fence {
			out = append(out, event)
		}
	}
	return out
}
