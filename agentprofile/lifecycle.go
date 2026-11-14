package agentprofile

import (
	"bytes"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// LifecycleSnapshot is the durable execution-side state that complements the
// original task/step profile. All records are immutable identities; updates
// replace the same logical record only when the version or idempotency fields
// match.
type LifecycleSnapshot struct {
	Queries       []Query
	Activities    []Activity
	Attempts      []Attempt
	Results       []ActivityResult
	Messages      []Message
	Consumptions  []MessageConsumption
	Continuations []Continuation
	Recovery      *RecoverySnapshot `json:"recovery,omitempty"`
}

type Query struct {
	ID, Session string
	Space       model.SpaceID
	Owner       model.PrincipalID
	Input       []byte
	InputFence  model.CommitFence
	Status      string
	Version     uint64
	Answer      []byte
}

type Activity struct {
	ID         string
	Execution  model.ExecutionID
	Kind       string
	Invocation string
	Step       uint64
	InputFence model.CommitFence
	Status     string
	Version    uint64
}

type Attempt struct {
	ID, Activity string
	Sequence     uint64
	Incarnation  uint64
	Status       string
}

type ActivityResult struct {
	ID, Activity, Attempt string
	RequestDigest         model.ObjectID
	Incarnation           uint64
	Outcome               string
	Payload               []byte
	Capture               string
	Response              string
	OutputChunks          []model.ObjectID
}

type Message struct {
	ID                string
	Sender, Recipient model.ExecutionID
	Payload           []byte
	GoalEpoch         uint64
}

type MessageConsumption struct {
	Message                string
	Recipient              model.ExecutionID
	Incarnation, GoalEpoch uint64
}

type Continuation struct {
	Execution            model.ExecutionID
	Version, Incarnation uint64
	PendingKey           string
	PendingGeneration    uint64
	InputFence           model.CommitFence
	Phase                string
	// Position is the mandatory V1 recovery location.
	Position *ContinuationPosition `json:"position,omitempty"`
}

// ContinuationPosition records the adapter's exact program location without
// asking a recovering model to infer it from conversation text. Step may be zero
// before the first step. NextActions is a sorted set of immutable generations.
type ContinuationPosition struct {
	Space           model.SpaceID
	Principal       model.PrincipalID
	Step            uint64
	KnowledgeFence  model.CommitFence
	KnowledgeCursor model.Cursor
	KnowledgeKey    model.KnowledgeKey
	AdapterVersion  string
	NextActions     []PendingActionRef
	// KnowledgeSource binds a seed created by the same Work transaction without
	// embedding that transaction's final CommitFence in its own payload.
	KnowledgeSource *model.SourcePublicationRef `json:"knowledge_source,omitempty"`
}

type PendingActionRef struct {
	Key        string
	Generation uint64
}

func cloneContinuation(v Continuation) Continuation {
	if v.Position != nil {
		position := *v.Position
		position.NextActions = append([]PendingActionRef(nil), position.NextActions...)
		if position.KnowledgeSource != nil {
			source := position.KnowledgeSource.Clone()
			position.KnowledgeSource = &source
		}
		v.Position = &position
	}
	return v
}

type LifecycleCommand struct {
	PutQuery        *Query
	PutActivity     *Activity
	PutAttempt      *Attempt
	PutResult       *ActivityResult
	PutMessage      *Message
	ConsumeMessage  *MessageConsumption
	PutContinuation *Continuation
}

func (s LifecycleSnapshot) empty() bool {
	return len(s.Queries)+len(s.Activities)+len(s.Attempts)+len(s.Results)+len(s.Messages)+len(s.Consumptions)+len(s.Continuations) == 0 && s.Recovery == nil
}

func cloneLifecycle(s LifecycleSnapshot) LifecycleSnapshot {
	o := LifecycleSnapshot{
		Queries: append([]Query(nil), s.Queries...), Activities: append([]Activity(nil), s.Activities...),
		Attempts: append([]Attempt(nil), s.Attempts...), Results: append([]ActivityResult(nil), s.Results...),
		Messages: append([]Message(nil), s.Messages...), Consumptions: append([]MessageConsumption(nil), s.Consumptions...),
		Continuations: append([]Continuation(nil), s.Continuations...),
		Recovery:      cloneRecovery(s.Recovery),
	}
	for i := range o.Queries {
		o.Queries[i].Input = bytes.Clone(o.Queries[i].Input)
		o.Queries[i].Answer = bytes.Clone(o.Queries[i].Answer)
	}
	for i := range o.Results {
		o.Results[i].Payload = bytes.Clone(o.Results[i].Payload)
		o.Results[i].OutputChunks = append([]model.ObjectID(nil), o.Results[i].OutputChunks...)
	}
	for i := range o.Messages {
		o.Messages[i].Payload = bytes.Clone(o.Messages[i].Payload)
	}
	for i := range o.Continuations {
		o.Continuations[i] = cloneContinuation(o.Continuations[i])
	}
	return o
}

func (l *Ledger) lifecycleSnapshot() LifecycleSnapshot { return cloneLifecycle(l.lifecycle) }

func lifecycleSort(s *LifecycleSnapshot) {
	sort.Slice(s.Queries, func(i, j int) bool { return s.Queries[i].ID < s.Queries[j].ID })
	sort.Slice(s.Activities, func(i, j int) bool { return s.Activities[i].ID < s.Activities[j].ID })
	sort.Slice(s.Attempts, func(i, j int) bool { return s.Attempts[i].ID < s.Attempts[j].ID })
	sort.Slice(s.Results, func(i, j int) bool { return s.Results[i].ID < s.Results[j].ID })
	sort.Slice(s.Messages, func(i, j int) bool { return s.Messages[i].ID < s.Messages[j].ID })
	sort.Slice(s.Consumptions, func(i, j int) bool {
		if s.Consumptions[i].Message != s.Consumptions[j].Message {
			return s.Consumptions[i].Message < s.Consumptions[j].Message
		}
		return s.Consumptions[i].Recipient < s.Consumptions[j].Recipient
	})
	sort.Slice(s.Continuations, func(i, j int) bool { return s.Continuations[i].Execution < s.Continuations[j].Execution })
	if s.Recovery != nil {
		sortRecovery(s.Recovery)
	}
}

func (l *Ledger) GetQuery(id string) (Query, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, v := range l.lifecycle.Queries {
		if v.ID == id {
			v.Input = bytes.Clone(v.Input)
			v.Answer = bytes.Clone(v.Answer)
			return v, nil
		}
	}
	return Query{}, ErrUnknown
}
func (l *Ledger) GetActivity(id string) (Activity, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, v := range l.lifecycle.Activities {
		if v.ID == id {
			return v, nil
		}
	}
	return Activity{}, ErrUnknown
}
func (l *Ledger) GetAttempt(id string) (Attempt, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, v := range l.lifecycle.Attempts {
		if v.ID == id {
			return v, nil
		}
	}
	return Attempt{}, ErrUnknown
}
func (l *Ledger) GetActivityResult(id string) (ActivityResult, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, v := range l.lifecycle.Results {
		if v.ID == id {
			v.Payload = bytes.Clone(v.Payload)
			v.OutputChunks = append([]model.ObjectID(nil), v.OutputChunks...)
			return v, nil
		}
	}
	return ActivityResult{}, ErrUnknown
}
func (l *Ledger) GetMessage(id string) (Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, v := range l.lifecycle.Messages {
		if v.ID == id {
			v.Payload = bytes.Clone(v.Payload)
			return v, nil
		}
	}
	return Message{}, ErrUnknown
}
func (l *Ledger) GetContinuation(id model.ExecutionID) (Continuation, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, v := range l.lifecycle.Continuations {
		if v.Execution == id {
			return cloneContinuation(v), nil
		}
	}
	return Continuation{}, ErrUnknown
}

func findLifecycle[T any](xs []T, f func(T) bool) (int, bool) {
	for i, v := range xs {
		if f(v) {
			return i, true
		}
	}
	return 0, false
}

func (l *Ledger) applyLifecycle(c LifecycleCommand) (Result, error) {
	s := &l.lifecycle
	if c.PutQuery != nil {
		v := *c.PutQuery
		if v.ID == "" || v.Space == "" || v.Owner == "" || v.Version == 0 {
			return Result{}, ErrInvalid
		}
		if i, ok := findLifecycle(s.Queries, func(x Query) bool { return x.ID == v.ID }); ok {
			if !reflect.DeepEqual(s.Queries[i], v) {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		v.Input = bytes.Clone(v.Input)
		v.Answer = bytes.Clone(v.Answer)
		s.Queries = append(s.Queries, v)
		return Result{}, nil
	}
	if c.PutActivity != nil {
		v := *c.PutActivity
		if v.ID == "" || v.Execution == "" || v.Version == 0 || v.Status == "" {
			return Result{}, ErrInvalid
		}
		if i, ok := findLifecycle(s.Activities, func(x Activity) bool { return x.ID == v.ID }); ok {
			if s.Activities[i] != v {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		s.Activities = append(s.Activities, v)
		return Result{}, nil
	}
	if c.PutAttempt != nil {
		v := *c.PutAttempt
		if v.ID == "" || v.Activity == "" || v.Sequence == 0 || v.Incarnation == 0 || v.Status == "" {
			return Result{}, ErrInvalid
		}
		if _, ok := findLifecycle(s.Activities, func(x Activity) bool { return x.ID == v.Activity }); !ok {
			return Result{}, ErrUnknown
		}
		if i, ok := findLifecycle(s.Attempts, func(x Attempt) bool { return x.ID == v.ID }); ok {
			if s.Attempts[i] != v {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		s.Attempts = append(s.Attempts, v)
		return Result{}, nil
	}
	if c.PutResult != nil {
		v := *c.PutResult
		if v.ID == "" || v.Activity == "" || v.Attempt == "" || v.Incarnation == 0 || v.Outcome == "" || !validResultCapture(v) {
			return Result{}, ErrInvalid
		}
		if _, ok := findLifecycle(s.Activities, func(x Activity) bool { return x.ID == v.Activity }); !ok {
			return Result{}, ErrUnknown
		}
		if _, ok := findLifecycle(s.Attempts, func(x Attempt) bool { return x.ID == v.Attempt }); !ok {
			return Result{}, ErrUnknown
		}
		if i, ok := findLifecycle(s.Results, func(x ActivityResult) bool { return x.ID == v.ID }); ok {
			if !reflect.DeepEqual(s.Results[i], v) {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		v.Payload = bytes.Clone(v.Payload)
		v.OutputChunks = append([]model.ObjectID(nil), v.OutputChunks...)
		s.Results = append(s.Results, v)
		return Result{}, nil
	}
	if c.PutMessage != nil {
		v := *c.PutMessage
		if v.ID == "" || v.Sender == "" || v.Recipient == "" || v.GoalEpoch == 0 {
			return Result{}, ErrInvalid
		}
		if i, ok := findLifecycle(s.Messages, func(x Message) bool { return x.ID == v.ID }); ok {
			if !reflect.DeepEqual(s.Messages[i], v) {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		v.Payload = bytes.Clone(v.Payload)
		s.Messages = append(s.Messages, v)
		return Result{}, nil
	}
	if c.ConsumeMessage != nil {
		v := *c.ConsumeMessage
		if v.Message == "" || v.Recipient == "" || v.Incarnation == 0 || v.GoalEpoch == 0 {
			return Result{}, ErrInvalid
		}
		if _, ok := findLifecycle(s.Messages, func(x Message) bool { return x.ID == v.Message }); !ok {
			return Result{}, ErrUnknown
		}
		if i, ok := findLifecycle(s.Consumptions, func(x MessageConsumption) bool { return x.Message == v.Message && x.Recipient == v.Recipient }); ok {
			if s.Consumptions[i] != v {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		s.Consumptions = append(s.Consumptions, v)
		return Result{}, nil
	}
	if c.PutContinuation != nil {
		v := cloneContinuation(*c.PutContinuation)
		if v.Execution == "" || v.Version == 0 || v.Incarnation == 0 || v.PendingKey == "" || v.Phase == "" {
			return Result{}, ErrInvalid
		}
		if i, ok := findLifecycle(s.Continuations, func(x Continuation) bool { return x.Execution == v.Execution }); ok {
			if !reflect.DeepEqual(s.Continuations[i], v) {
				return Result{}, ErrConflict
			}
			return Result{}, nil
		}
		s.Continuations = append(s.Continuations, v)
		return Result{}, nil
	}
	return Result{}, ErrCommandShape
}
