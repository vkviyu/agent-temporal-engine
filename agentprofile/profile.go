// Package agentprofile implements the host-independent agent/v1 state machine.
// It is intentionally pure: callers persist immutable records through ATE core.
package agentprofile

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/vkviyu/agent-temporal-engine/model"
	"sync"
)

const (
	ProfileID model.ProfileID = "agent"
	Version   uint32          = 1
)

type TaskStatus string

const (
	TaskOpen      TaskStatus = "open"
	TaskCompleted TaskStatus = "completed"
	TaskCanceled  TaskStatus = "canceled"
	TaskBlocked   TaskStatus = "blocked"
)

type ExecutionStatus string

const (
	ExecutionReady     ExecutionStatus = "ready"
	ExecutionRunning   ExecutionStatus = "running"
	ExecutionCompleted ExecutionStatus = "completed"
	ExecutionCanceled  ExecutionStatus = "canceled"
	ExecutionBlocked   ExecutionStatus = "blocked"
)

type StepStatus string

const (
	StepOpen      StepStatus = "open"
	StepAccepted  StepStatus = "accepted"
	StepCompleted StepStatus = "completed"
	StepBlocked   StepStatus = "blocked"
	StepCanceled  StepStatus = "canceled"
)

type InvocationStatus string

const (
	InvocationPending  InvocationStatus = "pending"
	InvocationAccepted InvocationStatus = "accepted"
	InvocationRejected InvocationStatus = "rejected"
	InvocationUnknown  InvocationStatus = "unknown"
	InvocationSettled  InvocationStatus = "settled"
)

type ActionStatus string

const (
	ActionPending ActionStatus = "pending"
	ActionApplied ActionStatus = "applied"
	ActionUnknown ActionStatus = "unknown"
	ActionFenced  ActionStatus = "fenced"
	ActionRebound ActionStatus = "rebound"
)

var (
	ErrInvalid       = errors.New("invalid agent profile record")
	ErrConflict      = errors.New("agent profile conflict")
	ErrUniqueSlot    = errors.New("agent profile unique action slot already bound")
	ErrGoalEpoch     = errors.New("agent goal epoch is stale")
	ErrCanceled      = errors.New("agent execution is canceled")
	ErrUnknown       = errors.New("unknown agent profile object")
	ErrActionUnknown = errors.New("pending action has unknown outcome")
)

type Task struct {
	ID          model.EntityID
	Space       model.SpaceID
	Owner       model.PrincipalID
	Goal        string
	GoalEpoch   uint64
	Status      TaskStatus
	CancelEpoch uint64
}
type Execution struct {
	ID                model.ExecutionID
	Task              model.EntityID
	Actor             model.ActorID
	Parent            model.ExecutionID
	Incarnation       uint64
	AcceptedGoalEpoch uint64
	Status            ExecutionStatus
	CancelEpoch       uint64
}
type Step struct {
	Execution         model.ExecutionID
	Sequence          uint64
	GoalEpoch         uint64
	Status            StepStatus
	InputAnchor       model.CommitFence
	PrimaryInvocation string
}
type Invocation struct {
	ID              string
	Execution       model.ExecutionID
	Step            uint64
	ProviderCallKey string
	Tool            string
	Args            []byte
	Status          InvocationStatus
	Result          []byte
	Attempt         uint64
	// Contract fields are mandatory in the current Invocation schema 2 for
	// model decisions and typed tool dispatch.
	AcceptedResult string
	ToolDefinition model.ObjectID
	ToolVersion    string
	DispatchClass  string
}
type PendingAction struct {
	Key           string
	Generation    uint64
	Request       model.RequestKey
	CommandDigest model.ObjectID
	InputFence    model.CommitFence
	Owner         model.ExecutionID
	Status        ActionStatus
	Supersedes    string
}
type GoalRebase struct {
	Task          model.EntityID
	NewGoal       string
	ExpectedEpoch uint64
}
type CreateTask struct {
	Task    Task
	Request model.RequestKey
}
type SpawnRequest struct {
	Execution Execution
	Request   model.RequestKey
}
type StepRequest struct {
	Step    Step
	Request model.RequestKey
}
type InvocationRequest struct {
	Invocation Invocation
	Request    model.RequestKey
}
type Ledger struct {
	mu          sync.RWMutex
	tasks       map[model.EntityID]Task
	executions  map[model.ExecutionID]Execution
	steps       map[string]Step
	invocations map[string]Invocation
	actions     map[string]PendingAction
	slots       map[string]string
	lifecycle   LifecycleSnapshot
}

func New() *Ledger {
	return &Ledger{tasks: map[model.EntityID]Task{}, executions: map[model.ExecutionID]Execution{}, steps: map[string]Step{}, invocations: map[string]Invocation{}, actions: map[string]PendingAction{}, slots: map[string]string{}}
}
func validText(s string) bool                      { return s != "" && len(s) <= 1024 }
func stepKey(e model.ExecutionID, n uint64) string { return fmt.Sprintf("%s/%020d", e, n) }
func (l *Ledger) GetTask(id model.EntityID) (Task, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.tasks[id]
	if !ok {
		return Task{}, ErrUnknown
	}
	return v, nil
}
func (l *Ledger) GetExecution(id model.ExecutionID) (Execution, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.executions[id]
	if !ok {
		return Execution{}, ErrUnknown
	}
	return v, nil
}
func (l *Ledger) GetInvocation(id string) (Invocation, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.invocations[id]
	if !ok {
		return Invocation{}, ErrUnknown
	}
	v.Args = bytes.Clone(v.Args)
	v.Result = bytes.Clone(v.Result)
	return v, nil
}

func (l *Ledger) GetStep(execution model.ExecutionID, sequence uint64) (Step, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.steps[stepKey(execution, sequence)]
	if !ok {
		return Step{}, ErrUnknown
	}
	return v, nil
}

func (l *Ledger) GetPending(key string) (PendingAction, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.actions[key]
	if !ok {
		return PendingAction{}, ErrUnknown
	}
	return v, nil
}

func (l *Ledger) CreateTask(r CreateTask) (Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := r.Task
	if !validText(string(t.ID)) || !validText(string(t.Space)) || !validText(string(t.Owner)) || t.Goal == "" || t.GoalEpoch != 1 || t.Status != TaskOpen || t.CancelEpoch != 0 {
		return Task{}, ErrInvalid
	}
	if _, ok := l.tasks[t.ID]; ok {
		return Task{}, ErrConflict
	}
	l.tasks[t.ID] = t
	return t, nil
}
func (l *Ledger) Spawn(r SpawnRequest) (Execution, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := r.Execution
	t, ok := l.tasks[e.Task]
	if !ok {
		return Execution{}, ErrUnknown
	}
	if t.Status != TaskOpen || e.AcceptedGoalEpoch != t.GoalEpoch {
		return Execution{}, ErrGoalEpoch
	}
	if e.Incarnation == 0 || !validText(string(e.ID)) || !validText(string(e.Actor)) || e.Status != ExecutionReady {
		return Execution{}, ErrInvalid
	}
	if e.Parent != "" {
		p, ok := l.executions[e.Parent]
		if !ok || p.Task != e.Task {
			return Execution{}, ErrConflict
		}
		if p.Status == ExecutionCanceled {
			return Execution{}, ErrCanceled
		}
	}
	if _, ok := l.executions[e.ID]; ok {
		return Execution{}, ErrConflict
	}
	slot := "spawn/" + string(e.Task) + "/" + string(e.ID)
	if old, ok := l.slots[slot]; ok && old != string(e.ID) {
		return Execution{}, ErrUniqueSlot
	}
	l.executions[e.ID] = e
	l.slots[slot] = string(e.ID)
	return e, nil
}
func (l *Ledger) StartStep(r StepRequest) (Step, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.executions[r.Step.Execution]
	if !ok {
		return Step{}, ErrUnknown
	}
	t, ok := l.tasks[e.Task]
	if !ok {
		return Step{}, ErrUnknown
	}
	if e.Status == ExecutionCanceled || e.CancelEpoch != t.CancelEpoch {
		return Step{}, ErrCanceled
	}
	if r.Step.GoalEpoch != t.GoalEpoch || r.Step.GoalEpoch != e.AcceptedGoalEpoch {
		return Step{}, ErrGoalEpoch
	}
	if r.Step.Sequence == 0 || r.Step.Status != StepOpen {
		return Step{}, ErrInvalid
	}
	key := stepKey(r.Step.Execution, r.Step.Sequence)
	if old, ok := l.steps[key]; ok {
		return old, nil
	}
	if r.Step.Sequence > 1 {
		prev, ok := l.steps[stepKey(r.Step.Execution, r.Step.Sequence-1)]
		if !ok || prev.Status != StepCompleted {
			return Step{}, ErrConflict
		}
	}
	e.Status = ExecutionRunning
	l.executions[e.ID] = e
	l.steps[key] = r.Step
	return r.Step, nil
}
func (l *Ledger) RecordInvocation(r InvocationRequest) (Invocation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := r.Invocation
	e, ok := l.executions[i.Execution]
	if !ok {
		return Invocation{}, ErrUnknown
	}
	t, ok := l.tasks[e.Task]
	if !ok {
		return Invocation{}, ErrUnknown
	}
	if e.Status == ExecutionCanceled || e.CancelEpoch != t.CancelEpoch {
		return Invocation{}, ErrCanceled
	}
	if i.Step == 0 || i.ProviderCallKey == "" || i.Tool == "" || i.Attempt == 0 {
		return Invocation{}, ErrInvalid
	}
	sk := stepKey(i.Execution, i.Step)
	step, ok := l.steps[sk]
	if !ok {
		return Invocation{}, ErrUnknown
	}
	if step.GoalEpoch != t.GoalEpoch {
		return Invocation{}, ErrGoalEpoch
	}
	if err := validateInvocationSource(i, l.lifecycle); err != nil {
		return Invocation{}, err
	}
	slot := "invocation/" + string(i.Execution) + fmt.Sprintf("/%d/", i.Step) + i.ProviderCallKey
	if old, ok := l.slots[slot]; ok {
		if old == i.ID {
			stored := l.invocations[i.ID]
			if (stored.AcceptedResult != "" || i.AcceptedResult != "") && !sameInvocationContract(stored, i) {
				return Invocation{}, ErrConflict
			}
			return l.invocations[i.ID], nil
		}
		return Invocation{}, ErrUniqueSlot
	}
	if old, ok := l.invocations[i.ID]; ok {
		if old.Execution == i.Execution && old.Step == i.Step && old.ProviderCallKey == i.ProviderCallKey {
			return old, nil
		}
		return Invocation{}, ErrConflict
	}
	l.invocations[i.ID] = i
	l.slots[slot] = i.ID
	return i, nil
}

// SettleInvocation records the durable outcome of an invocation after the
// executor has crossed the accepted/rejected boundary. Pending and unknown
// outcomes are intentionally fenced: callers must reconcile them before a
// step can be completed.
func (l *Ledger) SettleInvocation(id string, result []byte) (Invocation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, ok := l.invocations[id]
	if !ok {
		return Invocation{}, ErrUnknown
	}
	if i.Status == InvocationPending || i.Status == InvocationUnknown {
		return Invocation{}, ErrConflict
	}
	if i.Status == InvocationSettled {
		if bytes.Equal(i.Result, result) {
			i.Args = bytes.Clone(i.Args)
			i.Result = bytes.Clone(i.Result)
			return i, nil
		}
		return Invocation{}, ErrConflict
	}
	i.Status = InvocationSettled
	i.Result = bytes.Clone(result)
	l.invocations[id] = i
	i.Args = bytes.Clone(i.Args)
	i.Result = bytes.Clone(i.Result)
	return i, nil
}
func (l *Ledger) CompleteStep(eid model.ExecutionID, seq, epoch uint64) (Step, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.executions[eid]
	if !ok {
		return Step{}, ErrUnknown
	}
	t, ok := l.tasks[e.Task]
	if !ok {
		return Step{}, ErrUnknown
	}
	if epoch != t.GoalEpoch {
		return Step{}, ErrGoalEpoch
	}
	key := stepKey(eid, seq)
	s, ok := l.steps[key]
	if !ok {
		return Step{}, ErrUnknown
	}
	for _, i := range l.invocations {
		if i.Execution == eid && i.Step == seq && (i.Status == InvocationPending || i.Status == InvocationUnknown) {
			return Step{}, ErrConflict
		}
	}
	s.Status = StepCompleted
	l.steps[key] = s
	return s, nil
}

func (l *Ledger) CancelTask(id model.EntityID) (Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.tasks[id]
	if !ok {
		return Task{}, ErrUnknown
	}
	if t.Status == TaskCanceled {
		return t, nil
	}
	t.CancelEpoch++
	t.Status = TaskCanceled
	l.tasks[id] = t
	for id, e := range l.executions {
		if e.Task == t.ID && e.Status != ExecutionCompleted {
			e.Status = ExecutionCanceled
			e.CancelEpoch = t.CancelEpoch
			l.executions[id] = e
		}
	}
	// Child executions may own different Tasks. The parent relationship is the
	// cancellation boundary, so fence their outbox and descendants as well.
	for _, e := range l.executions {
		if e.Task == t.ID {
			l.cancelExecutionTree(e.ID)
		}
	}
	for key, a := range l.actions {
		if a.Owner != "" {
			if e, ok := l.executions[a.Owner]; ok && e.Task == t.ID && a.Status == ActionPending {
				a.Status = ActionFenced
				l.actions[key] = a
			}
		}
	}
	return t, nil
}
func (l *Ledger) RebaseGoal(r GoalRebase) (Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.tasks[r.Task]
	if !ok {
		return Task{}, ErrUnknown
	}
	if t.Status != TaskOpen || r.ExpectedEpoch != t.GoalEpoch {
		return Task{}, ErrGoalEpoch
	}
	if r.NewGoal == "" {
		return Task{}, ErrInvalid
	}
	t.Goal = r.NewGoal
	t.GoalEpoch++
	l.tasks[t.ID] = t
	return t, nil
}
func (l *Ledger) PutPending(a PendingAction) (PendingAction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if a.Key == "" || a.Generation == 0 || a.Request.RequestID == "" || !model.ValidDigest(string(a.CommandDigest)) || a.Owner == "" {
		return PendingAction{}, ErrInvalid
	}
	if old, ok := l.actions[a.Key]; ok {
		if old.Generation == a.Generation && old.CommandDigest == a.CommandDigest {
			return old, nil
		}
		if old.Status == ActionUnknown || old.Status == ActionPending {
			return PendingAction{}, ErrActionUnknown
		}
		if a.Generation <= old.Generation {
			return PendingAction{}, ErrConflict
		}
		a.Supersedes = a.Key
	}
	l.actions[a.Key] = a
	return a, nil
}
func (l *Ledger) RebindPending(key string, gen uint64, req model.RequestKey) (PendingAction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.actions[key]
	if !ok {
		return PendingAction{}, ErrUnknown
	}
	if a.Status == ActionUnknown {
		return PendingAction{}, ErrActionUnknown
	}
	if a.Status != ActionPending && a.Status != ActionFenced {
		return PendingAction{}, ErrConflict
	}
	if gen != a.Generation {
		return PendingAction{}, ErrConflict
	}
	a.Generation++
	a.Request = req
	a.Status = ActionRebound
	a.Supersedes = key
	l.actions[key] = a
	return a, nil
}
