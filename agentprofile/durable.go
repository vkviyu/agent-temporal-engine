package agentprofile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/journalstore"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// DurableLedger is a trusted-host adapter for the profile's typed command log.
// It uses its own Journal; it is not the core Engine's five-root authority or a
// model-facing authorization API. Callers own DurableIO and close it only after
// Close succeeds. Old snapshot logs are deliberately not reinterpreted here.
//
// Every command is validated against a private candidate before P1. Publication
// installs that candidate and the immutable original result together. A failed
// or uncertain publication never exposes candidate state; reopening reconstructs
// state and result receipts from the authenticated command prefix.
type DurableLedger struct {
	mu      sync.Mutex
	ledger  *Ledger
	store   *journalstore.Store
	results map[model.RequestKey]durableResult
	closing bool
	closed  bool
	fenced  error
}

var ErrDurableScope = errors.New("agent profile request does not match task space and owner")

const durableSchema uint64 = 2
const durableMagic = "ATE-AGENTPROFILE-COMMAND\x00"
const durableRecordKind = storage.KindEvent

type durableOperation uint64

const (
	durableCreateTask durableOperation = iota + 1
	durableSpawn
	durableStartStep
	durableRecordInvocation
	durableCompleteStep
	durableCancelTask
	durableRebaseGoal
	durablePutPending
	durableRebindPending
)

// This is a closed union. Only fields selected by operation are encoded; the
// decoder checks the full canonical command before a semantic transition runs.
type durableCommand struct {
	operation   durableOperation
	task        Task
	execution   Execution
	step        Step
	invocation  Invocation
	action      PendingAction
	taskID      model.EntityID
	executionID model.ExecutionID
	sequence    uint64
	epoch       uint64
	goal        GoalRebase
	actionKey   string
	generation  uint64
}
type durableResult struct {
	operation  durableOperation
	task       Task
	execution  Execution
	step       Step
	invocation Invocation
	action     PendingAction
}

func (r durableResult) owned() durableResult {
	r.invocation.Args = bytes.Clone(r.invocation.Args)
	r.invocation.Result = bytes.Clone(r.invocation.Result)
	return r
}

func newDurableLedger() *DurableLedger {
	return &DurableLedger{ledger: New(), results: make(map[model.RequestKey]durableResult)}
}
func CreateDurable(d storage.DurableIO, o storage.JournalOptions) (*DurableLedger, error) {
	s := newDurableLedger()
	store, err := journalstore.CreatePrepared(d, o, s.prepare)
	if err != nil {
		return nil, err
	}
	s.store = store
	return s, nil
}
func OpenDurable(d storage.DurableIO, o storage.JournalOptions) (*DurableLedger, storage.RecoveryReport, error) {
	// This candidate is private throughout replay and discarded on any physical
	// or semantic failure. Prepare runs before storage may repair unpublished tail.
	s := newDurableLedger()
	store, report, err := journalstore.OpenPrepared(d, o, s.prepare)
	if err != nil {
		return nil, report, err
	}
	s.store = store
	return s, report, nil
}
func (s *DurableLedger) ready() error {
	if s.closed || s.store == nil {
		return journalstore.ErrClosed
	}
	if s.closing {
		return journalstore.ErrClosing
	}
	if s.fenced != nil {
		return fmt.Errorf("%w: %w", storage.ErrStoreFenced, s.fenced)
	}
	return s.store.Ready()
}

// prepare is called only by journalstore while the adapter owner is exclusive
// (or during private Open replay). No external IO or mutable caller input enters
// install, and no semantic operation is repeated after publication.
func (s *DurableLedger) prepare(record journalstore.Record) (func(), error) {
	if record.Kind != durableRecordKind {
		return nil, ErrInvalid
	}
	command, err := decodeDurableCommand(record.Payload)
	if err != nil {
		return nil, err
	}
	if err = record.Request.Validate(); err != nil {
		return nil, err
	}
	if _, exists := s.results[record.Request]; exists {
		return nil, ErrConflict
	}
	if err = s.checkScope(command, record.Request); err != nil {
		return nil, err
	}
	candidate := s.ledger.clone()
	result, err := applyDurableCommand(candidate, command, record.Request)
	if err != nil {
		return nil, err
	}
	result = result.owned()
	return func() { s.ledger = candidate; s.results[record.Request] = result }, nil
}
func (s *DurableLedger) checkScope(command durableCommand, key model.RequestKey) error {
	if command.operation == durableCreateTask {
		if key.Space != command.task.Space || key.Principal != command.task.Owner {
			return ErrDurableScope
		}
		return nil
	}
	var task Task
	var err error
	switch command.operation {
	case durableSpawn:
		task, err = s.ledger.GetTask(command.execution.Task)
	case durableStartStep:
		task, err = s.taskForExecution(command.step.Execution)
	case durableRecordInvocation:
		task, err = s.taskForExecution(command.invocation.Execution)
	case durableCompleteStep:
		task, err = s.taskForExecution(command.executionID)
	case durableCancelTask:
		task, err = s.ledger.GetTask(command.taskID)
	case durableRebaseGoal:
		task, err = s.ledger.GetTask(command.goal.Task)
	case durablePutPending:
		if command.action.Request != key {
			return ErrDurableScope
		}
		task, err = s.taskForExecution(command.action.Owner)
	case durableRebindPending:
		s.ledger.mu.RLock()
		action, found := s.ledger.actions[command.actionKey]
		s.ledger.mu.RUnlock()
		if !found {
			return ErrUnknown
		}
		task, err = s.taskForExecution(action.Owner)
	default:
		return storage.ErrUnsupported
	}
	if err != nil {
		return err
	}
	if key.Space != task.Space || key.Principal != task.Owner {
		return ErrDurableScope
	}
	return nil
}
func (s *DurableLedger) taskForExecution(id model.ExecutionID) (Task, error) {
	execution, err := s.ledger.GetExecution(id)
	if err != nil {
		return Task{}, err
	}
	return s.ledger.GetTask(execution.Task)
}
func applyDurableCommand(candidate *Ledger, command durableCommand, key model.RequestKey) (durableResult, error) {
	result := durableResult{operation: command.operation}
	var err error
	switch command.operation {
	case durableCreateTask:
		result.task, err = candidate.CreateTask(CreateTask{Task: command.task, Request: key})
	case durableSpawn:
		result.execution, err = candidate.Spawn(SpawnRequest{Execution: command.execution, Request: key})
	case durableStartStep:
		result.step, err = candidate.StartStep(StepRequest{Step: command.step, Request: key})
	case durableRecordInvocation:
		result.invocation, err = candidate.RecordInvocation(InvocationRequest{Invocation: command.invocation, Request: key})
	case durableCompleteStep:
		result.step, err = candidate.CompleteStep(command.executionID, command.sequence, command.epoch)
	case durableCancelTask:
		result.task, err = candidate.CancelTask(command.taskID)
	case durableRebaseGoal:
		result.task, err = candidate.RebaseGoal(command.goal)
	case durablePutPending:
		result.action, err = candidate.PutPending(command.action)
	case durableRebindPending:
		result.action, err = candidate.RebindPending(command.actionKey, command.generation, key)
	default:
		err = storage.ErrUnsupported
	}
	return result, err
}

// execute owns Durable.mu. The persistent key is resolved by journalstore before
// Prepare, so retries after goal changes/cancellation still return the original
// typed result rather than revalidating the command against today's state.
func (s *DurableLedger) execute(ctx context.Context, key model.RequestKey, command durableCommand) (durableResult, error) {
	if err := ctx.Err(); err != nil {
		return durableResult{}, err
	}
	if err := s.ready(); err != nil {
		return durableResult{}, err
	}
	raw, err := encodeDurableCommand(command)
	if err != nil {
		return durableResult{}, err
	}
	if _, err = s.store.Append(ctx, journalstore.Record{Kind: durableRecordKind, Payload: raw, Request: key}); err != nil {
		return durableResult{}, err
	}
	result, found := s.results[key]
	if !found || result.operation != command.operation {
		s.fenced = fmt.Errorf("%w: missing original profile result", journalstore.ErrProjectionFailed)
		return durableResult{}, errors.Join(storage.ErrCommitUnknown, fmt.Errorf("%w: %w", storage.ErrStoreFenced, s.fenced))
	}
	return result.owned(), nil
}
func (s *DurableLedger) syntheticKey(task Task, command durableCommand) (model.RequestKey, error) {
	raw, err := encodeDurableCommand(command)
	if err != nil {
		return model.RequestKey{}, err
	}
	sum := sha256.Sum256(raw)
	// The command includes operation and subject. It never includes mutable
	// current state, server randomness, deadlines, or a caller's retry timestamp.
	return model.RequestKey{Space: task.Space, Principal: task.Owner, RequestID: model.RequestID("agent/v1/" + hex.EncodeToString(sum[:]))}, nil
}
func (s *DurableLedger) CreateTask(ctx context.Context, r CreateTask) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.execute(ctx, r.Request, durableCommand{operation: durableCreateTask, task: r.Task})
	return result.task, err
}
func (s *DurableLedger) Spawn(ctx context.Context, r SpawnRequest) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.execute(ctx, r.Request, durableCommand{operation: durableSpawn, execution: r.Execution})
	return result.execution, err
}
func (s *DurableLedger) StartStep(ctx context.Context, r StepRequest) (Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.execute(ctx, r.Request, durableCommand{operation: durableStartStep, step: r.Step})
	return result.step, err
}
func (s *DurableLedger) RecordInvocation(ctx context.Context, r InvocationRequest) (Invocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.execute(ctx, r.Request, durableCommand{operation: durableRecordInvocation, invocation: r.Invocation})
	return result.invocation, err
}
func (s *DurableLedger) CompleteStep(ctx context.Context, id model.ExecutionID, sequence, epoch uint64) (Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Step{}, err
	}
	task, err := s.taskForExecution(id)
	if err != nil {
		return Step{}, err
	}
	command := durableCommand{operation: durableCompleteStep, executionID: id, sequence: sequence, epoch: epoch}
	key, err := s.syntheticKey(task, command)
	if err != nil {
		return Step{}, err
	}
	result, err := s.execute(ctx, key, command)
	return result.step, err
}
func (s *DurableLedger) CancelTask(ctx context.Context, id model.EntityID) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Task{}, err
	}
	task, err := s.ledger.GetTask(id)
	if err != nil {
		return Task{}, err
	}
	command := durableCommand{operation: durableCancelTask, taskID: id}
	key, err := s.syntheticKey(task, command)
	if err != nil {
		return Task{}, err
	}
	result, err := s.execute(ctx, key, command)
	return result.task, err
}
func (s *DurableLedger) RebaseGoal(ctx context.Context, r GoalRebase) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Task{}, err
	}
	task, err := s.ledger.GetTask(r.Task)
	if err != nil {
		return Task{}, err
	}
	command := durableCommand{operation: durableRebaseGoal, goal: r}
	key, err := s.syntheticKey(task, command)
	if err != nil {
		return Task{}, err
	}
	result, err := s.execute(ctx, key, command)
	return result.task, err
}
func (s *DurableLedger) PutPending(ctx context.Context, a PendingAction) (PendingAction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.execute(ctx, a.Request, durableCommand{operation: durablePutPending, action: a})
	return result.action, err
}
func (s *DurableLedger) RebindPending(ctx context.Context, key string, generation uint64, req model.RequestKey) (PendingAction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.execute(ctx, req, durableCommand{operation: durableRebindPending, actionKey: key, generation: generation})
	return result.action, err
}
func (s *DurableLedger) GetTask(id model.EntityID) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Task{}, err
	}
	return s.ledger.GetTask(id)
}
func (s *DurableLedger) GetExecution(id model.ExecutionID) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Execution{}, err
	}
	return s.ledger.GetExecution(id)
}
func (s *DurableLedger) GetInvocation(id string) (Invocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Invocation{}, err
	}
	return s.ledger.GetInvocation(id)
}
func (s *DurableLedger) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closing = true
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			return err
		}
	}
	s.closed = true
	return nil
}

// clone is used only to prepare an unexposed candidate. Mutexes are never copied
// and all mutable invocation buffers are owned by the resulting ledger.
func (s *Ledger) clone() *Ledger {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := New()
	for k, v := range s.tasks {
		out.tasks[k] = v
	}
	for k, v := range s.executions {
		out.executions[k] = v
	}
	for k, v := range s.steps {
		out.steps[k] = v
	}
	for k, v := range s.invocations {
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		out.invocations[k] = v
	}
	for k, v := range s.actions {
		out.actions[k] = v
	}
	for k, v := range s.slots {
		out.slots[k] = v
	}
	return out
}

func encodeDurableCommand(command durableCommand) ([]byte, error) {
	var body []byte
	var err error
	text := func(value string) {
		if err != nil {
			return
		}
		if len(value) > 1024 {
			err = ErrInvalid
			return
		}
		body, err = codec.AppendString(body, value)
	}
	switch command.operation {
	case durableCreateTask:
		body, err = EncodeTask(command.task)
	case durableSpawn:
		body, err = EncodeExecution(command.execution)
	case durableStartStep:
		body, err = EncodeStep(command.step)
	case durableRecordInvocation:
		body, err = EncodeInvocation(command.invocation)
	case durableCompleteStep:
		text(string(command.executionID))
		body = codec.AppendUint(body, command.sequence)
		body = codec.AppendUint(body, command.epoch)
	case durableCancelTask:
		text(string(command.taskID))
	case durableRebaseGoal:
		text(string(command.goal.Task))
		text(command.goal.NewGoal)
		body = codec.AppendUint(body, command.goal.ExpectedEpoch)
	case durablePutPending:
		body, err = EncodePendingAction(command.action)
	case durableRebindPending:
		text(command.actionKey)
		body = codec.AppendUint(body, command.generation)
	default:
		return nil, storage.ErrUnsupported
	}
	if err != nil {
		return nil, err
	}
	if len(body) > MaxRecordBytes-len(durableMagic)-32 {
		return nil, storage.ErrLimit
	}
	raw := append([]byte(nil), durableMagic...)
	raw = codec.AppendUint(raw, durableSchema)
	raw = codec.AppendUint(raw, uint64(command.operation))
	raw, err = codec.AppendBytes(raw, body)
	return raw, err
}
func decodeDurableCommand(raw []byte) (durableCommand, error) {
	if len(raw) > MaxRecordBytes {
		return durableCommand{}, storage.ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(durableMagic)) {
		return durableCommand{}, fmt.Errorf("%w: agent command log", storage.ErrUnsupported)
	}
	d := codec.NewDecoder(raw[len(durableMagic):])
	version, err := d.Uint()
	if err != nil {
		return durableCommand{}, err
	}
	if version != durableSchema {
		return durableCommand{}, storage.ErrUnsupported
	}
	operation, err := d.Uint()
	if err != nil {
		return durableCommand{}, err
	}
	body, err := d.Bytes(MaxRecordBytes)
	if err != nil {
		return durableCommand{}, err
	}
	if err = d.Done(); err != nil {
		return durableCommand{}, err
	}
	command := durableCommand{operation: durableOperation(operation)}
	fields := codec.NewDecoder(body)
	text := func() (string, error) { return fields.String(1024) }
	switch command.operation {
	case durableCreateTask:
		command.task, err = DecodeTask(body)
	case durableSpawn:
		command.execution, err = DecodeExecution(body)
	case durableStartStep:
		command.step, err = DecodeStep(body)
	case durableRecordInvocation:
		command.invocation, err = DecodeInvocation(body)
	case durableCompleteStep:
		var id string
		id, err = text()
		command.executionID = model.ExecutionID(id)
		if err == nil {
			command.sequence, err = fields.Uint()
		}
		if err == nil {
			command.epoch, err = fields.Uint()
		}
		if err == nil {
			err = fields.Done()
		}
	case durableCancelTask:
		var id string
		id, err = text()
		command.taskID = model.EntityID(id)
		if err == nil {
			err = fields.Done()
		}
	case durableRebaseGoal:
		var id string
		id, err = text()
		command.goal.Task = model.EntityID(id)
		if err == nil {
			command.goal.NewGoal, err = text()
		}
		if err == nil {
			command.goal.ExpectedEpoch, err = fields.Uint()
		}
		if err == nil {
			err = fields.Done()
		}
	case durablePutPending:
		command.action, err = DecodePendingAction(body)
	case durableRebindPending:
		command.actionKey, err = text()
		if err == nil {
			command.generation, err = fields.Uint()
		}
		if err == nil {
			err = fields.Done()
		}
	default:
		return durableCommand{}, storage.ErrUnsupported
	}
	if err != nil {
		return durableCommand{}, err
	}
	canonical, err := encodeDurableCommand(command)
	if err != nil {
		return durableCommand{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return durableCommand{}, ErrInvalid
	}
	return command, nil
}
