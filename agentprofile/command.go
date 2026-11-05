package agentprofile

import (
	"bytes"
	"errors"
	"math"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const CommandSchema uint64 = 1
const MaxCommandBytes = MaxRecordBytes
const commandDomain = "ATE-AGENT-COMMAND\x00"

var (
	ErrCommandShape   = errors.New("agent command must select one valid operation")
	ErrCommandScope   = errors.New("agent command principal or space does not own the task")
	ErrCommandVersion = errors.New("unsupported agent command schema or operation")
)

// Command is a closed agent/v1 transition union. Request identity belongs to
// the authenticated outer envelope. Exactly one operation must be present;
// callers cannot submit a replacement snapshot or arbitrary control writes.
type Command struct {
	CreateTask       *Task
	Spawn            *Execution
	StartStep        *Step
	RecordInvocation *Invocation
	CompleteStep     *CompleteStepCommand
	CancelTask       *CancelTaskCommand
	RebaseGoal       *GoalRebase
	PutPending       *PendingAction
	RebindPending    *RebindPendingCommand
	SettleInvocation *SettleInvocationCommand
	Lifecycle        *LifecycleCommand
	Control          *ControlCommand
}

type CompleteStepCommand struct {
	Execution model.ExecutionID
	Sequence  uint64
	Epoch     uint64
}
type CancelTaskCommand struct{ Task model.EntityID }
type RebindPendingCommand struct {
	Key        string
	Generation uint64
}
type SettleInvocationCommand struct {
	ID     string
	Result []byte
}

// Result owns exactly one typed result of a successful ApplyCommand. It is
// independent of a publisher's receipt; durable callers retain their original
// receipt/result and reconcile it before trying to apply a command again.
type Result struct {
	Task       *Task
	Execution  *Execution
	Step       *Step
	Invocation *Invocation
	Pending    *PendingAction
}

func commandText(s string) bool { return validText(s) && utf8.ValidString(s) }

func (c Command) operation() (uint64, error) {
	op := uint64(0)
	for i, selected := range []bool{c.CreateTask != nil, c.Spawn != nil, c.StartStep != nil, c.RecordInvocation != nil, c.CompleteStep != nil, c.CancelTask != nil, c.RebaseGoal != nil, c.PutPending != nil, c.RebindPending != nil, false, c.SettleInvocation != nil, c.Lifecycle != nil, c.Control != nil} {
		if selected {
			if op != 0 {
				return 0, ErrCommandShape
			}
			op = uint64(i + 1)
		}
	}
	if op == 0 {
		return 0, ErrCommandShape
	}
	return op, nil
}

// Validate checks only the closed command's structure. State-dependent scope,
// epochs, cancellation barriers and unique slots are checked by ApplyCommand.
func (c Command) Validate() error {
	op, err := c.operation()
	if err != nil {
		return err
	}
	switch op {
	case 1:
		t := c.CreateTask
		if !commandText(string(t.ID)) || !commandText(string(t.Space)) || !commandText(string(t.Owner)) || !commandText(t.Goal) || t.Status != TaskOpen || t.GoalEpoch != 1 || t.CancelEpoch != 0 {
			return ErrCommandShape
		}
	case 2:
		x := c.Spawn
		if !commandText(string(x.ID)) || !commandText(string(x.Task)) || !commandText(string(x.Actor)) || (x.Parent != "" && !commandText(string(x.Parent))) || x.Incarnation == 0 || x.AcceptedGoalEpoch == 0 || x.Status != ExecutionReady || x.CancelEpoch != 0 {
			return ErrCommandShape
		}
	case 3:
		s := c.StartStep
		if !commandText(string(s.Execution)) || s.Sequence == 0 || s.GoalEpoch == 0 || s.Status != StepOpen || (s.PrimaryInvocation != "" && !commandText(s.PrimaryInvocation)) {
			return ErrCommandShape
		}
	case 4:
		i := c.RecordInvocation
		if !commandText(i.ID) || !commandText(string(i.Execution)) || !commandText(i.ProviderCallKey) || !commandText(i.Tool) || i.Step == 0 || i.Attempt == 0 || !validInvocationStatus(i.Status) || len(i.Args) > MaxCommandBytes || len(i.Result) > MaxCommandBytes {
			return ErrCommandShape
		}
	case 5:
		if !commandText(string(c.CompleteStep.Execution)) || c.CompleteStep.Sequence == 0 || c.CompleteStep.Epoch == 0 {
			return ErrCommandShape
		}
	case 6:
		if !commandText(string(c.CancelTask.Task)) {
			return ErrCommandShape
		}
	case 7:
		if !commandText(string(c.RebaseGoal.Task)) || !commandText(c.RebaseGoal.NewGoal) || c.RebaseGoal.ExpectedEpoch == 0 || c.RebaseGoal.ExpectedEpoch == math.MaxUint64 {
			return ErrCommandShape
		}
	case 8:
		a := c.PutPending
		if !commandText(a.Key) || !commandText(string(a.Owner)) || a.Generation == 0 || !model.ValidDigest(string(a.CommandDigest)) || a.Request.Validate() != nil || !validActionStatus(a.Status) || (a.Supersedes != "" && !commandText(a.Supersedes)) {
			return ErrCommandShape
		}
	case 9:
		if !commandText(c.RebindPending.Key) || c.RebindPending.Generation == 0 || c.RebindPending.Generation == math.MaxUint64 {
			return ErrCommandShape
		}
	case 11:
		if !commandText(c.SettleInvocation.ID) || len(c.SettleInvocation.Result) > MaxCommandBytes {
			return ErrCommandShape
		}
	case 12:
		if err := validateLifecycleCommand(*c.Lifecycle); err != nil {
			return err
		}
	case 13:
		return validateControlShape(*c.Control)
	}
	return nil
}

// EncodeCommand binds an explicit operation/schema to canonical child records.
// No wire JSON, RequestKey, mutable snapshot or map order participates here.
func EncodeCommand(c Command) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	op, _ := c.operation()
	var body []byte
	var err error
	text := func(s string) {
		if err == nil {
			body, err = codec.AppendString(body, s)
		}
	}
	switch op {
	case 1:
		body, err = EncodeTask(*c.CreateTask)
	case 2:
		body, err = EncodeExecution(*c.Spawn)
	case 3:
		body, err = EncodeStep(*c.StartStep)
	case 4:
		body, err = EncodeInvocation(*c.RecordInvocation)
	case 5:
		text(string(c.CompleteStep.Execution))
		body = codec.AppendUint(body, c.CompleteStep.Sequence)
		body = codec.AppendUint(body, c.CompleteStep.Epoch)
	case 6:
		text(string(c.CancelTask.Task))
	case 7:
		text(string(c.RebaseGoal.Task))
		text(c.RebaseGoal.NewGoal)
		body = codec.AppendUint(body, c.RebaseGoal.ExpectedEpoch)
	case 8:
		body, err = EncodePendingAction(*c.PutPending)
	case 9:
		text(c.RebindPending.Key)
		body = codec.AppendUint(body, c.RebindPending.Generation)
	case 11:
		text(c.SettleInvocation.ID)
		body, err = codec.AppendBytes(body, c.SettleInvocation.Result)
	case 12:
		body, err = encodeLifecycleCommand(*c.Lifecycle)
	case 13:
		body, err = encodeControlCommand(*c.Control)
	}
	if err != nil {
		return nil, err
	}
	if len(body) > MaxCommandBytes-len(commandDomain)-32 {
		return nil, ErrCommandShape
	}
	raw := append([]byte(nil), commandDomain...)
	raw = codec.AppendUint(raw, CommandSchema)
	raw = codec.AppendUint(raw, op)
	return codec.AppendBytes(raw, body)
}

// DecodeCommand returns owned bytes for the current V1 request contract.
func DecodeCommand(raw []byte) (Command, error) {
	return decodeCommand(raw)
}

func decodeCommand(raw []byte) (Command, error) {
	if len(raw) > MaxCommandBytes || !bytes.HasPrefix(raw, []byte(commandDomain)) {
		return Command{}, ErrCommandShape
	}
	d := codec.NewDecoder(raw[len(commandDomain):])
	version, err := d.Uint()
	if err != nil {
		return Command{}, err
	}
	if version != CommandSchema {
		return Command{}, ErrCommandVersion
	}
	op, err := d.Uint()
	if err != nil {
		return Command{}, err
	}
	body, err := d.Bytes(MaxCommandBytes)
	if err != nil {
		return Command{}, err
	}
	if err = d.Done(); err != nil {
		return Command{}, err
	}
	f := codec.NewDecoder(body)
	c := Command{}
	switch op {
	case 1:
		v, e := DecodeTask(body)
		c.CreateTask, err = &v, e
	case 2:
		v, e := DecodeExecution(body)
		c.Spawn, err = &v, e
	case 3:
		v, e := DecodeStep(body)
		c.StartStep, err = &v, e
	case 4:
		v, e := decodeInvocation(body)
		c.RecordInvocation, err = &v, e
	case 5:
		var id string
		id, err = f.String(1024)
		v := CompleteStepCommand{Execution: model.ExecutionID(id)}
		if err == nil {
			v.Sequence, err = f.Uint()
		}
		if err == nil {
			v.Epoch, err = f.Uint()
		}
		if err == nil {
			err = f.Done()
		}
		c.CompleteStep = &v
	case 6:
		var id string
		id, err = f.String(1024)
		if err == nil {
			err = f.Done()
		}
		c.CancelTask = &CancelTaskCommand{Task: model.EntityID(id)}
	case 7:
		var id, goal string
		id, err = f.String(1024)
		if err == nil {
			goal, err = f.String(1024)
		}
		v := GoalRebase{Task: model.EntityID(id), NewGoal: goal}
		if err == nil {
			v.ExpectedEpoch, err = f.Uint()
		}
		if err == nil {
			err = f.Done()
		}
		c.RebaseGoal = &v
	case 8:
		v, e := DecodePendingAction(body)
		c.PutPending, err = &v, e
	case 9:
		var key string
		key, err = f.String(1024)
		v := RebindPendingCommand{Key: key}
		if err == nil {
			v.Generation, err = f.Uint()
		}
		if err == nil {
			err = f.Done()
		}
		c.RebindPending = &v
	case 11:
		var id string
		id, err = f.String(1024)
		var result []byte
		if err == nil {
			result, err = f.Bytes(MaxCommandBytes)
		}
		if err == nil {
			err = f.Done()
		}
		c.SettleInvocation = &SettleInvocationCommand{ID: id, Result: result}
	case 12:
		var lifecycle *LifecycleCommand
		lifecycle, err = decodeLifecycleCommand(body)
		c.Lifecycle = lifecycle
	case 13:
		c.Control, err = decodeControlCommand(body)
	default:
		return Command{}, ErrCommandVersion
	}
	if err != nil {
		return Command{}, err
	}
	canonical, err := EncodeCommand(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Command{}, ErrCommandShape
	}
	return c, nil
}

// ApplyCommand atomically applies a validated pure transition. It first builds
// and validates a private candidate, then swaps its owned maps while holding
// the ledger mutex. A rejected command never changes visible state. No IO,
// callback, publication or external side effect occurs here.
func (l *Ledger) ApplyCommand(key model.RequestKey, command Command) (Result, error) {
	if err := key.Validate(); err != nil {
		return Result{}, err
	}
	raw, err := EncodeCommand(command)
	if err != nil {
		return Result{}, err
	}
	command, err = DecodeCommand(raw)
	if err != nil {
		return Result{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	candidate := l.commandCloneLocked()
	if err = commandScope(candidate, key, command); err != nil {
		return Result{}, err
	}
	if err = commandPrecondition(candidate, command); err != nil {
		return Result{}, err
	}
	result, err := applyCommand(candidate, key, command)
	if err != nil {
		return Result{}, err
	}
	image, err := EncodeSnapshot(candidate.Snapshot())
	if err != nil {
		return Result{}, err
	}
	candidate, err = RestoreSnapshot(image)
	if err != nil {
		return Result{}, err
	}
	l.tasks, l.executions, l.steps, l.invocations, l.actions, l.slots, l.lifecycle = candidate.tasks, candidate.executions, candidate.steps, candidate.invocations, candidate.actions, candidate.slots, candidate.lifecycle
	return result, nil
}

func (l *Ledger) commandCloneLocked() *Ledger {
	out := New()
	out.lifecycle = cloneLifecycle(l.lifecycle)
	for k, v := range l.tasks {
		out.tasks[k] = v
	}
	for k, v := range l.executions {
		out.executions[k] = v
	}
	for k, v := range l.steps {
		out.steps[k] = v
	}
	for k, v := range l.invocations {
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		out.invocations[k] = v
	}
	for k, v := range l.actions {
		out.actions[k] = v
	}
	for k, v := range l.slots {
		out.slots[k] = v
	}
	return out
}

func commandScope(l *Ledger, key model.RequestKey, c Command) error {
	if c.Control != nil {
		return l.controlScope(key, *c.Control)
	}
	if c.CreateTask != nil {
		if c.CreateTask.Space != key.Space || c.CreateTask.Owner != key.Principal {
			return ErrCommandScope
		}
		return nil
	}
	var task Task
	var err error
	taskForExecution := func(id model.ExecutionID) (Task, error) {
		x, e := l.GetExecution(id)
		if e != nil {
			return Task{}, e
		}
		return l.GetTask(x.Task)
	}
	switch {
	case c.Spawn != nil:
		task, err = l.GetTask(c.Spawn.Task)
	case c.StartStep != nil:
		task, err = taskForExecution(c.StartStep.Execution)
	case c.RecordInvocation != nil:
		task, err = taskForExecution(c.RecordInvocation.Execution)
	case c.CompleteStep != nil:
		task, err = taskForExecution(c.CompleteStep.Execution)
	case c.CancelTask != nil:
		task, err = l.GetTask(c.CancelTask.Task)
	case c.RebaseGoal != nil:
		task, err = l.GetTask(c.RebaseGoal.Task)
	case c.PutPending != nil:
		if c.PutPending.Request != key {
			return ErrCommandScope
		}
		task, err = taskForExecution(c.PutPending.Owner)
	case c.RebindPending != nil:
		var a PendingAction
		a, err = l.GetPending(c.RebindPending.Key)
		if err == nil {
			task, err = taskForExecution(a.Owner)
		}
	case c.SettleInvocation != nil:
		var i Invocation
		i, err = l.GetInvocation(c.SettleInvocation.ID)
		if err == nil {
			task, err = taskForExecution(i.Execution)
		}
	case c.Lifecycle != nil:
		// Lifecycle records carry their own execution identity. Scope is
		// checked against the authenticated task/owner when an execution is
		// referenced; query records carry Space/Owner directly.
		if q := c.Lifecycle.PutQuery; q != nil {
			if q.Space != key.Space || q.Owner != key.Principal {
				return ErrCommandScope
			}
			return nil
		}
		if a := c.Lifecycle.PutActivity; a != nil {
			task, err = taskForExecution(a.Execution)
		}
		if a := c.Lifecycle.PutAttempt; a != nil {
			act, e := l.GetActivity(a.Activity)
			if e != nil {
				err = e
			} else {
				task, err = taskForExecution(act.Execution)
			}
		}
		if r := c.Lifecycle.PutResult; r != nil {
			act, e := l.GetActivity(r.Activity)
			if e != nil {
				err = e
			} else {
				task, err = taskForExecution(act.Execution)
			}
		}
		if m := c.Lifecycle.PutMessage; m != nil {
			task, err = taskForExecution(m.Sender)
		}
		if m := c.Lifecycle.ConsumeMessage; m != nil {
			msg, e := l.GetMessage(m.Message)
			if e != nil {
				err = e
			} else {
				task, err = taskForExecution(msg.Recipient)
			}
		}
		if x := c.Lifecycle.PutContinuation; x != nil {
			task, err = taskForExecution(x.Execution)
		}
	}
	if err != nil {
		return err
	}
	if task.Space != key.Space || task.Owner != key.Principal {
		return ErrCommandScope
	}
	return nil
}

func commandPrecondition(l *Ledger, c Command) error {
	if c.Control != nil {
		return nil
	}
	if c.CancelTask != nil {
		t, err := l.GetTask(c.CancelTask.Task)
		if err != nil {
			return err
		}
		if t.Status != TaskCanceled && t.CancelEpoch == math.MaxUint64 {
			return ErrConflict
		}
		if t.Status == TaskCompleted {
			return ErrConflict
		}
	}
	var execution model.ExecutionID
	switch {
	case c.Spawn != nil:
		execution = c.Spawn.Parent
	case c.StartStep != nil:
		execution = c.StartStep.Execution
	case c.RecordInvocation != nil:
		execution = c.RecordInvocation.Execution
	case c.CompleteStep != nil:
		execution = c.CompleteStep.Execution
	case c.PutPending != nil:
		execution = c.PutPending.Owner
	case c.RebindPending != nil:
		a, err := l.GetPending(c.RebindPending.Key)
		if err != nil {
			return err
		}
		execution = a.Owner
	case c.SettleInvocation != nil:
		inv, getErr := l.GetInvocation(c.SettleInvocation.ID)
		if getErr != nil {
			return getErr
		}
		execution = inv.Execution
	case c.Lifecycle != nil:
		if err := l.lifecycleAuthorityGuard(*c.Lifecycle); err != nil {
			return err
		}
	}
	if execution != "" {
		if _, _, managed := l.authority(execution); managed {
			return ErrConflict
		}
	}
	if execution != "" {
		x, err := l.GetExecution(execution)
		if err != nil {
			return err
		}
		t, err := l.GetTask(x.Task)
		if err != nil {
			return err
		}
		if t.Status == TaskCanceled || x.Status == ExecutionCanceled || x.CancelEpoch != t.CancelEpoch {
			return ErrCanceled
		}
		if t.Status != TaskOpen || (x.Status != ExecutionReady && x.Status != ExecutionRunning) {
			return ErrConflict
		}
		if x.AcceptedGoalEpoch != t.GoalEpoch {
			return ErrGoalEpoch
		}
	}
	if c.StartStep != nil {
		if old, err := l.GetStep(c.StartStep.Execution, c.StartStep.Sequence); err == nil && old != *c.StartStep {
			return ErrConflict
		}
	}
	if c.CompleteStep != nil {
		old, err := l.GetStep(c.CompleteStep.Execution, c.CompleteStep.Sequence)
		if err != nil {
			return err
		}
		if old.GoalEpoch != c.CompleteStep.Epoch {
			return ErrGoalEpoch
		}
		if old.Status != StepOpen && old.Status != StepAccepted && old.Status != StepCompleted {
			return ErrConflict
		}
	}
	if c.RecordInvocation != nil {
		i := c.RecordInvocation
		step, err := l.GetStep(i.Execution, i.Step)
		if err != nil {
			return err
		}
		if step.Status != StepOpen && step.Status != StepAccepted {
			return ErrConflict
		}
		if old, err := l.GetInvocation(i.ID); err == nil {
			x, e := EncodeInvocation(old)
			y, f := EncodeInvocation(*i)
			if e != nil || f != nil || !bytes.Equal(x, y) {
				return ErrConflict
			}
		}
	}
	if c.SettleInvocation != nil {
		i, err := l.GetInvocation(c.SettleInvocation.ID)
		if err != nil {
			return err
		}
		if i.Status == InvocationSettled {
			if !bytes.Equal(i.Result, c.SettleInvocation.Result) {
				return ErrConflict
			}
		} else if i.Status == InvocationPending || i.Status == InvocationUnknown {
			return ErrConflict
		}
	}
	return nil
}

func applyCommand(l *Ledger, key model.RequestKey, c Command) (Result, error) {
	if c.Control != nil {
		return l.applyControl(key, *c.Control)
	}
	result := Result{}
	switch {
	case c.CreateTask != nil:
		v, err := l.CreateTask(CreateTask{Task: *c.CreateTask, Request: key})
		if err != nil {
			return Result{}, err
		}
		result.Task = &v
	case c.Spawn != nil:
		v, err := l.Spawn(SpawnRequest{Execution: *c.Spawn, Request: key})
		if err != nil {
			return Result{}, err
		}
		result.Execution = &v
	case c.StartStep != nil:
		v, err := l.StartStep(StepRequest{Step: *c.StartStep, Request: key})
		if err != nil {
			return Result{}, err
		}
		result.Step = &v
	case c.RecordInvocation != nil:
		v, err := l.RecordInvocation(InvocationRequest{Invocation: *c.RecordInvocation, Request: key})
		if err != nil {
			return Result{}, err
		}
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		result.Invocation = &v
	case c.CompleteStep != nil:
		v, err := l.CompleteStep(c.CompleteStep.Execution, c.CompleteStep.Sequence, c.CompleteStep.Epoch)
		if err != nil {
			return Result{}, err
		}
		result.Step = &v
	case c.CancelTask != nil:
		v, err := l.CancelTask(c.CancelTask.Task)
		if err != nil {
			return Result{}, err
		}
		result.Task = &v
	case c.RebaseGoal != nil:
		v, err := l.RebaseGoal(*c.RebaseGoal)
		if err != nil {
			return Result{}, err
		}
		result.Task = &v
	case c.PutPending != nil:
		v, err := l.PutPending(*c.PutPending)
		if err != nil {
			return Result{}, err
		}
		result.Pending = &v
	case c.RebindPending != nil:
		v, err := l.RebindPending(c.RebindPending.Key, c.RebindPending.Generation, key)
		if err != nil {
			return Result{}, err
		}
		result.Pending = &v
	case c.SettleInvocation != nil:
		v, err := l.SettleInvocation(c.SettleInvocation.ID, c.SettleInvocation.Result)
		if err != nil {
			return Result{}, err
		}
		result.Invocation = &v
	case c.Lifecycle != nil:
		if _, err := l.applyLifecycle(*c.Lifecycle); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}
