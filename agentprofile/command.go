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
