package agentstore

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
)

var ErrCommandShape = errors.New("agent command must contain exactly one operation")

// Command is the closed agent/v1 dispatcher union. The coordinator validates
// the selected operation and then routes it through the same candidate→core
// commit path as the typed convenience methods. Nested Request fields must
// carry the same key as Request.
type Command struct {
	Request          model.RequestKey
	CreateTask       *agentprofile.CreateTask
	Spawn            *agentprofile.SpawnRequest
	StartStep        *agentprofile.StepRequest
	RecordInvocation *agentprofile.InvocationRequest
	CompleteStep     *CompleteStepCommand
	CancelTask       *CancelTaskCommand
	RebaseGoal       *RebaseGoalCommand
	PutPending       *agentprofile.PendingAction
	RebindPending    *RebindPendingCommand
}

type CompleteStepCommand struct {
	Execution model.ExecutionID
	Sequence  uint64
	Epoch     uint64
}
type CancelTaskCommand struct{ Task model.EntityID }
type RebaseGoalCommand struct{ Goal agentprofile.GoalRebase }
type RebindPendingCommand struct {
	ActionKey  string
	Generation uint64
	Request    model.RequestKey
}

type Result struct {
	Task       *agentprofile.Task
	Execution  *agentprofile.Execution
	Step       *agentprofile.Step
	Invocation *agentprofile.Invocation
	Pending    *agentprofile.PendingAction
	Fence      model.CommitFence
}

func (c Command) selected() int {
	n := 0
	for _, present := range []bool{c.CreateTask != nil, c.Spawn != nil, c.StartStep != nil, c.RecordInvocation != nil, c.CompleteStep != nil, c.CancelTask != nil, c.RebaseGoal != nil, c.PutPending != nil, c.RebindPending != nil} {
		if present {
			n++
		}
	}
	return n
}

func sameKey(a, b model.RequestKey) bool { return a == b }

func (c Command) Validate() error {
	if c.selected() != 1 || c.Request.Validate() != nil {
		return ErrCommandShape
	}
	switch {
	case c.CreateTask != nil:
		if !sameKey(c.CreateTask.Request, c.Request) {
			return ErrCommandShape
		}
	case c.Spawn != nil:
		if !sameKey(c.Spawn.Request, c.Request) {
			return ErrCommandShape
		}
	case c.StartStep != nil:
		if !sameKey(c.StartStep.Request, c.Request) {
			return ErrCommandShape
		}
	case c.RecordInvocation != nil:
		if !sameKey(c.RecordInvocation.Request, c.Request) {
			return ErrCommandShape
		}
	case c.PutPending != nil:
		if !sameKey(c.PutPending.Request, c.Request) {
			return ErrCommandShape
		}
	case c.RebindPending != nil:
		if !sameKey(c.RebindPending.Request, c.Request) || c.RebindPending.Request.Space != c.Request.Space || c.RebindPending.Request.Principal != c.Request.Principal {
			return ErrCommandShape
		}
	case c.CompleteStep == nil && c.CancelTask == nil && c.RebaseGoal == nil:
		return ErrCommandShape
	}
	return nil
}

// Apply executes the closed command through the core dispatcher and returns
// its typed result and the original receipt's Fence, including on a retry.
func (s *Store) Apply(ctx context.Context, c Command) (Result, error) {
	if err := c.Validate(); err != nil {
		return Result{}, err
	}
	command := agentprofile.Command{}
	switch {
	case c.CreateTask != nil:
		command.CreateTask = &c.CreateTask.Task
	case c.Spawn != nil:
		command.Spawn = &c.Spawn.Execution
	case c.StartStep != nil:
		command.StartStep = &c.StartStep.Step
	case c.RecordInvocation != nil:
		command.RecordInvocation = &c.RecordInvocation.Invocation
	case c.CompleteStep != nil:
		command.CompleteStep = &agentprofile.CompleteStepCommand{Execution: c.CompleteStep.Execution, Sequence: c.CompleteStep.Sequence, Epoch: c.CompleteStep.Epoch}
	case c.CancelTask != nil:
		command.CancelTask = &agentprofile.CancelTaskCommand{Task: c.CancelTask.Task}
	case c.RebaseGoal != nil:
		command.RebaseGoal = &c.RebaseGoal.Goal
	case c.PutPending != nil:
		command.PutPending = c.PutPending
	case c.RebindPending != nil:
		command.RebindPending = &agentprofile.RebindPendingCommand{Key: c.RebindPending.ActionKey, Generation: c.RebindPending.Generation}
	}
	result, fence, err := s.execute(ctx, c.Request, command)
	if err != nil {
		return Result{}, err
	}
	return Result{Task: result.Task, Execution: result.Execution, Step: result.Step, Invocation: result.Invocation, Pending: result.Pending, Fence: fence}, nil
}
