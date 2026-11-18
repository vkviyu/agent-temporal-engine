// Package agentstore provides typed convenience methods over ATE's static
// agent/v1 core dispatcher. LiveControl, receipts and the sole Journal publisher
// own all state; this facade keeps no ledger, root cache or recovery authority.
package agentstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var (
	ErrInvalidOptions = errors.New("invalid agent store options")
	ErrProfileScope   = errors.New("agent profile request is outside the configured scope")
	ErrProfileState   = errors.New("agent profile state is unavailable or malformed")
	ErrUncertain      = errors.New("agent profile result requires reconciliation")
)

// CommandError preserves the original key and core outcome when a typed result
// is unavailable. WaitInterrupted/CommitUnknown never prove durable absence.
// Reconcile uses Key; a retry must keep the same command and RequestKey.
type CommandError struct {
	Key    model.RequestKey
	Result api.CommitResult
	Cause  error
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("agent command %q: %s: %v", e.Key.RequestID, e.Result.Code, e.Cause)
}
func (e *CommandError) Unwrap() error { return e.Cause }

type Options struct {
	ate.Options
	ProfileSpace model.SpaceID
	Principal    model.PrincipalID
}

func (o Options) checked() (Options, error) {
	if o.ProfileSpace == "" {
		o.ProfileSpace = "agent"
	}
	if o.Principal == "" {
		o.Principal = "agent-host"
	}
	if len(o.ProfileSpace) > model.MaxIdentityBytes || len(o.Principal) > model.MaxIdentityBytes || !utf8.ValidString(string(o.ProfileSpace)) || !utf8.ValidString(string(o.Principal)) || strings.HasPrefix(string(o.ProfileSpace), "ate:") {
		return o, ErrInvalidOptions
	}
	o.Options.AgentProfile = true
	return o, nil
}

type Store struct {
	core      *ate.Store
	space     model.SpaceID
	principal model.PrincipalID
}

func Create(path string, o Options) (*Store, error) {
	o, err := o.checked()
	if err != nil {
		return nil, err
	}
	core, err := ate.Create(path, o.Options)
	if err != nil {
		return nil, err
	}
	return &Store{core: core, space: o.ProfileSpace, principal: o.Principal}, nil
}
func OpenExisting(path string, o Options) (*Store, storage.RecoveryReport, error) {
	o, err := o.checked()
	if err != nil {
		return nil, storage.RecoveryReport{}, err
	}
	core, report, err := ate.OpenExisting(path, o.Options)
	if err != nil {
		return nil, report, err
	}
	return &Store{core: core, space: o.ProfileSpace, principal: o.Principal}, report, nil
}
func (s *Store) checkScope(key model.RequestKey) error {
	if s == nil || s.core == nil {
		return storage.ErrClosed
	}
	if key.Space != s.space || key.Principal != s.principal {
		return ErrProfileScope
	}
	return key.Validate()
}
func commandFailure(key model.RequestKey, result api.CommitResult, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("core returned %s", result.Code)
		if result.Code == api.CodeConflict {
			cause = errors.Join(agentprofile.ErrConflict, cause)
		}
	}
	if errors.Is(cause, agentprofile.ErrCommandScope) {
		cause = errors.Join(ErrProfileScope, cause)
	}
	if result.Code == api.CodeWaitInterrupted || result.Code == api.CodeCommitUnknown || result.Code == api.CodeInFlight || result.Code == api.CodeDurableAck {
		cause = errors.Join(ErrUncertain, cause)
	}
	return &CommandError{Key: key, Result: result, Cause: cause}
}

// execute resolves the typed result at the ACK's own authenticated fence,
// never from current state. Input ownership and idempotency belong to core.
func (s *Store) execute(ctx context.Context, key model.RequestKey, command agentprofile.Command) (agentprofile.Result, model.CommitFence, error) {
	if err := s.checkScope(key); err != nil {
		return agentprofile.Result{}, model.CommitFence{}, err
	}
	raw, err := agentprofile.EncodeCommand(command)
	if err != nil {
		return agentprofile.Result{}, model.CommitFence{}, err
	}
	owned, err := agentprofile.DecodeCommand(raw)
	if err != nil {
		return agentprofile.Result{}, model.CommitFence{}, err
	}
	ack, err := s.core.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key, Command: owned})
	if err != nil || ack.Code != api.CodeDurableAck {
		return agentprofile.Result{}, model.CommitFence{}, commandFailure(key, ack, err)
	}
	anchor := model.SnapshotAnchor{Space: key.Space, Fence: ack.Fence}
	state, err := s.core.GetAgentState(ctx, api.ReadAgentStateRequest{Space: key.Space, Principal: key.Principal, Anchor: &anchor})
	if err != nil {
		return agentprofile.Result{}, model.CommitFence{}, commandFailure(key, ack, err)
	}
	if !state.Found || state.Fence != ack.Fence || state.RootSet != ack.RootSet || state.LiveControlRoot != ack.StateRoot {
		return agentprofile.Result{}, model.CommitFence{}, commandFailure(key, ack, ErrProfileState)
	}
	result, err := resultAt(state.Snapshot, owned)
	if err != nil {
		return agentprofile.Result{}, model.CommitFence{}, commandFailure(key, ack, errors.Join(ErrProfileState, err))
	}
	return result, ack.Fence, nil
}
func resultAt(snapshot agentprofile.Snapshot, command agentprofile.Command) (agentprofile.Result, error) {
	ledger, err := agentprofile.RestoreStateSnapshot(snapshot)
	if err != nil {
		return agentprofile.Result{}, err
	}
	var out agentprofile.Result
	switch {
	case command.CreateTask != nil || command.CancelTask != nil || command.RebaseGoal != nil:
		var id model.EntityID
		switch {
		case command.CreateTask != nil:
			id = command.CreateTask.ID
		case command.CancelTask != nil:
			id = command.CancelTask.Task
		case command.RebaseGoal != nil:
			id = command.RebaseGoal.Task
		}
		v, err := ledger.GetTask(id)
		out.Task = &v
		return out, err
	case command.Spawn != nil:
		v, err := ledger.GetExecution(command.Spawn.ID)
		out.Execution = &v
		return out, err
	case command.StartStep != nil:
		v, err := ledger.GetStep(command.StartStep.Execution, command.StartStep.Sequence)
		out.Step = &v
		return out, err
	case command.CompleteStep != nil:
		v, err := ledger.GetStep(command.CompleteStep.Execution, command.CompleteStep.Sequence)
		out.Step = &v
		return out, err
	case command.RecordInvocation != nil:
		v, err := ledger.GetInvocation(command.RecordInvocation.ID)
		out.Invocation = &v
		return out, err
	case command.PutPending != nil:
		v, err := ledger.GetPending(command.PutPending.Key)
		out.Pending = &v
		return out, err
	case command.RebindPending != nil:
		v, err := ledger.GetPending(command.RebindPending.Key)
		out.Pending = &v
		return out, err
	default:
		return agentprofile.Result{}, ErrCommandShape
	}
}

// Reconcile delegates to the core receipt authority for the original key. It
// does not reload latest state or keep an independent uncertainty state machine.
func (s *Store) Reconcile(ctx context.Context, key model.RequestKey) (api.LookupRequestResult, error) {
	if err := s.checkScope(key); err != nil {
		return api.LookupRequestResult{}, err
	}
	return s.core.LookupRequest(ctx, api.LookupRequestRequest{Space: key.Space, Principal: key.Principal, RequestID: key.RequestID})
}
func (s *Store) CreateTask(ctx context.Context, r agentprofile.CreateTask) (agentprofile.Task, error) {
	result, _, err := s.execute(ctx, r.Request, agentprofile.Command{CreateTask: &r.Task})
	if err != nil {
		return agentprofile.Task{}, err
	}
	return *result.Task, nil
}
func (s *Store) Spawn(ctx context.Context, r agentprofile.SpawnRequest) (agentprofile.Execution, error) {
	result, _, err := s.execute(ctx, r.Request, agentprofile.Command{Spawn: &r.Execution})
	if err != nil {
		return agentprofile.Execution{}, err
	}
	return *result.Execution, nil
}
func (s *Store) StartStep(ctx context.Context, r agentprofile.StepRequest) (agentprofile.Step, error) {
	result, _, err := s.execute(ctx, r.Request, agentprofile.Command{StartStep: &r.Step})
	if err != nil {
		return agentprofile.Step{}, err
	}
	return *result.Step, nil
}
func (s *Store) RecordInvocation(ctx context.Context, r agentprofile.InvocationRequest) (agentprofile.Invocation, error) {
	result, _, err := s.execute(ctx, r.Request, agentprofile.Command{RecordInvocation: &r.Invocation})
	if err != nil {
		return agentprofile.Invocation{}, err
	}
	return *result.Invocation, nil
}

// CompleteStep preserves the retired command's typed rejection. Current Engine
// completion uses Core().ApplyWork with FinishStep and its checkpoint together.
func (s *Store) CompleteStep(ctx context.Context, key model.RequestKey, id model.ExecutionID, sequence, epoch uint64) (agentprofile.Step, error) {
	result, _, err := s.execute(ctx, key, agentprofile.Command{CompleteStep: &agentprofile.CompleteStepCommand{Execution: id, Sequence: sequence, Epoch: epoch}})
	if err != nil {
		return agentprofile.Step{}, err
	}
	return *result.Step, nil
}
func (s *Store) CancelTask(ctx context.Context, key model.RequestKey, id model.EntityID) (agentprofile.Task, error) {
	result, _, err := s.execute(ctx, key, agentprofile.Command{CancelTask: &agentprofile.CancelTaskCommand{Task: id}})
	if err != nil {
		return agentprofile.Task{}, err
	}
	return *result.Task, nil
}
func (s *Store) RebaseGoal(ctx context.Context, key model.RequestKey, r agentprofile.GoalRebase) (agentprofile.Task, error) {
	result, _, err := s.execute(ctx, key, agentprofile.Command{RebaseGoal: &r})
	if err != nil {
		return agentprofile.Task{}, err
	}
	return *result.Task, nil
}
func (s *Store) PutPending(ctx context.Context, r agentprofile.PendingAction) (agentprofile.PendingAction, error) {
	result, _, err := s.execute(ctx, r.Request, agentprofile.Command{PutPending: &r})
	if err != nil {
		return agentprofile.PendingAction{}, err
	}
	return *result.Pending, nil
}
func (s *Store) RebindPending(ctx context.Context, key model.RequestKey, actionKey string, generation uint64, req model.RequestKey) (agentprofile.PendingAction, error) {
	if req != key {
		return agentprofile.PendingAction{}, ErrCommandShape
	}
	result, _, err := s.execute(ctx, key, agentprofile.Command{RebindPending: &agentprofile.RebindPendingCommand{Key: actionKey, Generation: generation}})
	if err != nil {
		return agentprofile.PendingAction{}, err
	}
	return *result.Pending, nil
}
func (s *Store) currentLedger() (*agentprofile.Ledger, model.CommitFence, error) {
	if s == nil || s.core == nil {
		return nil, model.CommitFence{}, storage.ErrClosed
	}
	return readCurrentLedger(s.core, api.ReadAgentStateRequest{Space: s.space, Principal: s.principal})
}

func readCurrentLedger(core api.AgentCommands, request api.ReadAgentStateRequest) (*agentprofile.Ledger, model.CommitFence, error) {
	state, err := core.GetAgentState(context.Background(), request)
	if err != nil {
		return nil, model.CommitFence{}, err
	}
	ledger, err := agentprofile.RestoreStateSnapshot(state.Snapshot)
	if err != nil {
		return nil, model.CommitFence{}, errors.Join(ErrProfileState, err)
	}
	return ledger, state.Fence, nil
}
func (s *Store) GetTask(id model.EntityID) (agentprofile.Task, error) {
	ledger, _, err := s.currentLedger()
	if err != nil {
		return agentprofile.Task{}, err
	}
	return ledger.GetTask(id)
}
func (s *Store) GetExecution(id model.ExecutionID) (agentprofile.Execution, error) {
	ledger, _, err := s.currentLedger()
	if err != nil {
		return agentprofile.Execution{}, err
	}
	return ledger.GetExecution(id)
}
func (s *Store) GetInvocation(id string) (agentprofile.Invocation, error) {
	ledger, _, err := s.currentLedger()
	if err != nil {
		return agentprofile.Invocation{}, err
	}
	return ledger.GetInvocation(id)
}
func (s *Store) GetStep(execution model.ExecutionID, sequence uint64) (agentprofile.Step, error) {
	ledger, _, err := s.currentLedger()
	if err != nil {
		return agentprofile.Step{}, err
	}
	return ledger.GetStep(execution, sequence)
}
func (s *Store) GetPending(key string) (agentprofile.PendingAction, error) {
	ledger, _, err := s.currentLedger()
	if err != nil {
		return agentprofile.PendingAction{}, err
	}
	return ledger.GetPending(key)
}
func (s *Store) ProfileSnapshot() (agentprofile.Snapshot, model.CommitFence, error) {
	if s == nil || s.core == nil {
		return agentprofile.Snapshot{}, model.CommitFence{}, storage.ErrClosed
	}
	state, err := s.core.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: s.space, Principal: s.principal})
	if err != nil {
		return agentprofile.Snapshot{}, model.CommitFence{}, err
	}
	return state.Snapshot, state.Fence, nil
}
func (s *Store) Core() *ate.Store { return s.core }
func (s *Store) Close() error {
	if s == nil || s.core == nil {
		return storage.ErrClosed
	}
	return s.core.Close()
}
