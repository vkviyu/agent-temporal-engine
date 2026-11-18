package service

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type agentBackend interface {
	api.AgentCommands
	AgentProfileEnabled() bool
}

type agentRecordBackend interface {
	api.AgentRecordReader
	api.ReadDeliveryPreparer
	AgentProfileEnabled() bool
}

func (ss *session) getAgentStateRecords(ctx context.Context, raw []byte) (api.AgentStateRecordsPage, error) {
	var w protocol.ReadAgentStateRecordsRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	if !ss.allowed(w.Space) {
		return api.AgentStateRecordsPage{}, ErrSpaceDenied
	}
	backend, ok := ss.server.backend.(agentRecordBackend)
	if !ok || !backend.AgentProfileEnabled() {
		return api.AgentStateRecordsPage{}, apiError(api.ErrorUnsupported)
	}
	return backend.GetAgentStateRecords(ctx, api.ReadAgentStateRecordsRequest{Space: w.Space, Principal: ss.principal, Anchor: w.Anchor, Limit: w.Limit, Continuation: w.Continuation})
}

func (s *Server) agentBackend() (agentBackend, bool) {
	backend, ok := s.backend.(agentBackend)
	return backend, ok && backend.AgentProfileEnabled()
}

func (ss *session) applyAgent(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var w protocol.ApplyAgentRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, err
	}
	failure := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}
	if !ss.allowed(w.Space) {
		return failure, ErrSpaceDenied
	}
	backend, ok := ss.server.agentBackend()
	if !ok {
		return failure, apiError(api.ErrorUnsupported)
	}
	command, err := agentprofile.DecodeCommand(w.Command)
	if err != nil {
		// Parse errors are transport-local; a backend classifier must not turn
		// malformed input into corruption, fencing or an internal failure.
		if errors.Is(err, agentprofile.ErrCommandVersion) {
			return failure, apiError(api.ErrorUnsupported)
		}
		return failure, apiError(api.ErrorInvalidRequest)
	}
	return backend.ApplyAgent(ctx, api.ApplyAgentRequest{
		Key:     model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID},
		Command: command, ExpectedFence: w.ExpectedFence,
	})
}

func (ss *session) getAgentState(ctx context.Context, raw []byte) (api.AgentStateResult, error) {
	var w protocol.ReadAgentStateRequest
	if err := protocol.DecodeStrict(raw, &w); err != nil {
		return api.AgentStateResult{}, err
	}
	if !ss.allowed(w.Space) {
		return api.AgentStateResult{}, ErrSpaceDenied
	}
	backend, ok := ss.server.agentBackend()
	if !ok {
		return api.AgentStateResult{}, apiError(api.ErrorUnsupported)
	}
	return backend.GetAgentState(ctx, api.ReadAgentStateRequest{Space: w.Space, Principal: ss.principal, Anchor: w.Anchor})
}
