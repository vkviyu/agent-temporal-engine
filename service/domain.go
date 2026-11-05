package service

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

type graphBackend interface{ api.GraphCommands }
type memoryBackend interface{ api.MemoryCommands }
type knowledgeBackend interface{ api.KnowledgeCommands }

func (ss *session) applyGraph(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var w protocol.ApplyGraphRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, e
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}
	if !ss.allowed(w.Space) {
		return out, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(graphBackend)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	space, c, err := graph.DecodeCommand(w.Command)
	if err != nil || space != w.Space {
		return out, apiError(api.ErrorInvalidRequest)
	}
	return b.ApplyGraph(ctx, api.ApplyGraphRequest{Key: model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID}, Command: c, ExpectedFence: w.ExpectedFence})
}
func (ss *session) getGraphRevision(ctx context.Context, raw []byte) (api.GraphRevisionResult, error) {
	var w protocol.ReadGraphRevisionRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.GraphRevisionResult{}, e
	}
	if !ss.allowed(w.Space) {
		return api.GraphRevisionResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(graphBackend)
	if !ok {
		return api.GraphRevisionResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: w.Space, ID: w.ID, Anchor: w.Anchor})
}
func (ss *session) getGraphBranch(ctx context.Context, raw []byte) (api.GraphBranchResult, error) {
	var w protocol.ReadGraphBranchRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.GraphBranchResult{}, e
	}
	if !ss.allowed(w.Space) {
		return api.GraphBranchResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(graphBackend)
	if !ok {
		return api.GraphBranchResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: w.Space, ID: w.ID, Anchor: w.Anchor})
}
func (ss *session) applyMemory(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var w protocol.ApplyMemoryRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, e
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}
	if !ss.allowed(w.Space) {
		return out, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(memoryBackend)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	c, err := memory.DecodeCommand(w.Command)
	if err != nil {
		return out, apiError(api.ErrorInvalidRequest)
	}
	return b.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID}, Command: c, ExpectedFence: w.ExpectedFence})
}
func (ss *session) getMemory(ctx context.Context, raw []byte) (api.MemoryStateResult, error) {
	var w protocol.ReadMemoryRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.MemoryStateResult{}, e
	}
	if !ss.allowed(w.Space) {
		return api.MemoryStateResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(memoryBackend)
	if !ok {
		return api.MemoryStateResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: w.Space})
}
func (ss *session) applyKnowledge(ctx context.Context, raw []byte) (api.CommitResult, error) {
	var w protocol.ApplyKnowledgeRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.CommitResult{Code: api.CodeNotSubmitted}, e
	}
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID}
	if !ss.allowed(w.Space) {
		return out, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(knowledgeBackend)
	if !ok {
		return out, apiError(api.ErrorUnsupported)
	}
	c, err := knowledge.DecodeCommand(w.Command)
	if err != nil {
		return out, apiError(api.ErrorInvalidRequest)
	}
	return b.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID}, Command: c, ExpectedFence: w.ExpectedFence})
}
func (ss *session) getKnowledge(ctx context.Context, raw []byte) (api.KnowledgeStateResult, error) {
	var w protocol.ReadKnowledgeRequest
	if e := protocol.DecodeStrict(raw, &w); e != nil {
		return api.KnowledgeStateResult{}, e
	}
	if !ss.allowed(w.Space) {
		return api.KnowledgeStateResult{}, ErrSpaceDenied
	}
	b, ok := ss.server.backend.(knowledgeBackend)
	if !ok {
		return api.KnowledgeStateResult{}, apiError(api.ErrorUnsupported)
	}
	return b.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: w.Space, Principal: ss.principal, Anchor: w.Anchor, Knowledge: w.Knowledge})
}
