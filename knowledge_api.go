package ate

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
)

// The facade preserves the original command through admission and retries.
func (s *Store) ApplyKnowledge(ctx context.Context, req api.ApplyKnowledgeRequest) (api.CommitResult, error) {
	return s.core.ApplyKnowledge(ctx, req)
}
func (s *Store) GetKnowledgeState(ctx context.Context, req api.ReadKnowledgeStateRequest) (api.KnowledgeStateResult, error) {
	return s.core.GetKnowledgeState(ctx, req)
}

var _ api.KnowledgeCommands = (*Store)(nil)
