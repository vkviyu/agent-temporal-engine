package ate

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) GetMemoryState(ctx context.Context, req api.ReadMemoryStateRequest) (api.MemoryStateResult, error) {
	return s.core.GetMemoryState(ctx, req)
}
func (s *Store) ApplyMemory(ctx context.Context, req api.ApplyMemoryRequest) (api.CommitResult, error) {
	return s.core.ApplyMemory(ctx, req)
}

var _ api.MemoryCommands = (*Store)(nil)
