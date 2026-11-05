package ate

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) ApplySummary(ctx context.Context, req api.ApplySummaryRequest) (api.CommitResult, error) {
	return s.core.ApplySummary(ctx, req)
}

func (s *Store) GetSummary(ctx context.Context, req api.ReadSummaryRequest) (api.SummaryResult, error) {
	return s.core.GetSummary(ctx, req)
}

var _ api.SummaryCommands = (*Store)(nil)
