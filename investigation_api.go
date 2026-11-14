package ate

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) UpdateInvestigation(ctx context.Context, req api.UpdateInvestigationRequest) (api.CommitResult, error) {
	return s.core.UpdateInvestigation(ctx, req)
}

func (s *Store) GetInvestigation(ctx context.Context, req api.GetInvestigationRequest) (api.InvestigationResult, error) {
	return s.core.GetInvestigation(ctx, req)
}

var _ api.InvestigationCommands = (*Store)(nil)
