package ate

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) ApplyWork(ctx context.Context, r api.ApplyWorkRequest) (api.CommitResult, error) {
	return s.core.ApplyWork(ctx, r)
}
func (s *Store) GetQueryIntake(ctx context.Context, r api.ReadQueryIntakeRequest) (api.QueryIntakeResult, error) {
	return s.core.GetQueryIntake(ctx, r)
}
