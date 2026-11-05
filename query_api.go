package ate

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) GetQueryProgress(ctx context.Context, r api.ReadQueryProgressRequest) (api.QueryProgressResult, error) {
	return s.core.GetQueryProgress(ctx, r)
}
