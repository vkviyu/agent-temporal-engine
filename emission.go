package ate

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) ApplyEmission(ctx context.Context, r api.ApplyEmissionRequest) (api.CommitResult, error) {
	return s.core.ApplyEmission(ctx, r)
}
func (s *Store) GetEmission(ctx context.Context, r api.ReadEmissionRequest) (api.EmissionResult, error) {
	return s.core.GetEmission(ctx, r)
}

var _ api.EmissionCommands = (*Store)(nil)
