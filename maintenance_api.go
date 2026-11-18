package ate

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (s *Store) CompactAuditFull(ctx context.Context) (storage.CompactionReport, error) {
	return s.core.CompactAuditFull(ctx)
}

func (s *Store) ReclaimRetired(ctx context.Context) ([]string, int, error) {
	return s.core.ReclaimRetired(ctx)
}

// CreateRecoveryCheckpoint verifies the full history and publishes a recovery
// projection. It advances the physical generation without changing the fence.
// Future opens verify all history bytes and replay only the uncertified suffix.
func (s *Store) CreateRecoveryCheckpoint(ctx context.Context) (storage.Publication, error) {
	return s.core.CreateRecoveryCheckpoint(ctx)
}
