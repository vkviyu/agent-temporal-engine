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
