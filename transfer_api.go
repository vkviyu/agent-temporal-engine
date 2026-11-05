package ate

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

var _ api.TransferCommands = (*Store)(nil)

func (s *Store) ExportTransfer(ctx context.Context, r api.ExportTransferRequest) (transfer.Manifest, error) {
	return s.core.ExportTransfer(ctx, r)
}
func (s *Store) ImportTransfer(ctx context.Context, r api.ImportTransferRequest) (api.CommitResult, error) {
	return s.core.ImportTransfer(ctx, r)
}
func (s *Store) GetTransfer(ctx context.Context, r api.ReadTransferRequest) (api.TransferResult, error) {
	return s.core.GetTransfer(ctx, r)
}
