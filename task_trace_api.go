package ate

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) TaskTraceEnabled() bool {
	return s.core.TaskTraceEnabled()
}

func (s *Store) ApplyTaskTrace(ctx context.Context, req api.ApplyTaskTraceRequest) (api.CommitResult, error) {
	return s.core.ApplyTaskTrace(ctx, req)
}

func (s *Store) GetTaskTrace(ctx context.Context, req api.TaskTraceRequest) (api.TaskTraceResult, error) {
	return s.core.GetTaskTrace(ctx, req)
}

func (s *Store) GetTaskTraceEvidence(ctx context.Context, req api.TaskTraceEvidenceRequest) (api.TaskTraceEvidenceResult, error) {
	return s.core.GetTaskTraceEvidence(ctx, req)
}

var _ api.TaskTraceCommands = (*Store)(nil)
