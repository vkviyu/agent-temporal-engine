package ate

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
)

func (s *Store) ApplyAnnotation(ctx context.Context, req api.ApplyAnnotationRequest) (api.CommitResult, error) {
	return s.core.ApplyAnnotation(ctx, req)
}

func (s *Store) GetAnnotations(ctx context.Context, req api.ReadAnnotationsRequest) (api.AnnotationsResult, error) {
	return s.core.GetAnnotations(ctx, req)
}

var _ api.AnnotationCommands = (*Store)(nil)
