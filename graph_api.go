package ate

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

var ErrInvalidGraphCommand = errors.New("invalid graph command")

func (s *Store) ApplyGraph(ctx context.Context, req api.ApplyGraphRequest) (api.CommitResult, error) {
	return s.core.ApplyGraph(ctx, req)
}
func (s *Store) GetGraphRevision(ctx context.Context, req api.ReadGraphRevisionRequest) (api.GraphRevisionResult, error) {
	return s.core.GetGraphRevision(ctx, req)
}
func (s *Store) GetGraphBranch(ctx context.Context, req api.ReadGraphBranchRequest) (api.GraphBranchResult, error) {
	return s.core.GetGraphBranch(ctx, req)
}
func (s *Store) DiffGraph(ctx context.Context, req api.DiffGraphRequest) (api.GraphDiffResult, error) {
	return s.core.DiffGraph(ctx, req)
}
func (s *Store) PlanGraphJoin(ctx context.Context, req api.PlanGraphJoinRequest) (api.GraphJoinPlanResult, error) {
	return s.core.PlanGraphJoin(ctx, req)
}
func (s *Store) TraceGraph(ctx context.Context, req api.TraceGraphRequest) (api.GraphTraceResult, error) {
	return s.core.TraceGraph(ctx, req)
}
func (s *Store) LoadGraph(ctx context.Context, space model.SpaceID, head model.RevisionID, maxRevisions int) (*graph.Store, error) {
	return s.core.LoadGraph(ctx, space, head, maxRevisions)
}

var _ api.GraphCommands = (*Store)(nil)
var _ api.GraphQueries = (*Store)(nil)
