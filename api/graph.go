package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyGraph = "graph.apply/v1"

type ApplyGraphRequest struct {
	Key           model.RequestKey
	Command       graph.Command
	ExpectedFence *model.CommitFence
}

type GraphResult struct {
	Revision *graph.Revision
	Branch   *graph.Branch
}

type ReadGraphRevisionRequest struct {
	Space  model.SpaceID
	ID     model.RevisionID
	Anchor *model.SnapshotAnchor
}
type ReadGraphBranchRequest struct {
	Space  model.SpaceID
	ID     model.BranchID
	Anchor *model.SnapshotAnchor
}

type GraphRevisionResult struct {
	Found   bool
	Value   graph.Revision
	Fence   model.CommitFence
	Anchor  model.SnapshotAnchor
	RootSet model.ObjectID
}
type GraphBranchResult struct {
	Found   bool
	Value   graph.Branch
	Fence   model.CommitFence
	Anchor  model.SnapshotAnchor
	RootSet model.ObjectID
	Pruned  bool
	Prune   *graph.PruneRecord
}

type DiffGraphRequest struct {
	Space        model.SpaceID
	Anchor       *model.SnapshotAnchor
	From, To     model.RevisionID
	Limit        int
	Continuation string
	Limits       QueryLimits
}
type GraphDiffResult struct {
	Anchor       model.SnapshotAnchor
	RootSet      model.ObjectID
	Diff         graph.Diff
	Continuation string
	Truncated    bool
}
type PlanGraphJoinRequest struct {
	Space       model.SpaceID
	Anchor      *model.SnapshotAnchor
	Left, Right model.RevisionID
	Limits      QueryLimits
}
type GraphJoinPlanResult struct {
	Anchor  model.SnapshotAnchor
	RootSet model.ObjectID
	Plan    graph.JoinPlan
}
type TraceGraphRequest struct {
	Space        model.SpaceID
	Anchor       *model.SnapshotAnchor
	Start        model.RevisionID
	Direction    string
	Limit        int
	Continuation string
	Limits       QueryLimits
}
type GraphTraceNode struct {
	ID    model.RevisionID
	Depth int
}
type GraphTraceEdge struct{ Child, Parent model.RevisionID }
type GraphTraceResult struct {
	Anchor       model.SnapshotAnchor
	RootSet      model.ObjectID
	Nodes        []GraphTraceNode
	Edges        []GraphTraceEdge
	Continuation string
	Truncated    bool
}

type GraphQueries interface {
	DiffGraph(context.Context, DiffGraphRequest) (GraphDiffResult, error)
	PlanGraphJoin(context.Context, PlanGraphJoinRequest) (GraphJoinPlanResult, error)
	TraceGraph(context.Context, TraceGraphRequest) (GraphTraceResult, error)
}

type GraphCommands interface {
	ApplyGraph(context.Context, ApplyGraphRequest) (CommitResult, error)
	GetGraphRevision(context.Context, ReadGraphRevisionRequest) (GraphRevisionResult, error)
	GetGraphBranch(context.Context, ReadGraphBranchRequest) (GraphBranchResult, error)
}
