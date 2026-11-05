package api

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyKnowledge = "knowledge.apply/v1"

type ApplyKnowledgeRequest struct {
	Key           model.RequestKey
	Command       knowledge.Command
	ExpectedFence *model.CommitFence
}
type ReadKnowledgeStateRequest struct {
	Space model.SpaceID
	// Required in embedded mode; local service replaces it with the
	// authenticated connection. An omitted Knowledge selects this owner's
	// partition, never all principals in the Space.
	Principal model.PrincipalID
	Anchor    *model.SnapshotAnchor
	Knowledge *model.KnowledgeKey
}
type KnowledgeStateResult struct {
	Found     bool
	Snapshot  knowledge.Snapshot
	StateRoot model.ObjectID
	Fence     model.CommitFence
	RootSet   model.ObjectID
	Anchor    model.SnapshotAnchor
	Frontier  model.ObjectID
	// Revision is the authenticated immutable knowledge head selected by Key
	// and optional Cursor. Its StateRoot commits only adopted observations;
	// packet rendering and unrelated executions do not advance it.
	Revision *knowledge.Revision
}
type KnowledgeCommands interface {
	ApplyKnowledge(context.Context, ApplyKnowledgeRequest) (CommitResult, error)
	GetKnowledgeState(context.Context, ReadKnowledgeStateRequest) (KnowledgeStateResult, error)
}
