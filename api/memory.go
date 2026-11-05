package api

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyMemory = "memory.apply/v1"

type ApplyMemoryRequest struct {
	Key           model.RequestKey
	Command       memory.Command
	ExpectedFence *model.CommitFence
}
type ReadMemoryStateRequest struct{ Space model.SpaceID }
type MemoryStateResult struct {
	Found     bool
	Snapshot  memory.Snapshot
	StateRoot model.ObjectID
	Fence     model.CommitFence
	RootSet   model.ObjectID
}
type MemoryCommands interface {
	ApplyMemory(context.Context, ApplyMemoryRequest) (CommitResult, error)
	GetMemoryState(context.Context, ReadMemoryStateRequest) (MemoryStateResult, error)
}
