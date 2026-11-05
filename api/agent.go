package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyAgent = "agent.apply/v1"

// ApplyAgentRequest selects a static profile transition, never raw root writes.
// The trusted host supplies Key.Principal; local service identity replaces it.
type ApplyAgentRequest struct {
	Key           model.RequestKey
	Command       agentprofile.Command
	ExpectedFence *model.CommitFence
}

// ReadAgentStateRequest reads the profile partition owned by Principal at a
// published Fence. Anchor=nil captures latest once; Cursor/Knowledge selectors
// are not supported by this control-state projection.
type ReadAgentStateRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	Anchor    *model.SnapshotAnchor
}

type AgentStateResult struct {
	Found           bool
	Snapshot        agentprofile.Snapshot
	Anchor          model.SnapshotAnchor
	Fence           model.CommitFence
	RootSet         model.ObjectID
	LiveControlRoot model.ObjectID
}

type AgentCommands interface {
	ApplyAgent(context.Context, ApplyAgentRequest) (CommitResult, error)
	GetAgentState(context.Context, ReadAgentStateRequest) (AgentStateResult, error)
}
