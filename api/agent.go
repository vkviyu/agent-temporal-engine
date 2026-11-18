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

const (
	MaxAgentStatePageBytes   = 8 << 20
	MaxAgentStatePageRecords = 256
)

// ReadAgentStateRecordsRequest selects an output page from one fixed state.
// Continuation is a consistency cursor, not an authorization credential.
// Subsequent pages require Anchor. Limit=0 selects 256 records.
type ReadAgentStateRecordsRequest struct {
	Space        model.SpaceID
	Principal    model.PrincipalID
	Anchor       *model.SnapshotAnchor
	Limit        int
	Continuation string
}

type AgentStateRecordsPage struct {
	Found           bool
	Anchor          model.SnapshotAnchor
	Fence           model.CommitFence
	RootSet         model.ObjectID
	LiveControlRoot model.ObjectID
	Catalog         model.ObjectID // LiveControl partition envelope, not its radix root.
	RecordCount     uint32
	RecordBytes     uint64 // Total canonical raw bytes across the complete catalog.
	Records         []agentprofile.StateRecord
	Continuation    string
}

// AgentRecordReader is independent of the complete-snapshot AgentCommands API.
type AgentRecordReader interface {
	GetAgentStateRecords(context.Context, ReadAgentStateRecordsRequest) (AgentStateRecordsPage, error)
}
