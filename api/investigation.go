package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodUpdateInvestigation = "investigation.update/v1"

// UpdateInvestigationRequest publishes one bounded update under the caller's
// durable request key. The command compares the investigation's own version.
type UpdateInvestigationRequest struct {
	Key     model.RequestKey
	Command investigation.Update
}

// GetInvestigationRequest selects an owned checkpoint. Anchor selects a
// publication of the checkpoint itself, with no Cursor or Knowledge selector.
// A nil Anchor reads the latest published checkpoint.
type GetInvestigationRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	ID        string
	Anchor    *model.SnapshotAnchor
}

type InvestigationResult struct {
	Found      bool
	Checkpoint investigation.Checkpoint
	Anchor     model.SnapshotAnchor
	Fence      model.CommitFence
	RootSet    model.ObjectID
}

type InvestigationCommands interface {
	UpdateInvestigation(context.Context, UpdateInvestigationRequest) (CommitResult, error)
	GetInvestigation(context.Context, GetInvestigationRequest) (InvestigationResult, error)
}
