package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplySummary = "knowledge.summary.apply/v1"

type ApplySummaryRequest struct {
	Key           model.RequestKey
	Summary       knowledge.Summary
	ExpectedFence *model.CommitFence
}

// ReadSummaryRequest selects an immutable SummaryID in one owner's partition.
// Anchor fixes the publication containing the record, independently of the
// summary's internal source KnowledgeKey+Cursor. Selectors on Anchor are not
// silently substituted for that source position.
type ReadSummaryRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	ID        model.ObjectID
	Anchor    *model.SnapshotAnchor
}

type SummaryResult struct {
	Found   bool
	Summary knowledge.Summary
	Record  model.ObjectID // actual stored ValueBytes object, not the SummaryID
	Anchor  model.SnapshotAnchor
	Fence   model.CommitFence
	RootSet model.ObjectID
}

type SummaryCommands interface {
	ApplySummary(context.Context, ApplySummaryRequest) (CommitResult, error)
	GetSummary(context.Context, ReadSummaryRequest) (SummaryResult, error)
}
