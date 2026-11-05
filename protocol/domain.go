package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

type ApplyGraphRequest struct {
	Space         model.SpaceID
	RequestID     model.RequestID
	Command       []byte
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type ReadGraphRevisionRequest struct {
	Space  model.SpaceID
	ID     model.RevisionID
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
type ReadGraphBranchRequest struct {
	Space  model.SpaceID
	ID     model.BranchID
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
type ApplyMemoryRequest struct {
	Space         model.SpaceID
	RequestID     model.RequestID
	Command       []byte
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type ReadMemoryRequest struct{ Space model.SpaceID }
type ApplyKnowledgeRequest struct {
	Space         model.SpaceID
	RequestID     model.RequestID
	Command       []byte
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type ReadKnowledgeRequest struct {
	Space     model.SpaceID
	Anchor    *model.SnapshotAnchor `json:"anchor,omitempty"`
	Knowledge *model.KnowledgeKey   `json:"knowledge,omitempty"`
}
