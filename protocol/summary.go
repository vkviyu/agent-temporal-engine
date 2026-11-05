package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

type ApplySummaryRequest struct {
	Space         model.SpaceID
	RequestID     model.RequestID
	Summary       []byte
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}

type ReadSummaryRequest struct {
	Space  model.SpaceID
	ID     model.ObjectID
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
