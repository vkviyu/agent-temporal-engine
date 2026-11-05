package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

type ApplyEmissionRequest struct {
	Space         model.SpaceID      `json:"space"`
	RequestID     model.RequestID    `json:"request_id"`
	Command       []byte             `json:"command"`
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type ReadEmissionRequest struct {
	Space  model.SpaceID         `json:"space"`
	ID     string                `json:"id"`
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
