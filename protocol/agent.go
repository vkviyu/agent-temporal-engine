package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

// ApplyAgentRequest carries the profile's exact canonical command bytes.
// Principal is deliberately absent: the authenticated connection supplies it.
type ApplyAgentRequest struct {
	Space         model.SpaceID      `json:"space"`
	RequestID     model.RequestID    `json:"request_id"`
	Command       []byte             `json:"command"`
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}

type ReadAgentStateRequest struct {
	Space  model.SpaceID         `json:"space"`
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
