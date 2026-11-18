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

// ReadAgentStateRequest selects a full agent snapshot; there is no paged state
// response. A response exceeding MaxFrameBytes, including its JSON envelope,
// returns ResourceLimit with an empty object body and keeps the session open.
type ReadAgentStateRequest struct {
	Space  model.SpaceID         `json:"space"`
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}

// Principal is supplied only by the authenticated session.
type ReadAgentStateRecordsRequest struct {
	Space        model.SpaceID         `json:"space"`
	Anchor       *model.SnapshotAnchor `json:"anchor,omitempty"`
	Limit        int                   `json:"limit,omitempty"`
	Continuation string                `json:"continuation,omitempty"`
}
