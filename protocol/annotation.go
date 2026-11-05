package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

type ApplyAnnotationRequest struct {
	Space         model.SpaceID
	RequestID     model.RequestID
	Command       []byte
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}

type ReadAnnotationsRequest struct {
	Space  model.SpaceID
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
