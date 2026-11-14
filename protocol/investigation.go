package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

type UpdateInvestigationRequest struct {
	Space     model.SpaceID   `json:"space"`
	RequestID model.RequestID `json:"request_id"`
	Command   []byte          `json:"command"`
}

type GetInvestigationRequest struct {
	Space  model.SpaceID         `json:"space"`
	ID     string                `json:"id"`
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
