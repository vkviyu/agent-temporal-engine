package protocol

import "github.com/vkviyu/agent-temporal-engine/model"

type ReadQueryProgressRequest struct {
	Space  model.SpaceID         `json:"space"`
	Query  string                `json:"query"`
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
