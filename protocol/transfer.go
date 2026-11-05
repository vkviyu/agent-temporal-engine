package protocol

import (
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

type ExportTransferRequest struct {
	Space  model.SpaceID            `json:"space"`
	Anchor *model.SnapshotAnchor    `json:"anchor,omitempty"`
	Roots  []transfer.Reference     `json:"roots"`
	Omit   []model.ObjectID         `json:"omit,omitempty"`
	Policy transfer.RedactionPolicy `json:"policy"`
}
type ImportTransferRequest struct {
	Space          model.SpaceID      `json:"space"`
	RequestID      model.RequestID    `json:"request_id"`
	Manifest       []byte             `json:"manifest"`
	ApprovedDigest model.ObjectID     `json:"approved_digest"`
	ExpectedFence  *model.CommitFence `json:"expected_fence,omitempty"`
}
type ReadTransferRequest struct {
	Space  model.SpaceID         `json:"space"`
	ID     model.ObjectID        `json:"id"`
	Anchor *model.SnapshotAnchor `json:"anchor,omitempty"`
}
