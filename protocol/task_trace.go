package protocol

import (
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type ApplyTaskTraceRequest struct {
	Space     model.SpaceID   `json:"space"`
	RequestID model.RequestID `json:"request_id"`
	Edit      []byte          `json:"edit"`
}

type GetTaskTraceRequest struct {
	Space        model.SpaceID         `json:"space"`
	Subject      api.TraceSubject      `json:"subject"`
	Session      string                `json:"session,omitempty"`
	Anchor       *model.SnapshotAnchor `json:"anchor,omitempty"`
	Limit        int                   `json:"limit,omitempty"`
	Continuation string                `json:"continuation,omitempty"`
}

type GetTaskTraceEvidenceRequest struct {
	GetTaskTraceRequest
	From     model.EventID `json:"from"`
	To       model.EventID `json:"to"`
	Relation string        `json:"relation"`
}
