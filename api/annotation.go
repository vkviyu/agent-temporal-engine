package api

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyAnnotation = "annotation.apply/v1"

type ApplyAnnotationRequest struct {
	Key           model.RequestKey
	Command       annotation.Record
	ExpectedFence *model.CommitFence
}

// ReadAnnotationsRequest reads only the authenticated owner's append-only
// judgments at a fixed publication. Selectors belong to individual provenance
// records, so Anchor must not carry a Cursor or Knowledge selector.
type ReadAnnotationsRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	Anchor    *model.SnapshotAnchor
}
type AnnotationsResult struct {
	Snapshot annotation.Snapshot
	Anchor   model.SnapshotAnchor
	Fence    model.CommitFence
	RootSet  model.ObjectID
}
type AnnotationCommands interface {
	ApplyAnnotation(context.Context, ApplyAnnotationRequest) (CommitResult, error)
	GetAnnotations(context.Context, ReadAnnotationsRequest) (AnnotationsResult, error)
}
