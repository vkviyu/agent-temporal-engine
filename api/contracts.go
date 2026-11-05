// Package api defines experimental v0 contracts shared by the embedded and
// local-service forms of ATE. It contains no storage or host application types.
package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

type ResultCode string

const (
	CodeVolatileAck        ResultCode = "volatile_ack"
	CodeVolatileNotFound   ResultCode = "volatile_not_found"
	CodeNotSubmitted       ResultCode = "not_submitted"
	CodeDurableAck         ResultCode = "durable_ack"
	CodeRejected           ResultCode = "rejected"
	CodeConflict           ResultCode = "conflict"
	CodeWaitInterrupted    ResultCode = "wait_interrupted"
	CodeCommitUnknown      ResultCode = "commit_unknown"
	CodeStoreFenced        ResultCode = "store_fenced"
	CodeCorruptionDetected ResultCode = "corruption_detected"
	CodeResourceLimit      ResultCode = "resource_limit"
	CodeNotFoundAtFence    ResultCode = "not_found_at_fence"
	CodeIndeterminate      ResultCode = "indeterminate"
	CodeInFlight           ResultCode = "in_flight"
)

type ConflictReason string

const (
	ConflictIdempotency  ConflictReason = "idempotency_conflict"
	ConflictPrecondition ConflictReason = "precondition_failed"
)

type CommitRequest struct {
	Space         model.SpaceID
	Principal     model.PrincipalID
	RequestID     model.RequestID
	Method        string
	Body          []byte
	ExpectedFence *model.CommitFence
}

type CommitResult struct {
	Code          ResultCode
	RequestID     model.RequestID
	TxID          model.TxID
	Fence         model.CommitFence
	RequestDigest model.ObjectID
	ResultDigest  model.ObjectID
	Conflict      ConflictReason
	// StateRoot is the command-selected result root: domain state for
	// core.state.mutate/v1, LiveControl for agent.apply/v1. RootSet binds all
	// five authorities at the same publication; readers must select by Method.
	StateRoot model.ObjectID
	RootSet   model.ObjectID
}

type LookupRequestRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	RequestID model.RequestID
}

type LookupRequestResult struct {
	Code   ResultCode
	Result CommitResult
	// ObservedFence is captured with the lookup. Only a durable implementation
	// can prove durable absence; reference uses explicit volatile result codes.
	ObservedFence model.CommitFence
}

type OpenViewRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	Anchor    *model.SnapshotAnchor
	Knowledge *model.KnowledgeKey
	Mode      string
}

type ViewDescriptor struct {
	Ref        model.ViewRef
	Anchor     model.SnapshotAnchor
	ViewDigest model.ObjectID
}

type CloseViewRequest struct{ Ref model.ViewRef }

type QueryLimits struct {
	MaxNodes int
	MaxEdges int
	MaxBytes int
	MaxDepth int
}

// ListNodes hides active owner annotations by default. IncludeHidden restores
// the complete authorized navigation corpus without changing audit facts.
type ListNodesRequest struct {
	IncludeHidden bool
	View          model.ViewRef
	Layer         string
	EntityKind    model.EntityKind
	Limit         int
	Continuation  string
	Limits        QueryLimits
}

type NodeSummary struct {
	Ref   model.NodeRef
	Label string
	Kind  model.EntityKind
}

type NodePage struct {
	Anchor       model.SnapshotAnchor
	Items        []NodeSummary
	Continuation string
	Truncated    bool
}

type GetNodeRequest struct {
	View  model.ViewRef
	Ref   model.NodeRef
	Limit QueryLimits
}

type NodeDetail struct {
	Ref     model.NodeRef
	Summary NodeSummary
	Payload model.ObjectID
	Sources []model.NodeRef
	// Data is an owned record projection. Encoding names its versioned schema;
	// it is never used to compute the underlying immutable object identity.
	Data     []byte
	Encoding string
	Anchor   model.SnapshotAnchor
	// Anchors are declared read/input positions. They do not restore authority
	// and do not imply that a control-only record has an executable Revision.
	Anchors []NodeAnchor `json:",omitempty"`
}

type NodeAnchor struct {
	Relation string
	Anchor   model.SnapshotAnchor
}

type NodeContextGap struct {
	Section string
	Reason  string
}

// NodeStateChange compares a Revision to its explicit StateParent. Empty
// Before/After means absent; these IDs are not an extra content grant.
const MaxNodeStateKeyBytes = 1024

type NodeStateChange struct {
	Key           []byte
	Before, After model.ObjectID
}

type NodeContextRequest struct {
	View         model.ViewRef
	Ref          model.NodeRef
	Mode         string
	Projection   string
	Limits       QueryLimits
	Continuation string
	// Sections filters typed context partitions. Empty uses the mode defaults.
	// Legacy neighbors/parents/children requests keep their original behavior.
	Sections []string `json:",omitempty"`
}

type NodeContext struct {
	Ref          model.NodeRef
	Anchor       model.SnapshotAnchor
	Sections     map[string][]NodeSummary
	Truncated    bool
	Continuation string
	Mode         string            `json:",omitempty"`
	Coverage     string            `json:",omitempty"`
	DepthLimited bool              `json:",omitempty"`
	Anchors      []NodeAnchor      `json:",omitempty"`
	Gaps         []NodeContextGap  `json:",omitempty"`
	StateChanges []NodeStateChange `json:",omitempty"`
}

type Committer interface {
	Commit(context.Context, CommitRequest) (CommitResult, error)
	LookupRequest(context.Context, LookupRequestRequest) (LookupRequestResult, error)
}

type ViewOpener interface {
	OpenView(context.Context, OpenViewRequest) (ViewDescriptor, error)
	CloseView(context.Context, CloseViewRequest) error
}

type ViewEntriesReader interface {
	GetViewEntries(context.Context, ReadViewEntriesRequest) (StateEntriesResult, error)
}

type BasicNodeReader interface {
	ListNodes(context.Context, ListNodesRequest) (NodePage, error)
	GetNode(context.Context, GetNodeRequest) (NodeDetail, error)
	GetNodeContext(context.Context, NodeContextRequest) (NodeContext, error)
}
