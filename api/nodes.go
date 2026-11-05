package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// SearchNodesRequest searches only visible summaries in one fixed View. Text
// is a literal, case-sensitive UTF-8 string, never a query language or a model
// prompt. SearchVersion currently supports summary-text/v1; TextField is label
// (the default) or kind, and Match is contains (the default), exact, or prefix.
type SearchNodesRequest struct {
	IncludeHidden bool
	View          model.ViewRef
	Layer         string
	EntityKind    model.EntityKind
	Text          string
	TextField     string
	Match         string
	// EventOperation filters typed event occurrences by their registered
	// attribution operation. It is valid only for the events layer.
	EventOperation string
	SearchVersion  string
	Limit          int
	Continuation   string
	Limits         QueryLimits
}

type NodeSearchMatch struct {
	Node    NodeSummary
	Reasons []string
}

type NodeSearchResult struct {
	Anchor        model.SnapshotAnchor
	Items         []NodeSearchMatch
	SearchVersion string
	// Coverage describes the scanned corpus, independently of page truncation.
	// complete_layer means every visible summary in the selected layer was
	// considered. A resource failure returns no partial result.
	Coverage     string
	Truncated    bool
	Continuation string
}

// Trace follows published projection relationships within the selected View.
// Edges always point from child/evidence consumer to parent/source. Direction
// controls traversal, not edge orientation. Supported relations are parent,
// source, revision_parent, causal_parent and stream_previous. Empty Relations
// selects all five; stream order is distinct from semantic causality.
// Both traverses ancestors and descendants independently, never an implicit
// route from an ancestor into siblings.
type TraceRequest struct {
	View         model.ViewRef
	Start        model.NodeRef
	Direction    string // parents (default), children, both
	Relations    []string
	Limits       QueryLimits
	Continuation string
}

type NodeTraceEdge struct {
	From     model.NodeRef
	To       model.NodeRef
	Relation string
}

type NodeTrace struct {
	Anchor model.SnapshotAnchor
	Start  model.NodeRef
	Nodes  []NodeSummary
	Edges  []NodeTraceEdge
	// DepthLimited reports a visible relationship beyond the requested depth.
	// Truncated instead reports remaining pages within that depth.
	DepthLimited bool
	Truncated    bool
	Continuation string
}

// NodeExplorer extends basic browsing without adding any execution or mutation
// authority. Backends may advertise this capability independently.
type NodeExplorer interface {
	SearchNodes(context.Context, SearchNodesRequest) (NodeSearchResult, error)
	Trace(context.Context, TraceRequest) (NodeTrace, error)
}

const (
	DefaultContentChunkBytes = 64 << 10
	MaxContentChunkBytes     = 256 << 10
)

// ReadContentRequest addresses an actual, visible codec Object. Synthetic
// Observation/ContextPacket/Revision projection IDs do not grant raw object
// access. payload returns the canonical object's payload; value_bytes returns
// the logical bytes of a core byte value. Length zero selects the default chunk.
// MaxBytes bounds the complete encoded response, including base64 and metadata.
type ReadContentRequest struct {
	View       model.ViewRef
	Ref        model.NodeRef
	Projection string
	Offset     uint64
	Length     uint32
	MaxBytes   int
}

type ContentRange struct {
	Ref           model.NodeRef
	Anchor        model.SnapshotAnchor
	ObjectType    string
	ObjectVersion uint64
	Projection    string
	Offset        uint64
	TotalBytes    uint64
	// Digest is SHA-256 of the complete selected projection, not of this
	// range and not a replacement for the codec ObjectID in Ref.
	Digest model.ObjectID
	Data   []byte
	EOF    bool
}

type ContentReader interface {
	ReadContent(context.Context, ReadContentRequest) (ContentRange, error)
}

// ReadDestination identifies one outbound delivery. The transport supplies
// these values from its authenticated connection and current request.
type ReadDestination struct {
	Principal model.PrincipalID
	Session   string
	Request   string
}

// ReadEmissionGrant releases one authorized chunk to its exact destination.
// A failed or repeated consumption returns no content.
type ReadEmissionGrant interface {
	Consume(ReadDestination, ReadContentRequest) (ContentRange, error)
}

// ContentEmissionReader prepares a chunk without releasing its bytes. The
// trusted transport consumes the grant at its outbound boundary.
type ContentEmissionReader interface {
	PrepareContentRead(context.Context, ReadContentRequest, ReadDestination) (ReadEmissionGrant, error)
}
