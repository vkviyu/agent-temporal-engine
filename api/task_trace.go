package api

import (
	"bytes"
	"context"
	"encoding/json"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyTaskTrace = "agent.trace.apply/v1"

const (
	MaxTaskTraceEditBytes  = 256 << 10
	MaxTaskTraceMilestones = 128
	MaxTaskTraceAssertions = 256
	MaxTraceEvidenceRefs   = 16
)

// TraceSubject selects a single request or task. The Space and authenticated
// Principal remain part of its identity.
type TraceSubject struct {
	Kind string // query or task
	ID   string
}

func (s TraceSubject) Validate() error {
	if (s.Kind != "query" && s.Kind != "task") || !validTraceText(s.ID, model.MaxIdentityBytes) {
		return ErrInvalidRequest
	}
	return nil
}

// TraceCitation is a node's address at a historical read position. A citation
// carries no authorization and does not assert a causal relationship.
type TraceCitation = investigation.Citation

type TraceMilestone struct {
	Event    model.EventID
	Label    string
	Evidence []TraceCitation
}

// TraceAssertion is a sourced interpretation. The engine verifies its cited
// records and scope. Only the published event graph establishes recorded causes.
type TraceAssertion struct {
	From, To model.EventID
	Relation string // supports, refutes, explains
	Reason   string
	Evidence []TraceCitation
}

// TaskTraceEdit replaces the selected milestones and interpretations as one
// revision. Version zero creates the record. Source is the task history cut
// used to choose the milestones. The authenticated commit key owns the edit.
type TaskTraceEdit struct {
	Subject         TraceSubject
	ExpectedVersion uint64
	Source          model.SnapshotAnchor
	Milestones      []TraceMilestone
	Assertions      []TraceAssertion
}

func validTraceText(value string, max int) bool {
	return len(value) != 0 && len(value) <= max && utf8.ValidString(value)
}

func (x TaskTraceEdit) Validate() error {
	if x.Subject.Validate() != nil || x.Source.Validate() != nil || x.Source.Cursor != nil || x.Source.Knowledge != nil {
		return ErrInvalidRequest
	}
	if len(x.Milestones) > MaxTaskTraceMilestones || len(x.Assertions) > MaxTaskTraceAssertions {
		return ErrInvalidRequest
	}
	citations := func(list []TraceCitation) bool {
		if len(list) == 0 || len(list) > MaxTraceEvidenceRefs {
			return false
		}
		for _, citation := range list {
			anchor := citation.Anchor
			if citation.Validate() != nil || anchor.Space != x.Source.Space || anchor.Fence.Store != x.Source.Fence.Store || anchor.Fence.LSN > x.Source.Fence.LSN {
				return false
			}
		}
		return true
	}
	nodes := make(map[model.EventID]bool, len(x.Milestones))
	for _, node := range x.Milestones {
		if !model.ValidDigest(string(node.Event)) || nodes[node.Event] || !validTraceText(node.Label, 2048) || !citations(node.Evidence) {
			return ErrInvalidRequest
		}
		nodes[node.Event] = true
	}
	edges := map[[3]string]bool{}
	for _, edge := range x.Assertions {
		key := [3]string{string(edge.From), string(edge.To), edge.Relation}
		if edge.From == edge.To || !nodes[edge.From] || !nodes[edge.To] || edges[key] || !validTraceText(edge.Reason, 4096) || !citations(edge.Evidence) {
			return ErrInvalidRequest
		}
		switch edge.Relation {
		case "supports", "refutes", "explains":
		default:
			return ErrInvalidRequest
		}
		edges[key] = true
	}
	return nil
}

func EncodeTaskTraceEdit(x TaskTraceEdit) ([]byte, error) {
	if err := x.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(x)
	if err != nil || len(raw) > MaxTaskTraceEditBytes {
		return nil, ErrInvalidRequest
	}
	return raw, nil
}

func DecodeTaskTraceEdit(raw []byte) (TaskTraceEdit, error) {
	var x TaskTraceEdit
	if len(raw) > MaxTaskTraceEditBytes || json.Unmarshal(raw, &x) != nil {
		return x, ErrInvalidRequest
	}
	canonical, err := EncodeTaskTraceEdit(x)
	if err != nil || !bytes.Equal(raw, canonical) {
		return TaskTraceEdit{}, ErrInvalidRequest
	}
	return x, nil
}

type ApplyTaskTraceRequest struct {
	Key  model.RequestKey
	Edit TaskTraceEdit
}

// TaskTraceRequest scopes overview and evidence paging to a published cut.
// Overview continuation uses a stable LSN/ordinal/event position; every
// continuation requires the identical Anchor and query selectors.
type TaskTraceRequest struct {
	Space        model.SpaceID
	Principal    model.PrincipalID
	Subject      TraceSubject
	Session      string // optional consistency check for a Query from a past session
	Anchor       *model.SnapshotAnchor
	Limit        int
	Continuation string
}

type TaskTraceNode struct {
	At        TraceCitation
	Operation string
	Subject   model.EntityKey
	Execution model.ExecutionID
	Task      model.EntityID
	Query     string
	Session   string
	// Boundary identifies the single causal predecessor outside this trace.
	Boundary bool            `json:",omitempty"`
	Label    string          `json:",omitempty"`
	Evidence []TraceCitation `json:",omitempty"`
}

// TaskTraceEdge points from a predecessor to a dependent. "causal" comes
// from recorded dependencies and may span omitted events. Other relations
// are host assertions and include the actual cited evidence and explanation.
// Boundary marks a causal edge whose From is outside the selected subject.
type TaskTraceEdge struct {
	From, To model.EventID
	Relation string
	Boundary bool            `json:",omitempty"`
	Reason   string          `json:",omitempty"`
	Evidence []TraceCitation `json:",omitempty"`
}

type TaskTraceResult struct {
	Subject      TraceSubject
	Anchor       model.SnapshotAnchor
	Version      uint64
	Nodes        []TaskTraceNode
	Edges        []TaskTraceEdge
	Continuation string `json:",omitempty"`
}

type TaskTraceEvidenceRequest struct {
	TaskTraceRequest
	From, To model.EventID
	Relation string
}

type TaskTraceEvidenceResult struct {
	Anchor       model.SnapshotAnchor
	Events       []TaskTraceNode
	Citations    []TraceCitation `json:",omitempty"`
	Continuation string          `json:",omitempty"`
}

type TaskTraceCommands interface {
	ApplyTaskTrace(context.Context, ApplyTaskTraceRequest) (CommitResult, error)
	GetTaskTrace(context.Context, TaskTraceRequest) (TaskTraceResult, error)
	GetTaskTraceEvidence(context.Context, TaskTraceEvidenceRequest) (TaskTraceEvidenceResult, error)
}
