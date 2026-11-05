package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// QueryInvocation belongs to (Query, DecisionSequence), never a synthetic
// Execution. Activity names the stable tool operation; definition/version/args
// are immutable even if the host changes configuration during recovery.
type QueryInvocation struct {
	ID              string         `json:"id"`
	Activity        string         `json:"activity"`
	ProviderCallKey string         `json:"provider_call_key"`
	Tool            string         `json:"tool"`
	Definition      model.ObjectID `json:"definition"`
	ToolVersion     string         `json:"tool_version"`
	Arguments       []byte         `json:"arguments"`
	DispatchClass   string         `json:"dispatch_class"`
	AcceptedResult  string         `json:"accepted_result,omitempty"`
}

// QueryTurn protects one controller transition and the exact following action.
// ExpectedVersion is the QueryProgress version, distinct from Query.Version.
// An answer has no Next. Every other phase retains a complete request so a
// recovery controller can reconcile/retry it without asking a model to infer it.
type QueryTurn struct {
	ExpectedVersion uint64               `json:"expected_version"`
	Sequence        uint64               `json:"sequence"`
	Activity        string               `json:"activity,omitempty"`
	Invocations     []QueryInvocation    `json:"invocations,omitempty"`
	AdapterVersion  string               `json:"adapter_version"`
	Phase           string               `json:"phase"`
	Source          model.SnapshotAnchor `json:"source"`
	Next            *CommitRequest       `json:"next,omitempty"`
}

type QueryDecision struct {
	Sequence       uint64            `json:"sequence"`
	Activity       string            `json:"activity"`
	AcceptedResult string            `json:"accepted_result"`
	Invocations    []QueryInvocation `json:"invocations,omitempty"`
}

// QueryProgress stores controller position; the Query definition, Activities,
// Attempts and Results remain owned by the ordinary Agent ledger.
type QueryProgress struct {
	Query          string               `json:"query"`
	Version        uint64               `json:"version"`
	Sequence       uint64               `json:"sequence"`
	AdapterVersion string               `json:"adapter_version"`
	Phase          string               `json:"phase"`
	Source         model.SnapshotAnchor `json:"source"`
	Next           *CommitRequest       `json:"next,omitempty"`
	Decisions      []QueryDecision      `json:"decisions,omitempty"`
	Completed      bool                 `json:"completed"`
}

type ReadQueryProgressRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	Query     string
	Anchor    *model.SnapshotAnchor
}
type QueryProgressResult struct {
	Found    bool
	Progress QueryProgress
	Anchor   model.SnapshotAnchor
}
type QueryCommands interface {
	GetQueryProgress(context.Context, ReadQueryProgressRequest) (QueryProgressResult, error)
}
