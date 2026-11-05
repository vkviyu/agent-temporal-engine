package api

import "github.com/vkviyu/agent-temporal-engine/model"

// These limits are part of the experimental core mutation schema. Agent-facing
// commands will use protected profile operations rather than this host API.
const (
	MaxStateKeyBytes   = 1024
	MaxStateValueBytes = 1 << 20
	MaxStateMutations  = 1024
	MaxStateBatchBytes = 4 << 20
	MethodMutateState  = "core.state.mutate/v1"
)

type ValueKind string

const (
	ValueNull  ValueKind = "null"
	ValueBytes ValueKind = "bytes"
)

// Value preserves null versus empty bytes. Absence is expressed by Found=false
// or a delete operation, never by a zero Value.
type Value struct {
	Kind ValueKind
	Data []byte
}

type MutationKind string

const (
	MutationPut    MutationKind = "put"
	MutationDelete MutationKind = "delete"
)

type ConditionKind string

const (
	ConditionAny    ConditionKind = ""
	ConditionAbsent ConditionKind = "absent"
	ConditionValue  ConditionKind = "value"
)

type ValueCondition struct {
	Kind  ConditionKind
	Value Value
}
type StateMutation struct {
	Key       []byte
	Kind      MutationKind
	Value     Value
	Condition ValueCondition
}
type StateBatch struct {
	ExpectedRoot *model.ObjectID
	Operations   []StateMutation
}

type MutateRequest struct {
	Key           model.RequestKey
	Batch         StateBatch
	ExpectedFence *model.CommitFence
}

type ReadStateRequest struct {
	Space model.SpaceID
	Key   []byte
}
type StateValueResult struct {
	Found     bool
	Value     Value
	StateRoot model.ObjectID
	RootSet   model.ObjectID
	Fence     model.CommitFence
}

type StateSnapshot struct {
	Roots   model.RootSet
	RootSet model.ObjectID
	Fence   model.CommitFence
}
