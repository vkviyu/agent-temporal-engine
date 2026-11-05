package engine

import (
	"errors"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	systemSpace    model.SpaceID = "ate:system"
	valueType                    = "ate.value"
	commandType                  = "ate.command"
	operationsType               = "ate.stateops"
	receiptType                  = "ate.receipt"
	rootSetType                  = "ate.rootset"
)

var (
	ErrUnsupportedSchema = errors.New("unsupported core schema")
	ErrInvalidStateData  = errors.New("invalid core state data")
)

type stateChange struct {
	Key    []byte
	Before model.ObjectID
	After  model.ObjectID
}
type operationsRecord struct {
	Space      model.SpaceID
	BeforeRoot model.ObjectID
	AfterRoot  model.ObjectID
	Changes    []stateChange
}

// A receipt never includes its containing RootSet or final CommitDigest. Those
// are supplied by the verified transaction envelope, avoiding hash cycles.
type receiptRecord struct {
	Key           model.RequestKey
	RequestDigest model.ObjectID
	Command       model.ObjectID
	Operations    model.ObjectID
	TxID          model.TxID
	LSN           uint64
	StateRoot     model.ObjectID
}
