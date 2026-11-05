package engine

import (
	"bytes"
	"context"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func (e *Engine) agentActionReceipt(ctx context.Context, fence model.CommitFence, intent agentprofile.ActionIntent) (model.ObjectID, bool, error) {
	roots, _, err := e.rootsAtFence(ctx, fence, true)
	if err != nil {
		return "", false, err
	}
	return e.agentActionReceiptAtRoots(ctx, roots, intent)
}

func (e *Engine) agentActionReceiptAtRoots(ctx context.Context, roots model.RootSet, intent agentprofile.ActionIntent) (model.ObjectID, bool, error) {
	id, found, err := state.Get(ctx, e.index, systemSpace, roots.CommandReceipts, receiptKey(intent.Action.Request))
	if err != nil || !found {
		return "", found, err
	}
	object, err := readObject(ctx, e.index, id)
	if err != nil {
		return "", false, err
	}
	receipt, err := readReceipt(object)
	if err != nil {
		return "", false, err
	}
	object, err = readObject(ctx, e.index, receipt.Command)
	if err != nil {
		return "", false, err
	}
	command, err := readCommand(object)
	if err != nil {
		return "", false, err
	}
	if receipt.Key != intent.Action.Request || command.Key() != intent.Action.Request || command.Method != intent.Method || !bytes.Equal(command.Body, intent.ExactCommand) || !sameExpectedFence(command.ExpectedFence, intent.ExpectedFence) {
		return "", false, agentprofile.ErrConflict
	}
	return id, true, nil
}
