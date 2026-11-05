package agentprofile

import "github.com/vkviyu/agent-temporal-engine/codec"

// GoalKnowledge is the canonical version-1 content image an execution adopts
// before claiming that it has observed a changed task goal. The host publishes
// these bytes as content, then records a Knowledge Observation for that exact
// execution. Authority, status and cancellation remain current control state;
// changing them does not rewrite this immutable goal image.
//
// Format: domain || schema || TaskID || Space || Owner || GoalEpoch || Goal.
// Strings and the unsigned epoch use the shared canonical binary codec.
func GoalKnowledge(task Task) ([]byte, error) {
	if !commandText(string(task.ID)) || !commandText(string(task.Space)) || !commandText(string(task.Owner)) || !commandText(task.Goal) || task.GoalEpoch == 0 {
		return nil, ErrInvalid
	}
	b := codec.AppendUint([]byte("ATE-AGENT-GOAL\x00"), 1)
	for _, text := range []string{string(task.ID), string(task.Space), string(task.Owner)} {
		b, _ = codec.AppendString(b, text)
	}
	b = codec.AppendUint(b, task.GoalEpoch)
	b, _ = codec.AppendString(b, task.Goal)
	return b, nil
}
