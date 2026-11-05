package agentprofile_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
)

func TestGoalKnowledgeIndependentCanonicalImage(t *testing.T) {
	task := agentprofile.Task{ID: "t", Space: "s", Owner: "p", GoalEpoch: 2, Goal: "go"}
	// Independently written literal: ASCII domain, schema 1, one-byte UTF-8
	// identity lengths, epoch 2, and the two-byte goal string.
	want, err := hex.DecodeString("4154452d4147454e542d474f414c00010174017301700202676f")
	if err != nil {
		t.Fatal(err)
	}
	got, err := agentprofile.GoalKnowledge(task)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("goal image=%x want=%x %v", got, want, err)
	}
	status := task
	status.Status = agentprofile.TaskCanceled
	status.CancelEpoch = 5
	again, err := agentprofile.GoalKnowledge(status)
	if err != nil || !bytes.Equal(got, again) {
		t.Fatal("live control contaminated immutable goal image")
	}
	for name, change := range map[string]func(*agentprofile.Task){"task": func(x *agentprofile.Task) { x.ID = "other" }, "space": func(x *agentprofile.Task) { x.Space = "other" }, "owner": func(x *agentprofile.Task) { x.Owner = "other" }, "epoch": func(x *agentprofile.Task) { x.GoalEpoch++ }, "goal": func(x *agentprofile.Task) { x.Goal = "different" }} {
		t.Run(name, func(t *testing.T) {
			other := task
			change(&other)
			raw, err := agentprofile.GoalKnowledge(other)
			if err != nil || bytes.Equal(raw, got) {
				t.Fatal("goal scope/version not committed", err)
			}
		})
	}
	bad := task
	bad.GoalEpoch = 0
	if _, err = agentprofile.GoalKnowledge(bad); err == nil {
		t.Fatal("zero goal epoch accepted")
	}
}
