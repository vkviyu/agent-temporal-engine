package engine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestRecoveryControlNodesPreserveTypedDecisionsAndRelationships(t *testing.T) {
	object := func(ch string) model.ObjectID { return model.ObjectID(strings.Repeat(ch, 64)) }
	fence := model.CommitFence{Store: "store", LSN: 5, CommitDigest: model.CommitDigest(object("a"))}
	anchor := model.SnapshotAnchor{Space: "s", Fence: fence}
	snapshot := agentprofile.Snapshot{Tasks: []agentprofile.Task{{ID: "t"}}, Executions: []agentprofile.Execution{{ID: "e", Task: "t"}}, Actions: []agentprofile.PendingAction{{Key: "pending", Owner: "e", Generation: 1}}, Lifecycle: agentprofile.LifecycleSnapshot{Queries: []agentprofile.Query{{ID: "q"}}, Activities: []agentprofile.Activity{{ID: "activity", Kind: "routing"}}, Attempts: []agentprofile.Attempt{{ID: "attempt", Activity: "activity"}}, Results: []agentprofile.ActivityResult{{ID: "result", Activity: "activity", Attempt: "attempt"}}}}
	snapshot.Lifecycle.Recovery = &agentprofile.RecoverySnapshot{
		Authorities:      []agentprofile.ExecutionAuthority{{Execution: "e", Version: 2, Status: "fenced", Evidence: object("f")}},
		ActivitySpecs:    []agentprofile.ActivitySpec{{Activity: "activity", Query: "q", Role: "routing", Request: object("b")}},
		ActivityControls: []agentprofile.ActivityControl{{Activity: "activity", AcceptedResult: "result"}},
		AttemptControls:  []agentprofile.AttemptControl{{Attempt: "attempt", Activity: "activity", Status: "effect_unknown"}},
		Captures:         []agentprofile.ResultCapture{{Result: "result", Quarantined: true}},
		Acceptances:      []agentprofile.ActivityAcceptance{{Activity: "activity", Result: "result", Version: 2}},
		Imports:          []agentprofile.ActivityAcceptance{{Activity: "activity", Result: "result", Version: 3}},
		Settlements:      []agentprofile.ActivitySettlement{{Activity: "activity", Version: 1, Status: "unknown"}, {Activity: "activity", Version: 2, Status: "settled"}},
		Reconciliations:  []agentprofile.AttemptReconciliation{{Attempt: "attempt", Version: 1, Outcome: "unresolved"}},
		Answers:          []agentprofile.QueryAnswer{{Query: "q", Slot: "answer/a", Kind: "progress", Body: []byte("exact recorded answer")}},
		Starts:           []agentprofile.WorkStart{{Query: "q", Slot: "intake/a", Task: "t", Execution: "e"}},
		TaskLinks:        []agentprofile.TaskLink{{Task: "t", ParentExecution: "e", InheritedTask: "t"}},
		Goals:            []agentprofile.GoalObservation{{Execution: "e", Epoch: 2}},
		Outcomes:         []agentprofile.TaskOutcome{{Task: "t", Execution: "e", Result: object("c"), Validation: object("d")}},
		Actions:          []agentprofile.ActionIntent{{Action: agentprofile.PendingAction{Key: "pending", Generation: 1, Owner: "e"}, Method: "execute/v1", ExactCommand: []byte{0, 1, 2}, InputObjects: []model.ObjectID{object("b")}}},
		Resolutions:      []agentprofile.ActionResolution{{Key: "pending", Generation: 1, Outcome: "unknown", ObservedFence: fence}},
		Resumes:          []agentprofile.ResumePlan{{Execution: "e", Next: agentprofile.AuthorityBinding{Incarnation: 2}, AdapterVersion: "adapter/v1", Checkpoint: object("e")}},
		Joins:            []agentprofile.ExecutionJoin{{ID: "join", Parent: "e", Members: []model.ExecutionID{"e"}, Status: "pending"}},
	}
	c := nodeCatalog{nodes: map[model.NodeRef]nodeProjection{}}
	if err := addProfileNodes(&c, snapshot, object("0"), anchor); err != nil {
		t.Fatal(err)
	}
	kinds := map[model.EntityKind]int{}
	for _, p := range c.nodes {
		kinds[p.detail.Summary.Kind]++
		if p.detail.Ref.Validate() != nil || p.detail.Encoding == "" || p.detail.Anchor.Fence != fence || !json.Valid(p.detail.Data) {
			t.Fatalf("invalid control detail=%+v", p.detail)
		}
	}
	for _, kind := range []model.EntityKind{"ExecutionAuthority", "ActivitySpec", "ActivityControl", "AttemptControl", "ResultCapture", "ActivityAcceptance", "ActivityResultImport", "AttemptReconciliation", "QueryAnswer", "WorkStart", "TaskLink", "GoalObservation", "TaskOutcome", "ActionIntent", "ActionResolution", "ResumePlan", "ExecutionJoin"} {
		if kinds[kind] != 1 {
			t.Fatalf("missing/duplicate %s projection: %d", kind, kinds[kind])
		}
	}
	if kinds["ActivitySettlement"] != 2 {
		t.Fatal("versioned settlement decisions collapsed")
	}
	capture := c.nodes[agentEntity("ResultCapture", "result")]
	var typed agentprofile.ResultCapture
	if err := json.Unmarshal(capture.detail.Data, &typed); err != nil || !typed.Quarantined || capture.detail.Summary.Label != "quarantined" {
		t.Fatalf("quarantine fact lost=%+v %v", typed, err)
	}
	activity := c.nodes[agentEntity("Activity", "activity")]
	if len(activity.parents) != 1 || activity.parents[0] != agentEntity("Query", "q") {
		t.Fatalf("query-owned activity attached to execution: %+v", activity.parents)
	}
	for _, source := range c.nodes[agentEntity("ExecutionAuthority", "e")].detail.Sources {
		if source.Kind == model.NodeObject && source.Object == object("f") {
			t.Fatal("external attestation digest became a content reference")
		}
	}
	if recoveryNodeID("a/b", "c") == recoveryNodeID("a", "b/c") {
		t.Fatal("composite control identity is ambiguous")
	}
}
