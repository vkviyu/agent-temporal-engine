package reference_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentInvocationAndContinuationContracts(t *testing.T) {
	h := newHarness(t)
	h.root()
	h.step("root", 1)
	want := oracle.NewControl()
	auth := binding(h.expected.Authorities["root"])
	fence := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digest("f"))}
	compare := func(name string) {
		t.Helper()
		got := h.actual.Snapshot()
		invs := map[string]oracle.InvocationContract{}
		actions := map[string]oracle.ReservedAction{}
		positions := map[string]oracle.ResumePosition{}
		for _, i := range got.Invocations {
			invs[i.ID] = oracle.InvocationContract{Execution: string(i.Execution), Result: i.AcceptedResult, Call: i.ProviderCallKey, Tool: i.Tool, Definition: string(i.ToolDefinition), Version: i.ToolVersion, Effect: i.DispatchClass, Arguments: string(i.Args), Step: i.Step, Status: string(i.Status), Outcome: string(i.Result)}
		}
		if r := got.Lifecycle.Recovery; r != nil {
			for _, a := range r.Actions {
				actions[a.Action.Key] = oracle.ReservedAction{Execution: string(a.Action.Owner), Request: string(a.Action.Request.RequestID), Method: a.Method, Body: string(a.ExactCommand), Authority: a.Authority.Holder, Generation: a.Action.Generation}
			}
		}
		for _, c := range got.Lifecycle.Continuations {
			p := c.Position
			k := ""
			if p.KnowledgeFence != (model.CommitFence{}) {
				k = fmt.Sprintf("%d/%s/%s", p.KnowledgeFence.LSN, p.KnowledgeKey.Execution, p.KnowledgeCursor.Revision)
			}
			next := map[string]uint64{}
			for _, a := range p.NextActions {
				next[a.Key] = a.Generation
			}
			positions[string(c.Execution)] = oracle.ResumePosition{Execution: string(c.Execution), Space: string(p.Space), Principal: string(p.Principal), Adapter: p.AdapterVersion, Knowledge: k, Step: p.Step, Version: c.Version, Next: next}
		}
		if !reflect.DeepEqual(invs, want.Invocations) || !reflect.DeepEqual(actions, want.Actions) || !reflect.DeepEqual(positions, want.Continuations) {
			t.Fatalf("%s observable control mismatch\n inv=%+v want=%+v\n actions=%+v want=%+v\n positions=%+v want=%+v", name, invs, want.Invocations, actions, want.Actions, positions, want.Continuations)
		}
	}
	apply := func(name string, expected func(*oracle.Control) error, c agentprofile.ControlCommand) {
		t.Helper()
		before, err := agentprofile.EncodeSnapshot(h.actual.Snapshot())
		if err != nil {
			t.Fatal(err)
		}
		werr := want.Batch(expected)
		h.sequence++
		_, gerr := h.actual.ApplyCommand(key(fmt.Sprintf("contract-%d", h.sequence)), h.control(c))
		if (werr == nil) != (gerr == nil) {
			t.Fatalf("%s acceptance oracle=%v actual=%v", name, werr, gerr)
		}
		if gerr != nil {
			after, _ := agentprofile.EncodeSnapshot(h.actual.Snapshot())
			if !bytes.Equal(before, after) {
				t.Fatal("rejected contract changed state")
			}
		}
		compare(name)
	}
	inv := agentprofile.Invocation{ID: "tool", Execution: "root", Step: 1, ProviderCallKey: "call", Tool: "delegate", ToolDefinition: digest("d"), ToolVersion: "delegate/v1", AcceptedResult: "model-result-root", DispatchClass: "pure", Args: []byte("exact args"), Attempt: 1, Status: agentprofile.InvocationPending}
	abstract := func(i agentprofile.Invocation) oracle.InvocationContract {
		return oracle.InvocationContract{Execution: string(i.Execution), Result: i.AcceptedResult, Call: i.ProviderCallKey, Tool: i.Tool, Definition: string(i.ToolDefinition), Version: i.ToolVersion, Effect: i.DispatchClass, Arguments: string(i.Args), Step: i.Step, Status: string(i.Status), Outcome: string(i.Result)}
	}
	record := func(i agentprofile.Invocation) agentprofile.ControlCommand {
		return agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: i, Authority: auth}}
	}
	apply("missing primary", func(c *oracle.Control) error { return c.Invoke(inv.ID, abstract(inv)) }, record(inv))
	h.primaryResult("root")
	want.AcceptPrimary("root", 1, inv.AcceptedResult)
	apply("accepted primary", func(c *oracle.Control) error { return c.Invoke(inv.ID, abstract(inv)) }, record(inv))
	for name, change := range map[string]func(*agentprofile.Invocation){"wrong result": func(i *agentprofile.Invocation) { i.AcceptedResult = "missing" }, "tool version": func(i *agentprofile.Invocation) { i.ToolVersion = "v2" }, "arguments": func(i *agentprofile.Invocation) { i.Args = []byte("changed") }, "effect": func(i *agentprofile.Invocation) { i.DispatchClass = "non_idempotent" }, "duplicate call": func(i *agentprofile.Invocation) { i.ID = "another" }} {
		bad := inv
		change(&bad)
		apply(name, func(c *oracle.Control) error { return c.Invoke(bad.ID, abstract(bad)) }, record(bad))
	}
	task := agentprofile.Task{ID: "child-task", Space: "space", Owner: "owner", Goal: "child", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	child := agentprofile.Execution{ID: "child", Task: task.ID, Parent: "root", Actor: "child", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	apply("spawn result", func(c *oracle.Control) error { return c.AcceptSpawn(inv.ID, "child") }, agentprofile.ControlCommand{SpawnChild: &agentprofile.SpawnChildCommand{Parent: "root", Invocation: inv.ID, Task: task, Execution: child, InheritedEpoch: 1, Compatibility: digest("a"), Authority: auth}})
	body := []byte("immutable next action")
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "next", Generation: 1, Request: key("reserved"), Owner: "root", Status: agentprofile.ActionPending, CommandDigest: agentprofile.ExactRequestDigest(body), InputFence: fence}, Method: "host/v1", ExactCommand: body, Authority: auth}
	a := oracle.ReservedAction{Execution: "root", Request: "reserved", Method: intent.Method, Body: string(body), Authority: auth.Holder, Generation: 1}
	apply("reserve", func(c *oracle.Control) error { return c.Reserve("next", a) }, agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: auth}})
	duplicate := intent
	duplicate.Action.Key = "alias"
	apply("request namespace already reserved", func(c *oracle.Control) error { return c.Reserve("alias", a) }, agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: duplicate, Authority: auth}})
	p := oracle.ResumePosition{Execution: "root", Space: "space", Principal: "owner", Adapter: "fixture/v1", Step: 1, Version: 1, Next: map[string]uint64{"next": 1}}
	position := agentprofile.Continuation{Execution: "root", Version: 1, Incarnation: 1, PendingKey: "next", PendingGeneration: 1, InputFence: fence, Phase: "dispatch", Position: &agentprofile.ContinuationPosition{Space: "space", Principal: "owner", Step: 1, AdapterVersion: p.Adapter, NextActions: []agentprofile.PendingActionRef{{Key: "next", Generation: 1}}}}
	save := func(c agentprofile.Continuation, expected uint64) agentprofile.ControlCommand {
		return agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: c, ExpectedVersion: expected, Authority: auth}}
	}
	apply("initial unseeded position", func(c *oracle.Control) error { return c.Save(p, 0) }, save(position, 0))
	position.Version = 2
	position.Position.KnowledgeFence = fence
	position.Position.KnowledgeKey = model.KnowledgeKey{Actor: "driver", Execution: "root"}
	position.Position.KnowledgeCursor = model.Cursor{Store: "store", Space: "space", Revision: model.RevisionID(digest("b"))}
	p.Version = 2
	p.Knowledge = fmt.Sprintf("1/root/%s", digest("b"))
	apply("bind historical knowledge", func(c *oracle.Control) error { return c.Save(p, 1) }, save(position, 1))
	bad := position
	bp := *position.Position
	bad.Position = &bp
	bad.Version = 3
	bp.KnowledgeFence = model.CommitFence{}
	bp.KnowledgeKey = model.KnowledgeKey{}
	bp.KnowledgeCursor = model.Cursor{}
	downgrade := p
	downgrade.Version = 3
	downgrade.Knowledge = ""
	apply("knowledge cannot disappear", func(c *oracle.Control) error { return c.Save(downgrade, 2) }, save(bad, 2))
	bp = *position.Position
	bad.Position = &bp
	bp.AdapterVersion = "changed/v2"
	adapter := p
	adapter.Version = 3
	adapter.Adapter = bp.AdapterVersion
	apply("adapter cannot change", func(c *oracle.Control) error { return c.Save(adapter, 2) }, save(bad, 2))
	raw, err := agentprofile.EncodeSnapshot(h.actual.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	h.actual, err = agentprofile.RestoreSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	compare("restored exact contracts")
}
