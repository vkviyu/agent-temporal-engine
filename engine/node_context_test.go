package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func contextContains(out api.NodeContext, section string, ref model.NodeRef) bool {
	for _, n := range out.Sections[section] {
		if n.Ref == ref {
			return true
		}
	}
	return false
}

func TestTypedNodeContextFollowsPublishedOriginalInputAndRecoveryReceipts(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	proof := model.ObjectID(integrationID('a'))
	authority := agentprofile.AuthorityBinding{Deployment: "context-host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "context-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: authority, Status: "active", Evidence: proof}}})
	o := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "context-adopt", o)
	inputRaw := []byte("system: exact original framing\nuser: original request\n")
	inputAck := integrationMutate(t, s.e, integrationRequest("context-input", integrationPut([]byte("context-input"), api.Value{Kind: api.ValueBytes, Data: inputRaw})))
	inputID, gotRaw := recoveryValueAt(t, s, inputAck.Fence, "context-input")
	if !reflect.DeepEqual(gotRaw, inputRaw) {
		t.Fatal("bad input fixture")
	}
	p, published := goalCorePacket(t, s, "context-packet", o.Key, o, []knowledge.Observation{o}, inputID, inputAck.Fence)
	before := nodeView(t, s.e, "host-principal", nil)
	spec := agentprofile.ActivitySpec{Activity: "context-activity", Execution: "one", Role: "resource_prepare", GoalEpoch: 1, InputFence: published.Fence, ContextPacket: p.ID, Request: inputID, RequestDigest: agentprofile.ExactRequestDigest(inputRaw), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	recoveryCoreControl(t, s, "context-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: authority}})
	recoveryCoreControl(t, s, "context-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Attempt: "context-attempt", Activity: spec.Activity, Sequence: 1, Authority: authority}})
	recoveryCoreControl(t, s, "context-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "context-attempt", Authority: authority, Evidence: proof}})
	result := agentprofile.ActivityResult{Capture: "full", ID: "context-result", Activity: spec.Activity, Attempt: "context-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("actual result bytes")}
	recoveryCoreControl(t, s, "context-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreControl(t, s, "context-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: authority, Evidence: proof}})
	message := agentprofile.Message{ID: "context-message", Sender: "one", Recipient: "two", GoalEpoch: 1, Payload: []byte("exact inter-agent message")}
	recoveryCoreControl(t, s, "context-send", agentprofile.ControlCommand{SendMessage: &agentprofile.SendMessageCommand{Message: message, Authority: authority}})
	fixed := nodeView(t, s.e, "host-principal", nil)
	activityRef := nodeRef("Activity", spec.Activity)
	req := api.NodeContextRequest{View: fixed.Ref, Ref: activityRef, Mode: "recorded_input", Limits: api.QueryLimits{MaxDepth: 4}}
	first, err := s.e.GetNodeContext(ctx, req)
	if err != nil || !contextContains(first, "inputs", model.NodeRef{Kind: model.NodeObject, Object: p.ID}) || !contextContains(first, "inputs", model.NodeRef{Kind: model.NodeObject, Object: inputID}) {
		t.Fatalf("original input context: %+v %v", first, err)
	}
	if !contextContains(first, "inputs", nodeRef("ActivitySpec", spec.Activity)) {
		t.Fatal("input lost its authenticated ActivitySpec link")
	}
	if _, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: before.Ref, Ref: activityRef, Mode: "recorded_input"}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("future activity leaked: %v", err)
	}
	// Both the current state and its profile snapshot change. The old request
	// remains linked by the immutable ActivitySpec, rather than live key lookup.
	integrationMutate(t, s.e, integrationRequest("context-overwrite", integrationPut([]byte("context-input"), api.Value{Kind: api.ValueBytes, Data: []byte("different later prompt")})))
	later := nodeView(t, s.e, "host-principal", nil)
	for _, view := range []api.ViewDescriptor{fixed, later} {
		d, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: inputID}})
		if err != nil || len(d.Data) == 0 {
			t.Fatalf("original envelope not retrievable: %+v %v", d, err)
		}
		packet, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: p.ID}})
		var decoded knowledge.ContextPacket
		if err != nil || json.Unmarshal(packet.Data, &decoded) != nil || !reflect.DeepEqual(decoded, p) || len(packet.Anchors) != 3 || packet.Anchors[2].Relation != "recorded_input" || packet.Anchors[2].Anchor.Fence != inputAck.Fence {
			t.Fatalf("packet/anchor changed: %+v %v", packet, err)
		}
	}
	again, err := s.e.GetNodeContext(ctx, req)
	if err != nil || !reflect.DeepEqual(again, first) {
		t.Fatal("fixed View context followed live state", err)
	}
	evidence, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: later.Ref, Ref: activityRef, Mode: "evidence_neighborhood", Sections: []string{"activityReceipts"}, Limits: api.QueryLimits{MaxDepth: 4}})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"Attempt", "ActivityResult", "ResultCapture", "ActivityAcceptance"} {
		id := "context-result"
		if kind == "Attempt" {
			id = "context-attempt"
		}
		if kind == "ActivityAcceptance" {
			id = spec.Activity
		}
		if !contextContains(evidence, "activityReceipts", nodeRef(kind, id)) {
			t.Fatalf("missing authentic %s receipt: %+v", kind, evidence)
		}
	}
	messages, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: later.Ref, Ref: nodeRef("Execution", "one"), Mode: "evidence_neighborhood", Sections: []string{"messages"}})
	if err != nil || !contextContains(messages, "messages", nodeRef("Message", message.ID)) {
		t.Fatalf("message partition: %+v %v", messages, err)
	}
	other := nodeView(t, s.e, "other-principal", nil)
	if _, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: other.Ref, Ref: activityRef, Mode: "recorded_input"}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatalf("other principal saw input association: %v", err)
	}
	// Continuations are bound to mode, section filter, depth and View.
	pageReq := req
	pageReq.Limits.MaxNodes = 1
	page, err := s.e.GetNodeContext(ctx, pageReq)
	if err != nil || !page.Truncated {
		t.Fatalf("first input page: %+v %v", page, err)
	}
	bad := pageReq
	bad.Sections = []string{"node", "inputs"}
	bad.Continuation = page.Continuation
	if _, err = s.e.GetNodeContext(ctx, bad); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatal("changed sections accepted old continuation", err)
	}
	seen := map[model.NodeRef]bool{}
	for {
		for _, n := range page.Sections["inputs"] {
			if seen[n.Ref] {
				t.Fatal("duplicate input page")
			}
			seen[n.Ref] = true
		}
		if !page.Truncated {
			break
		}
		pageReq.Continuation = page.Continuation
		page, err = s.e.GetNodeContext(ctx, pageReq)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !seen[model.NodeRef{Kind: model.NodeObject, Object: inputID}] {
		t.Fatal("input pagination omitted envelope")
	}
	if err = s.e.CloseView(ctx, api.CloseViewRequest{Ref: fixed.Ref}); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	reopened := nodeView(t, s.e, "host-principal", &later.Anchor)
	req.View = reopened.Ref
	if out, err := s.e.GetNodeContext(ctx, req); err != nil || !contextContains(out, "inputs", model.NodeRef{Kind: model.NodeObject, Object: inputID}) {
		t.Fatalf("reopen lost published input links: %+v %v", out, err)
	}
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	reopened = nodeView(t, s.e, "host-principal", &later.Anchor)
	req.View = reopened.Ref
	if out, err := s.e.GetNodeContext(ctx, req); err != nil || !contextContains(out, "inputs", model.NodeRef{Kind: model.NodeObject, Object: inputID}) {
		t.Fatalf("compaction lost original input: %+v %v", out, err)
	}
	// The original envelope is no longer in live domain state. Its explicit
	// profile link must still recheck current withdrawal policy at output.
	applyMemory(t, s.e, "context-input-erased", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: inputID, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "original request withdrawn"}}})
	beforeDenial := integrationSnapshot(t, s.e)
	if _, err := s.e.GetNodeContext(ctx, req); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatal("historical link bypassed current context denial", err)
	}
	if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: req.View, Ref: model.NodeRef{Kind: model.NodeObject, Object: inputID}}); !errors.Is(err, memory.ErrHistoryAccessDenied) {
		t.Fatal("historical envelope bypassed current disclosure denial", err)
	}
	if after := integrationSnapshot(t, s.e); after != beforeDenial {
		t.Fatal("context policy denial fenced or changed store")
	}
}

func TestTypedSummaryNodeDoesNotExpandUnadoptedOriginalSources(t *testing.T) {
	ctx := context.Background()
	s, summary := summaryCoreFixture(t)
	ack, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey("node-publish-summary"), Summary: summary})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatal(ack, err)
	}
	o := knowledgeObservation(summary.ID, ack.Fence, "two", 1)
	o.PayloadKind = "summary"
	o.ID = knowledge.ObservationID(o)
	_, adopted := applyObservation(t, s.e, "node-adopt-summary", o)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: adopted.Fence, Knowledge: &o.Key}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	ref := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "knowledge", Kind: "summary", ID: model.EntityID(summary.ID)}}
	detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref})
	var got knowledge.Summary
	if err != nil || detail.Encoding != "knowledge/Summary/v1+json" || json.Unmarshal(detail.Data, &got) != nil || !reflect.DeepEqual(got, summary) || len(detail.Sources) != len(knowledge.SummaryContentReferences(summary)) {
		t.Fatalf("adopted typed summary: %+v %v", detail, err)
	}
	for _, id := range append([]model.ObjectID{summary.ID}, knowledge.SummaryContentReferences(summary)...) {
		if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: id}}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("summary granted unadopted original %s: %v", id, err)
		}
	}
	out, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: ref, Mode: "as_known_at_anchor", Sections: []string{"upstream"}})
	if err != nil || len(out.Sections["upstream"]) != 0 || len(out.Gaps) != 1 || out.Gaps[0].Reason != "unavailable_in_view" {
		t.Fatalf("summary source boundary: %+v %v", out, err)
	}
}

func TestTypedNodeContextModesGapsAndKnowledgeIsolation(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t)
	o := knowledgeObservation(payload, source, "one", 1)
	_, ack := applyObservation(t, s.e, "typed-mode-adopt", o)
	key := o.Key
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence, Knowledge: &key}
	known, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: known.Ref})
	view := nodeView(t, s.e, "host-principal", nil)
	ref := model.NodeRef{Kind: model.NodeObject, Object: o.ID}
	out, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: known.Ref, Ref: ref, Mode: "as_known_at_anchor"})
	if err != nil || out.Coverage != "execution_knowledge_at_anchor" {
		t.Fatalf("known context: %+v %v", out, err)
	}
	for _, ss := range out.Sections {
		for _, n := range ss {
			if n.Ref.Kind == model.NodeEntity && n.Ref.Entity.Profile == "agent" {
				t.Fatal("known context merged global agent ledger")
			}
		}
	}
	if _, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: known.Ref, Ref: nodeRef("Execution", "one"), Mode: "as_known_at_anchor"}); !errors.Is(err, engine.ErrNodeNotFound) {
		t.Fatal("knowledge view revealed Execution", err)
	}
	for _, mode := range []string{"as_known_at_anchor", "retrospective"} {
		if _, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: nodeRef("Execution", "one"), Mode: mode}); !errors.Is(err, api.ErrInvalidRequest) {
			t.Fatalf("mode %s allowed wrong view: %v", mode, err)
		}
	}
	noInput, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: nodeRef("Task", "kt"), Mode: "recorded_input"})
	if err != nil || len(noInput.Gaps) != 1 || noInput.Gaps[0].Reason != "no_recorded_input" {
		t.Fatalf("missing record invented input: %+v %v", noInput, err)
	}
	audit, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "audit_snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: audit.Ref})
	if out, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: audit.Ref, Ref: nodeRef("Task", "kt"), Mode: "retrospective"}); err != nil || out.Coverage != "retrospective_at_view_fence" {
		t.Fatalf("explicit retrospective: %+v %v", out, err)
	}
	for _, sections := range [][]string{{"inputs", "inputs"}, {"unknown"}} {
		if _, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: nodeRef("Task", "kt"), Sections: sections}); !errors.Is(err, api.ErrInvalidRequest) {
			t.Fatalf("bad filter accepted %v: %v", sections, err)
		}
	}
	if _, err = s.e.GetNodeContext(ctx, api.NodeContextRequest{View: view.Ref, Ref: nodeRef("Task", "kt"), Mode: "evidence_neighborhood", Limits: api.QueryLimits{MaxBytes: 1}}); engine.ClassifyError(err) != api.ErrorResourceLimit {
		t.Fatal("context byte bound missing", err)
	}
	if _, err = s.e.Snapshot(ctx); err != nil {
		t.Fatal("invalid context request fenced engine", err)
	}
}

func TestTypedNodeStateDiffUsesExplicitStateParentAtCapturedFence(t *testing.T) {
	ctx := context.Background()
	s, base, left, _, _ := graphDivergence(t, false)
	view := nodeView(t, s.e, "host-principal", nil)
	ref := model.NodeRef{Kind: model.NodeRevision, Revision: left.ID}
	req := api.NodeContextRequest{View: view.Ref, Ref: ref, Mode: "evidence_neighborhood", Sections: []string{"stateDiff"}}
	out, err := s.e.GetNodeContext(ctx, req)
	if err != nil || len(out.StateChanges) != 1 || string(out.StateChanges[0].Key) != "a" || out.StateChanges[0].Before != "" || out.StateChanges[0].After != left.Entries[0].Value {
		t.Fatalf("explicit parent diff: %+v %v", out, err)
	}
	if len(base.Entries) != 0 {
		t.Fatal("invalid baseline fixture")
	}
	integrationMutate(t, s.e, integrationRequest("diff-later", integrationPut([]byte("a"), api.Value{Kind: api.ValueBytes, Data: []byte("latest differs")})))
	if again, err := s.e.GetNodeContext(ctx, req); err != nil || !reflect.DeepEqual(again, out) {
		t.Fatal("stateDiff used live domain", err)
	}
	// Returned state keys are caller-owned.
	out.StateChanges[0].Key[0] = 'z'
	if again, err := s.e.GetNodeContext(ctx, req); err != nil || string(again.StateChanges[0].Key) != "a" {
		t.Fatal("caller mutated state diff", err)
	}
}
