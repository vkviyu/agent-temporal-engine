package host_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// answerCompletedQuery runs through the same public API in embedded and Unix
// SDK modes. A failed final model Attempt survives reopening while the already
// completed tasks, their validation evidence and all work Attempts stay fixed.
func (h *host) answerCompletedQuery(query agentprofile.Query, body []byte) {
	h.t.Helper()
	ctx := context.Background()
	before, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatal(err)
	}
	var outcomes []agentprofile.TaskOutcome
	for _, start := range before.Snapshot.Lifecycle.Recovery.Starts {
		if start.Query == query.ID {
			for _, outcome := range before.Snapshot.Lifecycle.Recovery.Outcomes {
				if outcome.Task == start.Task {
					outcomes = append(outcomes, outcome)
				}
			}
		}
	}
	if len(outcomes) == 0 {
		h.t.Fatal("final answer has no completed task outcome")
	}
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].Task < outcomes[j].Task })
	// This deterministic renderer explicitly places the exact outcomes into
	// the complete provider request. Its immutable witness names that policy.
	request, err := json.Marshal(struct {
		Query    string
		Outcomes []agentprofile.TaskOutcome
		Prompt   string
	}{query.ID, outcomes, "Explain the validated completed result"})
	if err != nil {
		h.t.Fatal(err)
	}
	input, inputAck := h.put("final-answer-request", "final-answer-request", request)
	packet := knowledge.ContextPacket{Space: "project", Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: inputAck.Fence}}, Renderer: "final-answer/v1", Tokenizer: "fixture/v1", Input: &knowledge.RecordedInput{Object: input, Anchor: model.SnapshotAnchor{Space: "project", Fence: inputAck.Fence}, Encoding: "application/json"}}
	packet.ID = knowledge.PacketID(packet)
	h.ack(h.peer.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("final-answer-packet"), Command: knowledge.Command{Packet: &packet}}))
	witness := agentprofile.FinalAnswerInput{SchemaVersion: 1, Query: query.ID, Outcomes: outcomes, ContextPacket: packet.ID, Request: input, RequestDigest: requestDigest(request), HostPolicyVersion: packet.Renderer}
	raw, err := agentprofile.EncodeFinalAnswerInput(witness)
	if err != nil {
		h.t.Fatal(err)
	}
	evidence, witnessAck := h.put("final-answer-input-evidence", "final-answer-input-evidence", raw)
	spec := agentprofile.ActivitySpec{Activity: "final-answer-model", Query: query.ID, Role: "query_decision", GoalEpoch: query.Version, InputFence: witnessAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: requestDigest(request), Recipient: "deterministic-local-provider", Effect: "pure", ResourceScope: "final-answer", FinalAnswerInput: evidence}
	reject := func(id string, c agentprofile.ControlCommand) {
		h.t.Helper()
		cut := h.snapshot()
		out, err := h.peer.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key(id), Command: agentprofile.Command{Control: &c}})
		if err == nil || out.Code == api.CodeDurableAck || h.snapshot() != cut {
			h.t.Fatalf("invalid final answer changed publication %s: %+v %v", id, out, err)
		}
	}
	reject("bare-final-answer", agentprofile.ControlCommand{AnswerQuery: &agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "final", Kind: "final", Final: true, ExpectedVersion: query.Version, Body: body}})
	forged := spec
	forged.Activity = "final-model-with-wrong-request"
	forged.RequestDigest = proof()
	reject("wrong-final-input", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: forged}})
	h.dispatch(spec, agentprofile.AuthorityBinding{})
	failed := agentprofile.ActivityResult{Capture: "full", ID: "failed-final-answer", Activity: spec.Activity, Attempt: spec.Activity + "-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "failure", Payload: []byte("provider failed after task completion")}
	h.control("failed-final-answer-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: failed}})
	answer := agentprofile.AnswerQueryCommand{Query: query.ID, Slot: "final", Kind: "final", Final: true, ExpectedVersion: query.Version, Body: body, GeneratedFrom: failed.ID, Outcomes: outcomes, InputEvidence: evidence}
	reject("unaccepted-final-answer", agentprofile.ControlCommand{AnswerQuery: &answer})
	h.reopen()
	pending, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatal(err)
	}
	sort.Slice(before.Snapshot.Tasks, func(i, j int) bool { return before.Snapshot.Tasks[i].ID < before.Snapshot.Tasks[j].ID })
	sort.Slice(pending.Snapshot.Tasks, func(i, j int) bool { return pending.Snapshot.Tasks[i].ID < pending.Snapshot.Tasks[j].ID })
	sort.Slice(before.Snapshot.Executions, func(i, j int) bool { return before.Snapshot.Executions[i].ID < before.Snapshot.Executions[j].ID })
	sort.Slice(pending.Snapshot.Executions, func(i, j int) bool { return pending.Snapshot.Executions[i].ID < pending.Snapshot.Executions[j].ID })
	if !reflect.DeepEqual(pending.Snapshot.Tasks, before.Snapshot.Tasks) || !reflect.DeepEqual(pending.Snapshot.Executions, before.Snapshot.Executions) || !reflect.DeepEqual(pending.Snapshot.Lifecycle.Recovery.Outcomes, before.Snapshot.Lifecycle.Recovery.Outcomes) {
		h.t.Fatal("failed final model changed completed task evidence")
	}
	for _, current := range pending.Snapshot.Lifecycle.Queries {
		if current.ID == query.ID && (current.Status != "open" || current.Version != query.Version) {
			h.t.Fatal("failed final model closed the query")
		}
	}
	for _, attempt := range before.Snapshot.Lifecycle.Attempts {
		found := false
		for _, current := range pending.Snapshot.Lifecycle.Attempts {
			found = found || reflect.DeepEqual(current, attempt)
		}
		if !found {
			h.t.Fatal("answer recovery modified a task attempt")
		}
	}
	h.control("retry-final-answer", agentprofile.ControlCommand{RetryActivity: &agentprofile.RetryActivityCommand{Activity: spec.Activity, ExpectedVersion: 1}})
	attempt := spec.Activity + "-retry-attempt"
	h.control("claim-final-answer-retry", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: attempt, Sequence: 2}})
	h.dispatchClaimed(spec, agentprofile.AuthorityBinding{}, attempt)
	result := agentprofile.ActivityResult{Capture: "full", ID: "accepted-final-answer", Activity: spec.Activity, Attempt: attempt, RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: body}
	h.control("capture-final-answer-retry", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	h.control("accept-final-answer-retry", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 2, Evidence: proof()}})
	answer.GeneratedFrom = result.ID
	wrong := answer
	wrong.InputEvidence = ""
	reject("answer-without-final-input-witness", agentprofile.ControlCommand{AnswerQuery: &wrong})
	wrong = answer
	wrong.Outcomes = nil
	// Once this Query has started a Task, omitting its TaskOutcome must not
	// downgrade the request to the independent Query-only completion path.
	reject("answer-without-completion-evidence", agentprofile.ControlCommand{AnswerQuery: &wrong})
	wrong = answer
	wrong.Body = []byte("unrecorded final body")
	reject("answer-with-changed-body", agentprofile.ControlCommand{AnswerQuery: &wrong})
	ack := h.control("answer-query", agentprofile.ControlCommand{AnswerQuery: &answer})
	h.reopen()
	if retry := h.control("answer-query", agentprofile.ControlCommand{AnswerQuery: &answer}); retry != ack {
		h.t.Fatal("lost answer response caused a new answer publication")
	}
	if _, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"}); err != nil {
		h.t.Fatal(err)
	}
}
