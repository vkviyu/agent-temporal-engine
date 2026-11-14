package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Both transports use the actual Store. Embedded calls repeat the same durable
// request, so every assertion compares one publication rather than independent
// stores with different StoreIDs and commit digests.
func TestWorkSummaryEmissionEmbeddedSDKConformance(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			core, path := nativeAgentStore(t, true)
			server, owner, other := conformanceClients(t, core, transport)
			binding := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
			proof := model.ObjectID(strings.Repeat("a", 64))
			key := func(id string) model.RequestKey {
				return model.RequestKey{Space: "s", Principal: "owner", RequestID: model.RequestID(id)}
			}
			control := func(id string, command agentprofile.ControlCommand) {
				t.Helper()
				ack, err := core.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key(id), Command: agentprofile.Command{Control: &command}})
				domainAck(t, ack, err)
			}
			put := func(id, name string, data []byte) (model.ObjectID, api.CommitResult) {
				t.Helper()
				ack, err := core.Mutate(ctx, api.MutateRequest{Key: key(id), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte(name), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: data}}}}})
				domainAck(t, ack, err)
				v, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner"})
				if err != nil {
					t.Fatal(err)
				}
				entries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: v.Ref})
				if err != nil {
					t.Fatal(err)
				}
				if err := core.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref}); err != nil {
					t.Fatal(err)
				}
				for _, item := range entries.Items {
					if string(item.Key) == name {
						return item.Value, ack
					}
				}
				t.Fatal("fixture value missing")
				return "", ack
			}
			fence, err := core.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			step := func(c agentprofile.ControlCommand) api.WorkStep {
				t.Helper()
				s, err := api.AgentWorkStep(agentprofile.Command{Control: &c})
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			body, err := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte("next"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("exact action")}}}})
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(body)
			task := agentprofile.Task{ID: "task", Space: "s", Owner: "owner", Goal: "retain evidence", GoalEpoch: 1, Status: agentprofile.TaskOpen}
			x := agentprofile.Execution{ID: "root", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
			initialInput := []byte("preserve my exact question")
			goalImage, err := agentprofile.GoalKnowledge(task)
			if err != nil {
				t.Fatal(err)
			}
			queryPayload, _ := put("initial-query", "initial-query", initialInput)
			goalPayload, initialSource := put("initial-goal", "initial-goal", goalImage)
			fence, err = core.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seedStep := func(payload model.ObjectID, sequence uint64) api.WorkStep {
				t.Helper()
				o := domainObservation(payload, initialSource.Fence, model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}, sequence)
				step, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &o})
				if err != nil {
					t.Fatal(err)
				}
				return step
			}
			intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "first", Owner: x.ID, Generation: 1, Status: agentprofile.ActionPending, Request: key("first-action"), InputFence: fence.Fence, CommandDigest: model.ObjectID(hex.EncodeToString(digest[:]))}, Method: api.MethodMutateState, ExactCommand: body, Authority: binding}
			start := api.ApplyWorkRequest{Key: key("start"), Unit: api.WorkUnit{Kind: "query_start", Query: "q", Steps: []api.WorkStep{
				step(agentprofile.ControlCommand{StartWork: &agentprofile.StartWorkCommand{Query: "q", Slot: "root", ExpectedQueryVersion: 1, Task: task, Execution: x}}),
				step(agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: x.ID, Status: "active", Binding: binding, Evidence: proof}}}),
				seedStep(queryPayload, 1), seedStep(goalPayload, 2),
				step(agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: agentprofile.Continuation{Execution: x.ID, Version: 1, Incarnation: 1, PendingKey: "first", PendingGeneration: 1, InputFence: fence.Fence, Phase: "pending-render", Position: &agentprofile.ContinuationPosition{Space: "s", Principal: "owner", AdapterVersion: "service/v1", NextActions: []agentprofile.PendingActionRef{{Key: "first", Generation: 1}}}}, Authority: binding}}),
			}}}
			startBody, err := api.EncodeWorkUnit(start.Unit)
			if err != nil {
				t.Fatal(err)
			}
			query := agentprofile.Query{ID: "q", Session: "session", Space: "s", Owner: "owner", Input: initialInput, InputFence: fence.Fence, Status: "open", Version: 1}
			intake := api.ApplyWorkRequest{Key: key("intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: query.ID, Intake: &api.QueryIntake{Query: query, Route: "driver/v1", Configuration: []byte(`{"model":"fixture"}`), Next: api.CommitRequest{Space: "s", Principal: "owner", RequestID: start.Key.RequestID, Method: api.MethodApplyWork, Body: startBody}}}}
			workRead := api.ReadQueryIntakeRequest{Space: "s", Principal: "owner", Query: "q"}
			assertReadEqual(t, func() (api.QueryIntakeResult, error) { return core.GetQueryIntake(ctx, workRead) }, func() (api.QueryIntakeResult, error) { return owner.GetQueryIntake(ctx, workRead) })
			intakeAck, err := owner.ApplyWork(ctx, intake)
			domainAck(t, intakeAck, err)
			assertAckEqual(t, intakeAck, func() (api.CommitResult, error) { return core.ApplyWork(ctx, intake) })
			pending, err := owner.GetQueryIntake(ctx, workRead)
			if err != nil || !pending.Found || pending.State.Started || !reflect.DeepEqual(pending.State.Intake, *intake.Unit.Intake) {
				t.Fatalf("pending=%+v %v", pending, err)
			}
			startAck, err := owner.ApplyWork(ctx, start)
			domainAck(t, startAck, err)
			assertAckEqual(t, startAck, func() (api.CommitResult, error) { return core.ApplyWork(ctx, start) })
			seedRead := api.ReadAgentStateRequest{Space: "s", Principal: "owner", Anchor: &model.SnapshotAnchor{Space: "s", Fence: startAck.Fence}}
			assertReadEqual(t, func() (api.AgentStateResult, error) { return core.GetAgentState(ctx, seedRead) }, func() (api.AgentStateResult, error) { return owner.GetAgentState(ctx, seedRead) })
			seedState, err := owner.GetAgentState(ctx, seedRead)
			if err != nil || len(seedState.Snapshot.Lifecycle.Continuations) != 1 {
				t.Fatal(seedState, err)
			}
			seedSource := seedState.Snapshot.Lifecycle.Continuations[0].Position.KnowledgeSource
			if seedSource == nil || seedSource.ProducerTx != startAck.TxID {
				t.Fatalf("SDK lost exact seed producer: %+v", seedSource)
			}
			assertReadEqual(t, func() (api.SourcePublication, error) {
				return core.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "owner", Source: *seedSource})
			}, func() (api.SourcePublication, error) {
				return owner.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "owner", Source: *seedSource})
			})

			pastWork := workRead
			pastWork.Anchor = &pending.Anchor
			assertReadEqual(t, func() (api.QueryIntakeResult, error) { return core.GetQueryIntake(ctx, pastWork) }, func() (api.QueryIntakeResult, error) { return owner.GetQueryIntake(ctx, pastWork) })
			changed := intake
			clone := *intake.Unit.Intake
			clone.Route = "different/v1"
			changed.Unit.Intake = &clone
			conflict, err := owner.ApplyWork(ctx, changed)
			if err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
				t.Fatalf("work conflict=%+v %v", conflict, err)
			}
			assertAckEqual(t, conflict, func() (api.CommitResult, error) { return core.ApplyWork(ctx, changed) })

			// Authenticate the summary's real packet, accepted auxiliary Activity,
			// and exact request/output bytes before testing its SDK publication.
			payload, source := put("source", "evidence", []byte("verified evidence"))
			knowledgeKey := model.KnowledgeKey{Actor: x.Actor, Execution: x.ID}
			observation := domainObservation(payload, source.Fence, knowledgeKey, 3)
			ack, err := core.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("observe"), Command: knowledge.Command{Observation: &observation}})
			domainAck(t, ack, err)
			request := []byte("summarize the adopted evidence")
			input, inputAck := put("summary-input", "summary-input", request)
			known, err := core.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "s", Principal: "owner", Knowledge: &knowledgeKey})
			if err != nil || known.Revision == nil {
				t.Fatalf("knowledge=%+v %v", known, err)
			}
			anchor := known.Anchor.Clone()
			anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: "s", Revision: known.Revision.ID}
			view, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner", Mode: "execution_knowledge", Anchor: &anchor})
			if err != nil {
				t.Fatal(err)
			}
			packet := knowledge.ContextPacket{Space: "s", Key: &knowledgeKey, Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: view.ViewDigest, Frontier: known.Frontier}, GoalRef: payload, Items: []knowledge.ContextItem{{Observation: observation.ID, Content: payload, Label: "evidence"}}, Renderer: "summary/v1", Tokenizer: "fixture/v1", Input: &knowledge.RecordedInput{Object: input, Anchor: model.SnapshotAnchor{Space: "s", Fence: inputAck.Fence}, Encoding: "text/plain"}}
			packet.ID = knowledge.PacketID(packet)
			if err := core.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
				t.Fatal(err)
			}
			packetAck, err := core.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("packet"), Command: knowledge.Command{Packet: &packet}})
			domainAck(t, packetAck, err)
			spec := agentprofile.ActivitySpec{Activity: "summary", Execution: x.ID, Role: "aux_summary", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: "fixture-model/v1", Effect: "pure", ResourceScope: "session"}
			control("summary-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding}})
			control("summary-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "attempt", Sequence: 1, Authority: binding}})
			summaryGrant := api.EmissionSpec{
				ID: "summary-grant", Execution: x.ID, Task: task.ID, GoalEpoch: spec.GoalEpoch,
				Authority: binding, AuthorityVersion: 1, Content: packet.ID, ContentKind: "packet",
				Anchor: model.SnapshotAnchor{Space: "s", Fence: packetAck.Fence}, Projection: "value_bytes", Length: uint64(len(request)),
				Destination: spec.Recipient, Activity: spec.Activity, Attempt: "attempt", RequestDigest: spec.RequestDigest,
			}
			summaryIssue := api.ApplyEmissionRequest{Key: key("summary-issue"), Command: api.EmissionCommand{Issue: &summaryGrant}}
			summaryIssueAck, err := owner.ApplyEmission(ctx, summaryIssue)
			domainAck(t, summaryIssueAck, err)
			assertAckEqual(t, summaryIssueAck, func() (api.CommitResult, error) { return core.ApplyEmission(ctx, summaryIssue) })
			dispatch := agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: summaryGrant.Attempt, Authority: binding, Evidence: proof}}
			direct := api.ApplyAgentRequest{Key: key("summary-direct-dispatch"), Command: agentprofile.Command{Control: &dispatch}}
			directResult, err := owner.ApplyAgent(ctx, direct)
			remoteCode(t, err, api.ErrorConflict)
			if directResult.Code != api.CodeRejected {
				t.Fatalf("direct summary dispatch: %+v", directResult)
			}
			lookup, err := owner.LookupRequest(ctx, api.LookupRequestRequest{Space: direct.Key.Space, RequestID: direct.Key.RequestID})
			if err != nil || lookup.Code != api.CodeNotFoundAtFence || lookup.ObservedFence != summaryIssueAck.Fence {
				t.Fatalf("direct summary dispatch changed publication or left a receipt: %+v %v", lookup, err)
			}
			summaryClaim, err := api.EmissionWorkStep(api.EmissionCommand{ClaimID: summaryGrant.ID, ClaimVersion: 1, ClaimAuthority: binding})
			if err != nil {
				t.Fatal(err)
			}
			summaryDispatch := api.ApplyWorkRequest{Key: key("summary-dispatch"), Unit: api.WorkUnit{Kind: "emission_dispatch", Steps: []api.WorkStep{
				summaryClaim,
				step(dispatch),
			}}}
			summaryDispatchAck, err := owner.ApplyWork(ctx, summaryDispatch)
			domainAck(t, summaryDispatchAck, err)
			assertAckEqual(t, summaryDispatchAck, func() (api.CommitResult, error) { return core.ApplyWork(ctx, summaryDispatch) })
			outputBytes := []byte("retain the verified evidence")
			result := agentprofile.ActivityResult{Capture: "full", ID: "result", Activity: spec.Activity, Attempt: "attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: outputBytes}
			control("summary-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
			control("summary-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: binding, Evidence: proof}})
			output, outputAck := put("summary-output", "summary-output", outputBytes)
			stamp := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
			summary := knowledge.Summary{Space: "s", Source: packet.Source, Sources: []knowledge.SummarySource{{Observation: observation.ID, Content: payload}}, Topics: []knowledge.SummaryTopic{{Name: "evidence", Observations: []model.ObjectID{observation.ID}}}, Applicability: knowledge.SummaryApplicability{SchemaVersion: 1, Goal: task.Goal}, Confidence: 700, Generation: knowledge.SummaryGeneration{Activity: spec.Activity, Attempt: result.Attempt, Result: result.ID, Model: spec.Recipient, PromptVersion: "prompt/v1", PolicyVersion: "policy/v1", Input: knowledge.RecordedContent{Object: input, Anchor: packet.Input.Anchor, Encoding: packet.Input.Encoding}, Output: knowledge.RecordedContent{Object: output, Anchor: model.SnapshotAnchor{Space: "s", Fence: outputAck.Fence}, Encoding: "text/plain"}}, GeneratedAt: stamp, VisibleAt: stamp, Validation: knowledge.SummaryValidation{Status: "unreviewed"}}
			summary.SourceSetDigest = knowledge.SummarySourceSetDigest(summary.Source, summary.Sources)
			summary.ID = knowledge.SummaryID(summary)
			summaryRequest := api.ApplySummaryRequest{Key: key("publish-summary"), Summary: summary}
			summaryRead := api.ReadSummaryRequest{Space: "s", Principal: "owner", ID: summary.ID}
			assertReadEqual(t, func() (api.SummaryResult, error) { return core.GetSummary(ctx, summaryRead) }, func() (api.SummaryResult, error) { return owner.GetSummary(ctx, summaryRead) })
			summaryAck, err := owner.ApplySummary(ctx, summaryRequest)
			domainAck(t, summaryAck, err)
			assertAckEqual(t, summaryAck, func() (api.CommitResult, error) { return core.ApplySummary(ctx, summaryRequest) })
			pastSummary := summaryRead
			pastSummary.Anchor = &model.SnapshotAnchor{Space: "s", Fence: outputAck.Fence}
			assertReadEqual(t, func() (api.SummaryResult, error) { return core.GetSummary(ctx, pastSummary) }, func() (api.SummaryResult, error) { return owner.GetSummary(ctx, pastSummary) })
			changedSummary := summaryRequest
			changedSummary.Summary = summary.Clone()
			changedSummary.Summary.Generation.PromptVersion = "other/v1"
			changedSummary.Summary.ID = knowledge.SummaryID(changedSummary.Summary)
			conflict, err = owner.ApplySummary(ctx, changedSummary)
			if err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
				t.Fatalf("summary conflict=%+v %v", conflict, err)
			}
			assertAckEqual(t, conflict, func() (api.CommitResult, error) { return core.ApplySummary(ctx, changedSummary) })
			staleSummary := summaryRequest
			staleSummary.Key = key("stale-summary-fence")
			staleSummary.ExpectedFence = &source.Fence
			cas, err := owner.ApplySummary(ctx, staleSummary)
			if err != nil || cas.Code != api.CodeConflict || cas.Conflict != api.ConflictPrecondition {
				t.Fatalf("summary CAS=%+v %v", cas, err)
			}
			assertAckEqual(t, cas, func() (api.CommitResult, error) { return core.ApplySummary(ctx, staleSummary) })

			grant := api.EmissionSpec{ID: "grant", Execution: x.ID, Task: task.ID, GoalEpoch: 1, Authority: binding, AuthorityVersion: 1, Content: payload, Anchor: model.SnapshotAnchor{Space: "s", Fence: source.Fence}, Projection: "value_bytes", Offset: 1, Length: 5, Destination: "local-test-sink"}
			emissionRequest := api.ApplyEmissionRequest{Key: key("issue-emission"), Command: api.EmissionCommand{Issue: &grant}}
			emissionRead := api.ReadEmissionRequest{Space: "s", Principal: "owner", ID: grant.ID}
			assertReadEqual(t, func() (api.EmissionResult, error) { return core.GetEmission(ctx, emissionRead) }, func() (api.EmissionResult, error) { return owner.GetEmission(ctx, emissionRead) })
			emissionAck, err := owner.ApplyEmission(ctx, emissionRequest)
			domainAck(t, emissionAck, err)
			assertAckEqual(t, emissionAck, func() (api.CommitResult, error) { return core.ApplyEmission(ctx, emissionRequest) })
			issued, err := owner.GetEmission(ctx, emissionRead)
			fullDigest, rangeDigest := sha256.Sum256([]byte("verified evidence")), sha256.Sum256([]byte("erifi"))
			if err != nil || issued.Record.Proof == nil || issued.Record.Proof.Projection != "value_bytes" || issued.Record.Proof.TotalBytes != uint64(len("verified evidence")) || string(issued.Record.Proof.ContentDigest) != hex.EncodeToString(fullDigest[:]) || string(issued.Record.Proof.RangeDigest) != hex.EncodeToString(rangeDigest[:]) {
				t.Fatalf("SDK emission proof does not bind exact projected bytes: %+v %v", issued, err)
			}
			claim := api.ApplyEmissionRequest{Key: key("claim-emission"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: binding}}
			claimAck, err := owner.ApplyEmission(ctx, claim)
			domainAck(t, claimAck, err)
			assertAckEqual(t, claimAck, func() (api.CommitResult, error) { return core.ApplyEmission(ctx, claim) })
			pastEmission := emissionRead
			pastEmission.Anchor = &model.SnapshotAnchor{Space: "s", Fence: emissionAck.Fence}
			assertReadEqual(t, func() (api.EmissionResult, error) { return core.GetEmission(ctx, pastEmission) }, func() (api.EmissionResult, error) { return owner.GetEmission(ctx, pastEmission) })
			staleClaim := claim
			staleClaim.Key = key("stale-claim")
			rejected, remoteErr := owner.ApplyEmission(ctx, staleClaim)
			localRejected, localErr := core.ApplyEmission(ctx, staleClaim)
			if localErr == nil || rejected != localRejected || rejected.Code != api.CodeRejected {
				t.Fatalf("emission rejection remote=%+v %v embedded=%+v %v", rejected, remoteErr, localRejected, localErr)
			}
			remoteCode(t, remoteErr, core.ClassifyError(localErr))
			for _, read := range []func() error{
				func() error {
					_, err := owner.GetQueryIntake(ctx, api.ReadQueryIntakeRequest{Space: "foreign", Query: "q"})
					return err
				},
				func() error {
					_, err := owner.GetSummary(ctx, api.ReadSummaryRequest{Space: "foreign", ID: summary.ID})
					return err
				},
				func() error {
					_, err := owner.GetEmission(ctx, api.ReadEmissionRequest{Space: "foreign", ID: grant.ID})
					return err
				},
			} {
				remoteCode(t, read(), api.ErrorUnauthorized)
			}
			for _, read := range []func() error{
				func() error {
					_, err := owner.GetQueryIntake(ctx, api.ReadQueryIntakeRequest{Space: "s", Principal: "other", Query: "q"})
					return err
				},
				func() error {
					_, err := owner.GetSummary(ctx, api.ReadSummaryRequest{Space: "s", Principal: "other", ID: summary.ID})
					return err
				},
				func() error {
					_, err := owner.GetEmission(ctx, api.ReadEmissionRequest{Space: "s", Principal: "other", ID: grant.ID})
					return err
				},
			} {
				if err := read(); err != client.ErrPrincipal {
					t.Fatalf("SDK accepted principal spoof: %v", err)
				}
			}

			check := func(c *client.Client, other *client.Client, core *ate.Store) {
				t.Helper()
				assertReadEqual(t, func() (api.QueryIntakeResult, error) { return core.GetQueryIntake(ctx, workRead) }, func() (api.QueryIntakeResult, error) { return c.GetQueryIntake(ctx, workRead) })
				assertReadEqual(t, func() (api.SummaryResult, error) { return core.GetSummary(ctx, summaryRead) }, func() (api.SummaryResult, error) { return c.GetSummary(ctx, summaryRead) })
				assertReadEqual(t, func() (api.EmissionResult, error) { return core.GetEmission(ctx, emissionRead) }, func() (api.EmissionResult, error) { return c.GetEmission(ctx, emissionRead) })
				assertAckEqual(t, intakeAck, func() (api.CommitResult, error) { return c.ApplyWork(ctx, intake) })
				assertAckEqual(t, summaryDispatchAck, func() (api.CommitResult, error) { return c.ApplyWork(ctx, summaryDispatch) })
				assertAckEqual(t, summaryAck, func() (api.CommitResult, error) { return c.ApplySummary(ctx, summaryRequest) })
				assertAckEqual(t, emissionAck, func() (api.CommitResult, error) { return c.ApplyEmission(ctx, emissionRequest) })
				if got, err := other.GetQueryIntake(ctx, api.ReadQueryIntakeRequest{Space: "s", Query: "q"}); err != nil || got.Found {
					t.Fatalf("query partition leak=%+v %v", got, err)
				}
				if got, err := other.GetSummary(ctx, api.ReadSummaryRequest{Space: "s", ID: summary.ID}); err != nil || got.Found {
					t.Fatalf("summary partition leak=%+v %v", got, err)
				}
				if got, err := other.GetEmission(ctx, api.ReadEmissionRequest{Space: "s", ID: grant.ID}); err != nil || got.Found {
					t.Fatalf("emission partition leak=%+v %v", got, err)
				}
			}
			check(owner, other, core)
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if err := core.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, report, err := ate.OpenExisting(path, ate.Options{AgentProfile: true})
			if err != nil || !report.Verified {
				t.Fatalf("reopen=%+v %v", report, err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			_, owner, other = conformanceClients(t, reopened, transport)
			check(owner, other, reopened)
		})
	}
}

func assertReadEqual[T any](t *testing.T, embedded func() (T, error), remote func() (T, error)) {
	t.Helper()
	want, err := embedded()
	if err != nil {
		t.Fatal(err)
	}
	got, err := remote()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("SDK read=%+v embedded=%+v err=%v", got, want, err)
	}
}

func assertAckEqual(t *testing.T, want api.CommitResult, run func() (api.CommitResult, error)) {
	t.Helper()
	got, err := run()
	if err != nil || got != want {
		t.Fatalf("ack=%+v want=%+v err=%v", got, want, err)
	}
}

func conformanceClients(t *testing.T, core *ate.Store, transport string, credentials ...Options) (*Server, *client.Client, *client.Client) {
	t.Helper()
	options := agentCredentials()
	if len(credentials) > 0 {
		options = credentials[0]
	}
	server := newServer(t, core, options)
	if transport == "pipe" {
		return server, pipeClient(t, server, "owner-token"), pipeClient(t, server, "other-token")
	}
	path := filepath.Join(socketDirectory(t), "ate.sock")
	if err := server.Listen(path); err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("sandbox disallows native Unix sockets; pipe conformance runs separately")
		}
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := server.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	dial := func(token string) *client.Client {
		t.Helper()
		c, err := client.Dial(context.Background(), path, client.Options{Token: token})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	return server, dial("owner-token"), dial("other-token")
}
