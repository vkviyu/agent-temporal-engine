package service

import (
	"context"
	"reflect"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestQueryOnlyEmbeddedSDKConformanceAndResume(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			core, path := nativeAgentStore(t, true)
			server, owner, other := conformanceClients(t, core, transport)
			key := func(id string) model.RequestKey {
				return model.RequestKey{Space: "s", Principal: "owner", RequestID: model.RequestID(id)}
			}
			input := []byte("exact short-query model request")
			ack, err := core.Mutate(ctx, api.MutateRequest{Key: key("query-input"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("query-input"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: input}}}}})
			domainAck(t, ack, err)
			view, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			entries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
			if err != nil || len(entries.Items) != 1 {
				t.Fatal(entries, err)
			}
			if err := core.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
				t.Fatal(err)
			}
			inputID := entries.Items[0].Value
			source := model.SnapshotAnchor{Space: "s", Fence: ack.Fence}
			packet := knowledge.ContextPacket{Space: "s", Source: knowledge.SourceAnchor{Anchor: source}, Renderer: "query/v1", Tokenizer: "test/v1", Input: &knowledge.RecordedInput{Object: inputID, Anchor: source, Encoding: "query/v1"}}
			packet.ID = knowledge.PacketID(packet)
			packetAck, err := core.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key("query-packet"), Command: knowledge.Command{Packet: &packet}})
			domainAck(t, packetAck, err)
			query := agentprofile.Query{ID: "short", Session: "session", Space: "s", Owner: "owner", Input: []byte("answer this"), InputFence: ack.Fence, Status: "open", Version: 1}
			spec := agentprofile.ActivitySpec{Activity: "query-model", Query: query.ID, Role: "query_decision", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: inputID, RequestDigest: agentprofile.ExactRequestDigest(input), Recipient: "model/v1", Effect: "pure", ResourceScope: "query"}
			step, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}}})
			if err != nil {
				t.Fatal(err)
			}
			claimBody, err := agentprofile.EncodeCommand(agentprofile.Command{Control: &agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "query-attempt", Sequence: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			claim := api.CommitRequest{Space: "s", Principal: "owner", RequestID: "query-claim", Method: api.MethodApplyAgent, Body: claimBody}
			open := api.ApplyWorkRequest{Key: key("query-open"), Unit: api.WorkUnit{Kind: "query_open", Query: query.ID, QueryTurn: &api.QueryTurn{Activity: spec.Activity, AdapterVersion: "query/v1", Phase: "model", Source: model.SnapshotAnchor{Space: "s", Fence: packetAck.Fence}, Next: &claim}, Steps: []api.WorkStep{step}}}
			body, err := api.EncodeWorkUnit(open.Unit)
			if err != nil {
				t.Fatal(err)
			}
			intake := api.ApplyWorkRequest{Key: key("query-intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: query.ID, Intake: &api.QueryIntake{Query: query, Route: "query/v1", Configuration: []byte(`{"model":"fixture"}`), Next: api.CommitRequest{Space: "s", Principal: "owner", RequestID: open.Key.RequestID, Method: api.MethodApplyWork, Body: body}}}}
			read := api.ReadQueryProgressRequest{Space: "s", Principal: "owner", Query: query.ID}
			assertReadEqual(t, func() (api.QueryProgressResult, error) { return core.GetQueryProgress(ctx, read) }, func() (api.QueryProgressResult, error) { return owner.GetQueryProgress(ctx, read) })
			intakeAck, err := owner.ApplyWork(ctx, intake)
			domainAck(t, intakeAck, err)
			past := read
			past.Anchor = &model.SnapshotAnchor{Space: "s", Fence: intakeAck.Fence}
			openAck, err := owner.ApplyWork(ctx, open)
			domainAck(t, openAck, err)
			assertAckEqual(t, openAck, func() (api.CommitResult, error) { return core.ApplyWork(ctx, open) })
			first, err := owner.GetQueryProgress(ctx, read)
			if err != nil || !first.Found || first.Progress.Version != 1 || !reflect.DeepEqual(first.Progress.Next, &claim) {
				t.Fatalf("query progress=%+v %v", first, err)
			}
			assertReadEqual(t, func() (api.QueryProgressResult, error) { return core.GetQueryProgress(ctx, past) }, func() (api.QueryProgressResult, error) { return owner.GetQueryProgress(ctx, past) })
			missing, err := owner.GetQueryProgress(ctx, past)
			if err != nil || missing.Found {
				t.Fatalf("future progress leaked into intake history: %+v %v", missing, err)
			}
			claimAck, err := owner.Commit(ctx, claim)
			domainAck(t, claimAck, err)
			assertAckEqual(t, claimAck, func() (api.CommitResult, error) { return core.Commit(ctx, claim) })
			check := func(store *ate.Store, c, foreign *client.Client) {
				t.Helper()
				assertReadEqual(t, func() (api.QueryProgressResult, error) { return store.GetQueryProgress(ctx, read) }, func() (api.QueryProgressResult, error) { return c.GetQueryProgress(ctx, read) })
				state, err := c.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "s"})
				if err != nil || len(state.Snapshot.Tasks) != 0 || len(state.Snapshot.Executions) != 0 || len(state.Snapshot.Lifecycle.Queries) != 1 {
					t.Fatalf("query created synthetic task/execution: %+v %v", state, err)
				}
				assertAckEqual(t, openAck, func() (api.CommitResult, error) { return c.ApplyWork(ctx, open) })
				assertAckEqual(t, claimAck, func() (api.CommitResult, error) { return c.Commit(ctx, claim) })
				if got, err := foreign.GetQueryProgress(ctx, api.ReadQueryProgressRequest{Space: "s", Query: query.ID}); err != nil || got.Found {
					t.Fatalf("foreign progress leaked %+v %v", got, err)
				}
				spoof := read
				spoof.Principal = "other"
				if _, err := c.GetQueryProgress(ctx, spoof); err != client.ErrPrincipal {
					t.Fatalf("principal spoof accepted: %v", err)
				}
			}
			check(core, owner, other)
			_ = owner.Close()
			_ = other.Close()
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if err := core.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, report, err := ate.OpenExisting(path, ate.Options{AgentProfile: true})
			if err != nil || !report.Verified {
				t.Fatal(report, err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			_, owner, other = conformanceClients(t, reopened, transport)
			check(reopened, owner, other)
		})
	}
}
