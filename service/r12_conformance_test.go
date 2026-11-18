package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func TestR12StateViewEmbeddedSDKConformance(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			core, _ := nativeAgentStore(t, false)
			_, owner, other := conformanceClients(t, core, transport)
			key := func(id string) model.RequestKey {
				return model.RequestKey{Space: "s", Principal: "owner", RequestID: model.RequestID(id)}
			}
			write := api.MutateRequest{
				Key: key("r12-state"),
				Batch: api.StateBatch{Operations: []api.StateMutation{
					{Key: []byte("r12"), Kind: api.MutationPut, Value: api.Value{
						Kind: api.ValueBytes, Data: []byte("before"),
					}},
				}},
			}
			embeddedAck, err := core.Mutate(ctx, write)
			domainAck(t, embeddedAck, err)
			assertAckEqual(t, embeddedAck, func() (api.CommitResult, error) {
				return owner.Mutate(ctx, write)
			})

			read := api.ReadStateRequest{Space: "s", Key: []byte("r12")}
			assertReadEqual(t, func() (api.StateValueResult, error) {
				return core.GetState(ctx, read)
			}, func() (api.StateValueResult, error) {
				return owner.GetState(ctx, read)
			})

			embeddedView, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner", Mode: "state"})
			if err != nil {
				t.Fatal(err)
			}
			remoteView, err := owner.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner", Mode: "state"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(embeddedView.Anchor, remoteView.Anchor) ||
				embeddedView.ViewDigest != remoteView.ViewDigest {
				t.Fatalf("view cut differs: embedded=%+v remote=%+v", embeddedView, remoteView)
			}

			update := api.MutateRequest{
				Key: key("r12-state-update"),
				Batch: api.StateBatch{Operations: []api.StateMutation{
					{Key: []byte("r12"), Kind: api.MutationPut, Value: api.Value{
						Kind: api.ValueBytes, Data: []byte("after"),
					}},
				}},
			}
			updateAck, err := core.Mutate(ctx, update)
			domainAck(t, updateAck, err)
			assertAckEqual(t, updateAck, func() (api.CommitResult, error) {
				return owner.Mutate(ctx, update)
			})

			viewRead := api.ReadViewStateRequest{Key: []byte("r12")}
			viewRead.View = embeddedView.Ref
			embeddedPast, err := core.GetViewState(ctx, viewRead)
			if err != nil {
				t.Fatal(err)
			}
			viewRead.View = remoteView.Ref
			remotePast, err := owner.GetViewState(ctx, viewRead)
			if err != nil || !reflect.DeepEqual(remotePast, embeddedPast) || string(remotePast.Value.Data) != "before" {
				t.Fatalf("view read differs: embedded=%+v remote=%+v err=%v", embeddedPast, remotePast, err)
			}
			latest, err := owner.GetState(ctx, read)
			if err != nil || string(latest.Value.Data) != "after" {
				t.Fatalf("latest state escaped view cut: %+v %v", latest, err)
			}

			embeddedEntries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: embeddedView.Ref})
			if err != nil {
				t.Fatal(err)
			}
			remoteEntries, err := owner.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: remoteView.Ref})
			if err != nil || !reflect.DeepEqual(remoteEntries, embeddedEntries) {
				t.Fatalf("view entries differ: embedded=%+v remote=%+v err=%v", embeddedEntries, remoteEntries, err)
			}
			if err := core.CloseView(ctx, api.CloseViewRequest{Ref: embeddedView.Ref}); err != nil {
				t.Fatal(err)
			}
			if err := owner.CloseView(ctx, api.CloseViewRequest{Ref: remoteView.Ref}); err != nil {
				t.Fatal(err)
			}
			if _, err := owner.GetViewState(ctx, api.ReadViewStateRequest{View: remoteView.Ref, Key: []byte("r12")}); err == nil {
				t.Fatal("closed remote view remained readable")
			}
			assertReadEqual(t, func() (api.StateValueResult, error) { return core.GetState(ctx, read) },
				func() (api.StateValueResult, error) { return other.GetState(ctx, read) })
			_, err = other.GetState(ctx, api.ReadStateRequest{Space: "forbidden", Key: read.Key})
			remoteCode(t, err, api.ErrorUnauthorized)
		})
	}
}

func TestR12TaskTraceEmbeddedSDKConformance(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
				t.Skip("native core DirectoryIO is supported on Darwin/Linux")
			}
			core, err := ate.Create(t.TempDir(), ate.Options{AgentProfile: true, EventProfile: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := core.Close(); err != nil {
					t.Error(err)
				}
			})
			_, owner, other := conformanceClients(t, core, transport)
			key := func(id string) model.RequestKey {
				return model.RequestKey{Space: "s", Principal: "owner", RequestID: model.RequestID(id)}
			}
			task := agentprofile.Task{
				ID: "r12-task", Space: "s", Owner: "owner",
				Goal: "conformance", GoalEpoch: 1, Status: agentprofile.TaskOpen,
			}
			create := api.ApplyAgentRequest{
				Key:     key("r12-task-create"),
				Command: agentprofile.Command{CreateTask: &task},
			}
			ack, err := core.ApplyAgent(ctx, create)
			domainAck(t, ack, err)
			assertAckEqual(t, ack, func() (api.CommitResult, error) {
				return owner.ApplyAgent(ctx, create)
			})
			execution := agentprofile.Execution{
				ID: "r12-execution", Task: task.ID, Actor: "r12-actor",
				Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady,
			}
			spawn := api.ApplyAgentRequest{
				Key:     key("r12-task-spawn"),
				Command: agentprofile.Command{Spawn: &execution},
			}
			ack, err = core.ApplyAgent(ctx, spawn)
			domainAck(t, ack, err)
			assertAckEqual(t, ack, func() (api.CommitResult, error) {
				return owner.ApplyAgent(ctx, spawn)
			})
			for _, id := range []model.ExecutionID{"child-a", "child-b"} {
				child := execution
				child.ID, child.Parent = id, execution.ID
				req := api.ApplyAgentRequest{Key: key(string(id)), Command: agentprofile.Command{Spawn: &child}}
				ack, err := owner.ApplyAgent(ctx, req)
				domainAck(t, ack, err)
				assertAckEqual(t, ack, func() (api.CommitResult, error) { return core.ApplyAgent(ctx, req) })
			}

			read := api.TaskTraceRequest{
				Space: "s", Principal: "owner",
				Subject: api.TraceSubject{Kind: "task", ID: string(task.ID)},
				Limit:   1,
			}
			embedded, err := core.GetTaskTrace(ctx, read)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := owner.GetTaskTrace(ctx, read)
			if err != nil || !reflect.DeepEqual(remote, embedded) {
				t.Fatalf("task trace differs: embedded=%+v remote=%+v err=%v", embedded, remote, err)
			}
			if _, err := other.GetTaskTrace(ctx, api.TaskTraceRequest{
				Space: "s", Subject: read.Subject, Session: read.Session,
			}); err == nil {
				t.Fatal("foreign principal read task trace")
			}
			if embedded.Continuation == "" || len(embedded.Nodes) != 1 {
				t.Fatalf("fixture must exercise overview pagination: %+v", embedded)
			}
			{
				next := read
				next.Anchor = &embedded.Anchor
				next.Continuation = embedded.Continuation
				next.Limit = 1
				embeddedNext, err := core.GetTaskTrace(ctx, next)
				if err != nil {
					t.Fatal(err)
				}
				remoteNext, err := owner.GetTaskTrace(ctx, next)
				if err != nil || !reflect.DeepEqual(remoteNext, embeddedNext) {
					t.Fatalf("task trace continuation differs: embedded=%+v remote=%+v err=%v", embeddedNext, remoteNext, err)
				}
				bound := next
				bound.Limit = 2
				_, localErr := core.GetTaskTrace(ctx, bound)
				_, remoteErr := owner.GetTaskTrace(ctx, bound)
				if localErr == nil {
					t.Fatal("overview token accepted changed limit")
				}
				remoteCode(t, remoteErr, core.ClassifyError(localErr))
				if len(embeddedNext.Nodes) != 1 || embeddedNext.Continuation == "" {
					t.Fatal("fixture must include both child events")
				}
				lastRequest := next
				lastRequest.Continuation = embeddedNext.Continuation
				last, err := core.GetTaskTrace(ctx, lastRequest)
				if err != nil || len(last.Nodes) != 1 {
					t.Fatalf("overview omitted second child: %+v %v", last, err)
				}
				assertReadEqual(t, func() (api.TaskTraceResult, error) { return core.GetTaskTrace(ctx, lastRequest) },
					func() (api.TaskTraceResult, error) { return owner.GetTaskTrace(ctx, lastRequest) })
				if last.Continuation != "" {
					end := lastRequest
					end.Continuation = last.Continuation
					empty, err := core.GetTaskTrace(ctx, end)
					if err != nil || len(empty.Nodes) != 0 || empty.Continuation != "" {
						t.Fatalf("overview failed to terminate: %+v %v", empty, err)
					}
					assertReadEqual(t, func() (api.TaskTraceResult, error) { return core.GetTaskTrace(ctx, end) },
						func() (api.TaskTraceResult, error) { return owner.GetTaskTrace(ctx, end) })
				}
				from, to := embeddedNext.Nodes[0].At, last.Nodes[0].At
				edit := api.ApplyTaskTraceRequest{Key: key("r12-trace-edit"), Edit: api.TaskTraceEdit{
					Subject: read.Subject, Source: embedded.Anchor,
					Milestones: []api.TraceMilestone{
						{Event: from.Ref.Event, Label: "first child", Evidence: []api.TraceCitation{from}},
						{Event: to.Ref.Event, Label: "second child", Evidence: []api.TraceCitation{to}},
					},
					Assertions: []api.TraceAssertion{{From: from.Ref.Event, To: to.Ref.Event,
						Relation: "explains", Reason: "host interpretation", Evidence: []api.TraceCitation{from}}},
				}}
				edited, err := owner.ApplyTaskTrace(ctx, edit)
				domainAck(t, edited, err)
				assertAckEqual(t, edited, func() (api.CommitResult, error) { return core.ApplyTaskTrace(ctx, edit) })
				evidence := api.TaskTraceEvidenceRequest{
					TaskTraceRequest: read, Relation: "explains",
					From: from.Ref.Event, To: to.Ref.Event,
				}
				evidence.Anchor = &model.SnapshotAnchor{Space: "s", Fence: edited.Fence}
				first, err := core.GetTaskTraceEvidence(ctx, evidence)
				if err != nil || len(first.Events) != 1 || first.Continuation == "" {
					t.Fatalf("fixture must exercise evidence pagination: %+v %v", first, err)
				}
				assertReadEqual(t, func() (api.TaskTraceEvidenceResult, error) { return core.GetTaskTraceEvidence(ctx, evidence) },
					func() (api.TaskTraceEvidenceResult, error) { return owner.GetTaskTraceEvidence(ctx, evidence) })
				evidence.Continuation = first.Continuation
				assertReadEqual(t, func() (api.TaskTraceEvidenceResult, error) { return core.GetTaskTraceEvidence(ctx, evidence) },
					func() (api.TaskTraceEvidenceResult, error) { return owner.GetTaskTraceEvidence(ctx, evidence) })
				original := evidence
				evidence.Limit = 2
				_, localErr = core.GetTaskTraceEvidence(ctx, evidence)
				_, remoteErr = owner.GetTaskTraceEvidence(ctx, evidence)
				if localErr == nil {
					t.Fatal("evidence token accepted changed limit")
				}
				remoteCode(t, remoteErr, core.ClassifyError(localErr))
				withdraw, err := core.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("r12-trace-withdraw"),
					Command: memory.Command{RetractSource: &memory.RetractSourceCommand{
						Source:     model.ObjectID(from.Ref.Event),
						Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "withdraw cited event"},
					}}})
				domainAck(t, withdraw, err)
				localEvidence, localErr := core.GetTaskTraceEvidence(ctx, original)
				remoteEvidence, remoteErr := owner.GetTaskTraceEvidence(ctx, original)
				if localErr == nil || len(localEvidence.Events) != 0 || len(remoteEvidence.Events) != 0 || len(remoteEvidence.Citations) != 0 {
					t.Fatal("withdrawn evidence was disclosed")
				}
				remoteCode(t, remoteErr, core.ClassifyError(localErr))
				historical := read
				historical.Anchor = original.Anchor
				historical.Limit = 128
				localTrace, localErr := core.GetTaskTrace(ctx, historical)
				remoteTrace, remoteErr := owner.GetTaskTrace(ctx, historical)
				if localErr == nil || len(localTrace.Nodes) != 0 || len(remoteTrace.Nodes) != 0 || len(remoteTrace.Edges) != 0 {
					t.Fatal("withdrawn overview was disclosed")
				}
				remoteCode(t, remoteErr, core.ClassifyError(localErr))
			}
		})
	}
}

func TestR12GraphQueryEmbeddedSDKConformance(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			core, _ := nativeAgentStore(t, false)
			_, owner, other := conformanceClients(t, core, transport)
			key := func(id string) model.RequestKey {
				return model.RequestKey{Space: "s", Principal: "owner", RequestID: model.RequestID(id)}
			}
			put := func(id, value string) api.CommitResult {
				t.Helper()
				ack, err := core.Mutate(ctx, api.MutateRequest{
					Key: key(id),
					Batch: api.StateBatch{Operations: []api.StateMutation{{
						Key: []byte(id), Kind: api.MutationPut,
						Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)},
					}}},
				})
				domainAck(t, ack, err)
				assertAckEqual(t, ack, func() (api.CommitResult, error) {
					return owner.Mutate(ctx, api.MutateRequest{
						Key: key(id),
						Batch: api.StateBatch{Operations: []api.StateMutation{{
							Key: []byte(id), Kind: api.MutationPut,
							Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)},
						}}},
					})
				})
				return ack
			}
			put("r12-graph-a", "a")
			firstView, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			entries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: firstView.Ref})
			if err != nil {
				t.Fatal(err)
			}
			if err := core.CloseView(ctx, api.CloseViewRequest{Ref: firstView.Ref}); err != nil {
				t.Fatal(err)
			}
			firstRevision, err := graph.BuildRevision("s", "", nil, entries.StateRoot, nil, toGraphEntries(entries.Items))
			if err != nil {
				t.Fatal(err)
			}
			addFirst := api.ApplyGraphRequest{
				Key: key("r12-graph-revision-a"),
				Command: graph.Command{AddRevision: &graph.AddRevisionCommand{
					Root: entries.StateRoot, Entries: toGraphEntries(entries.Items),
				}},
			}
			ack, err := core.ApplyGraph(ctx, addFirst)
			domainAck(t, ack, err)
			assertAckEqual(t, ack, func() (api.CommitResult, error) {
				return owner.ApplyGraph(ctx, addFirst)
			})
			put("r12-graph-b", "b")
			put("r12-graph-c", "c")
			secondView, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			secondEntries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: secondView.Ref})
			if err != nil {
				t.Fatal(err)
			}
			if err := core.CloseView(ctx, api.CloseViewRequest{Ref: secondView.Ref}); err != nil {
				t.Fatal(err)
			}
			secondRevision, err := graph.BuildRevision("s", firstRevision.ID, nil, secondEntries.StateRoot, nil, toGraphEntries(secondEntries.Items))
			if err != nil {
				t.Fatal(err)
			}
			addSecond := api.ApplyGraphRequest{
				Key: key("r12-graph-revision-b"),
				Command: graph.Command{AddRevision: &graph.AddRevisionCommand{
					Parent: firstRevision.ID, Root: secondEntries.StateRoot, Entries: toGraphEntries(secondEntries.Items),
				}},
			}
			ack, err = core.ApplyGraph(ctx, addSecond)
			domainAck(t, ack, err)
			assertAckEqual(t, ack, func() (api.CommitResult, error) {
				return owner.ApplyGraph(ctx, addSecond)
			})
			diffRequest := api.DiffGraphRequest{
				Space: "s", From: firstRevision.ID, To: secondRevision.ID, Limit: 1,
			}
			embedded, err := core.DiffGraph(ctx, diffRequest)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := owner.DiffGraph(ctx, diffRequest)
			if err != nil || !reflect.DeepEqual(remote, embedded) {
				t.Fatalf("graph diff differs: embedded=%+v remote=%+v err=%v", embedded, remote, err)
			}
			if embedded.Continuation == "" || len(embedded.Diff.Changes) != 1 {
				t.Fatal("fixture must exercise diff pagination")
			}
			{
				next := diffRequest
				next.Anchor = &embedded.Anchor
				next.Continuation = embedded.Continuation
				put("r12-graph-later", "later publication")
				embeddedNext, err := core.DiffGraph(ctx, next)
				if err != nil {
					t.Fatal(err)
				}
				remoteNext, err := owner.DiffGraph(ctx, next)
				if err != nil || !reflect.DeepEqual(remoteNext, embeddedNext) {
					t.Fatalf("graph continuation differs: embedded=%+v remote=%+v err=%v", embeddedNext, remoteNext, err)
				}
				bound := next
				bound.From, bound.To = bound.To, bound.From
				_, localErr := core.DiffGraph(ctx, bound)
				_, remoteErr := owner.DiffGraph(ctx, bound)
				if localErr == nil {
					t.Fatal("graph token accepted changed selectors")
				}
				remoteCode(t, remoteErr, core.ClassifyError(localErr))
			}
			assertReadEqual(t, func() (api.GraphDiffResult, error) { return core.DiffGraph(ctx, diffRequest) },
				func() (api.GraphDiffResult, error) { return other.DiffGraph(ctx, diffRequest) })
			plan := api.PlanGraphJoinRequest{Space: "s", Anchor: &embedded.Anchor, Left: firstRevision.ID, Right: secondRevision.ID}
			assertReadEqual(t, func() (api.GraphJoinPlanResult, error) { return core.PlanGraphJoin(ctx, plan) },
				func() (api.GraphJoinPlanResult, error) { return owner.PlanGraphJoin(ctx, plan) })
			trace := api.TraceGraphRequest{Space: "s", Anchor: &embedded.Anchor, Start: secondRevision.ID, Direction: "ancestors", Limit: 1}
			traceFirst, err := core.TraceGraph(ctx, trace)
			if err != nil || len(traceFirst.Nodes) != 1 || traceFirst.Continuation == "" {
				t.Fatalf("fixture must exercise graph trace pagination: %+v %v", traceFirst, err)
			}
			assertReadEqual(t, func() (api.GraphTraceResult, error) { return core.TraceGraph(ctx, trace) },
				func() (api.GraphTraceResult, error) { return owner.TraceGraph(ctx, trace) })
			trace.Continuation = traceFirst.Continuation
			assertReadEqual(t, func() (api.GraphTraceResult, error) { return core.TraceGraph(ctx, trace) },
				func() (api.GraphTraceResult, error) { return owner.TraceGraph(ctx, trace) })
			trace.Direction = "descendants"
			_, localErr := core.TraceGraph(ctx, trace)
			_, remoteErr := owner.TraceGraph(ctx, trace)
			if localErr == nil {
				t.Fatal("trace token accepted changed direction")
			}
			remoteCode(t, remoteErr, core.ClassifyError(localErr))
			forbidden := diffRequest
			forbidden.Space = "forbidden"
			_, err = other.DiffGraph(ctx, forbidden)
			remoteCode(t, err, api.ErrorUnauthorized)
		})
	}
}

func toGraphEntries(items []api.StateEntry) []graph.Entry {
	entries := make([]graph.Entry, len(items))
	for i, item := range items {
		entries[i] = graph.Entry{Key: item.Key, Value: item.Value}
	}
	return entries
}

// Drop a complete real server response before the SDK receives even its frame
// header. Capturing it proves the fault happened after durable acknowledgement,
// without timing a disconnect against publication or fabricating a backend ACK.
// Only the synchronous RPC caller reads/arms this connection.
type r12LostACKConn struct {
	net.Conn
	armed   bool
	dropped []byte
}

func (c *r12LostACKConn) Read(p []byte) (int, error) {
	if !c.armed {
		return c.Conn.Read(p)
	}
	c.armed = false
	var err error
	c.dropped, err = protocol.ReadFrame(c.Conn)
	_ = c.Conn.Close()
	if err != nil {
		return 0, err
	}
	return 0, io.EOF
}

func r12Connection(t *testing.T, ctx context.Context, server *Server, transport string) net.Conn {
	t.Helper()
	var conn net.Conn
	switch transport {
	case "pipe":
		a, b := net.Pipe()
		if !server.acceptConn(b) {
			_ = a.Close()
			_ = b.Close()
			t.Fatal("connection refused")
		}
		conn = a
	case "unix":
		var err error
		conn, err = (&net.Dialer{}).DialContext(ctx, "unix", server.socketPath)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported transport %q", transport)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestR12DurableCommitLostACKReconcilesEmbeddedSDK(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			core, _ := nativeAgentStore(t, false)
			server, initialOwner, other := conformanceClients(t, core, transport)
			_ = initialOwner.Close()
			_ = other.Close()
			before, err := core.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request := api.MutateRequest{
				Key: model.RequestKey{Space: "s", Principal: "owner", RequestID: "r12-lost-ack"},
				Batch: api.StateBatch{Operations: []api.StateMutation{{
					Key: []byte("once"), Kind: api.MutationPut,
					Value: api.Value{Kind: api.ValueBytes, Data: []byte("durably published")},
				}}},
				ExpectedFence: &before.Fence,
			}
			conn := &r12LostACKConn{Conn: r12Connection(t, ctx, server, transport)}
			owner, err := client.New(ctx, conn, client.Options{Token: "owner-token"})
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			conn.armed = true // hello succeeded; lose only the mutation response.
			result, err := owner.Mutate(ctx, request)
			var unknown *client.OutcomeUnknownError
			if !errors.As(err, &unknown) || unknown.Key != request.Key || !errors.Is(err, io.EOF) {
				t.Fatalf("lost ACK did not retain authenticated identity: %+v %v", result, err)
			}
			if result != (api.CommitResult{}) {
				t.Fatalf("lost ACK exposed a receipt to the SDK: %+v", result)
			}
			response, err := protocol.DecodeResponse(conn.dropped)
			if err != nil || response.Error != nil {
				t.Fatalf("fault did not intercept a successful response: %+v %v", response, err)
			}
			var ack api.CommitResult
			if err := protocol.DecodeStrict(response.Body, &ack); err != nil {
				t.Fatal(err)
			}
			domainAck(t, ack, nil)
			if ack.RequestID != request.Key.RequestID || ack.Fence.LSN != before.Fence.LSN+1 {
				t.Fatalf("fault did not follow the first durable publication: %+v", ack)
			}
			published, err := core.Snapshot(ctx)
			if err != nil || published.Fence != ack.Fence || published.RootSet != ack.RootSet {
				t.Fatalf("lost ACK was not installed: %+v %v", published, err)
			}

			// Reconnect to the same real server and reconcile the original key.
			resumed, err := client.New(ctx, r12Connection(t, ctx, server, transport), client.Options{Token: "owner-token"})
			if err != nil {
				t.Fatal(err)
			}
			defer resumed.Close()
			lookup := api.LookupRequestRequest{
				Space: unknown.Key.Space, Principal: unknown.Key.Principal, RequestID: unknown.Key.RequestID,
			}
			local, err := core.LookupRequest(ctx, lookup)
			if err != nil || local.Code != api.CodeDurableAck || local.Result != ack || local.ObservedFence != ack.Fence {
				t.Fatalf("embedded lookup lost the durable receipt: %+v %v", local, err)
			}
			remote, err := resumed.LookupRequest(ctx, lookup)
			if err != nil || !reflect.DeepEqual(remote, local) {
				t.Fatalf("reconnected lookup differs: embedded=%+v remote=%+v err=%v", local, remote, err)
			}
			assertUnchanged := func() {
				t.Helper()
				after, err := core.Snapshot(ctx)
				if err != nil || after != published {
					t.Fatalf("reconciliation advanced publication: before=%+v after=%+v err=%v", published, after, err)
				}
			}
			assertUnchanged()
			// ExpectedFence still names the pre-commit cut: exact retry must
			// return the original receipt rather than reapply the condition.
			assertAckEqual(t, ack, func() (api.CommitResult, error) { return resumed.Mutate(ctx, request) })
			assertUnchanged()
			assertAckEqual(t, ack, func() (api.CommitResult, error) { return core.Mutate(ctx, request) })
			assertUnchanged()
			read := api.ReadStateRequest{Space: "s", Key: []byte("once")}
			value, err := resumed.GetState(ctx, read)
			if err != nil || !value.Found || string(value.Value.Data) != "durably published" {
				t.Fatalf("reconciled value=%+v err=%v", value, err)
			}
			assertReadEqual(t, func() (api.StateValueResult, error) { return core.GetState(ctx, read) },
				func() (api.StateValueResult, error) { return resumed.GetState(ctx, read) })
		})
	}
}

func TestR12ContentBudgetEmbeddedSDKConformance(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			core, _ := nativeAgentStore(t, false)
			_, owner, _ := conformanceClients(t, core, transport)
			data := bytes.Repeat([]byte{0, 1, 255, 42}, 4096)
			ack, err := owner.Mutate(ctx, api.MutateRequest{
				Key: model.RequestKey{Space: "s", RequestID: "r12-content-budget"},
				Batch: api.StateBatch{Operations: []api.StateMutation{{
					Key: []byte("content"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: data},
				}}},
			})
			domainAck(t, ack, err)
			before, err := core.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			open := api.OpenViewRequest{Space: "s", Principal: "owner", Mode: "state"}
			localView, err := core.OpenView(ctx, open)
			if err != nil {
				t.Fatal(err)
			}
			defer core.CloseView(ctx, api.CloseViewRequest{Ref: localView.Ref})
			remoteView, err := owner.OpenView(ctx, open)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.CloseView(ctx, api.CloseViewRequest{Ref: remoteView.Ref})
			if !reflect.DeepEqual(localView.Anchor, remoteView.Anchor) || localView.ViewDigest != remoteView.ViewDigest {
				t.Fatal("content views selected different cuts")
			}
			entries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: localView.Ref})
			if err != nil || len(entries.Items) != 1 {
				t.Fatalf("content fixture=%+v err=%v", entries, err)
			}
			localRequest := api.ReadContentRequest{
				View: localView.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value},
				Projection: "value_bytes", Offset: 5, Length: 8192, MaxBytes: 64 << 10,
			}
			remoteRequest := localRequest
			remoteRequest.View = remoteView.Ref
			want, err := core.ReadContent(ctx, localRequest)
			if err != nil || !bytes.Equal(want.Data, data[5:8197]) || want.TotalBytes != uint64(len(data)) || want.EOF {
				t.Fatalf("embedded content range=%+v err=%v", want, err)
			}
			got, err := owner.ReadContent(ctx, remoteRequest)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("content range differs: embedded=%+v remote=%+v err=%v", want, got, err)
			}
			// Both budgets are valid API inputs. The smaller one cannot hold
			// this result, so this tests output limits, not malformed requests.
			const budget = 4096
			raw, err := json.Marshal(want)
			if err != nil || len(raw) <= budget || len(raw) > localRequest.MaxBytes {
				t.Fatalf("fixture does not cross the output budget: size=%d err=%v", len(raw), err)
			}
			localRequest.MaxBytes, remoteRequest.MaxBytes = budget, budget
			localDenied, localErr := core.ReadContent(ctx, localRequest)
			remoteDenied, remoteErr := owner.ReadContent(ctx, remoteRequest)
			if localErr == nil || core.ClassifyError(localErr) != api.ErrorResourceLimit {
				t.Fatalf("embedded output budget classification=%v", localErr)
			}
			remoteCode(t, remoteErr, core.ClassifyError(localErr))
			if !reflect.DeepEqual(localDenied, api.ContentRange{}) || !reflect.DeepEqual(remoteDenied, api.ContentRange{}) {
				t.Fatal("budget rejection disclosed a partial range")
			}
			// Keep the same client, view and budget; a smaller range must work.
			localRequest.Length, remoteRequest.Length = 32, 32
			assertReadEqual(t, func() (api.ContentRange, error) { return core.ReadContent(ctx, localRequest) },
				func() (api.ContentRange, error) { return owner.ReadContent(ctx, remoteRequest) })
			small, err := owner.ReadContent(ctx, remoteRequest)
			if err != nil || !bytes.Equal(small.Data, data[5:37]) {
				t.Fatalf("same connection/view unusable after rejection: %+v %v", small, err)
			}
			after, err := core.Snapshot(ctx)
			if err != nil || after != before {
				t.Fatalf("content reads changed publication: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}
