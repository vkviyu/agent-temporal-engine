package engine_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func localGraphExecution(t *testing.T, s *integrationStore, name string) graph.ExecutionAuthority {
	t.Helper()
	task := agentTask(model.EntityID(name + "-task"))
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey(name + "-task"), Command: agentprofile.Command{CreateTask: &task}})
	execution := agentprofile.Execution{ID: model.ExecutionID(name), Task: task.ID, Actor: model.ActorID(name + "-actor"), Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey(name + "-execution"), Command: agentprofile.Command{Spawn: &execution}})
	bind := agentprofile.AuthorityBinding{Deployment: "local-graph", Holder: name, OwnerEpoch: 1, Incarnation: 1}
	recoveryCoreControl(t, s, name+"-grant", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: execution.ID, Binding: bind, Status: "active", Evidence: model.ObjectID(integrationID('a'))}}})
	return graph.ExecutionAuthority{Execution: execution.ID, Deployment: bind.Deployment, Holder: bind.Holder, OwnerEpoch: bind.OwnerEpoch, Incarnation: bind.Incarnation}
}

func localGraphPosition(t *testing.T, s *integrationStore, id model.BranchID) graph.BranchPosition {
	t.Helper()
	result, err := s.e.GetGraphBranch(t.Context(), api.ReadGraphBranchRequest{Space: "host-space", ID: id})
	if err != nil || !result.Found {
		t.Fatalf("branch %s: %+v, %v", id, result, err)
	}
	return graph.BranchPosition{Branch: id, Head: result.Value.Head, Version: result.Value.Version}
}

func localGraphBatch(t *testing.T, ops ...api.StateMutation) []byte {
	t.Helper()
	b, err := engine.EncodeStateBatch(api.StateBatch{Operations: ops})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func localGraphRevision(t *testing.T, s *integrationStore, pos graph.BranchPosition) graph.Revision {
	t.Helper()
	r, err := s.e.GetGraphRevision(t.Context(), api.ReadGraphRevisionRequest{Space: "host-space", ID: pos.Head})
	if err != nil || !r.Found {
		t.Fatalf("revision %s: %+v, %v", pos.Head, r, err)
	}
	return r.Value
}

func assertGraphState(t *testing.T, s *integrationStore, key string, value string, exists bool) api.StateValueResult {
	t.Helper()
	r, err := s.e.GetState(t.Context(), api.ReadStateRequest{Space: "host-space", Key: []byte(key)})
	if err != nil || r.Found != exists || string(r.Value.Data) != value {
		t.Fatalf("state %s: %+v, %v", key, r, err)
	}
	return r
}

func assertBranchState(t *testing.T, s *integrationStore, pos graph.BranchPosition, key, value string) {
	t.Helper()
	ctx := t.Context()
	r := localGraphRevision(t, s, pos)
	if len(r.Entries) == 0 {
		t.Fatalf("missing entries on %s", pos.Branch)
	}
	var id model.ObjectID
	for _, e := range r.Entries {
		if string(e.Key) == key {
			id = e.Value
			break
		}
	}
	if id == "" {
		t.Fatalf("missing key %s on %s", key, pos.Branch)
	}
	fence := integrationSnapshot(t, s.e).Fence
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &model.SnapshotAnchor{Fence: fence, Space: "host-space", Cursor: &model.Cursor{Store: fence.Store, Space: "host-space", Revision: pos.Head}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref})
	read, err := s.e.GetViewState(ctx, api.ReadViewStateRequest{View: view.Ref, Key: []byte(key)})
	if err != nil || !read.Found || string(read.Value.Data) != value || read.StateRoot != r.StateRoot {
		t.Fatalf("branch %s %s: %+v, %v", pos.Branch, key, read, err)
	}
}

func TestNativeBranchIsolatedConcurrentExplorationAndScopedPublication(t *testing.T) {
	ctx := t.Context()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	leftAuth, rightAuth := localGraphExecution(t, s, "left-exec"), localGraphExecution(t, s, "right-exec")
	base := graphRevisionFromCurrent(t, s, "local-base", "")
	leftFork := graphCoreApply(t, s.e, "fork-left", graph.Command{ForkLocal: &graph.ForkLocalCommand{Branch: "left", Source: base.ID, Authority: leftAuth}})
	graphCoreApply(t, s.e, "fork-right", graph.Command{ForkLocal: &graph.ForkLocalCommand{Branch: "right", Source: base.ID, Authority: rightAuth}})
	leftStart, rightStart := localGraphPosition(t, s, "left"), localGraphPosition(t, s, "right")
	leftWrite := api.ApplyGraphRequest{Key: graphCoreKey("left-write"), Command: graph.Command{WriteBranch: &graph.WriteBranchCommand{Target: leftStart, Authority: leftAuth, Batch: localGraphBatch(t, integrationPut([]byte("same"), integrationBytes('l')))}}}
	rightWrite := api.ApplyGraphRequest{Key: graphCoreKey("right-write"), Command: graph.Command{WriteBranch: &graph.WriteBranchCommand{Target: rightStart, Authority: rightAuth, Batch: localGraphBatch(t, integrationPut([]byte("same"), integrationBytes('r')))}}}
	type completion struct {
		ack api.CommitResult
		err error
	}
	results := make(chan completion, 2)
	for _, req := range []api.ApplyGraphRequest{leftWrite, rightWrite} {
		go func() { ack, err := s.e.ApplyGraph(ctx, req); results <- completion{ack, err} }()
	}
	var leftAck api.CommitResult
	for range 2 {
		got := <-results
		if got.err != nil || got.ack.Code != api.CodeDurableAck {
			t.Fatalf("concurrent branch write: %+v, %v", got.ack, got.err)
		}
		if got.ack.RequestID == leftWrite.Key.RequestID {
			leftAck = got.ack
		}
	}
	left, right := localGraphPosition(t, s, "left"), localGraphPosition(t, s, "right")
	assertBranchState(t, s, left, "same", "l")
	assertBranchState(t, s, right, "same", "r")
	assertGraphState(t, s, "same", "", false)
	leftRevision := localGraphRevision(t, s, left)
	producer := leftRevision.Producer
	if producer == nil || producer.Execution != leftAuth.Execution || producer.Actor != "left-exec-actor" || producer.Task != "left-exec-task" || producer.GoalEpoch != 1 || producer.AuthorityVersion != 1 || producer.BranchVersion != left.Version {
		t.Fatalf("wrong execution producer: %+v", producer)
	}
	fact := domainFact(t, s, leftAck, "Graph.WriteRevision", "graph", "Revision", string(left.Head))
	forkFact := domainFact(t, s, leftFork, "Graph.ForkLocal", "graph", "Branch", "left")
	if fact.Attribution.Execution != leftAuth.Execution || fact.Attribution.Task != producer.Task || fact.Attribution.InitiatingActor != producer.Actor || !slices.Contains(fact.CausalParents, forkFact.ID) {
		t.Fatalf("write event lost authenticated producer: %+v", fact.Attribution)
	}
	wrong := *leftWrite.Command.WriteBranch
	wrong.Target, wrong.Authority = left, rightAuth
	if result, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("wrong-execution"), Command: graph.Command{WriteBranch: &wrong}}); !errors.Is(err, graph.ErrBranchScope) || result.Code != api.CodeRejected {
		t.Fatalf("unowned write accepted: %+v, %v", result, err)
	}
	join := graph.JoinLocalCommand{Target: left, Source: right, Authority: leftAuth, Events: []model.EventID{fact.ID}}
	if result, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("unresolved-local-join"), Command: graph.Command{JoinLocal: &join}}); !errors.Is(err, graph.ErrJoinConflict) || result.Code != api.CodeRejected {
		t.Fatalf("unresolved join accepted: %+v, %v", result, err)
	}
	global := integrationMutate(t, s.e, integrationRequest("unrelated", integrationPut([]byte("other-task"), integrationBytes('x'))))
	join.Resolutions = []graph.Resolution{{Key: []byte("same"), Value: leftRevision.Entries[0].Value}}
	joinAck := graphCoreApply(t, s.e, "joined-local", graph.Command{JoinLocal: &join})
	joined := localGraphPosition(t, s, "left")
	r := localGraphRevision(t, s, joined)
	if r.StateParent != left.Head || !slices.Equal(r.JoinParents, []model.RevisionID{right.Head}) || !slices.Equal(r.Events, []model.EventID{fact.ID}) || localGraphPosition(t, s, "right") != right {
		t.Fatalf("local join lost immutable ancestry or moved its source: %+v", r)
	}
	joinFact := domainFact(t, s, joinAck, "Graph.JoinLocalRevision", "graph", "Revision", string(joined.Head))
	if !slices.Contains(joinFact.CausalParents, fact.ID) {
		t.Fatalf("join event lost evidence: %+v", joinFact)
	}
	assertGraphState(t, s, "same", "", false)
	assertGraphState(t, s, "other-task", "x", true)
	publish := api.ApplyGraphRequest{Key: graphCoreKey("publish-local"), Command: graph.Command{PublishBranch: &graph.PublishBranchCommand{Source: joined, Authority: leftAuth, ExpectedDomainRoot: global.StateRoot}}}
	publishAck := graphCoreApply(t, s.e, string(publish.Key.RequestID), publish.Command)
	assertGraphState(t, s, "same", "l", true)
	assertGraphState(t, s, "other-task", "x", true)
	current := assertGraphState(t, s, "same", "l", true)
	if result, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("conflicting-publication"), Command: graph.Command{PublishBranch: &graph.PublishBranchCommand{Source: right, Authority: rightAuth, ExpectedDomainRoot: current.StateRoot}}}); !errors.Is(err, graph.ErrPublishConflict) || result.Code != api.CodeRejected {
		t.Fatalf("competing publication accepted: %+v, %v", result, err)
	}
	final := integrationSnapshot(t, s.e)
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	if current := integrationSnapshot(t, s.e); current != final {
		t.Fatalf("replay changed branch or domain roots: before=%+v after=%+v", final, current)
	}
	if again, err := s.e.ApplyGraph(ctx, leftWrite); err != nil || again != leftAck {
		t.Fatalf("write receipt changed: %+v, %v", again, err)
	}
	if again, err := s.e.ApplyGraph(ctx, publish); err != nil || again != publishAck {
		t.Fatalf("publication receipt changed: %+v, %v", again, err)
	}
	assertBranchState(t, s, right, "same", "r")
	assertGraphState(t, s, "other-task", "x", true)
}

func TestNativeBranchCanReserveExactActionForOwningExecution(t *testing.T) {
	ctx := t.Context()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	auth := localGraphExecution(t, s, "reserve-writer")
	other := localGraphExecution(t, s, "reserve-other")
	base := graphRevisionFromCurrent(t, s, "reserve-base", "")
	fork := graphCoreApply(t, s.e, "reserve-fork", graph.Command{ForkLocal: &graph.ForkLocalCommand{Branch: "reserved", Source: base.ID, Authority: auth}})
	command := graph.Command{WriteBranch: &graph.WriteBranchCommand{Target: localGraphPosition(t, s, "reserved"), Authority: auth, Batch: localGraphBatch(t, integrationPut([]byte("planned"), integrationBytes('p')))}}
	raw, err := graph.EncodeCommand("host-space", command)
	if err != nil {
		t.Fatal(err)
	}
	request := graphCoreKey("reserved-write")
	intentFor := func(auth graph.ExecutionAuthority) agentprofile.ControlCommand {
		binding := agentprofile.AuthorityBinding{Deployment: auth.Deployment, Holder: auth.Holder, OwnerEpoch: auth.OwnerEpoch, Incarnation: auth.Incarnation}
		intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: "branch-action", Owner: auth.Execution, Generation: 1, Status: agentprofile.ActionPending, Request: request, InputFence: fork.Fence, CommandDigest: agentprofile.ExactRequestDigest(raw)}, Method: api.MethodApplyGraph, ExactCommand: raw, Authority: binding}
		return agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent, Authority: binding}}
	}
	bad := intentFor(other)
	if got, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("wrong-reservation"), Command: agentprofile.Command{Control: &bad}}); !errors.Is(err, agentprofile.ErrCommandScope) || got.Code != api.CodeRejected {
		t.Fatalf("wrong execution reserved branch command: %+v, %v", got, err)
	}
	recoveryCoreControl(t, s, "owned-reservation", intentFor(auth))
	changed := *command.WriteBranch
	changed.Batch = localGraphBatch(t, integrationPut([]byte("planned"), integrationBytes('q')))
	if got, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: request, Command: graph.Command{WriteBranch: &changed}}); !errors.Is(err, engine.ErrActionReservation) || got.Code != api.CodeRejected {
		t.Fatalf("reservation allowed changed branch command: %+v, %v", got, err)
	}
	ack, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: request, Command: command})
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("reserved branch command did not commit: %+v, %v", ack, err)
	}
	s.reopen()
	if got, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: request, Command: command}); err != nil || got != ack {
		t.Fatalf("reserved write receipt changed: %+v, %v", got, err)
	}
}

func TestNativeBranchPublicationBaselineAndStaleAuthority(t *testing.T) {
	ctx := t.Context()
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	auth := localGraphExecution(t, s, "writer")
	base := graphRevisionFromCurrent(t, s, "incremental-base", "")
	graphCoreApply(t, s.e, "incremental-fork", graph.Command{ForkLocal: &graph.ForkLocalCommand{Branch: "incremental", Source: base.ID, Authority: auth}})
	position := localGraphPosition(t, s, "incremental")
	write := &graph.WriteBranchCommand{Target: position, Authority: auth, Batch: localGraphBatch(t, integrationPut([]byte("value"), integrationBytes('a')))}
	graphCoreApply(t, s.e, "incremental-write", graph.Command{WriteBranch: write})
	position = localGraphPosition(t, s, "incremental")
	graphCoreApply(t, s.e, "incremental-publish", graph.Command{PublishBranch: &graph.PublishBranchCommand{Source: position, Authority: auth}})
	current := assertGraphState(t, s, "value", "a", true)
	position = localGraphPosition(t, s, "incremental")
	write.Target = position
	write.Batch = localGraphBatch(t, api.StateMutation{Kind: api.MutationDelete, Key: []byte("value"), Condition: api.ValueCondition{Kind: api.ConditionValue, Value: integrationBytes('a')}})
	graphCoreApply(t, s.e, "incremental-delete", graph.Command{WriteBranch: write})
	position = localGraphPosition(t, s, "incremental")
	graphCoreApply(t, s.e, "incremental-publish-delete", graph.Command{PublishBranch: &graph.PublishBranchCommand{Source: position, Authority: auth, ExpectedDomainRoot: current.StateRoot}})
	assertGraphState(t, s, "value", "", false)
	position = localGraphPosition(t, s, "incremental")
	recoveryCoreControl(t, s, "incremental-fence", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: auth.Execution, ExpectedVersion: 1, Evidence: model.ObjectID(integrationID('b'))}})
	write.Target = position
	write.Batch = localGraphBatch(t, integrationPut([]byte("value"), integrationBytes('b')))
	if result, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: graphCoreKey("fenced-writer"), Command: graph.Command{WriteBranch: write}}); !errors.Is(err, graph.ErrBranchScope) || result.Code != api.CodeRejected {
		t.Fatalf("stale authority changed branch: %+v, %v", result, err)
	}
	if next := localGraphPosition(t, s, "incremental"); next != position {
		t.Fatalf("failed write changed branch: before=%+v after=%+v", position, next)
	}
}
