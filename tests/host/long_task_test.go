package host_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/service"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// This host uses public interfaces exclusively. The same scenario runs through
// the embedded library and a real Unix socket/SDK connection. Model responses
// are deterministic local test data; no provider or GSEA dependency is involved.
type hostAPI interface {
	service.Backend
	api.AgentCommands
	api.WorkCommands
	api.EmissionCommands
	api.GraphCommands
	api.GraphQueries
	api.KnowledgeCommands
	api.MemoryCommands
	api.BasicNodeReader
	api.NodeExplorer
	api.TaskTraceCommands
	api.ContentReader
	api.ViewEntriesReader
}

type host struct {
	t       *testing.T
	wire    bool
	path    string
	run     string
	core    *ate.Store
	peer    hostAPI
	client  *client.Client
	server  *service.Server
	served  chan error
	options ate.Options
}

func newHost(t *testing.T, wire bool) *host {
	t.Helper()
	run, err := os.MkdirTemp("/tmp", "ate-host-")
	if err != nil {
		t.Fatal(err)
	}
	h := &host{t: t, wire: wire, path: t.TempDir(), run: run, options: ate.Options{AgentProfile: true, EventProfile: true, Journal: storage.JournalOptions{SegmentTargetBytes: 16 << 10}}}
	h.core, err = ate.Create(h.path, h.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.close(); _ = os.RemoveAll(run) })
	h.connect()
	return h
}

func (h *host) connect() {
	h.t.Helper()
	h.peer = h.core
	if !h.wire {
		return
	}
	var err error
	h.server, err = service.New(h.core, service.Options{Credentials: map[string]service.Credential{"host-token": {Principal: "owner", Spaces: []model.SpaceID{"project"}, AllowGlobalSnapshot: true}}})
	if err != nil {
		h.t.Fatal(err)
	}
	socket := filepath.Join(h.run, "agent.sock")
	if err = h.server.Listen(socket); err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			h.t.Skipf("native Unix socket binding is denied by this execution environment: %v", err)
		}
		h.t.Fatal(err)
	}
	h.served = make(chan error, 1)
	go func(server *service.Server) { h.served <- server.Serve(context.Background()) }(h.server)
	h.client, err = client.Dial(context.Background(), socket, client.Options{Token: "host-token"})
	if err != nil {
		h.t.Fatal(err)
	}
	h.peer = h.client
}

func (h *host) close() {
	h.t.Helper()
	if h.client != nil {
		if err := h.client.Close(); err != nil {
			h.t.Error(err)
		}
		h.client = nil
	}
	if h.server != nil {
		if err := h.server.Shutdown(context.Background()); err != nil {
			h.t.Error(err)
		}
		if h.served != nil {
			if err := <-h.served; err != nil {
				h.t.Error(err)
			}
			h.served = nil
		}
		h.server = nil
	}
	if h.core != nil {
		if err := h.core.Close(); err != nil {
			h.t.Error(err)
		}
		h.core = nil
	}
}

func (h *host) reopen() {
	h.t.Helper()
	h.close()
	var report storage.RecoveryReport
	var err error
	h.core, report, err = ate.OpenExisting(h.path, h.options)
	if err != nil || !report.Verified || !report.PublicationSynced {
		h.t.Fatalf("host reopen: %+v %v", report, err)
	}
	h.connect()
}

func key(id string) model.RequestKey {
	return model.RequestKey{Space: "project", Principal: "owner", RequestID: model.RequestID(id)}
}
func proof() model.ObjectID {
	return model.ObjectID(fmt.Sprintf("%x", sha256.Sum256([]byte("trusted host fence attestation"))))
}
func requestDigest(raw []byte) model.ObjectID {
	return agentprofile.ExactRequestDigest(raw)
}
func (h *host) ack(result api.CommitResult, err error) api.CommitResult {
	h.t.Helper()
	if err != nil || result.Code != api.CodeDurableAck {
		h.t.Fatalf("host command: %+v %v", result, err)
	}
	return result
}
func (h *host) control(id string, c agentprofile.ControlCommand) api.CommitResult {
	h.t.Helper()
	return h.ack(h.peer.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: key(id), Command: agentprofile.Command{Control: &c}}))
}
func (h *host) snapshot() api.StateSnapshot {
	h.t.Helper()
	s, err := h.peer.Snapshot(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}
func (h *host) entries() (model.ObjectID, []graph.Entry) {
	h.t.Helper()
	v, err := h.peer.OpenView(context.Background(), api.OpenViewRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatalf("entries open view: %v", err)
	}
	defer h.peer.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref})
	page, err := h.peer.GetViewEntries(context.Background(), api.ReadViewEntriesRequest{View: v.Ref})
	if err != nil {
		h.t.Fatalf("entries read view: %v", err)
	}
	out := make([]graph.Entry, len(page.Items))
	for i, item := range page.Items {
		out[i] = graph.Entry{Key: item.Key, Value: item.Value}
	}
	return page.StateRoot, out
}
func (h *host) put(id, name string, raw []byte) (model.ObjectID, api.CommitResult) {
	h.t.Helper()
	ack := h.ack(h.peer.Mutate(context.Background(), api.MutateRequest{Key: key(id), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte(name), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: raw}}}}}))
	_, entries := h.entries()
	for _, entry := range entries {
		if string(entry.Key) == name {
			return entry.Value, ack
		}
	}
	h.t.Fatal("published domain value missing")
	return "", api.CommitResult{}
}
func (h *host) revision(id string, parent model.RevisionID) (graph.Revision, api.CommitResult) {
	h.t.Helper()
	root, entries := h.entries()
	r, err := graph.BuildRevision("project", parent, nil, root, nil, entries)
	if err != nil {
		h.t.Fatal(err)
	}
	ack := h.ack(h.peer.ApplyGraph(context.Background(), api.ApplyGraphRequest{Key: key(id), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{Parent: parent, Root: root, Entries: entries}}}))
	return r, ack
}
func (h *host) grant(id model.ExecutionID) agentprofile.AuthorityBinding {
	h.t.Helper()
	binding := agentprofile.AuthorityBinding{Deployment: "host-one", OwnerEpoch: 1, Incarnation: 1, Holder: string(id)}
	h.control(string(id)+"-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: id, Binding: binding, Status: "active", Evidence: proof()}}})
	return binding
}
func (h *host) resume(id model.ExecutionID, previous agentprofile.AuthorityBinding, checkpoint model.ObjectID) agentprofile.AuthorityBinding {
	h.t.Helper()
	h.control(string(id)+"-fence", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: id, ExpectedVersion: 1, Evidence: proof()}})
	next := previous
	next.Deployment, next.OwnerEpoch, next.Incarnation = "host-two", 2, 2
	h.control(string(id)+"-resume", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: id, ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: next, AdapterVersion: "host-test/v1", Checkpoint: checkpoint, CompatibilityEvidence: proof(), ResourceFenceEvidence: proof()}})
	return next
}

func (h *host) knowledge(key model.KnowledgeKey) api.KnowledgeStateResult {
	h.t.Helper()
	s, err := h.peer.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "project", Principal: "owner", Knowledge: &key})
	if err != nil {
		h.t.Fatalf("knowledge %s/%s: %v", key.Actor, key.Execution, err)
	}
	return s
}

func (h *host) observe(id string, execution model.ExecutionID, payload model.ObjectID, source model.CommitFence, sequence uint64) knowledge.Observation {
	h.t.Helper()
	o := knowledge.Observation{Key: model.KnowledgeKey{Actor: model.ActorID(execution), Execution: execution}, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: source}}, Payload: payload, Sequence: sequence, Label: id}
	o.ID = knowledge.ObservationID(o)
	h.ack(h.peer.ApplyKnowledge(context.Background(), api.ApplyKnowledgeRequest{Key: key(id), Command: knowledge.Command{Observation: &o}}))
	return o
}
func cursorAnchor(s api.KnowledgeStateResult) model.SnapshotAnchor {
	a := s.Anchor.Clone()
	a.Cursor = &model.Cursor{Store: s.Fence.Store, Space: s.Anchor.Space, Revision: s.Revision.ID}
	return a
}

type modelRecord struct {
	packet   knowledge.ContextPacket
	raw      []byte
	input    model.ObjectID
	inputAck api.CommitResult
}

func (h *host) model(id model.ExecutionID, binding agentprofile.AuthorityBinding, goal model.ObjectID) modelRecord {
	h.t.Helper()
	return h.decision(id, binding, goal, nil)
}

// decision accepts an exact provider result and leaves the step open for its
// tool calls. A delegation decision cannot complete before those calls exist.
func (h *host) decision(id model.ExecutionID, binding agentprofile.AuthorityBinding, goal model.ObjectID, invocationSet []agentprofile.Invocation) modelRecord {
	h.t.Helper()
	ctx := context.Background()
	k := model.KnowledgeKey{Actor: model.ActorID(id), Execution: id}
	known := h.knowledge(k)
	var goalObservation knowledge.Observation
	for _, observation := range known.Snapshot.Observations {
		if observation.Payload == goal {
			goalObservation = observation
		}
	}
	if goalObservation.ID == "" {
		h.t.Fatal("execution knowledge has no canonical goal observation")
	}
	anchor := cursorAnchor(known)
	v, err := h.peer.OpenView(ctx, api.OpenViewRequest{Space: "project", Principal: "owner", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		h.t.Fatalf("decision open view %s: %v", id, err)
	}
	defer h.peer.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	raw := []byte(fmt.Sprintf("{\"messages\":[{\"role\":\"user\",\"content\":\"explore %s\"}],\"tools\":[\"file\"],\"framing_version\":1}\n", id))
	input, inputAck := h.put(string(id)+"-exact-request", "request-"+string(id), raw)
	packet := knowledge.ContextPacket{Space: "project", Key: &k, Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: v.ViewDigest, Frontier: known.Frontier}, GoalRef: goal, Renderer: "host-framing/v1", Tokenizer: "test/v1", Input: &knowledge.RecordedInput{Object: input, Anchor: model.SnapshotAnchor{Space: "project", Fence: inputAck.Fence}, Encoding: "application/json"}}
	controlAnchor := model.SnapshotAnchor{Space: "project", Fence: packet.Source.Anchor.Fence}
	agentState, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner", Anchor: &controlAnchor})
	if err != nil {
		h.t.Fatalf("decision agent state %s: %v", id, err)
	}
	images, err := agentprofile.ContextRequirements(agentState.Snapshot, id)
	if err != nil {
		h.t.Fatal(err)
	}
	facts := make([]knowledge.ContextRequirement, 0, len(images))
	itemsByObservation := make(map[model.ObjectID]knowledge.ContextItem, len(images))
	for _, image := range images {
		found := false
		for _, observation := range known.Snapshot.Observations {
			content, readErr := h.peer.ReadContent(ctx, api.ReadContentRequest{View: v.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: observation.Payload}, Projection: "value_bytes"})
			if readErr == nil && bytes.Equal(content.Data, image.Bytes) {
				facts = append(facts, knowledge.ContextRequirement{Kind: image.Kind, ID: image.ID, Observation: observation.ID, Content: observation.Payload})
				itemsByObservation[observation.ID] = knowledge.ContextItem{Observation: observation.ID, Content: observation.Payload, Label: image.Kind}
				found = true
				break
			}
		}
		if !found {
			h.t.Fatalf("missing context requirement image kind=%s id=%s", image.Kind, image.ID)
		}
	}
	mandatory := make([]model.ObjectID, 0, len(facts))
	costs := make([]knowledge.ContextCost, 0, len(facts))
	for _, fact := range facts {
		mandatory = append(mandatory, fact.Observation)
		costs = append(costs, knowledge.ContextCost{Observation: fact.Observation, Tokens: 10})
	}
	selection, err := knowledge.RecordContextSelection(knowledge.ContextSelectionRequest{Snapshot: known.Snapshot, Source: packet.Source, Capacity: knowledge.ContextCapacity{Window: 1024, OutputReserve: 128, FramingReserve: 32}, Mandatory: mandatory, Costs: costs, Tokenizer: packet.Tokenizer, CountQuality: knowledge.TokenCountEstimated})
	if err != nil {
		h.t.Fatal(err)
	}
	packet.Selection = &selection
	for _, observation := range selection.Result.Selected {
		item, ok := itemsByObservation[observation]
		if !ok {
			h.t.Fatal("selected context observation has no item")
		}
		item.Ordinal = uint64(len(packet.Items))
		packet.Items = append(packet.Items, item)
	}
	packet.Requirements = &knowledge.ContextRequirements{RulesVersion: agentprofile.ContextRequirementsVersion, HostPolicyVersion: "host-framing/v1", Facts: facts}
	packet.EstimatedTokens = selection.Result.UsedTokens + selection.Proposal.Capacity.FramingReserve
	packet.ID = knowledge.PacketID(packet)
	if err := packet.Validate(); err != nil {
		h.t.Fatalf("host packet invalid before publish: %v", err)
	}
	if err := knowledge.VerifyPacketSelection(packet, known.Snapshot); err != nil {
		h.t.Fatalf("host packet selection invalid before publish: %v", err)
	}
	packetAck := h.ack(h.peer.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key(string(id) + "-packet"), Command: knowledge.Command{Packet: &packet}}))
	reloadedKnowledge, err := h.peer.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "project", Principal: "owner", Knowledge: &k})
	if err != nil {
		h.t.Fatalf("decision %s persisted packet cannot reload: %v", id, err)
	}
	persistedPacket := false
	for _, persisted := range reloadedKnowledge.Snapshot.Packets {
		if persisted.ID == packet.ID {
			persistedPacket = true
			if !reflect.DeepEqual(persisted.Requirements, packet.Requirements) {
				h.t.Fatalf("decision %s persisted context requirements changed", id)
			}
		}
	}
	if !persistedPacket {
		h.t.Fatalf("decision %s published packet missing after reload", id)
	}
	if _, err := h.peer.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "project", Principal: "owner", Knowledge: &k, Anchor: &model.SnapshotAnchor{Space: "project", Fence: packetAck.Fence}}); err != nil {
		h.t.Fatalf("decision %s packet fence cannot reload: %v", id, err)
	}
	h.control(string(id)+"-step", agentprofile.ControlCommand{BeginStep: &agentprofile.BeginStepCommand{Step: agentprofile.Step{Execution: id, Sequence: 1, GoalEpoch: 1, Status: agentprofile.StepOpen, InputAnchor: packetAck.Fence}, Authority: binding}})
	activity := agentprofile.ActivitySpec{Activity: string(id) + "-model", Execution: id, Role: "primary_decision", Step: 1, GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: requestDigest(raw), Recipient: "deterministic-local-provider", Effect: "pure", ResourceScope: "model-request"}
	record := modelRecord{packet: packet, raw: raw, input: input, inputAck: inputAck}
	h.dispatch(activity, binding)
	// This deterministic provider consumes the stored request bytes through
	// the same public replay path; it does not silently render a fresh prompt.
	actualInput := h.exactInput(record)
	result := agentprofile.ActivityResult{Capture: "full", ID: activity.Activity + "-result", Activity: activity.Activity, Attempt: activity.Activity + "-attempt", RequestDigest: activity.RequestDigest, Incarnation: binding.Incarnation, Outcome: "success", Payload: []byte("decision for exact request " + string(requestDigest(actualInput)))}
	h.control(result.ID+"-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	state, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatal(err)
	}
	var current agentprofile.Continuation
	for _, candidate := range state.Snapshot.Lifecycle.Continuations {
		if candidate.Execution == id {
			current = candidate
		}
	}
	if current.Position == nil {
		h.t.Fatal("model decision has no recovery continuation")
	}
	actionKey := string(id) + "-model-action"
	actionBody := []byte("model decision checkpoint " + string(id))
	position := agentprofile.ContinuationPosition{
		Space: current.Position.Space, Principal: current.Position.Principal, Step: 1,
		AdapterVersion: current.Position.AdapterVersion, NextActions: []agentprofile.PendingActionRef{{Key: actionKey, Generation: 1}},
	}
	continuation := current
	continuation.Version++
	continuation.Incarnation = binding.Incarnation
	continuation.PendingKey = actionKey
	continuation.PendingGeneration = 1
	continuation.InputFence = packetAck.Fence
	continuation.Phase = "model-decision"
	continuation.Position = &position
	intent := agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: actionKey, Owner: id, Generation: 1, Status: agentprofile.ActionPending, Request: key(actionKey), InputFence: packetAck.Fence, CommandDigest: requestDigest(actionBody)}, Method: api.MethodMutateState, ExactCommand: actionBody, Authority: binding}
	decisionBindings := make([]api.DecisionInvocation, 0, len(invocationSet))
	steps := make([]api.WorkStep, 0, len(invocationSet)+3)
	acceptStep, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: activity.Activity, Result: result.ID, ExpectedVersion: 1, Authority: binding, Evidence: proof()}}})
	if err != nil {
		h.t.Fatal(err)
	}
	steps = append(steps, acceptStep)
	for _, invocation := range invocationSet {
		decisionBindings = append(decisionBindings, api.DecisionInvocation{Invocation: invocation.ID, Definition: invocation.ToolDefinition, Version: invocation.ToolVersion, DispatchClass: invocation.DispatchClass})
		step, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{RecordInvocation: &agentprofile.RecordInvocationCommand{Invocation: invocation, Authority: binding}}})
		if err != nil {
			h.t.Fatal(err)
		}
		steps = append(steps, step)
	}
	checkpointStep, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: intent, Continuation: continuation, ExpectedVersion: current.Version, Authority: binding}}})
	if err != nil {
		h.t.Fatal(err)
	}
	steps = append(steps, checkpointStep)
	if len(invocationSet) == 0 {
		finishStep, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{FinishStep: &agentprofile.FinishStepCommand{Execution: id, Sequence: 1, Epoch: 1, Authority: binding}}})
		if err != nil {
			h.t.Fatal(err)
		}
		steps = append(steps, finishStep)
	}
	decision := api.ApplyWorkRequest{Key: key(string(id) + "-model-decision"), Unit: api.WorkUnit{Kind: "model_decision", Decision: &api.DecisionBinding{Activity: activity.Activity, Result: result.ID, Invocations: decisionBindings}, Steps: steps}}
	h.ack(h.peer.ApplyWork(ctx, decision))
	h.control(string(id)+"-model-settled", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: activity.Activity, Status: "settled", Evidence: proof()}})
	return record
}
func (h *host) dispatch(activity agentprofile.ActivitySpec, binding agentprofile.AuthorityBinding) {
	h.t.Helper()
	h.control(activity.Activity+"-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: activity, Authority: binding}})
	h.control(activity.Activity+"-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: activity.Activity, Attempt: activity.Activity + "-attempt", Sequence: 1, Authority: binding}})
	attempt := activity.Activity + "-attempt"
	h.dispatchClaimed(activity, binding, attempt)
}

func (h *host) dispatchClaimed(activity agentprofile.ActivitySpec, binding agentprofile.AuthorityBinding, attempt string) {
	h.t.Helper()
	state, err := h.peer.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
	if err != nil {
		h.t.Fatal(err)
	}
	var task model.EntityID
	var authorityVersion uint64
	for _, execution := range state.Snapshot.Executions {
		if execution.ID == activity.Execution {
			task = execution.Task
		}
	}
	if state.Snapshot.Lifecycle.Recovery != nil {
		for _, authority := range state.Snapshot.Lifecycle.Recovery.Authorities {
			if authority.Execution == activity.Execution {
				authorityVersion = authority.Version
			}
		}
	}
	view, err := h.peer.OpenView(context.Background(), api.OpenViewRequest{Space: "project", Principal: "owner", Anchor: &model.SnapshotAnchor{Space: "project", Fence: activity.InputFence}})
	if err != nil {
		h.t.Fatal(err)
	}
	part, err := h.peer.ReadContent(context.Background(), api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: activity.Request}, Projection: "value_bytes"})
	_ = h.peer.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref})
	if err != nil {
		h.t.Fatal(err)
	}
	grant := api.EmissionSpec{ID: attempt + "-grant", Query: activity.Query, Execution: activity.Execution, Task: task, GoalEpoch: activity.GoalEpoch, Authority: binding, AuthorityVersion: authorityVersion, Content: activity.Request, Anchor: model.SnapshotAnchor{Space: "project", Fence: activity.InputFence}, Projection: "value_bytes", Length: part.TotalBytes, Destination: activity.Recipient, Activity: activity.Activity, Attempt: attempt, RequestDigest: activity.RequestDigest}
	if activity.ContextPacket != "" {
		grant.Content, grant.ContentKind = activity.ContextPacket, "packet"
	}
	h.ack(h.peer.ApplyEmission(context.Background(), api.ApplyEmissionRequest{Key: key(attempt + "-issue"), Command: api.EmissionCommand{Issue: &grant}}))
	claimStep, err := api.EmissionWorkStep(api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: binding})
	if err != nil {
		h.t.Fatal(err)
	}
	dispatchStep, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: attempt, Authority: binding, Evidence: proof()}}})
	if err != nil {
		h.t.Fatal(err)
	}
	h.ack(h.peer.ApplyWork(context.Background(), api.ApplyWorkRequest{Key: key(attempt + "-dispatch"), Unit: api.WorkUnit{Kind: "emission_dispatch", Query: activity.Query, Steps: []api.WorkStep{claimStep, dispatchStep}}}))
}
func (h *host) captureAccept(activity agentprofile.ActivitySpec, binding agentprofile.AuthorityBinding, outcome string, raw []byte) {
	h.t.Helper()
	r := agentprofile.ActivityResult{Capture: "full", ID: activity.Activity + "-result", Activity: activity.Activity, Attempt: activity.Activity + "-attempt", RequestDigest: activity.RequestDigest, Incarnation: binding.Incarnation, Outcome: outcome, Payload: raw}
	h.control(r.ID+"-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: r}})
	h.control(r.ID+"-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: activity.Activity, Result: r.ID, ExpectedVersion: 1, Authority: binding, Evidence: proof()}})
}
func (h *host) exactInput(record modelRecord) []byte {
	h.t.Helper()
	ctx := context.Background()
	view, err := h.peer.OpenView(ctx, api.OpenViewRequest{Space: "project", Principal: "owner", Mode: "execution_knowledge", Knowledge: record.packet.Key})
	if err != nil {
		h.t.Fatalf("exact input view: %v", err)
	}
	defer h.peer.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	var raw []byte
	for {
		chunk, err := h.peer.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: record.input}, Projection: "value_bytes", Offset: uint64(len(raw)), Length: 13})
		if err != nil {
			h.t.Fatalf("exact input read: %v", err)
		}
		raw = append(raw, chunk.Data...)
		if chunk.EOF {
			break
		}
		if len(chunk.Data) == 0 {
			h.t.Fatal("input replay made no progress")
		}
	}
	if !bytes.Equal(raw, record.raw) {
		h.t.Fatal("actual framed request changed on replay")
	}
	return raw
}

// The external worker dies after syncing its side effect and before returning
// a result. The replacement host must reconcile it; executing it again appends
// another line and is caught by the final assertion.
func TestExternalEffectWorker(t *testing.T) {
	if os.Getenv("ATE_HOST_EFFECT_HELPER") != "1" {
		return
	}
	f, err := os.OpenFile(os.Getenv("ATE_HOST_EFFECT_PATH"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		os.Exit(91)
	}
	if _, err = f.WriteString("selected-result\n"); err != nil {
		os.Exit(92)
	}
	if err = f.Sync(); err != nil {
		os.Exit(93)
	}
	if err = f.Close(); err != nil {
		os.Exit(94)
	}
	os.Exit(77)
}

func TestLongTaskPublicHostAcceptance(t *testing.T) {
	for _, wire := range []bool{false, true} {
		t.Run(map[bool]string{false: "embedded", true: "unix-sdk"}[wire], func(t *testing.T) {
			h := newHost(t, wire)
			ctx := context.Background()
			genesis := h.snapshot()
			query := agentprofile.Query{ID: "query", Session: "session", Space: "project", Owner: "owner", Input: []byte("explore alternatives and retain the useful counterexample"), InputFence: genesis.Fence, Status: "open", Version: 1}
			h.ack(h.peer.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key("query"), Command: agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &query}}}))
			rootTask := agentprofile.Task{ID: "root-task", Space: "project", Owner: "owner", Goal: "choose a validated result", GoalEpoch: 1, Status: agentprofile.TaskOpen}
			root := agentprofile.Execution{ID: "root", Task: rootTask.ID, Actor: "root", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
			rootBinding := agentprofile.AuthorityBinding{Deployment: "host-one", OwnerEpoch: 1, Incarnation: 1, Holder: "root"}
			queryPayload, queryAck := h.put("initial-query", "initial-query", query.Input)
			goalImage, err := agentprofile.GoalKnowledge(rootTask)
			if err != nil {
				t.Fatal(err)
			}
			goal, goalAck := h.put("initial-goal", "initial-goal", goalImage)
			rootKey := model.KnowledgeKey{Actor: "root", Execution: "root"}
			queryObservation := knowledge.Observation{Key: rootKey, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: queryAck.Fence}}, Payload: queryPayload, Sequence: 1, Label: "initial Query input"}
			queryObservation.ID = knowledge.ObservationID(queryObservation)
			o := knowledge.Observation{Key: rootKey, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: goalAck.Fence}}, Payload: goal, Sequence: 2, Label: "initial Goal"}
			o.ID = knowledge.ObservationID(o)
			checkpointBody := []byte("root initial checkpoint")
			initialAction := agentprofile.PendingAction{Key: "root-initial-action", Owner: root.ID, Generation: 1, Status: agentprofile.ActionPending, Request: key("root-initial-action"), InputFence: goalAck.Fence, CommandDigest: requestDigest(checkpointBody)}
			actionRaw, err := agentprofile.EncodeContextRequirementImage("action", "root-initial-action/1", initialAction)
			if err != nil {
				t.Fatal(err)
			}
			actionPayload, actionAck := h.put("initial-action", "initial-action", actionRaw)
			actionObservation := knowledge.Observation{Key: rootKey, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: actionAck.Fence}}, Payload: actionPayload, Sequence: 3, Label: "initial action"}
			actionObservation.ID = knowledge.ObservationID(actionObservation)
			checkpoint := agentprofile.CheckpointCommand{
				Intent:       agentprofile.ActionIntent{Action: initialAction, Method: api.MethodMutateState, ExactCommand: checkpointBody, Authority: rootBinding},
				Continuation: agentprofile.Continuation{Execution: root.ID, Version: 1, Incarnation: 1, PendingKey: "root-initial-action", PendingGeneration: 1, InputFence: goalAck.Fence, Phase: "pending-render", Position: &agentprofile.ContinuationPosition{Space: "project", Principal: "owner", AdapterVersion: "host-test/v1", NextActions: []agentprofile.PendingActionRef{{Key: "root-initial-action", Generation: 1}}}},
				Authority:    rootBinding,
			}
			workStep := func(c agentprofile.ControlCommand) api.WorkStep {
				s, err := api.AgentWorkStep(agentprofile.Command{Control: &c})
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			knowledgeStep := func(observation knowledge.Observation) api.WorkStep {
				s, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &observation})
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			start := api.ApplyWorkRequest{Key: key("start-work"), Unit: api.WorkUnit{Kind: "query_start", Query: query.ID, Steps: []api.WorkStep{
				workStep(agentprofile.ControlCommand{StartWork: &agentprofile.StartWorkCommand{Query: query.ID, Slot: "work", ExpectedQueryVersion: 1, Task: rootTask, Execution: root}}),
				workStep(agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: root.ID, Status: "active", Binding: rootBinding, Evidence: proof()}}}),
				knowledgeStep(queryObservation), knowledgeStep(o), knowledgeStep(actionObservation), workStep(agentprofile.ControlCommand{Checkpoint: &checkpoint}),
			}}}
			startBody, err := api.EncodeWorkUnit(start.Unit)
			if err != nil {
				t.Fatal(err)
			}
			intake := api.ApplyWorkRequest{Key: key("query-intake"), Unit: api.WorkUnit{Kind: "query_intake", Query: query.ID, Intake: &api.QueryIntake{Query: query, Route: "host-test/v1", Configuration: []byte(`{"model":"deterministic"}`), Next: api.CommitRequest{Space: start.Key.Space, Principal: start.Key.Principal, RequestID: start.Key.RequestID, Method: api.MethodApplyWork, Body: startBody}}}}
			h.ack(h.peer.ApplyWork(ctx, intake))
			h.ack(h.peer.ApplyWork(ctx, start))
			seed := h.knowledge(rootKey)
			base, _ := h.revision("base-revision", "")
			h.forkLocal("chosen", base.ID, "root", rootBinding)
			delegate := agentprofile.ToolDefinition{Name: "delegate", Version: "delegate/v1", SchemaEncoding: "json-schema/v1", Schema: []byte(`{"type":"object"}`), DispatchMode: "delegate", DispatchClass: "pure", Contract: agentprofile.EffectContract{
				Version: "effect/v1", Recipient: "host-controller", VerifiedPure: true, DifferentRequest: "unsupported", ResultQuery: "none", ResourceFence: "none", Compensation: "none", LateArrivalFence: "none",
			}}
			delegateBytes, err := delegate.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			delegateID, _ := h.put("delegate-definition", "delegate-definition", delegateBytes)
			rootInvocations := []agentprofile.Invocation{
				{ID: "spawn-left", Execution: "root", Step: 1, ProviderCallKey: "delegate-left", Tool: "delegate", Attempt: 1, Status: agentprofile.InvocationPending, AcceptedResult: "root-model-result", ToolDefinition: delegateID, ToolVersion: "delegate/v1", DispatchClass: "pure"},
				{ID: "spawn-right", Execution: "root", Step: 1, ProviderCallKey: "delegate-right", Tool: "delegate", Attempt: 1, Status: agentprofile.InvocationPending, AcceptedResult: "root-model-result", ToolDefinition: delegateID, ToolVersion: "delegate/v1", DispatchClass: "pure"},
			}
			rootModel := h.decision("root", rootBinding, goal, rootInvocations)
			bindings := map[model.ExecutionID]agentprofile.AuthorityBinding{}
			childGoals := map[model.ExecutionID]model.ObjectID{}
			for _, id := range []model.ExecutionID{"left", "right"} {
				inv := rootInvocations[0]
				if id == "right" {
					inv = rootInvocations[1]
				}
				task := agentprofile.Task{ID: model.EntityID(string(id) + "-task"), Space: "project", Owner: "owner", Goal: "explore " + string(id), GoalEpoch: 1, Status: agentprofile.TaskOpen}
				e := agentprofile.Execution{ID: id, Task: task.ID, Parent: "root", Actor: model.ActorID(id), Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
				childBinding := agentprofile.AuthorityBinding{Deployment: "host-one", OwnerEpoch: 1, Incarnation: 1, Holder: string(id)}
				bindings[id] = childBinding
				command := knowledge.ImportCommand{Target: model.KnowledgeKey{Actor: e.Actor, Execution: id}, Source: knowledge.SourceAnchor{Anchor: cursorAnchor(seed), Frontier: seed.Frontier}, Observations: []model.ObjectID{o.ID}}
				childGoalRaw, err := agentprofile.GoalKnowledge(task)
				if err != nil {
					t.Fatal(err)
				}
				childGoal, childGoalAck := h.put("child-goal-"+string(id), "child-goal-"+string(id), childGoalRaw)
				childGoals[id] = childGoal
				childKey := model.KnowledgeKey{Actor: e.Actor, Execution: e.ID}
				childGoalObservation := knowledge.Observation{Key: childKey, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: childGoalAck.Fence}}, Payload: childGoal, Sequence: 2, Label: "initial child Goal"}
				childGoalObservation.ID = knowledge.ObservationID(childGoalObservation)
				childCheckpointBody := []byte("child initial checkpoint " + string(id))
				childAction := agentprofile.PendingAction{Key: string(id) + "-initial-action", Owner: id, Generation: 1, Status: agentprofile.ActionPending, Request: key(string(id) + "-initial-action"), InputFence: childGoalAck.Fence, CommandDigest: requestDigest(childCheckpointBody)}
				actionRaw, err := agentprofile.EncodeContextRequirementImage("action", string(id)+"-initial-action/1", childAction)
				if err != nil {
					t.Fatal(err)
				}
				actionPayload, actionAck := h.put("child-action-"+string(id), "child-action-"+string(id), actionRaw)
				childActionObservation := knowledge.Observation{Key: childKey, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: actionAck.Fence}}, Payload: actionPayload, Sequence: 3, Label: "initial child action"}
				childActionObservation.ID = knowledge.ObservationID(childActionObservation)
				childCheckpoint := agentprofile.CheckpointCommand{Intent: agentprofile.ActionIntent{Action: childAction, Method: api.MethodMutateState, ExactCommand: childCheckpointBody, Authority: childBinding}, Continuation: agentprofile.Continuation{Execution: id, Version: 1, Incarnation: 1, PendingKey: string(id) + "-initial-action", PendingGeneration: 1, InputFence: childGoalAck.Fence, Phase: "pending-render", Position: &agentprofile.ContinuationPosition{Space: "project", Principal: "owner", AdapterVersion: "host-test/v1", NextActions: []agentprofile.PendingActionRef{{Key: string(id) + "-initial-action", Generation: 1}}}}, Authority: childBinding}
				seedStep, err := api.KnowledgeWorkStep(knowledge.Command{Import: &command})
				if err != nil {
					t.Fatal(err)
				}
				goalStep, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &childGoalObservation})
				if err != nil {
					t.Fatal(err)
				}
				childStart := api.ApplyWorkRequest{Key: key("spawn-child-" + string(id)), Unit: api.WorkUnit{Kind: "child_spawn", Steps: []api.WorkStep{
					workStep(agentprofile.ControlCommand{SpawnChild: &agentprofile.SpawnChildCommand{Parent: "root", Invocation: inv.ID, Task: task, Execution: e, InheritedEpoch: 1, Compatibility: proof(), Authority: rootBinding}}),
					workStep(agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: id, Binding: childBinding, Status: "active", Evidence: proof()}}}),
					seedStep, goalStep, func() api.WorkStep {
						s, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &childActionObservation})
						if err != nil {
							t.Fatal(err)
						}
						return s
					}(), workStep(agentprofile.ControlCommand{Checkpoint: &childCheckpoint}),
				}}}
				h.ack(h.peer.ApplyWork(ctx, childStart))
				h.forkLocal(model.BranchID(id), base.ID, id, childBinding)
			}
			leftModel, rightModel := h.model("left", bindings["left"], childGoals["left"]), h.model("right", bindings["right"], childGoals["right"])
			toolRequest := []byte("append exactly one selected result")
			toolInput, toolAck := h.put("tool-request", "tool-request", toolRequest)
			tool := agentprofile.ActivitySpec{Activity: "left-file-tool", Execution: "left", Role: "resource_prepare", GoalEpoch: 1, InputFence: toolAck.Fence, Request: toolInput, RequestDigest: requestDigest(toolRequest), Recipient: "external-file-worker", Effect: "non_idempotent", ResourceScope: "host-workspace"}
			h.dispatch(tool, bindings["left"])
			effectPath := filepath.Join(h.run, "effect")
			cmd := exec.Command(os.Args[0], "-test.run=^TestExternalEffectWorker$")
			cmd.Env = append(os.Environ(), "ATE_HOST_EFFECT_HELPER=1", "ATE_HOST_EFFECT_PATH="+effectPath)
			if err := cmd.Run(); err == nil {
				t.Fatal("worker failed to stop before response")
			} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 77 {
				t.Fatal(err)
			}
			before, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			h.reopen()
			after, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
			beforeBytes, beforeErr := agentprofile.EncodeSnapshot(before.Snapshot)
			afterBytes, afterErr := agentprofile.EncodeSnapshot(after.Snapshot)
			if err != nil || beforeErr != nil || afterErr != nil || !bytes.Equal(beforeBytes, afterBytes) {
				t.Fatal("host restart changed committed facts", err)
			}
			rootBinding = h.resume("root", rootBinding, toolInput)
			for _, id := range []model.ExecutionID{"left", "right"} {
				bindings[id] = h.resume(id, bindings[id], toolInput)
			}
			if out, err := h.peer.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key("unsafe-retry"), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{RetryActivity: &agentprofile.RetryActivityCommand{Activity: tool.Activity, ExpectedVersion: 1, Authority: bindings["left"]}}}}); err == nil || out.Code != api.CodeRejected {
				t.Fatal("unknown side effect was retried", out, err)
			}
			effect, err := os.ReadFile(effectPath)
			if err != nil || string(effect) != "selected-result\n" {
				t.Fatal("cannot reconcile actual side effect", err)
			}
			late := agentprofile.ActivityResult{Capture: "full", ID: "left-file-result", Activity: tool.Activity, Attempt: tool.Activity + "-attempt", RequestDigest: tool.RequestDigest, Incarnation: 1, Outcome: "success", Payload: effect}
			h.control("late-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: late}})
			h.control("effect-reconciled", agentprofile.ControlCommand{ReconcileAttempt: &agentprofile.ReconcileAttemptCommand{Attempt: late.Attempt, Outcome: "occurred", Evidence: proof()}})
			h.control("adopt-reconciled-result", agentprofile.ControlCommand{ImportActivityResult: &agentprofile.AcceptActivityCommand{Activity: tool.Activity, Result: late.ID, ExpectedVersion: 1, Authority: bindings["left"], Evidence: proof()}})
			h.control("left-tool-settled", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: tool.Activity, Status: "settled", Evidence: proof()}})
			winner, winnerAck := h.put("left-evidence", "winning-evidence", effect)
			leftKnowledge := h.knowledge(model.KnowledgeKey{Actor: "left", Execution: "left"})
			winnerObservation := h.observe("left-observed-result", "left", winner, winnerAck.Fence, uint64(len(leftKnowledge.Snapshot.Observations)+1))
			left, leftAck := h.writeBranch("left", base.ID, "left", bindings["left"], []api.StateMutation{branchValue("answer", effect)})
			send := func(id model.ExecutionID, body string) {
				t.Helper()
				m := agentprofile.Message{ID: string(id) + "-message", Sender: id, Recipient: "root", Payload: []byte(body), GoalEpoch: 1}
				messagePayload, messagePayloadAck := h.put(m.ID+"-payload", m.ID+"-payload", m.Payload)
				h.control(m.ID, agentprofile.ControlCommand{SendMessage: &agentprofile.SendMessageCommand{Message: m, Authority: bindings[id]}})
				rootKnowledge := h.knowledge(rootKey)
				observation := knowledge.Observation{Key: rootKey, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "project", Fence: messagePayloadAck.Fence}}, Payload: messagePayload, Sequence: uint64(len(rootKnowledge.Snapshot.Observations) + 1), Label: "received message"}
				observation.ID = knowledge.ObservationID(observation)
				state, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
				if err != nil {
					t.Fatal(err)
				}
				var current agentprofile.Continuation
				for _, candidate := range state.Snapshot.Lifecycle.Continuations {
					if candidate.Execution == "root" {
						current = candidate
					}
				}
				if current.Position == nil {
					t.Fatal("root continuation missing before message consume")
				}
				continuation := current
				continuation.Version++
				continuation.Incarnation = rootBinding.Incarnation
				continuation.Phase = "message-consume"
				position := *current.Position
				position.KnowledgeSource = nil
				position.KnowledgeFence = model.CommitFence{}
				position.KnowledgeCursor = model.Cursor{}
				position.KnowledgeKey = model.KnowledgeKey{}
				continuation.Position = &position
				consumeStep, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{ConsumeMessage: &agentprofile.ConsumeMessageCommand{Consumption: agentprofile.MessageConsumption{Message: m.ID, Recipient: "root", Incarnation: rootBinding.Incarnation, GoalEpoch: 1}, Authority: rootBinding}}})
				if err != nil {
					t.Fatal(err)
				}
				observationStep, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &observation})
				if err != nil {
					t.Fatal(err)
				}
				saveStep, err := api.AgentWorkStep(agentprofile.Command{Control: &agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: continuation, ExpectedVersion: current.Version, Authority: rootBinding}}})
				if err != nil {
					t.Fatal(err)
				}
				h.ack(h.peer.ApplyWork(ctx, api.ApplyWorkRequest{Key: key(m.ID + "-consume"), Unit: api.WorkUnit{Kind: "message_consume", Steps: []api.WorkStep{observationStep, consumeStep, saveStep}}}))
			}
			send("left", "validated file output available")
			resolvedFence := h.snapshot().Fence
			h.control("root-model-action-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "root-model-action", Generation: 1, Outcome: "absent", ObservedFence: resolvedFence}}})
			h.control("left-model-action-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "left-model-action", Generation: 1, Outcome: "absent", ObservedFence: resolvedFence}}})
			h.control("left-initial-action-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "left-initial-action", Generation: 1, Outcome: "absent", ObservedFence: resolvedFence}}})
			h.completeTask("left-complete", agentprofile.CompleteTaskCommand{Task: "left-task", Execution: "left", Epoch: 1, Result: winner, Authority: bindings["left"]})
			rightTool := agentprofile.ActivitySpec{Activity: "right-validator", Execution: "right", Role: "resource_prepare", GoalEpoch: 1, InputFence: toolAck.Fence, Request: toolInput, RequestDigest: requestDigest(toolRequest), Recipient: "file-validator", Effect: "pure", ResourceScope: "host-workspace"}
			h.dispatch(rightTool, bindings["right"])
			h.captureAccept(rightTool, bindings["right"], "failure", []byte("counterexample: alternative disagrees with observed file"))
			h.control("right-tool-settled", agentprofile.ControlCommand{SettleActivity: &agentprofile.SettleActivityCommand{Activity: rightTool.Activity, Status: "settled", Evidence: proof()}})
			counter, counterAck := h.put("right-counterexample", "counterexample", []byte("alternative does not match the actual file; retain this constraint"))
			rightKnowledge := h.knowledge(model.KnowledgeKey{Actor: "right", Execution: "right"})
			counterObservation := h.observe("right-observed-counterexample", "right", counter, counterAck.Fence, uint64(len(rightKnowledge.Snapshot.Observations)+1))
			right, rightAck := h.writeBranch("right", base.ID, "right", bindings["right"], []api.StateMutation{branchValue("answer", []byte("invalid-alternative")), branchValue("counterexample", []byte("alternative does not match the actual file; retain this constraint"))})
			send("right", "failed alternative; useful counterexample retained")
			resolvedFence = h.snapshot().Fence
			h.control("right-model-action-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "right-model-action", Generation: 1, Outcome: "absent", ObservedFence: resolvedFence}}})
			h.control("right-initial-action-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "right-initial-action", Generation: 1, Outcome: "absent", ObservedFence: resolvedFence}}})
			h.completeTask("right-complete", agentprofile.CompleteTaskCommand{Task: "right-task", Execution: "right", Epoch: 1, Result: counter, Authority: bindings["right"]})
			// Message delivery/consumption does not silently grant payloads. The
			// root explicitly adopts each child's selected observation with its
			// complete source position before deciding what to merge.
			for i, adopted := range []knowledge.Observation{winnerObservation, counterObservation} {
				known := h.knowledge(adopted.Key)
				rootKnown := h.knowledge(rootKey)
				// Import CAS compares against the number of observations already
				// present for the target and source space. The newly imported item
				// receives the next sequence inside applyImport.
				expectedSequence := uint64(0)
				for _, observation := range rootKnown.Snapshot.Observations {
					if observation.Key == rootKey && observation.Source.Anchor.Space == "project" {
						expectedSequence++
					}
				}
				command := knowledge.ImportCommand{Target: rootKey, Source: knowledge.SourceAnchor{Anchor: cursorAnchor(known), Frontier: known.Frontier}, ExpectedSequence: expectedSequence, Observations: []model.ObjectID{adopted.ID}}
				h.ack(h.peer.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: key(fmt.Sprintf("root-adopt-child-%d", i)), Command: knowledge.Command{Import: &command}}))
			}
			leftPosition := graph.BranchPosition{Branch: "left", Head: left.ID, Version: 1}
			rightPosition := graph.BranchPosition{Branch: "right", Head: right.ID, Version: 1}
			rootAuthority := branchAuthority("root", rootBinding)
			h.ack(h.peer.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key("join-root-left"), Command: graph.Command{JoinLocal: &graph.JoinLocalCommand{Target: graph.BranchPosition{Branch: "chosen", Head: base.ID}, Source: leftPosition, Authority: rootAuthority}}}))
			chosen := h.branch("chosen")
			plan, err := h.peer.PlanGraphJoin(ctx, api.PlanGraphJoinRequest{Space: "project", Left: chosen.Head, Right: right.ID})
			if err != nil || len(plan.Plan.Conflicts) != 1 {
				t.Fatalf("join plan=%+v %v", plan, err)
			}
			joinReq := api.ApplyGraphRequest{Key: key("join-chosen"), Command: graph.Command{JoinLocal: &graph.JoinLocalCommand{
				Target: graph.BranchPosition{Branch: chosen.ID, Head: chosen.Head, Version: chosen.Version}, Source: rightPosition, Authority: rootAuthority,
				Resolutions: []graph.Resolution{{Key: []byte("answer"), Value: winner}},
			}}}
			joined := h.ack(h.peer.ApplyGraph(ctx, joinReq))
			chosen = h.branch("chosen")
			current, _ := h.entries()
			h.ack(h.peer.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key("publish-chosen"), Command: graph.Command{PublishBranch: &graph.PublishBranchCommand{
				Source: graph.BranchPosition{Branch: chosen.ID, Head: chosen.Head, Version: chosen.Version}, Authority: rootAuthority, ExpectedDomainRoot: current,
			}}}))
			value, err := h.peer.GetState(ctx, api.ReadStateRequest{Space: "project", Key: []byte("answer")})
			if err != nil || !bytes.Equal(value.Value.Data, effect) {
				t.Fatal("join did not adopt selected result", err)
			}
			leftAnchor := model.SnapshotAnchor{Space: "project", Fence: leftAck.Fence, Cursor: &model.Cursor{Store: leftAck.Fence.Store, Space: "project", Revision: left.ID}}
			rightAnchor := model.SnapshotAnchor{Space: "project", Fence: rightAck.Fence, Cursor: &model.Cursor{Store: rightAck.Fence.Store, Space: "project", Revision: right.ID}}
			winningSource := memory.Source{SourceID: winner, Anchor: leftAnchor}
			counterSource := memory.Source{SourceID: counter, Anchor: rightAnchor}
			m := memory.Memory{Space: "project", Kind: memory.KindDecision, Claim: []byte("choose validated file output; avoid the failed alternative"), Sources: []memory.Source{winningSource}, Evidence: []memory.Evidence{{Source: counterSource, Counter: true, Note: "useful counterexample"}}}
			h.ack(h.peer.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("memory-candidate"), Command: memory.Command{PutCandidate: &m}}))
			mem, err := h.peer.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "project"})
			if err != nil {
				t.Fatal(err)
			}
			memoryID := mem.Snapshot.Memories[0].ID
			validation := memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{winningSource}, Counterexamples: []memory.Source{counterSource}}
			h.ack(h.peer.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("memory-validate"), Command: memory.Command{Validate: &memory.ValidateCommand{ID: memoryID, Validation: validation}}}))
			mem, err = h.peer.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "project"})
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range mem.Snapshot.Memories {
				if item.Status == memory.StatusValidated {
					memoryID = item.ID
				}
			}
			h.ack(h.peer.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("memory-promote"), Command: memory.Command{Promote: &memoryID}}))
			mem, err = h.peer.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "project"})
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range mem.Snapshot.Memories {
				if item.Status == memory.StatusPromoted {
					memoryID = item.ID
				}
			}
			h.ack(h.peer.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("experience-path"), Command: memory.Command{BuildPath: &memory.BuildPathCommand{Space: "project", Anchor: model.SnapshotAnchor{Space: "project", Fence: h.snapshot().Fence}, Steps: []memory.Step{{Node: model.NodeRef{Kind: model.NodeRevision, Revision: left.ID}, Memory: memoryID, Outcome: "validated choice"}}, Omitted: []model.NodeRef{{Kind: model.NodeRevision, Revision: right.ID}}, Validation: validation}}}))
			h.ack(h.peer.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key("prune-failed"), Command: graph.Command{Prune: &graph.PruneBranchCommand{Branch: "right", Expected: 1, Head: right.ID, Reason: "failed exploration retained as counterexample"}}}))
			ancestor, err := h.peer.GetGraphRevision(ctx, api.ReadGraphRevisionRequest{Space: "project", ID: base.ID})
			if err != nil || !ancestor.Found {
				t.Fatal("pruning removed shared ancestor", err)
			}
			rootFinish := agentprofile.ControlCommand{FinishStep: &agentprofile.FinishStepCommand{Execution: "root", Sequence: 1, Epoch: 1, Authority: rootBinding}}
			beforeFinish := h.snapshot()
			direct := key("standalone-root-finish")
			if out, err := h.peer.ApplyAgent(ctx, api.ApplyAgentRequest{Key: direct, Command: agentprofile.Command{Control: &rootFinish}}); err == nil || out.Code != api.CodeRejected || h.snapshot() != beforeFinish {
				t.Fatalf("standalone step finish: %+v %v", out, err)
			}
			if got, err := h.peer.LookupRequest(ctx, api.LookupRequestRequest{Space: direct.Space, Principal: direct.Principal, RequestID: direct.RequestID}); err != nil || got.Code != api.CodeNotFoundAtFence || got.ObservedFence != beforeFinish.Fence {
				t.Fatalf("standalone step finish left a receipt: %+v %v", got, err)
			}
			state, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			var rootPosition agentprofile.Continuation
			for _, c := range state.Snapshot.Lifecycle.Continuations {
				if c.Execution == "root" {
					rootPosition = c
				}
			}
			if rootPosition.Position == nil {
				t.Fatal("root step has no prior recovery position")
			}
			actionKey := "root-after-step"
			actionBatch := api.StateBatch{Operations: []api.StateMutation{{
				Key: []byte(actionKey), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("root step completed")},
			}}}
			actionBody, err := engine.EncodeStateBatch(actionBatch)
			if err != nil {
				t.Fatal(err)
			}
			next := agentprofile.ActionIntent{
				Action: agentprofile.PendingAction{Key: actionKey, Owner: "root", Generation: 1, Status: agentprofile.ActionPending, Request: key(actionKey), InputFence: beforeFinish.Fence, CommandDigest: requestDigest(actionBody)},
				Method: api.MethodMutateState, ExactCommand: actionBody, Authority: rootBinding,
			}
			closed := agentprofile.Continuation{
				Execution: "root", Version: rootPosition.Version + 1, Incarnation: rootBinding.Incarnation,
				PendingKey: actionKey, PendingGeneration: 1, InputFence: beforeFinish.Fence, Phase: "step-completed",
				Position: &agentprofile.ContinuationPosition{Space: "project", Principal: "owner", Step: 1, AdapterVersion: rootPosition.Position.AdapterVersion, NextActions: []agentprofile.PendingActionRef{{Key: actionKey, Generation: 1}}},
			}
			finishStep, err := api.AgentWorkStep(agentprofile.Command{Control: &rootFinish})
			if err != nil {
				t.Fatal(err)
			}
			closeControl := agentprofile.ControlCommand{Checkpoint: &agentprofile.CheckpointCommand{Intent: next, Continuation: closed, ExpectedVersion: rootPosition.Version, Authority: rootBinding}}
			checkpointStep, err := api.AgentWorkStep(agentprofile.Command{Control: &closeControl})
			if err != nil {
				t.Fatal(err)
			}
			h.ack(h.peer.ApplyWork(ctx, api.ApplyWorkRequest{Key: key("root-step-finished"), Unit: api.WorkUnit{Kind: "checkpoint", Steps: []api.WorkStep{finishStep, checkpointStep}}}))
			closedActionAck := h.ack(h.peer.Mutate(ctx, api.MutateRequest{Key: next.Action.Request, Batch: actionBatch}))
			h.control("root-after-step-applied", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: actionKey, Generation: 1, Outcome: "applied", ObservedFence: closedActionAck.Fence, Receipt: closedActionAck.ResultDigest}}})
			resolvedFence = h.snapshot().Fence
			h.control("root-initial-action-absent", agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "root-initial-action", Generation: 1, Outcome: "absent", ObservedFence: resolvedFence}}})
			h.completeTask("root-completed", agentprofile.CompleteTaskCommand{Task: "root-task", Execution: "root", Epoch: 1, Result: winner, Authority: rootBinding})
			h.answerCompletedQuery(query, []byte("validated result with retained counterexample"))
			traceRequest, trace := h.recordLongTaskTrace(query.ID)
			var fork model.EventID
			for _, node := range trace.Nodes {
				if node.Operation == "Graph.ForkLocal" && node.Subject.ID == "chosen" {
					fork = node.At.Ref.Event
				}
			}
			var baseBoundary model.EventID
			for _, edge := range trace.Edges {
				if edge.Relation == "causal" && edge.To == fork && edge.Boundary {
					baseBoundary = edge.From
				}
			}
			if fork == "" || baseBoundary == "" {
				t.Fatal("task trace lost the causal boundary to its shared state baseline")
			}
			boundaryPage, err := h.peer.GetTaskTraceEvidence(ctx, api.TaskTraceEvidenceRequest{TaskTraceRequest: traceRequest, From: baseBoundary, To: fork, Relation: "causal"})
			if err != nil || len(boundaryPage.Events) != 2 || !boundaryPage.Events[0].Boundary || boundaryPage.Events[0].Operation != "Graph.AddRevision" ||
				boundaryPage.Events[0].Subject.ID != model.EntityID(base.ID) || boundaryPage.Events[1].Boundary || boundaryPage.Events[1].At.Ref.Event != fork {
				t.Fatalf("scoped causal boundary: %+v %v", boundaryPage, err)
			}
			h.exactInput(leftModel)
			h.exactInput(rightModel)
			h.exactInput(rootModel)
			strict, err := h.peer.OpenView(ctx, api.OpenViewRequest{Space: "project", Principal: "owner", Mode: "execution_knowledge", Anchor: &leftModel.packet.Source.Anchor})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = h.peer.GetNode(ctx, api.GetNodeRequest{View: strict.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: counter}}); err == nil {
				t.Fatal("strict past disclosed future counterexample")
			}
			h.peer.CloseView(ctx, api.CloseViewRequest{Ref: strict.Ref})
			audit, err := h.peer.OpenView(ctx, api.OpenViewRequest{Space: "project", Principal: "owner", Mode: "audit_snapshot"})
			if err != nil {
				t.Fatal(err)
			}
			beforeCompact, err := h.peer.GetNode(ctx, api.GetNodeRequest{View: audit.Ref, Ref: model.NodeRef{Kind: model.NodeRevision, Revision: right.ID}})
			if err != nil {
				t.Fatal(err)
			}
			report, err := h.core.CompactAuditFull(ctx)
			if err != nil || report.Before.Fence() != report.After.Fence() {
				t.Fatal("compaction changed logical cut", err)
			}
			afterCompact, err := h.peer.GetNode(ctx, api.GetNodeRequest{View: audit.Ref, Ref: beforeCompact.Ref})
			if err != nil || !reflect.DeepEqual(beforeCompact, afterCompact) {
				t.Fatal("active reader changed during compaction", err)
			}
			h.peer.CloseView(ctx, api.CloseViewRequest{Ref: audit.Ref})
			h.reopen()
			if recovered, err := h.peer.GetTaskTrace(ctx, traceRequest); err != nil || !reflect.DeepEqual(trace, recovered) {
				t.Fatalf("trace changed after compaction and recovery: %+v %v", recovered, err)
			}
			h.exactInput(leftModel)
			h.exactInput(rightModel)
			h.exactInput(rootModel)
			if retry := h.ack(h.peer.ApplyGraph(ctx, joinReq)); retry != joined {
				t.Fatal("reopen changed original merge receipt")
			}
			final, err := h.peer.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "project", Principal: "owner"})
			if err != nil || len(final.Snapshot.Lifecycle.Recovery.Outcomes) != 3 || len(final.Snapshot.Lifecycle.Recovery.ValidationRequirements) != 3 || len(final.Snapshot.Lifecycle.Consumptions) != 2 || len(final.Snapshot.Lifecycle.Recovery.Imports) != 1 || len(final.Snapshot.Lifecycle.Activities) != 6 || len(final.Snapshot.Lifecycle.Attempts) != 7 {
				t.Fatal("long task recovery lost control decisions", err)
			}
			mem, err = h.peer.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "project"})
			if err != nil || len(mem.Snapshot.Paths) != 1 || len(mem.Snapshot.Paths[0].Omitted) != 1 || len(mem.Snapshot.Paths[0].Validation.Counterexamples) != 1 {
				t.Fatal("experience omitted provenance or useful failure", err)
			}
			branch, err := h.peer.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "project", ID: "right"})
			if err != nil || !branch.Pruned {
				t.Fatal("failed branch prune not durable", err)
			}
			effect, err = os.ReadFile(effectPath)
			if err != nil || string(effect) != "selected-result\n" {
				t.Fatal("recovery duplicated the non-idempotent tool", err)
			}
			if winnerAck.Fence.LSN >= counterAck.Fence.LSN {
				t.Fatal("bad history fixture")
			}
			known, err := h.peer.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "project", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			observationCounts := map[model.ExecutionID]int{}
			importedOrigins := map[model.ExecutionID]map[model.ObjectID]bool{}
			for _, observation := range known.Snapshot.Observations {
				observationCounts[observation.Key.Execution]++
				if observation.ImportedFrom != "" {
					if importedOrigins[observation.Key.Execution] == nil {
						importedOrigins[observation.Key.Execution] = map[model.ObjectID]bool{}
					}
					importedOrigins[observation.Key.Execution][observation.ImportedFrom] = true
				}
			}
			// Owner-scoped reads include all three executions. Each child's
			// initial parent-goal import is distinct from root adopting results.
			wantCounts := map[model.ExecutionID]int{"root": 7, "left": 4, "right": 4}
			wantImports := map[model.ExecutionID]map[model.ObjectID]bool{
				"root": {winnerObservation.ID: true, counterObservation.ID: true},
				"left": {o.ID: true}, "right": {o.ID: true},
			}
			if !reflect.DeepEqual(observationCounts, wantCounts) || !reflect.DeepEqual(importedOrigins, wantImports) || len(known.Snapshot.Packets) != 4 {
				t.Fatalf("explicit multi-execution adoption history changed: observations=%v imports=%v packets=%d", observationCounts, importedOrigins, len(known.Snapshot.Packets))
			}
			t.Logf("verified LSN=%d; executions=3, managed activities/attempts=6/7, messages/consumptions=2/2, observations=15 (root=7, left=4, right=4), exact packets=4, memory audit versions=%d, experience paths=1, pruned branches=1, non-idempotent external writes=1", final.Fence.LSN, len(mem.Snapshot.Memories))
		})
	}
}
