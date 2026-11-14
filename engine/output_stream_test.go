package engine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func outputStreamFixture(t *testing.T) (*integrationStore, agentprofile.ActivitySpec, agentprofile.AuthorityBinding) {
	t.Helper()
	s, request, source := knowledgeFixture(t, engine.Options{AgentProfile: true, EventProfile: true})
	_, requestBytes := recoveryValueAt(t, s, source, "evidence")
	auth := agentprofile.AuthorityBinding{Deployment: "host", OwnerEpoch: 1, Incarnation: 1, Holder: "worker"}
	recoveryCoreControl(t, s, "stream-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: auth, Status: "active", Evidence: request}}})
	spec := agentprofile.ActivitySpec{Activity: "stream", Execution: "one", Role: "resource_prepare", GoalEpoch: 1, InputFence: source, Request: request, RequestDigest: agentprofile.ExactRequestDigest(requestBytes), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
	recoveryCoreControl(t, s, "stream-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: auth}})
	recoveryCoreControl(t, s, "stream-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "stream-attempt", Sequence: 1, Authority: auth}})
	recoveryCoreControl(t, s, "stream-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "stream-attempt", Authority: auth, Evidence: request}})
	return s, spec, auth
}

func streamChunk(spec agentprofile.ActivitySpec, sequence uint64, payload string) agentprofile.OutputChunk {
	chunk := agentprofile.OutputChunk{Activity: spec.Activity, Attempt: "stream-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Response: "provider-response", Sequence: sequence, Payload: []byte(payload)}
	chunk.ID = agentprofile.OutputChunkID(chunk)
	return chunk
}

func TestStreamOutputPrefixReopensAndPartialNeverBecomesAccepted(t *testing.T) {
	ctx := context.Background()
	s, spec, auth := outputStreamFixture(t)
	first := streamChunk(spec, 1, "first")
	command := agentprofile.ControlCommand{CaptureOutputChunk: &agentprofile.CaptureOutputChunkCommand{Chunk: first}}
	ack := recoveryCoreControl(t, s, "stream-first", command)
	s.reopen()
	state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Lifecycle.Recovery.OutputChunks) != 1 || len(state.Snapshot.Lifecycle.Results) != 0 || !bytes.Equal(state.Snapshot.Lifecycle.Recovery.OutputChunks[0].Payload, first.Payload) {
		t.Fatal("lost durable unfinalized prefix", state, err)
	}
	if again := recoveryCoreControl(t, s, "stream-first", command); again != ack {
		t.Fatal("chunk receipt changed")
	}
	partial := agentprofile.ActivityResult{ID: "partial-result", Activity: spec.Activity, Attempt: first.Attempt, RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: first.Payload, Capture: "partial", Response: first.Response, OutputChunks: []model.ObjectID{first.ID}}
	recoveryCoreControl(t, s, "stream-partial", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: partial}})
	before := integrationSnapshot(t, s.e)
	out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("stream-partial-accept"), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: partial.ID, ExpectedVersion: 1, Authority: auth, Evidence: spec.Request}}}})
	if !errors.Is(err, agentprofile.ErrConflict) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatal("partial accepted or changed publication", out, err)
	}
	late := streamChunk(spec, 2, "second")
	out, err = s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: agentKey("stream-late"), Command: agentprofile.Command{Control: &agentprofile.ControlCommand{CaptureOutputChunk: &agentprofile.CaptureOutputChunkCommand{Chunk: late}}}})
	if !errors.Is(err, agentprofile.ErrUniqueSlot) || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
		t.Fatal("terminal partial allowed more chunks", out, err)
	}
	if _, err = s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	state, err = s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Lifecycle.Results) != 1 || state.Snapshot.Lifecycle.Results[0].Capture != "partial" || state.Snapshot.Lifecycle.Recovery.ActivityControls[0].AcceptedResult != "" {
		t.Fatal("recovery changed partial completion", state, err)
	}
}

func TestStreamOutputCurrentFaultRecoversOnlyCompleteChunkPublication(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[published], func(t *testing.T) {
			ctx := context.Background()
			s, spec, _ := outputStreamFixture(t)
			chunk := streamChunk(spec, 1, "durable prefix")
			command := agentprofile.Command{Control: &agentprofile.ControlCommand{CaptureOutputChunk: &agentprofile.CaptureOutputChunkCommand{Chunk: chunk}}}
			key := agentKey("fault-chunk")
			before := integrationSnapshot(t, s.e)
			if err := s.e.Close(); err != nil {
				t.Fatal(err)
			}
			fault := &workCurrentFaultIO{DurableIO: s.d, after: published}
			var err error
			s.e, _, err = engine.OpenExisting(fault, s.options)
			if err != nil {
				t.Fatal(err)
			}
			fault.armed = true
			if out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key, Command: command}); out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkCurrent) {
				t.Fatal("chunk fault did not remain unknown", out, err)
			}
			s.options.Journal.TailPolicy = storage.TailQuarantine
			s.reopen()
			state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: key.Space, Principal: key.Principal})
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 0
			wantCode := api.CodeNotFoundAtFence
			if published {
				wantCount = 1
				wantCode = api.CodeDurableAck
			}
			lookup := integrationLookup(t, s.e, key)
			if len(state.Snapshot.Lifecycle.Recovery.OutputChunks) != wantCount || len(state.Snapshot.Lifecycle.Results) != 0 || lookup.Code != wantCode {
				t.Fatal("mixed chunk publication", state, lookup)
			}
			if !published && integrationSnapshot(t, s.e) != before {
				t.Fatal("unpublished chunk changed history")
			}
			ack := coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: key, Command: command})
			if published && lookup.Result != ack {
				t.Fatal("published chunk receipt changed")
			}
		})
	}
}
