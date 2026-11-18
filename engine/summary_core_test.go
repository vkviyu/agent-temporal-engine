package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func summaryCoreFixture(t *testing.T, options ...engine.Options) (*integrationStore, knowledge.Summary) {
	t.Helper()
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t, options...)
	observation := knowledgeObservation(payload, source, "one", 1)
	applyObservation(t, s.e, "summary-adopt", observation)
	request := []byte("framed summarizer input containing source observation and its evidence bytes")
	requestAck := integrationMutate(t, s.e, integrationRequest("summary-input", integrationPut([]byte("summary-input"), api.Value{Kind: api.ValueBytes, Data: request})))
	input, _ := recoveryValueAt(t, s, requestAck.Fence, "summary-input")
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &observation.Key})
	if err != nil || known.Revision == nil {
		t.Fatal(known, err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &anchor})
	if err != nil {
		t.Fatal(err)
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &observation.Key, Source: knowledge.SourceAnchor{Anchor: anchor, ViewDigest: view.ViewDigest, Frontier: known.Frontier}, GoalRef: payload, Items: []knowledge.ContextItem{{Observation: observation.ID, Content: payload, Label: "early evidence"}}, Renderer: "summary-request/v1", Tokenizer: "test/v1", Input: &knowledge.RecordedInput{Object: input, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: requestAck.Fence}, Encoding: "application/summary-request-v1"}}
	packet.ID = knowledge.PacketID(packet)
	if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
		t.Fatal(err)
	}
	packetAck, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("summary-packet"), Command: knowledge.Command{Packet: &packet}})
	if err != nil || packetAck.Code != api.CodeDurableAck {
		t.Fatal(packetAck, err)
	}
	proof := model.ObjectID(integrationID('a'))
	binding := agentprofile.AuthorityBinding{Deployment: "summary-host", OwnerEpoch: 1, Incarnation: 1, Holder: "summarizer"}
	recoveryCoreControl(t, s, "summary-authority", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Binding: binding, Status: "active", Evidence: proof}}})
	spec := agentprofile.ActivitySpec{Activity: "summary-generation", Execution: "one", Role: "aux_summary", GoalEpoch: 1, InputFence: packetAck.Fence, ContextPacket: packet.ID, Request: input, RequestDigest: agentprofile.ExactRequestDigest(request), Recipient: "fixture-model/v1", Effect: "pure", ResourceScope: "session"}
	recoveryCoreControl(t, s, "summary-activity", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec, Authority: binding}})
	recoveryCoreControl(t, s, "summary-claim", agentprofile.ControlCommand{ClaimAttempt: &agentprofile.ClaimAttemptCommand{Activity: spec.Activity, Attempt: "summary-attempt", Sequence: 1, Authority: binding}})
	recoveryCoreControl(t, s, "summary-dispatch", agentprofile.ControlCommand{DispatchAttempt: &agentprofile.DispatchAttemptCommand{Attempt: "summary-attempt", Authority: binding, Evidence: proof}})
	output := []byte("Early evidence remains relevant. Keep the original source for audit.")
	result := agentprofile.ActivityResult{Capture: "full", ID: "summary-result", Activity: spec.Activity, Attempt: "summary-attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: output}
	recoveryCoreControl(t, s, "summary-capture", agentprofile.ControlCommand{CaptureResult: &agentprofile.CaptureResultCommand{Result: result}})
	recoveryCoreControl(t, s, "summary-accept", agentprofile.ControlCommand{AcceptActivity: &agentprofile.AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: binding, Evidence: proof}})
	outputAck := integrationMutate(t, s.e, integrationRequest("summary-output", integrationPut([]byte("summary-output"), api.Value{Kind: api.ValueBytes, Data: output})))
	outputID, _ := recoveryValueAt(t, s, outputAck.Fence, "summary-output")
	stamp := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
	summary := knowledge.Summary{Space: "host-space", Source: packet.Source, Sources: []knowledge.SummarySource{{Observation: observation.ID, Content: observation.Payload}}, Topics: []knowledge.SummaryTopic{{Name: "original evidence", Observations: []model.ObjectID{observation.ID}}}, Applicability: knowledge.SummaryApplicability{SchemaVersion: 1, Goal: "retain earlier evidence", Environment: "fixture", Tags: []string{"test"}}, Confidence: 700, Generation: knowledge.SummaryGeneration{Activity: spec.Activity, Attempt: result.Attempt, Result: result.ID, Model: spec.Recipient, PromptVersion: "summary-prompt/v1", PolicyVersion: "summary-policy/v1", Input: knowledge.RecordedContent{Object: input, Anchor: packet.Input.Anchor, Encoding: packet.Input.Encoding}, Output: knowledge.RecordedContent{Object: outputID, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: outputAck.Fence}, Encoding: "text/plain;charset=utf-8"}}, GeneratedAt: stamp, VisibleAt: stamp, Validation: knowledge.SummaryValidation{Status: "unreviewed"}}
	summary.SourceSetDigest = knowledge.SummarySourceSetDigest(summary.Source, summary.Sources)
	summary.ID = knowledge.SummaryID(summary)
	if _, err := summary.Canonical(); err != nil {
		t.Fatal(err)
	}
	return s, summary
}

func TestSummaryCorePublicationReceiptHistoricalVisibilityReopenAndCompaction(t *testing.T) {
	ctx := context.Background()
	s, summary := summaryCoreFixture(t)
	before := integrationSnapshot(t, s.e)
	req := api.ApplySummaryRequest{Key: agentKey("publish-summary"), Summary: summary}
	ack, err := s.e.ApplySummary(ctx, req)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("summary=%+v %v", ack, err)
	}
	read := api.ReadSummaryRequest{Space: "host-space", Principal: "host-principal", ID: summary.ID}
	check := func() {
		t.Helper()
		got, err := s.e.GetSummary(ctx, read)
		if err != nil || !got.Found || !reflect.DeepEqual(got.Summary, summary) || got.Record == summary.ID || got.Record == "" {
			t.Fatalf("stored Summary differs: %+v %v", got, err)
		}
		past := read
		past.Anchor = &model.SnapshotAnchor{Space: "host-space", Fence: before.Fence}
		if old, err := s.e.GetSummary(ctx, past); err != nil || old.Found {
			t.Fatalf("later-generated summary leaked into old publication: %+v %v", old, err)
		}
		other := read
		other.Principal = "another-owner"
		if hidden, err := s.e.GetSummary(ctx, other); err != nil || hidden.Found {
			t.Fatalf("owner partition leaked: %+v %v", hidden, err)
		}
		if again, err := s.e.ApplySummary(ctx, req); err != nil || again != ack {
			t.Fatalf("receipt was reconstructed differently: %+v %v", again, err)
		}
	}
	check()
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
	changed := req
	changed.Summary = summary.Clone()
	changed.Summary.Generation.PromptVersion = "changed-prompt"
	changed.Summary.ID = knowledge.SummaryID(changed.Summary)
	if out, err := s.e.ApplySummary(ctx, changed); err != nil || out.Code != api.CodeConflict || out.Conflict != api.ConflictIdempotency {
		t.Fatalf("different proposal reused receipt: %+v %v", out, err)
	}
}

func TestSummaryCoreRejectsForgedProvenanceWithoutFencing(t *testing.T) {
	ctx := context.Background()
	s, summary := summaryCoreFixture(t)
	mutations := map[string]func(*knowledge.Summary){
		"unknown-activity": func(s *knowledge.Summary) { s.Generation.Activity = "missing-activity" },
		"unknown-attempt":  func(s *knowledge.Summary) { s.Generation.Attempt = "missing-attempt" },
		"unknown-result":   func(s *knowledge.Summary) { s.Generation.Result = "missing-result" },
		"claimed-model":    func(s *knowledge.Summary) { s.Generation.Model = "different-model" },
		"input-object":     func(s *knowledge.Summary) { s.Generation.Input.Object = s.Generation.Output.Object },
		// The input bytes remain visible at this later cut, but generation
		// must still reference the exact anchor recorded in its packet.
		"input-anchor": func(s *knowledge.Summary) {
			s.Generation.Input.Anchor = s.Generation.Output.Anchor.Clone()
		},
		"input-encoding":  func(s *knowledge.Summary) { s.Generation.Input.Encoding = "different-framing" },
		"output-object":   func(s *knowledge.Summary) { s.Generation.Output.Object = s.Sources[0].Content },
		"unknown-content": func(s *knowledge.Summary) { s.Generation.Output.Object = model.ObjectID(integrationID('f')) },
		"forged-source": func(s *knowledge.Summary) {
			s.Source.Anchor.Fence.CommitDigest = model.CommitDigest(integrationID('b'))
		},
		"forged-frontier": func(s *knowledge.Summary) { s.Source.Frontier = model.ObjectID(integrationID('c')) },
		"cross-owner-key": func(s *knowledge.Summary) { s.Source.Anchor.Knowledge.Actor = "another-actor" },
		"unadopted-source": func(s *knowledge.Summary) {
			s.Sources[0].Observation = model.ObjectID(integrationID('d'))
			s.Topics[0].Observations[0] = s.Sources[0].Observation
		},
		"unpublished-predecessor": func(s *knowledge.Summary) { s.Replaces = model.ObjectID(integrationID('e')) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			bad := summary.Clone()
			mutate(&bad)
			bad.SourceSetDigest = knowledge.SummarySourceSetDigest(bad.Source, bad.Sources)
			bad.ID = knowledge.SummaryID(bad)
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey(name), Summary: bad})
			if err == nil || (out.Code != api.CodeRejected && out.Code != api.CodeNotSubmitted) || integrationSnapshot(t, s.e) != before {
				t.Fatalf("invalid provenance changed/fenced store: %+v %v", out, err)
			}
		})
	}
	if out, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey("valid-after-rejections"), Summary: summary}); err != nil || out.Code != api.CodeDurableAck {
		t.Fatal(out, err)
	}
}

func TestSummaryCoreCurrentWithdrawalGatesReuseAndHistoricalDisclosure(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionFactualCorrection, memory.RetractionUseBan, memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, summary := summaryCoreFixture(t)
			ack, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey("summary-before-ban"), Summary: summary})
			if err != nil {
				t.Fatal(ack, err)
			}
			applyMemory(t, s.e, "summary-source-withdrawn", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: summary.Sources[0].Content, Retraction: memory.Retraction{Kind: kind, Reason: "source withdrawn after derivation"}}})
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey("summary-after-ban"), Summary: summary})
			if out.Code != api.CodeRejected || (!errors.Is(err, memory.ErrUseDenied) && !errors.Is(err, memory.ErrHistoryAccessDenied)) {
				t.Fatalf("withdrawn evidence reused or store fenced: %+v %v", out, err)
			}
			check := func() {
				t.Helper()
				got, err := s.e.GetSummary(ctx, api.ReadSummaryRequest{Space: "host-space", Principal: "host-principal", ID: summary.ID, Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}})
				privacy := kind == memory.RetractionPrivacyErasure || kind == memory.RetractionACLRevocation
				if privacy && (!errors.Is(err, memory.ErrHistoryAccessDenied) || got.Found) {
					t.Fatalf("historical derived content escaped current denial: %+v %v", got, err)
				}
				if !privacy && (err != nil || !got.Found) {
					t.Fatalf("correction/use ban rewrote audit history: %+v %v", got, err)
				}
			}
			check()
			if integrationSnapshot(t, s.e) != before {
				t.Fatal("denial changed publication")
			}
			s.reopen()
			check()
		})
	}
}

func TestSummaryCoreCurrentWithdrawalGatesRawGenerationOutput(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionFactualCorrection, memory.RetractionUseBan, memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, summary := summaryCoreFixture(t)
			request := api.ApplySummaryRequest{Key: agentKey("raw-output-summary"), Summary: summary}
			ack, err := s.e.ApplySummary(ctx, request)
			if err != nil || ack.Code != api.CodeDurableAck {
				t.Fatal(ack, err)
			}
			view := nodeView(t, s.e, "host-principal", nil)
			read := api.ReadStateRequest{Space: "host-space", Key: []byte("summary-output")}
			original, err := s.e.GetState(ctx, read)
			if err != nil || !original.Found {
				t.Fatalf("published raw output unavailable: %+v %v", original, err)
			}
			applyMemory(t, s.e, "raw-output-withdraw-source", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: summary.Sources[0].Content, Retraction: memory.Retraction{Kind: kind, Reason: "raw derived output source withdrawn"}}})
			before := integrationSnapshot(t, s.e)
			privacy := kind == memory.RetractionACLRevocation || kind == memory.RetractionPrivacyErasure
			check := func() {
				t.Helper()
				value, err := s.e.GetState(ctx, read)
				if privacy {
					if value.Found || !errors.Is(err, memory.ErrHistoryAccessDenied) {
						t.Fatalf("raw output escaped source withdrawal: %+v %v", value, err)
					}
				} else if err != nil || !value.Found || !reflect.DeepEqual(value.Value, original.Value) {
					t.Fatalf("historical audit changed after correction/use ban: %+v %v", value, err)
				}
			}
			check()
			_, stateErr := s.e.GetViewState(ctx, api.ReadViewStateRequest{View: view.Ref, Key: read.Key})
			_, contentErr := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: summary.Generation.Output.Object}, Projection: "value_bytes"})
			for _, err := range []error{stateErr, contentErr} {
				if privacy && !errors.Is(err, memory.ErrHistoryAccessDenied) || !privacy && err != nil {
					t.Fatalf("fixed View disclosure diverged: %v", err)
				}
			}
			if integrationSnapshot(t, s.e) != before {
				t.Fatal("disclosure rejection changed or fenced the store")
			}
			s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
			s.reopen()
			check()
			if _, err := s.e.CompactAuditFull(ctx); err != nil {
				t.Fatal(err)
			}
			s.reopen()
			check()
			if retry, err := s.e.ApplySummary(ctx, request); err != nil || retry != ack {
				t.Fatalf("derived publication receipt changed: %+v %v", retry, err)
			}
		})
	}
}
