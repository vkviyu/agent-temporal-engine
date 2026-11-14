package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestTaskTraceHistoricalEvidenceHonorsSourceWithdrawal(t *testing.T) {
	for _, source := range []string{"outcome", "citation", "event-citation"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			s, spec, input := finalAnswerFixture(t)
			spec.Activity = "trace-final-activity"
			recoveryCoreControl(t, s, "trace-final-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})
			req := api.TaskTraceRequest{Space: "host-space", Principal: "host-principal", Subject: api.TraceSubject{Kind: "query", ID: "query"}, Session: "session"}
			raw, err := s.e.GetTaskTrace(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			var intake, outcome api.TraceCitation
			for _, node := range raw.Nodes {
				switch node.Operation {
				case "Work.query_intake":
					intake = node.At
				case "Control.CompleteTask":
					outcome = node.At
				}
			}
			if intake.Ref.Event == "" || outcome.Ref.Event == "" {
				t.Fatal("missing initial and terminal milestones")
			}
			ack := integrationMutate(t, s.e, integrationRequest("host-trace-source", integrationPut([]byte("host-trace-source"), api.Value{Kind: api.ValueBytes, Data: []byte("host explanation evidence")})))
			citationID, _ := recoveryValueAt(t, s, ack.Fence, "host-trace-source")
			citation := api.TraceCitation{Ref: model.NodeRef{Kind: model.NodeObject, Object: citationID}, Anchor: model.SnapshotAnchor{Space: req.Space, Fence: ack.Fence}}
			sourceAnchor := citation.Anchor
			if source == "event-citation" {
				for _, event := range readProfileOccurrences(t, s) {
					if a := event.Attribution; a != nil && a.Operation == "Control.CreateActivity" && string(a.Subject.ID) == spec.Activity {
						citation = api.TraceCitation{Ref: model.NodeRef{Kind: model.NodeEvent, Event: event.ID}, Anchor: raw.Anchor}
					}
				}
				if citation.Ref.Event == "" {
					t.Fatal("missing cited model activity")
				}
			}
			edit := api.TaskTraceEdit{Subject: req.Subject, Source: sourceAnchor,
				Milestones: []api.TraceMilestone{
					{Event: intake.Ref.Event, Label: "Initial request", Evidence: []api.TraceCitation{intake}},
					{Event: outcome.Ref.Event, Label: "Validated result", Evidence: []api.TraceCitation{outcome}},
				},
				Assertions: []api.TraceAssertion{{From: intake.Ref.Event, To: outcome.Ref.Event, Relation: "explains", Reason: "Host explanation based on evidence", Evidence: []api.TraceCitation{citation}}},
			}
			written, err := s.e.ApplyTaskTrace(ctx, api.ApplyTaskTraceRequest{Key: agentKey("historical-trace-edit"), Edit: edit})
			if err != nil || written.Code != api.CodeDurableAck {
				t.Fatalf("trace edit: %+v %v", written, err)
			}
			anchor := model.SnapshotAnchor{Space: req.Space, Fence: written.Fence}
			req.Anchor = &anchor
			badAnchor := anchor
			badAnchor.Fence.CommitDigest = model.CommitDigest(strings.Repeat("c", 64))
			bad := req
			bad.Anchor = &badAnchor
			if _, err := s.e.GetTaskTrace(ctx, bad); !errors.Is(err, model.ErrAnchorMismatch) {
				t.Fatalf("incorrect historical fence: %v", err)
			}
			detail := api.TaskTraceEvidenceRequest{TaskTraceRequest: req, From: intake.Ref.Event, To: outcome.Ref.Event, Relation: "explains"}
			if out, err := s.e.GetTaskTraceEvidence(ctx, detail); err != nil || len(out.Citations) != 1 {
				t.Fatalf("before withdrawal: %+v %v", out, err)
			}
			withdraw := citationID
			if source == "outcome" {
				withdraw = input.Outcomes[0].Validation
			} else if source == "event-citation" {
				withdraw = spec.Request
			}
			applyMemory(t, s.e, "withdraw-historical-trace-source", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: withdraw, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "withdraw source of retrospective disclosure"}}})
			for _, stage := range []string{"live", "reopened"} {
				if stage == "reopened" {
					s.reopen()
				}
				if out, err := s.e.GetTaskTrace(ctx, req); !errors.Is(err, memory.ErrHistoryAccessDenied) || len(out.Nodes) != 0 || len(out.Edges) != 0 {
					t.Errorf("%s leaked historical trace: %+v %v", stage, out, err)
				}
				if out, err := s.e.GetTaskTraceEvidence(ctx, detail); !errors.Is(err, memory.ErrHistoryAccessDenied) || len(out.Events) != 0 || len(out.Citations) != 0 {
					t.Errorf("%s leaked historical evidence: %+v %v", stage, out, err)
				}
			}
		})
	}
}
