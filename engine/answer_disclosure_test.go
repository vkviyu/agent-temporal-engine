package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestFinalAnswerCurrentFaultPublishesActivityAndRawProvenanceTogether(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[published], func(t *testing.T) {
			ctx := context.Background()
			s, spec, input := finalAnswerFixture(t)
			spec.Activity = "atomic-final-answer"
			command := agentprofile.Command{Control: &agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}}}
			key := agentKey("atomic-final-answer-create")
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
			out, err := s.e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key, Command: command})
			if out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkCurrent) {
				t.Fatalf("CURRENT fault did not remain unknown: %+v %v", out, err)
			}
			s.options.Journal.TailPolicy = storage.TailQuarantine
			s.reopen()
			state, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: key.Space, Principal: key.Principal})
			if err != nil {
				t.Fatal(err)
			}
			activityPresent := false
			for _, activity := range state.Snapshot.Lifecycle.Activities {
				activityPresent = activityPresent || activity.ID == spec.Activity
			}
			lookup := integrationLookup(t, s.e, key)
			wantCode := api.CodeNotFoundAtFence
			if published {
				wantCode = api.CodeDurableAck
			}
			if activityPresent != published || lookup.Code != wantCode || (!published && integrationSnapshot(t, s.e) != before) {
				t.Fatalf("mixed final Activity publication: activity=%v receipt=%+v", activityPresent, lookup)
			}
			view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: key.Space, Principal: key.Principal})
			if err != nil {
				t.Fatal(err)
			}
			defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
			applyMemory(t, s.e, "atomic-final-source-withdrawal", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: input.Outcomes[0].Validation, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "test exact publication of final input provenance"}}})
			for _, object := range []struct {
				key string
				id  model.ObjectID
			}{
				{"final-provider-request", spec.Request},
				{"final-input-witness", spec.FinalAnswerInput},
			} {
				raw, rawErr := s.e.GetState(ctx, api.ReadStateRequest{Space: key.Space, Key: []byte(object.key)})
				part, partErr := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: object.id}, Projection: "value_bytes", Length: 16})
				if published {
					if !errors.Is(rawErr, memory.ErrHistoryAccessDenied) || raw.Found || !errors.Is(partErr, memory.ErrHistoryAccessDenied) || len(part.Data) != 0 {
						t.Fatalf("published final Activity lost provenance %s: raw=%+v/%v content=%+v/%v", object.key, raw, rawErr, part, partErr)
					}
				} else if rawErr != nil || !raw.Found {
					t.Fatalf("unpublished final Activity leaked provenance state %s: raw=%+v/%v content=%+v/%v", object.key, raw, rawErr, part, partErr)
				}
			}
		})
	}
}

func TestFinalAnswerWitnessWithdrawalProtectsRawRequestAndWitnessReads(t *testing.T) {
	for _, source := range []string{"artifact", "validation"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			s, spec, input := finalAnswerFixture(t)
			spec.Activity = "final-answer-disclosure"
			recoveryCoreControl(t, s, "final-answer-disclosure-create", agentprofile.ControlCommand{CreateActivity: &agentprofile.CreateActivityCommand{Spec: spec}})
			anchor := model.SnapshotAnchor{Space: "host-space", Fence: integrationSnapshot(t, s.e).Fence}
			open := func() api.ViewDescriptor {
				t.Helper()
				view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor})
				if err != nil {
					t.Fatal(err)
				}
				return view
			}
			view := open()
			objects := []struct {
				key string
				id  model.ObjectID
			}{
				{"final-provider-request", spec.Request},
				{"final-input-witness", spec.FinalAnswerInput},
			}
			for _, object := range objects {
				raw, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte(object.key)})
				if err != nil || !raw.Found || len(raw.Value.Data) == 0 {
					t.Fatalf("authorized raw fixture is unreadable %s: %+v %v", object.key, raw, err)
				}
				part, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: object.id}, Projection: "value_bytes", Length: 16})
				if err != nil || len(part.Data) == 0 {
					t.Fatalf("authorized content fixture is unreadable %s: %+v %v", object.key, part, err)
				}
			}
			withdraw := input.Outcomes[0].Result
			if source == "validation" {
				withdraw = input.Outcomes[0].Validation
			}
			applyMemory(t, s.e, "withdraw-final-raw-source", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: withdraw, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "withdraw the completed task evidence from all derived disclosures"}}})
			check := func(stage string) {
				t.Helper()
				for _, object := range objects {
					raw, err := s.e.GetState(ctx, api.ReadStateRequest{Space: "host-space", Key: []byte(object.key)})
					if !errors.Is(err, memory.ErrHistoryAccessDenied) || raw.Found || len(raw.Value.Data) != 0 {
						t.Errorf("%s raw final data bypassed %s withdrawal %s: found=%v bytes=%d err=%v", stage, source, object.key, raw.Found, len(raw.Value.Data), err)
					}
					part, err := s.e.ReadContent(ctx, api.ReadContentRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: object.id}, Projection: "value_bytes", Length: 16})
					if !errors.Is(err, memory.ErrHistoryAccessDenied) || len(part.Data) != 0 {
						t.Errorf("%s fixed-view final data bypassed %s withdrawal %s: bytes=%d err=%v", stage, source, object.key, len(part.Data), err)
					}
				}
			}
			check("live")
			if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
				t.Fatal(err)
			}
			s.reopen()
			view = open()
			defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
			check("reopened")
		})
	}
}
